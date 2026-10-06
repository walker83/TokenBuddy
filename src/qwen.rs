//! Qwen Code collector — reads `${QWEN_DATA_DIR:-~/.qwen}/projects/<sanitized-cwd>/chats/<session>.jsonl`.
//!
//! Qwen Code is a Gemini CLI fork, but the log format has diverged: the
//! record layer is an append-only `uuid` chain whose `type == "assistant"`
//! lines carry a camelCase `usageMetadata` block (verified against ccusage's
//! qwen adapter and QwenLM/qwen-code's `ChatRecord` interface; synthetic
//! fixtures only — no install on the dev machine).
//!
//! `promptTokenCount` already contains the cached share, so input is
//! normalised to uncached and thoughts join the output side — the same
//! semantics every other source in the ledger uses.

use crate::{FileCacheMap, Source, TokenRecord};
use anyhow::Result;
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

static FILE_CACHE: OnceLock<Mutex<Option<FileCacheMap>>> = OnceLock::new();

pub fn release_caches() {
    let cell = FILE_CACHE.get_or_init(|| Mutex::new(None));
    let mut cache = cell.lock().unwrap_or_else(|e| e.into_inner());
    *cache = None;
}

/// Where this collector reads from, when that place exists on this machine.
pub fn log_path() -> Option<PathBuf> {
    log_paths().into_iter().find(|p| p.exists())
}

/// Candidate roots for `tokenbuddy doctor`.
pub fn log_paths() -> Vec<PathBuf> {
    let base = match std::env::var("QWEN_DATA_DIR") {
        Ok(v) if !v.trim().is_empty() => PathBuf::from(v.trim().to_string()),
        _ => match dirs::home_dir() {
            Some(h) => h.join(".qwen"),
            None => return vec![],
        },
    };
    vec![base.join("projects")]
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let cell = FILE_CACHE.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock().unwrap_or_else(|e| e.into_inner());
    let cache_map = guard.get_or_insert_with(HashMap::new);

    let mut all_records = Vec::new();
    let mut current_paths: std::collections::HashSet<String> = std::collections::HashSet::new();

    for file_path in chat_files() {
        let path_str = file_path.to_string_lossy().to_string();
        current_paths.insert(path_str.clone());

        let mtime = std::fs::metadata(&file_path)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

        let needs_reparse = match cache_map.get(&path_str) {
            Some((cached_mtime, _)) => mtime > *cached_mtime,
            None => true,
        };
        if needs_reparse {
            let records = parse_chat_file(&file_path).unwrap_or_default();
            cache_map.insert(path_str.clone(), (mtime, records));
        }
        if let Some((_, records)) = cache_map.get(&path_str) {
            all_records.extend(records.iter().cloned());
        }
    }
    cache_map.retain(|path, _| current_paths.contains(path));

    Ok(all_records)
}

/// `projects/<project>/chats/<file>.jsonl`, exactly three levels — the
/// reference adapters reject shallower or deeper jsonl, and so do we: stray
/// files are more likely noise than sessions.
fn chat_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    for root in log_paths() {
        let Ok(projects) = fs::read_dir(&root) else {
            continue;
        };
        for project in projects.flatten() {
            let chats = project.path().join("chats");
            let Ok(entries) = fs::read_dir(&chats) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                    files.push(path);
                }
            }
        }
    }
    files.sort();
    files
}

fn parse_chat_file(file_path: &Path) -> Result<Vec<TokenRecord>> {
    let fallback_ts = crate::file_mtime(file_path).unwrap_or(0);
    let file = fs::File::open(file_path)?;
    let mut records = Vec::new();

    for line in BufReader::new(file).lines() {
        let Ok(line) = line else { continue };
        let line = line.trim();
        if line.is_empty() || !line.contains("usageMetadata") {
            // Cheap pre-filter: only assistant turns carry usage.
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if value.get("type").and_then(|v| v.as_str()) != Some("assistant") {
            continue;
        }
        let usage = match value.get("usageMetadata") {
            Some(u) => u,
            None => continue,
        };
        let get = |k: &str| usage.get(k).and_then(|v| v.as_u64()).unwrap_or(0);
        let prompt = get("promptTokenCount");
        let candidates = get("candidatesTokenCount");
        let thoughts = get("thoughtsTokenCount");
        let cached = get("cachedContentTokenCount").min(prompt);
        let input = prompt - cached;
        let output = candidates + thoughts;
        if input == 0 && output == 0 {
            continue;
        }
        let session_id = value
            .get("sessionId")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let model = value
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let timestamp = value
            .get("timestamp")
            .and_then(|v| v.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.timestamp())
            .unwrap_or(fallback_ts);
        let project = value
            .get("cwd")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        // Resume/branch copies of one logical entry carry the same
        // (session, timestamp, model, counts) tuple — that tuple is the
        // stable identity, matching the reference adapter's entry key.
        let record_id = format!(
            "qw_{session_id}|{}|{model}|{input}|{output}|{cached}",
            timestamp
        );

        records.push(TokenRecord {
            source: Source::Qwen,
            model,
            input_tokens: input,
            output_tokens: output,
            cache_read_tokens: cached,
            cache_creation_tokens: 0,
            timestamp,
            session_id: Some(session_id),
            project,
            duration_ms: None,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: Some(record_id),
            sidechain: false,
            merge_key: None,
            request_count: 1,
        });
    }

    Ok(records)
}

/// R80 — conversational text for context search. Shapes verified by this
/// repo's own R7 fixtures: `type:"user"` lines carry `message.parts[]`
/// (`{text}`), assistant lines are `role:"model"` with parts where
/// `thought:true` is thinking (not user-visible) — skip those, join the
/// rest. `$set`/`ui_telemetry` system lines have no user/assistant type
/// and are skipped by the filter.
pub fn drain_messages(sink: &mut dyn FnMut(crate::context::ContextMessage)) {
    for dir in log_paths() {
        if !dir.is_dir() {
            continue;
        }
        let mut files: Vec<PathBuf> = Vec::new();
        let mut stack = vec![dir.clone()];
        while let Some(d) = stack.pop() {
            let Ok(entries) = fs::read_dir(&d) else {
                continue;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                    files.push(p);
                }
            }
        }
        for path in files {
            let Ok(file) = fs::File::open(&path) else {
                continue;
            };
            let mut session_id = String::from("unknown");
            let mut project = String::new();
            for line in BufReader::new(file).lines().map_while(Result::ok) {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                let mtype = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
                let role = match mtype {
                    "user" => "user",
                    "assistant" => "assistant",
                    _ => {
                        continue;
                    }
                };
                if let Some(sid) = v.get("sessionId").and_then(|s| s.as_str()) {
                    if session_id == "unknown" {
                        session_id = sid.to_string();
                    }
                }
                if let Some(cwd) = v.get("cwd").and_then(|c| c.as_str()) {
                    if project.is_empty() {
                        project = cwd.to_string();
                    }
                }
                let ts = v
                    .get("timestamp")
                    .and_then(|t| t.as_str())
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.timestamp())
                    .unwrap_or(0);
                let Some(message) = v.get("message") else {
                    continue;
                };
                let Some(parts) = message.get("parts").and_then(|p| p.as_array()) else {
                    continue;
                };
                let mut text = String::new();
                for part in parts {
                    if part.get("thought").and_then(|t| t.as_bool()) == Some(true) {
                        continue;
                    }
                    if let Some(t) = part.get("text").and_then(|t| t.as_str()) {
                        text.push_str(t);
                    }
                }
                let content = text.trim();
                if content.is_empty() {
                    continue;
                }
                sink(crate::context::ContextMessage {
                    source: crate::Source::Qwen,
                    session_id: session_id.clone(),
                    role,
                    timestamp: ts,
                    text: content.to_string(),
                    project: project.clone(),
                    title: String::new(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_chat_file;

    /// Only assistant turns with usageMetadata bill; input loses the cached
    /// share; thoughts join output; the cwd becomes project attribution; the
    /// composite tuple is the stable record id.
    #[test]
    fn assistant_usage_metadata_bills_with_normalized_tokens() {
        let dir = std::env::temp_dir().join(format!("tb-qwen-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("adc026b4.jsonl");
        let lines = [
            r#"{"uuid":"u1","parentUuid":null,"sessionId":"sess-a","timestamp":"2026-05-05T11:08:38.572Z","type":"user","cwd":"/Users/alice/code/sample","version":"0.15.6","message":{"role":"user","parts":[{"text":"Calculate .089 * 7.85788"}]}}"#,
            r#"{"uuid":"u2","type":"system","subtype":"ui_telemetry","cwd":"/Users/alice/code/sample","timestamp":"2026-05-05T11:08:46.382Z","systemPayload":{"uiEvent":{"model":"qwen3-coder-plus","input_token_count":18009}}}"#,
            r#"{"uuid":"u3","sessionId":"sess-a","timestamp":"2026-05-05T11:08:46.529Z","type":"assistant","cwd":"/Users/alice/code/sample","version":"0.15.6","model":"qwen3-coder-plus","message":{"role":"model","parts":[{"text":"thinking","thought":true},{"text":"0.699"}]},"usageMetadata":{"promptTokenCount":18009,"candidatesTokenCount":47,"thoughtsTokenCount":36,"totalTokenCount":18056,"cachedContentTokenCount":12}}"#,
        ];
        std::fs::write(&path, lines.join("\n")).unwrap();

        let records = parse_chat_file(&path).expect("parses");
        assert_eq!(records.len(), 1, "only the assistant turn bills");
        let r = &records[0];
        assert_eq!(r.input_tokens, 18009 - 12, "cached split out of prompt");
        assert_eq!(r.cache_read_tokens, 12);
        assert_eq!(r.output_tokens, 47 + 36, "thoughts join output");
        assert_eq!(r.model, "qwen3-coder-plus");
        assert_eq!(r.project, "/Users/alice/code/sample");
        assert!(r.record_id.as_deref().unwrap().starts_with("qw_sess-a|"));

        let again = parse_chat_file(&path).unwrap();
        assert_eq!(
            records[0].record_id, again[0].record_id,
            "re-parse stability"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod drain_tests {
    use super::drain_messages;
    use crate::context::ContextMessage;

    /// 端到端:user parts 提取、thought 跳过(只留正文)、cwd 归项目、
    /// system 行跳过。
    #[test]
    fn qwen_drain_extracts_parts_skipping_thoughts() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-qw-drain-{}", std::process::id()));
        let proj = dir.join("projects/users-alice-code-sample");
        std::fs::create_dir_all(&proj).unwrap();
        std::env::set_var("QWEN_DATA_DIR", &dir);

        let lines = [
            r#"{"uuid":"u1","parentUuid":null,"sessionId":"sess-a","timestamp":"2026-05-05T11:08:38.572Z","type":"user","cwd":"/Users/alice/code/sample","version":"0.15.6","message":{"role":"user","parts":[{"text":"Calculate .089 * 7.85788"}]}}"#,
            r#"{"uuid":"u2","type":"system","subtype":"ui_telemetry","timestamp":"2026-05-05T11:08:46.382Z","systemPayload":{}}"#,
            r#"{"uuid":"u3","sessionId":"sess-a","timestamp":"2026-05-05T11:08:46.529Z","type":"assistant","model":"qwen3-coder-plus","message":{"role":"model","parts":[{"text":"thinking","thought":true},{"text":"0.699"}]}}"#,
        ];
        std::fs::write(proj.join("chat.jsonl"), lines.join("\n")).unwrap();

        let mut sink: Vec<ContextMessage> = Vec::new();
        drain_messages(&mut |m| sink.push(m));
        std::env::remove_var("QWEN_DATA_DIR");
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(
            sink.len(),
            2,
            "user + assistant (thought skipped): {sink:?}"
        );
        assert_eq!(sink[0].role, "user");
        assert_eq!(sink[0].text, "Calculate .089 * 7.85788");
        assert_eq!(sink[0].project, "/Users/alice/code/sample");
        assert_eq!(sink[1].role, "assistant");
        assert_eq!(sink[1].text, "0.699", "thought part excluded");
    }

    #[test]
    fn qwen_drain_missing_dir_is_empty() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-qw-none-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("QWEN_DATA_DIR", &dir);
        let mut sink: Vec<ContextMessage> = Vec::new();
        drain_messages(&mut |m| sink.push(m));
        std::env::remove_var("QWEN_DATA_DIR");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(sink.is_empty());
    }
}
