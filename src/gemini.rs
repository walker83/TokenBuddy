//! Gemini CLI collector — reads `${GEMINI_DATA_DIR:-~/.gemini}/tmp/**/chats/`.
//!
//! Format verified against the ccusage adapter and Gemini CLI's official
//! `chatRecordingTypes.ts` (no Gemini install on the dev machine — synthetic
//! fixtures only, marked as such in the README):
//!
//! * JSONL: a header line names the session, message lines follow, plus
//!   `{"$set":…}` / `{"$rewindTo":…}` control lines with no `type` — skipped.
//! * Only `type == "gemini"` lines carry `tokens`
//!   `{input, output, cached, thoughts, tool, total}`. The recording service
//!   re-appends a completed copy of the last message under the **same id**,
//!   so within a file the later line with an id wins.
//! * Gemini's `promptTokenCount` includes cached content; `input` is
//!   normalised to uncached (`input + tool − min(input, cached)`) and
//!   thoughts join the output side, matching the other sources' semantics.

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
    let base = match std::env::var("GEMINI_DATA_DIR") {
        Ok(v) if !v.trim().is_empty() => PathBuf::from(v.trim().to_string()),
        _ => match dirs::home_dir() {
            Some(h) => h.join(".gemini"),
            None => return vec![],
        },
    };
    vec![base.join("tmp")]
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

/// `tmp/**/chats/*.{json,jsonl}`, recursive: subagent chats nest *inside*
/// chats/, and the project dir name is not stable across versions (hash →
/// registry slug), so nothing about the layout is assumed beyond `chats/`.
fn chat_files() -> Vec<PathBuf> {
    fn walk(dir: &Path, depth: u8, files: &mut Vec<PathBuf>) {
        if depth > 5 {
            return;
        }
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, depth + 1, files);
            } else if matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("json" | "jsonl")
            ) {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    for root in log_paths() {
        if root.is_dir() {
            walk(&root, 0, &mut files);
        }
    }
    files.sort();
    files
}

/// Normalise one gemini usage block into (input_uncached, cache_read,
/// output_with_thoughts). `promptTokenCount` includes the cached share;
/// tool-prompt tokens bill like input; thoughts bill like output.
fn normalize_tokens(t: &serde_json::Value) -> (u64, u64, u64) {
    let get = |k: &str| t.get(k).and_then(|v| v.as_u64()).unwrap_or(0);
    let input = get("input");
    let cached = get("cached");
    let tool = get("tool");
    let output = get("output");
    let thoughts = get("thoughts");
    let cache_read = cached.min(input);
    (
        input.saturating_sub(cache_read) + tool,
        cache_read,
        output + thoughts,
    )
}

fn usage_record(
    session_id: &str,
    msg_id: &str,
    model: &str,
    ts: i64,
    tokens: &serde_json::Value,
) -> TokenRecord {
    let (input, cache_read, output) = normalize_tokens(tokens);
    TokenRecord {
        source: Source::Gemini,
        model: model.to_string(),
        input_tokens: input,
        output_tokens: output,
        cache_read_tokens: cache_read,
        cache_creation_tokens: 0,
        timestamp: ts,
        session_id: Some(session_id.to_string()),
        project: String::new(),
        duration_ms: None,
        ttft_ms: None,
        credits: 0.0,
        context_ratio: 0.0,
        // New source: stable from day one — (session, message id).
        record_id: Some(format!("gm_{session_id}|{msg_id}")),
        sidechain: false,
        merge_key: None,
        request_count: 1,
    }
}

fn extract_message(
    msg: &serde_json::Value,
    session_id: &str,
    fallback_ts: i64,
) -> Option<TokenRecord> {
    if msg.get("type").and_then(|v| v.as_str()) != Some("gemini") {
        return None;
    }
    // A null tokens block means "not known yet" — the completed re-append
    // carries the real numbers.
    let tokens = msg.get("tokens").filter(|t| !t.is_null())?;
    let msg_id = msg.get("id").and_then(|v| v.as_str())?;
    let model = msg
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let ts = msg
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp())
        .unwrap_or(fallback_ts);
    Some(usage_record(session_id, msg_id, model, ts, tokens))
}

fn parse_chat_file(file_path: &Path) -> Result<Vec<TokenRecord>> {
    let fallback_ts = crate::file_mtime(file_path).unwrap_or(0);
    let mut records: HashMap<String, TokenRecord> = HashMap::new();

    let is_jsonl = file_path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e == "jsonl");
    if is_jsonl {
        let file = fs::File::open(file_path)?;
        // The header line (no type) names the session; message lines inherit it.
        let mut current_session = String::from("unknown");
        for line in BufReader::new(file).lines() {
            let Ok(line) = line else { continue };
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if let Some(sid) = value.get("sessionId").and_then(|v| v.as_str()) {
                current_session = sid.to_string();
            }
            // Control lines ($set / $rewindTo) carry no type and are skipped
            // by the filter inside extract_message.
            if let Some(msg) = extract_message(&value, &current_session, fallback_ts) {
                // Later line with the same id wins: the completed re-append.
                let key = msg.record_id.clone().unwrap_or_default();
                records.insert(key, msg);
            }
        }
    } else {
        // Legacy whole-document format: {"messages": [...]} (+ a stats block
        // this collector ignores — it summarizes what the messages already say).
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&fs::read_to_string(file_path)?)
        else {
            return Ok(vec![]);
        };
        let session_id = session_id_of(&value, "unknown");
        if let Some(messages) = value.get("messages").and_then(|m| m.as_array()) {
            for msg in messages {
                if let Some(record) = extract_message(msg, &session_id, fallback_ts) {
                    let key = record.record_id.clone().unwrap_or_default();
                    records.insert(key, record);
                }
            }
        }
    }

    Ok(records.into_values().collect())
}

fn session_id_of(value: &serde_json::Value, default: &str) -> String {
    value
        .get("sessionId")
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
}

/// R80 — conversational text for context search. Content shapes ARE
/// verified — by this repo's own R7 fixtures: `type:"user"` lines carry a
/// plain string `content` (the prompt), `type:"gemini"` lines carry the
/// model's answer as a string (the completed re-append holds the full
/// text; last line wins, same as token counting). Legacy whole-document
/// `messages[]` gets the same treatment.
pub fn drain_messages(sink: &mut dyn FnMut(crate::context::ContextMessage)) {
    for dir in log_paths() {
        if !dir.is_dir() {
            continue;
        }
        for path in walk_chat_files(&dir) {
            let fallback_ts = crate::file_mtime(&path).unwrap_or(0);
            let text = match fs::read_to_string(&path) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let mut session_id = String::from("unknown");
            for line in text.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                // Legacy whole-document file: one object holding messages[].
                if let Some(messages) = value.get("messages").and_then(|m| m.as_array()) {
                    let session_id = session_id_of(&value, &session_id);
                    for m in messages {
                        let role = match m.get("type").and_then(|t| t.as_str()) {
                            Some("user") => "user",
                            Some("gemini") => "assistant",
                            _ => continue,
                        };
                        if let Some(c) = m.get("content").and_then(|c| c.as_str()) {
                            let content = c.trim();
                            if !content.is_empty() {
                                sink(crate::context::ContextMessage {
                                    source: crate::Source::Gemini,
                                    session_id: session_id.clone(),
                                    role,
                                    timestamp: fallback_ts,
                                    text: content.to_string(),
                                    project: String::new(),
                                    title: String::new(),
                                });
                            }
                        }
                    }
                    continue;
                }
                if let Some(sid) = value.get("sessionId").and_then(|v| v.as_str()) {
                    if session_id == "unknown" {
                        session_id = sid.to_string();
                    }
                }
                let role = match value.get("type").and_then(|t| t.as_str()) {
                    Some("user") => "user",
                    Some("gemini") => "assistant",
                    _ => continue,
                };
                let Some(c) = value.get("content").and_then(|c| c.as_str()) else {
                    continue;
                };
                let content = c.trim();
                if content.is_empty() {
                    continue;
                }
                let ts = value
                    .get("timestamp")
                    .and_then(|v| v.as_str())
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.timestamp())
                    .unwrap_or(fallback_ts);
                sink(crate::context::ContextMessage {
                    source: crate::Source::Gemini,
                    session_id: session_id.clone(),
                    role,
                    timestamp: ts,
                    text: content.to_string(),
                    project: String::new(),
                    title: String::new(),
                });
            }
        }
    }
}

/// `*.jsonl` / `*.json` chat files under a chats dir, recursively.
fn walk_chat_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if matches!(
                p.extension().and_then(|e| e.to_str()),
                Some("jsonl") | Some("json")
            ) {
                out.push(p);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::parse_chat_file;

    /// The recording service appends a completed copy of the last message
    /// under the same id: the later line wins. Control lines ($set) and the
    /// header (no type) pass through. Cached overlaps input and is split
    /// out; tool bills as input; thoughts bill as output.
    #[test]
    fn same_id_reappend_wins_and_tokens_normalize() {
        let dir = std::env::temp_dir().join(format!("tb-gemini-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session-x.jsonl");
        let lines = [
            r#"{"sessionId":"8f0c2e1a","projectHash":"a1b2","startTime":"2026-05-17T11:07:00Z"}"#,
            r#"{"id":"u1","timestamp":"2026-05-17T11:07:05Z","type":"user","content":"Fix the login bug"}"#,
            r#"{"id":"a1","timestamp":"2026-05-17T11:07:32Z","type":"gemini","model":"gemini-2.5-pro","content":"partial","tokens":{"input":15327,"output":23,"cached":11526,"thoughts":919,"tool":7,"total":16276}}"#,
            r#"{"id":"a1","timestamp":"2026-05-17T11:07:32Z","type":"gemini","model":"gemini-2.5-pro","content":"full answer","tokens":{"input":15327,"output":1450,"cached":11526,"thoughts":919,"tool":7,"total":17703}}"#,
            r#"{"$set":{"lastUpdated":"2026-05-17T11:09:30Z"}}"#,
        ];
        std::fs::write(&path, lines.join("\n")).unwrap();

        let records = parse_chat_file(&path).expect("parses");
        assert_eq!(records.len(), 1, "same id folds to one row");
        let r = &records[0];
        assert_eq!(r.session_id.as_deref(), Some("8f0c2e1a"));
        assert_eq!(r.model, "gemini-2.5-pro");
        assert_eq!(r.output_tokens, 1450 + 919, "thoughts join output");
        assert_eq!(r.cache_read_tokens, 11526);
        assert_eq!(
            r.input_tokens,
            15327 - 11526 + 7,
            "uncached input + tool tokens"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The legacy whole-document format parses too.
    #[test]
    fn legacy_json_document_still_parses() {
        let dir = std::env::temp_dir().join(format!("tb-gemini-o-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("old-session.json");
        std::fs::write(
            &path,
            r#"{"sessionId":"old1","messages":[
                {"id":"m1","type":"gemini","model":"gemini-2.0-flash","timestamp":"2026-01-01T00:00:00Z","tokens":{"input":100,"output":20,"cached":40,"total":120}}
            ]}"#,
        )
        .unwrap();
        let records = parse_chat_file(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].input_tokens, 60);
        assert_eq!(records[0].cache_read_tokens, 40);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod drain_tests {
    use super::drain_messages;
    use crate::context::ContextMessage;

    #[test]
    fn gemini_drain_extracts_user_and_assistant() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-gem-drain-{}", std::process::id()));
        let chats = dir.join("tmp/chats");
        std::fs::create_dir_all(&chats).unwrap();
        std::env::set_var("GEMINI_DATA_DIR", &dir);

        let lines = [
            r#"{"sessionId":"8f0c2e1a","startTime":"2026-05-17T11:07:00Z"}"#,
            r#"{"id":"u1","timestamp":"2026-05-17T11:07:05Z","type":"user","content":"Fix the login bug"}"#,
            r#"{"id":"a1","timestamp":"2026-05-17T11:07:32Z","type":"gemini","model":"gemini-2.5-pro","content":"partial"}"#,
            r#"{"id":"a1","timestamp":"2026-05-17T11:07:40Z","type":"gemini","model":"gemini-2.5-pro","content":"full answer with details"}"#,
            r#"{"$set":{"lastUpdated":"2026-05-17T11:09:30Z"}}"#,
        ];
        std::fs::write(chats.join("session-x.jsonl"), lines.join("\n")).unwrap();

        let mut sink: Vec<ContextMessage> = Vec::new();
        drain_messages(&mut |m| sink.push(m));
        std::env::remove_var("GEMINI_CLI_DIR");
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(sink.len(), 3, "user + two gemini lines: {sink:?}");
        assert_eq!(sink[0].role, "user");
        assert_eq!(sink[0].text, "Fix the login bug");
        assert_eq!(sink[0].session_id, "8f0c2e1a");
        // 完成的 re-append 持完整文本,逐行都收(去重是 context 层的事)。
        assert_eq!(sink[2].text, "full answer with details");
    }

    #[test]
    fn gemini_drain_legacy_document_and_missing_dir() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-gem-drain2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("GEMINI_DATA_DIR", &dir);

        // legacy 文档格式。
        let legacy_dir = dir.join("tmp/chats");
        std::fs::create_dir_all(&legacy_dir).unwrap();
        std::fs::write(
            legacy_dir.join("old-session.json"),
            r#"{"sessionId":"legacy-1","messages":[{"type":"user","content":"hi"},{"type":"gemini","content":"hello"}]}"#,
        )
        .unwrap();
        let mut sink: Vec<ContextMessage> = Vec::new();
        drain_messages(&mut |m| sink.push(m));
        assert_eq!(sink.len(), 2, "legacy document parses");

        // 缺目录:空。
        std::fs::remove_dir_all(&legacy_dir).unwrap();
        let mut sink2: Vec<ContextMessage> = Vec::new();
        drain_messages(&mut |m| sink2.push(m));
        std::env::remove_var("GEMINI_CLI_DIR");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(sink2.is_empty());
    }
}
