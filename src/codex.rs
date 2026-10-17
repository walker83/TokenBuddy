//! Codex CLI collector — reads `~/.codex/sessions/**/rollout-*.jsonl`.
//!
//! Format (verified against the ccusage project's official fixtures and its
//! fix for issue #1288; no Codex install was available on the dev machine,
//! so validation is synthetic-fixture only):
//!
//! * `{"type":"session_meta","payload":{"id":…}}` opens a rollout and names
//!   the session; `turn_context` lines carry the active model.
//! * Usage arrives as `event_msg` lines with
//!   `payload.type == "token_count"`:
//!   **`last_token_usage` is the per-request increment, `total_token_usage`
//!   the session cumulative** — summing anything but the increment double
//!   counts. UI refreshes re-send identical `token_count` events with a new
//!   timestamp; the cumulative total is the sentinel: an event counts only
//!   when the total advanced.

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

/// Candidate roots for `tokenbuddy doctor` and the quota file reader.
/// `archived_sessions/` holds the same rollout format — `codex` moves old
/// threads there instead of deleting them, and usage that left `sessions/`
/// is still usage (multiple independent write-ups document the directory).
/// Xcode-hosted Codex (macOS) keeps its own store; a missing path is a no-op.
pub fn log_paths() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(home) = std::env::var("CODEX_HOME") {
        let trimmed = home.trim();
        if !trimmed.is_empty() {
            let base = PathBuf::from(trimmed);
            roots.push(base.join("sessions"));
            roots.push(base.join("archived_sessions"));
            return roots;
        }
    }
    if let Some(home) = dirs::home_dir() {
        let base = home.join(".codex");
        roots.push(base.join("sessions"));
        roots.push(base.join("archived_sessions"));
    }
    // Xcode-hosted Codex (macOS only in practice; is_dir() guards elsewhere).
    if let Some(home) = dirs::home_dir() {
        let base = home.join("Library/Developer/Xcode/CodingAssistant/codex");
        roots.push(base.join("sessions"));
        roots.push(base.join("archived_sessions"));
    }
    roots
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let cell = FILE_CACHE.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock().unwrap_or_else(|e| e.into_inner());
    let cache_map = guard.get_or_insert_with(HashMap::new);

    let mut all_records = Vec::new();
    let mut current_paths: std::collections::HashSet<String> = std::collections::HashSet::new();
    let windows = model_windows();

    for file_path in rollout_files() {
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
            let records = parse_rollout_with(&file_path, &windows).unwrap_or_default();
            cache_map.insert(path_str.clone(), (mtime, records));
        }
        if let Some((_, records)) = cache_map.get(&path_str) {
            all_records.extend(records.iter().cloned());
        }
    }
    cache_map.retain(|path, _| current_paths.contains(path));

    Ok(all_records)
}

/// `slug -> (context_window tokens, effective percent)`, from the model
/// catalog Codex itself caches at `models_cache.json`. Multiple independent
/// write-ups and openai/codex issues confirm the entry shape:
/// `{ "slug": "gpt-5.5", "context_window": 372000,
///    "effective_context_window_percent": 95, ... }` — the runtime window is
/// the product-side catalog number, often far below the model spec, which is
/// exactly why the context-fill panel must use it and not the spec. Both the
/// `{ "models": [...] }` and bare-array cache shapes parse; anything without
/// a slug + positive window is ignored.
pub(crate) fn model_windows() -> std::collections::HashMap<String, (u64, f64)> {
    let mut map = std::collections::HashMap::new();
    let mut roots: Vec<PathBuf> = Vec::new();
    if let Ok(home) = std::env::var("CODEX_HOME") {
        let trimmed = home.trim();
        if !trimmed.is_empty() {
            roots.push(PathBuf::from(trimmed));
        }
    }
    if let Some(home) = dirs::home_dir() {
        roots.push(home.join(".codex"));
    }
    for root in roots {
        let Ok(text) = fs::read_to_string(root.join("models_cache.json")) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
            continue;
        };
        let entries = match &value {
            serde_json::Value::Array(items) => items.clone(),
            v => v
                .get("models")
                .and_then(|m| m.as_array())
                .cloned()
                .unwrap_or_default(),
        };
        for entry in entries {
            let Some(slug) = entry.get("slug").and_then(|v| v.as_str()) else {
                continue;
            };
            let Some(window) = entry.get("context_window").and_then(|v| v.as_u64()) else {
                continue;
            };
            if window == 0 {
                continue;
            }
            let pct = entry
                .get("effective_context_window_percent")
                .and_then(|v| v.as_f64())
                .map(|p| p.clamp(1.0, 100.0))
                .unwrap_or(100.0);
            map.entry(slug.to_string()).or_insert((window, pct));
        }
        if !map.is_empty() {
            break;
        }
    }
    map
}

/// `sessions/YYYY/MM/DD/rollout-*.jsonl`, found recursively (depth-capped).
fn rollout_files() -> Vec<PathBuf> {
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
            } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
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

/// `windows` maps model slug -> (context_window, effective percent). When the
/// running model has an entry, each billed record carries the context fill of
/// its own request: `last_token_usage.input_tokens` is the last call's prompt
/// size — the cumulative total spans the whole session and is NOT the fill.
/// Unknown model or zero window leaves 0.0, the schema's "not reported".
fn parse_rollout_with(
    file_path: &Path,
    windows: &std::collections::HashMap<String, (u64, f64)>,
) -> Result<Vec<TokenRecord>> {
    let file = fs::File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();

    let mut session_id = String::from("unknown");
    let mut model = String::from("unknown");
    // The cumulative sentinel: an event whose total did not advance is a UI
    // refresh resend (#1288), never new consumption.
    let mut last_total: u64 = 0;

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let ts = value
            .get("timestamp")
            .and_then(|v| v.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.timestamp())
            .unwrap_or_else(|| crate::file_mtime(file_path).unwrap_or(0));

        let payload = match value.get("payload") {
            Some(p) => p,
            None => continue,
        };

        match value.get("type").and_then(|v| v.as_str()) {
            Some("session_meta") => {
                if let Some(id) = payload.get("id").and_then(|v| v.as_str()) {
                    session_id = id.to_string();
                }
            }
            Some("turn_context") => {
                if let Some(m) = payload.get("model").and_then(|v| v.as_str()) {
                    model = m.to_string();
                }
            }
            Some("event_msg") => {
                if payload.get("type").and_then(|v| v.as_str()) != Some("token_count") {
                    continue;
                }
                let info = match payload.get("info") {
                    Some(i) => i,
                    None => continue,
                };
                let total = info
                    .get("total_token_usage")
                    .and_then(|t| t.get("total_tokens"))
                    .and_then(|v| v.as_u64());
                let Some(total_after) = total else { continue };
                if total_after <= last_total {
                    continue;
                }
                last_total = total_after;

                let Some(usage) = info.get("last_token_usage") else {
                    continue;
                };
                let input = usage
                    .get("input_tokens")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let output = usage
                    .get("output_tokens")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                if input == 0 && output == 0 {
                    continue;
                }
                let context_ratio = windows
                    .get(&model)
                    .and_then(|(window, pct)| {
                        let effective = (*window as f64) * (pct / 100.0);
                        if effective > 0.0 {
                            Some((input as f64 / effective).clamp(0.0, 1.0))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0.0);
                records.push(TokenRecord {
                    source: Source::Codex,
                    model: model.clone(),
                    input_tokens: input,
                    output_tokens: output,
                    cache_read_tokens: usage
                        .get("cached_input_tokens")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0),
                    cache_creation_tokens: 0,
                    timestamp: ts,
                    session_id: Some(session_id.clone()),
                    project: String::new(),
                    duration_ms: None,
                    ttft_ms: None,
                    credits: 0.0,
                    context_ratio,
                    // New source: stable from day one. One response =
                    // (session, cumulative total after it) — unique and
                    // rewrite-stable, unlike timestamp fingerprints.
                    record_id: Some(format!("cx_{session_id}|t{total_after}")),
                    sidechain: false,
                    merge_key: None,
                    request_count: 1,
                });
            }
            _ => {}
        }
    }

    Ok(records)
}

/// R79 — conversational text for context search, from the same rollout
/// files the usage collector walks. Schema (verified against a public
/// format-converter's extractor): `response_item` rows with
/// `payload.type == "message"` carry `role` plus a `content` array of
/// `{type: "input_text"|"output_text", text}` blocks. User text that starts
/// with `#` or `<` is environment/context scaffolding, not a prompt —
/// the reference extractor skips it and so do we (wrong guesses would
/// pollute the index). Sidechain-free: Codex rollouts have no subagent
/// transcripts to exclude.
pub fn drain_messages(sink: &mut dyn FnMut(crate::context::ContextMessage)) {
    for file_path in rollout_files() {
        let Ok(file) = fs::File::open(&file_path) else {
            continue;
        };
        let mut session_id = String::from("unknown");
        let mut cwd = String::new();
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if value.get("type").and_then(|t| t.as_str()) == Some("session_meta") {
                if let Some(id) = value
                    .get("payload")
                    .and_then(|p| p.get("id"))
                    .and_then(|v| v.as_str())
                {
                    session_id = id.to_string();
                }
                if let Some(c) = value
                    .get("payload")
                    .and_then(|p| p.get("cwd"))
                    .and_then(|v| v.as_str())
                {
                    cwd = c.to_string();
                }
                continue;
            }
            if value.get("type").and_then(|t| t.as_str()) != Some("response_item") {
                continue;
            }
            let payload = match value.get("payload") {
                Some(p) => p,
                None => continue,
            };
            if payload.get("type").and_then(|t| t.as_str()) != Some("message") {
                continue;
            }
            let role = match payload.get("role").and_then(|r| r.as_str()) {
                Some("user") => "user",
                Some("assistant") => "assistant",
                _ => continue,
            };
            let timestamp = value
                .get("timestamp")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.timestamp())
                .unwrap_or_else(|| crate::file_mtime(&file_path).unwrap_or(0));
            if let Some(content) = payload.get("content").and_then(|c| c.as_array()) {
                for block in content {
                    let btype = block.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    let text = block.get("text").and_then(|t| t.as_str()).unwrap_or("");
                    let wanted = matches!(
                        (role, btype),
                        ("user", "input_text") | ("assistant", "output_text")
                    );
                    if !wanted || text.trim().is_empty() {
                        continue;
                    }
                    if role == "user" && (text.starts_with('#') || text.starts_with('<')) {
                        continue;
                    }
                    sink(crate::context::ContextMessage {
                        source: Source::Codex,
                        session_id: session_id.clone(),
                        role,
                        timestamp,
                        text: text.to_string(),
                        project: cwd.clone(),
                        title: String::new(),
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_rollout_with;
    use crate::TokenRecord;

    /// No catalog windows: billing semantics still parse, fill stays 0.0.
    fn parse_rollout(file_path: &std::path::Path) -> anyhow::Result<Vec<TokenRecord>> {
        parse_rollout_with(file_path, &std::collections::HashMap::new())
    }

    /// ccusage fixture semantics: only `last_token_usage` increments bill;
    /// a resent event with an unchanged cumulative total is skipped (#1288);
    /// `cached_input_tokens` lands in cache_read; the model rides along from
    /// the nearest `turn_context`.
    #[test]
    fn token_count_advancement_is_the_billing_sentinel() {
        let dir = std::env::temp_dir().join(format!("tb-codex-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rollout-2026-09-28-x.jsonl");
        let lines = [
            r#"{"timestamp":"2026-05-01T10:00:00Z","type":"session_meta","payload":{"id":"sess-1"}}"#,
            r#"{"timestamp":"2026-05-01T10:00:01Z","type":"turn_context","payload":{"model":"gpt-5.3-codex"}}"#,
            r#"{"timestamp":"2026-05-01T10:00:02Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1000,"output_tokens":100,"total_tokens":1100},"last_token_usage":{"input_tokens":1000,"output_tokens":100,"total_tokens":1100}}}}"#,
            // Resend: same totals, new timestamp — skipped.
            r#"{"timestamp":"2026-05-01T10:00:02.500Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1000,"output_tokens":100,"total_tokens":1100},"last_token_usage":{"input_tokens":1000,"output_tokens":100,"total_tokens":1100}}}}"#,
            // Real increment: +2000/+200, with cached input.
            r#"{"timestamp":"2026-05-01T10:00:10Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":3000,"output_tokens":300,"total_tokens":3300},"last_token_usage":{"input_tokens":2000,"cached_input_tokens":1500,"output_tokens":200,"total_tokens":2200}}}}"#,
        ];
        std::fs::write(&path, lines.join("\n")).unwrap();

        let records = parse_rollout(&path).expect("parses");
        assert_eq!(records.len(), 2, "resend skipped, increments billed");
        let first = &records[0];
        assert_eq!(first.session_id.as_deref(), Some("sess-1"));
        assert_eq!(first.model, "gpt-5.3-codex");
        assert_eq!(first.input_tokens, 1000);
        assert_eq!(first.output_tokens, 100);
        let second = &records[1];
        assert_eq!(second.input_tokens, 2000);
        assert_eq!(second.cache_read_tokens, 1500);
        assert_eq!(second.output_tokens, 200);
        // Stable ids so a re-read dedupes at the store.
        assert!(second
            .record_id
            .as_deref()
            .unwrap()
            .starts_with("cx_sess-1|"));

        let again = parse_rollout(&path).unwrap();
        let keys = |rs: &[TokenRecord]| {
            let mut k: Vec<_> = rs.iter().map(|r| r.record_id.clone()).collect();
            k.sort();
            k
        };
        assert_eq!(keys(&records), keys(&again), "re-parse stability");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Context fill = last call's prompt input over the catalog's *effective*
    /// window (window × effective percent) — not the session cumulative.
    #[test]
    fn context_fill_uses_effective_window_and_last_input() {
        use super::parse_rollout_with;
        let dir = std::env::temp_dir().join(format!("tb-codex-fill-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rollout-2026-09-28-fill.jsonl");
        let lines = [
            r#"{"timestamp":"2026-05-01T10:00:00Z","type":"session_meta","payload":{"id":"sess-f"}}"#,
            r#"{"timestamp":"2026-05-01T10:00:01Z","type":"turn_context","payload":{"model":"gpt-5.6-sol"}}"#,
            // last input 129_200 over effective 272000×95%=258400 → 0.5
            r#"{"timestamp":"2026-05-01T10:00:02Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":129200},"last_token_usage":{"input_tokens":129200,"output_tokens":10,"total_tokens":129210}}}}"#,
        ];
        std::fs::write(&path, lines.join("\n")).unwrap();

        let mut windows = std::collections::HashMap::new();
        windows.insert("gpt-5.6-sol".to_string(), (272_000_u64, 95.0));
        let records = parse_rollout_with(&path, &windows).expect("parses");
        assert_eq!(records.len(), 1);
        let ratio = records[0].context_ratio;
        assert!(
            (ratio - 0.5).abs() < 1e-9,
            "129200 / (272000*0.95) == 0.5, got {ratio}"
        );

        // Unknown model → 0.0 ("not reported"), never a guessed ratio.
        let empty = std::collections::HashMap::new();
        let records = parse_rollout_with(&path, &empty).unwrap();
        assert_eq!(records[0].context_ratio, 0.0);

        // Over-fill clamps to 1.0 rather than showing nonsense.
        let mut tight = std::collections::HashMap::new();
        tight.insert("gpt-5.6-sol".to_string(), (100_000_u64, 100.0));
        let records = parse_rollout_with(&path, &tight).unwrap();
        assert_eq!(records[0].context_ratio, 1.0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn log_paths_cover_archived_sessions_and_xcode_store() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-codex-paths-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("archived_sessions")).unwrap();
        std::env::set_var("CODEX_HOME", &dir);
        let paths = super::log_paths();
        std::env::remove_var("CODEX_HOME");
        let _ = std::fs::remove_dir_all(&dir);
        let names: Vec<&str> = paths
            .iter()
            .filter_map(|p| p.file_name()?.to_str())
            .collect();
        assert!(names.contains(&"sessions"), "live sessions root: {names:?}");
        assert!(
            names.contains(&"archived_sessions"),
            "archived rollouts are still usage: {names:?}"
        );
    }

    /// 同一会话若同时存在于 sessions/ 与 archived_sessions/,record_id
    /// (session|cumulative)相同 → store 去重天然吸收,不会双计。
    #[test]
    fn archived_copy_of_same_session_dedupes_by_record_id() {
        let dir = std::env::temp_dir().join(format!("tb-codex-arch-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("sessions")).unwrap();
        std::fs::create_dir_all(dir.join("archived_sessions")).unwrap();
        let line = r#"{"timestamp":"2026-05-01T10:00:02Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1000,"output_tokens":100,"total_tokens":1100},"last_token_usage":{"input_tokens":1000,"output_tokens":100,"total_tokens":1100}}}}"#;
        let meta = r#"{"timestamp":"2026-05-01T10:00:00Z","type":"session_meta","payload":{"id":"sess-dup"}}"#;
        let body = format!("{meta}\n{line}\n");
        std::fs::write(dir.join("sessions/rollout-a.jsonl"), &body).unwrap();
        std::fs::write(dir.join("archived_sessions/rollout-b.jsonl"), &body).unwrap();

        let live = parse_rollout_with(
            std::path::Path::new(&dir.join("sessions/rollout-a.jsonl")),
            &std::collections::HashMap::new(),
        )
        .unwrap();
        let archived = parse_rollout_with(
            std::path::Path::new(&dir.join("archived_sessions/rollout-b.jsonl")),
            &std::collections::HashMap::new(),
        )
        .unwrap();
        assert_eq!(
            live[0].record_id, archived[0].record_id,
            "identical ids → store dedupes"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn model_windows_parses_both_cache_shapes() {
        // CODEX_HOME is process-global; quota tests read it too, so both
        // sides serialize on the crate-wide lock.
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-codex-mc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let wrapped = r#"{"client_version":"0.146.0","models":[
            {"slug":"gpt-5.6-sol","context_window":272000,"max_context_window":272000,"effective_context_window_percent":95},
            {"context_window":100}
        ]}"#;
        std::fs::write(dir.join("models_cache.json"), wrapped).unwrap();
        std::env::set_var("CODEX_HOME", &dir);
        let map = super::model_windows();
        std::env::remove_var("CODEX_HOME");
        assert_eq!(map.get("gpt-5.6-sol"), Some(&(272_000, 95.0)));
        assert_eq!(map.len(), 1, "entries without slug are ignored");

        // Bare-array shape.
        let bare = r#"[{"slug":"m2","context_window":8000}]"#;
        std::fs::write(dir.join("models_cache.json"), bare).unwrap();
        std::env::set_var("CODEX_HOME", &dir);
        let map = super::model_windows();
        std::env::remove_var("CODEX_HOME");
        assert_eq!(map.get("m2"), Some(&(8_000, 100.0)), "missing pct → 100");

        // Garbage cache → empty map, never a panic.
        std::fs::write(dir.join("models_cache.json"), "{broken").unwrap();
        std::env::set_var("CODEX_HOME", &dir);
        assert!(super::model_windows().is_empty());
        std::env::remove_var("CODEX_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod drain_tests {
    use super::drain_messages;
    use crate::context::ContextMessage;

    fn collect(dir: &std::path::Path) -> Vec<ContextMessage> {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        std::env::set_var("CODEX_HOME", dir);
        let mut sink: Vec<ContextMessage> = Vec::new();
        drain_messages(&mut |m| sink.push(m));
        std::env::remove_var("CODEX_HOME");
        sink
    }

    #[test]
    fn drain_extracts_user_and_assistant_text() {
        let dir = std::env::temp_dir().join(format!("tb-cx-drain-{}", std::process::id()));
        let day = dir.join("sessions/2026/05/01");
        std::fs::create_dir_all(&day).unwrap();
        let lines = r##"{"type":"session_meta","payload":{"id":"sess-d","cwd":"/tmp/proj"}}
{"timestamp":"2026-05-01T10:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"怎么修这个编译错误"}]}}
{"timestamp":"2026-05-01T10:00:05Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"把 trait 改成 associated type 就好了"}]}}
{"timestamp":"2026-05-01T10:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"<environment_context>macOS</environment_context>"}]}}
{"timestamp":"2026-05-01T10:00:02Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"# AGENTS.md 摘要"}]}}
{"timestamp":"2026-05-01T10:00:03Z","type":"event_msg","payload":{"type":"token_count","info":{}}}
{broken"##;
        std::fs::write(day.join("rollout-x.jsonl"), lines).unwrap();

        let msgs = collect(&dir);
        assert_eq!(msgs.len(), 2, "scaffolding and tool rows skipped: {msgs:?}");
        assert_eq!(msgs[0].role, "user");
        assert_eq!(msgs[0].source, crate::Source::Codex);
        assert_eq!(msgs[0].session_id, "sess-d");
        assert_eq!(msgs[0].project, "/tmp/proj");
        assert!(msgs[0].text.contains("编译错误"));
        assert_eq!(msgs[1].role, "assistant");
        assert!(msgs[1].text.contains("associated type"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 并发测试间共享 CODEX_HOME 的容错:无目录 → 空语料。
    #[test]
    fn drain_missing_home_is_empty() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-cx-drain-none-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("CODEX_HOME", &dir);
        let mut sink: Vec<ContextMessage> = Vec::new();
        drain_messages(&mut |m| sink.push(m));
        std::env::remove_var("CODEX_HOME");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(sink.is_empty());
    }
}
