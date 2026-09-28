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

/// Candidate roots for `tokenbuddy doctor`.
pub fn log_paths() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(home) = std::env::var("CODEX_HOME") {
        let trimmed = home.trim();
        if !trimmed.is_empty() {
            roots.push(PathBuf::from(trimmed).join("sessions"));
            return roots;
        }
    }
    if let Some(home) = dirs::home_dir() {
        roots.push(home.join(".codex/sessions"));
    }
    roots
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let cell = FILE_CACHE.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock().unwrap_or_else(|e| e.into_inner());
    let cache_map = guard.get_or_insert_with(HashMap::new);

    let mut all_records = Vec::new();
    let mut current_paths: std::collections::HashSet<String> = std::collections::HashSet::new();

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
            let records = parse_rollout(&file_path).unwrap_or_default();
            cache_map.insert(path_str.clone(), (mtime, records));
        }
        if let Some((_, records)) = cache_map.get(&path_str) {
            all_records.extend(records.iter().cloned());
        }
    }
    cache_map.retain(|path, _| current_paths.contains(path));

    Ok(all_records)
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

fn parse_rollout(file_path: &Path) -> Result<Vec<TokenRecord>> {
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
                    context_ratio: 0.0,
                    // New source: stable from day one. One response =
                    // (session, cumulative total after it) — unique and
                    // rewrite-stable, unlike timestamp fingerprints.
                    record_id: Some(format!("cx_{session_id}|t{total_after}")),
                    merge_key: None,
                });
            }
            _ => {}
        }
    }

    Ok(records)
}

/// Conversational text for context search: not implemented yet — the rollout
/// message schema has not been verified against a real corpus, and wrong
/// guesses would pollute the index. Usage billing above is the verified part.
pub fn drain_messages(_sink: &mut dyn FnMut(crate::context::ContextMessage)) {}

#[cfg(test)]
mod tests {
    use super::parse_rollout;
    use crate::TokenRecord;

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
}
