//! Collector for MiniMax Code.
//!
//! MiniMax Code keeps one directory per session under
//! `~/.minimax/v2/sessions/<year>/<month>/<day>/<stamp>-session_<id>/`; the
//! conversation lives in `messages.jsonl`, one JSON object per line wrapping
//! the protocol message in a `message` field. Assistant lines carry the
//! per-call usage as four disjoint components — `input`, `output`, `cacheRead`,
//! `cacheWrite` sum to `totalTokens` — so the mapping to `TokenRecord` is
//! direct, with no cached-prefix subtraction like zcode's.
//!
//! Every assistant line also carries a provider `responseId`; it becomes the
//! sync dedupe key, because the file is appended to as the session grows and
//! identical token counts at the same second are plausible otherwise.
//!
//! `usage.cost` is the API dollar figure (0 on subscription plans). It is
//! deliberately not recorded as `credits`: that field is a per-source unit
//! (Qoder credits) and the dashboard sums credits across sources, so dollars
//! in would corrupt the aggregate.

use crate::{file_mtime, FileCacheMap, Source, TokenRecord};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

static FILE_CACHE: Mutex<Option<FileCacheMap>> = Mutex::new(None);

/// Drop the resident parse cache. The cache only exists to make a *second*
/// sync cheaper than the first; left in place it pins every record of every
/// session log in the heap for the life of the process, growing with total
/// history and eating the resident-memory budget the dashboard is measured
/// against. `store::sync` calls this once the parquet has been written, so
/// the saving is paid back only by whoever asks for the next sync.
pub fn release_caches() {
    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    *cache = None;
}

/// Where this collector reads from, when that place exists on this machine.
/// Powers the dashboard's source-health panel and the first-run prompt, so a
/// user with a missing or unmoved tool directory is told which one instead of
/// just seeing zeros.
pub fn log_path() -> Option<std::path::PathBuf> {
    log_paths().into_iter().find(|p| p.exists())
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let session_files = collect_message_files(&sessions_dir());

    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let cache_map = cache.get_or_insert_with(HashMap::new);

    let mut all_records = Vec::new();
    let mut current_paths: HashSet<String> = HashSet::new();

    for file_path in &session_files {
        let path_str = file_path.to_string_lossy().to_string();
        current_paths.insert(path_str.clone());

        let mtime = fs::metadata(file_path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let needs_reparse = match cache_map.get(&path_str) {
            Some((cached_mtime, _)) => mtime > *cached_mtime,
            None => true,
        };

        if needs_reparse {
            let records = parse_usage_records(file_path).unwrap_or_default();
            cache_map.insert(path_str.clone(), (mtime, records));
        }

        if let Some((_, records)) = cache_map.get(&path_str) {
            all_records.extend(records.iter().cloned());
        }
    }

    cache_map.retain(|path, _| current_paths.contains(path));

    Ok(all_records)
}

/// Candidate log locations, for `tokenbuddy doctor`: presence is optional,
/// the doctor reports what exists and what does not.
pub fn log_paths() -> Vec<PathBuf> {
    vec![sessions_dir()]
}

fn sessions_dir() -> PathBuf {
    if let Ok(custom) = std::env::var("MINIMAX_HOME") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed).join("v2/sessions");
        }
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".minimax/v2/sessions")
}

/// Every `messages.jsonl` under the dated session tree. The layout string in
/// `manifest.json` has already changed once (`v2-final-dated-session`), so the
/// walk is depth-bounded but name-driven rather than fixed to three levels.
fn collect_message_files(sessions_dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![(sessions_dir.to_path_buf(), 0)];
    while let Some((dir, depth)) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if depth < 6 {
                    stack.push((path, depth + 1));
                }
            } else if path.file_name().and_then(|n| n.to_str()) == Some("messages.jsonl") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

/// Session id from the sibling `manifest.json` (`sessionId`), falling back to
/// the session directory name the same file lives in.
fn session_id_for(messages_path: &Path) -> String {
    if let Some(dir) = messages_path.parent() {
        let manifest = dir.join("manifest.json");
        if let Ok(text) = fs::read_to_string(&manifest) {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(id) = value.get("sessionId").and_then(|v| v.as_str()) {
                    return id.to_string();
                }
            }
        }
        if let Some(name) = dir.file_name().and_then(|n| n.to_str()) {
            return name.to_string();
        }
    }
    String::new()
}

fn parse_usage_records(messages_path: &Path) -> Result<Vec<TokenRecord>> {
    let text = fs::read_to_string(messages_path)?;
    let session_id = session_id_for(messages_path);

    let mut records = Vec::new();
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let message = match value.get("message") {
            Some(m) => m,
            None => continue,
        };
        if message.get("role").and_then(|v| v.as_str()) != Some("assistant") {
            continue;
        }
        let usage = match message.get("usage") {
            Some(u) => u,
            None => continue,
        };
        let input = usage.get("input").and_then(|v| v.as_u64()).unwrap_or(0);
        let output = usage.get("output").and_then(|v| v.as_u64()).unwrap_or(0);
        let cache_read = usage.get("cacheRead").and_then(|v| v.as_u64()).unwrap_or(0);
        let cache_write = usage
            .get("cacheWrite")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if input == 0 && output == 0 && cache_read == 0 && cache_write == 0 {
            continue;
        }

        // `timestamp` is epoch milliseconds; a missing or unparseable one
        // falls back to the file mtime so the row still lands inside real
        // time filters instead of 1970.
        let timestamp = message
            .get("timestamp")
            .and_then(|v| v.as_i64())
            .filter(|ms| *ms > 0)
            .map(|ms| ms / 1000)
            .or_else(|| file_mtime(messages_path))
            .unwrap_or(0);

        records.push(TokenRecord {
            source: Source::MiniMax,
            model: message
                .get("model")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            input_tokens: input,
            output_tokens: output,
            cache_read_tokens: cache_read,
            cache_creation_tokens: cache_write,
            timestamp,
            session_id: Some(session_id.clone()),
            project: String::new(),
            // The log carries one completion timestamp per call, not a span.
            duration_ms: None,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: message
                .get("responseId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            merge_key: None,
        });
    }

    Ok(records)
}

/// Conversational text for context search: user prompts and assistant replies.
/// Assistant `thinking` and `toolCall` parts, and every `toolResult` message,
/// are the re-sent cached context that drowns real content — skipped, same as
/// Claude's sidechains. User prompts open with injected
/// `<system-reminder>` blocks (agent context, output reminders) that are
/// stripped so the indexed text is the question itself.
pub fn drain_messages(sink: &mut dyn FnMut(crate::context::ContextMessage)) {
    use crate::context::ContextMessage;

    for file_path in collect_message_files(&sessions_dir()) {
        let Ok(text) = fs::read_to_string(&file_path) else {
            continue;
        };
        let session_id = session_id_for(&file_path);

        for line in text.lines() {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            let message = match value.get("message") {
                Some(m) => m,
                None => continue,
            };
            let role = match message.get("role").and_then(|v| v.as_str()) {
                Some("user") => "user",
                Some("assistant") => "assistant",
                _ => continue,
            };
            let timestamp = message
                .get("timestamp")
                .and_then(|v| v.as_i64())
                .filter(|ms| *ms > 0)
                .map(|ms| ms / 1000)
                .or_else(|| file_mtime(&file_path))
                .unwrap_or(0);

            let raw = content_text(message.get("content"));
            let text = if role == "user" {
                strip_injected_blocks(&raw)
            } else {
                raw
            };
            if text.trim().is_empty() {
                continue;
            }
            // The session log carries no working directory; the project stays
            // unknown for this source, like WorkBuddy.
            sink(ContextMessage {
                source: Source::MiniMax,
                session_id: session_id.clone(),
                role,
                timestamp,
                text,
                project: String::new(),
                title: String::new(),
            });
        }
    }
}

/// Concatenated `text` parts of a message's typed content array.
fn content_text(content: Option<&serde_json::Value>) -> String {
    match content {
        Some(serde_json::Value::Array(parts)) => parts
            .iter()
            .filter(|p| p.get("type").and_then(|t| t.as_str()) == Some("text"))
            .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join("\n"),
        Some(serde_json::Value::String(s)) => s.clone(),
        _ => String::new(),
    }
}

/// Drop every `<system-reminder>…</system-reminder>` block. They sit at the
/// head of user prompts, one per injected notice, and the tags themselves
/// never nest.
fn strip_injected_blocks(text: &str) -> String {
    if !text.contains("<system-reminder>") {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("<system-reminder>") {
        out.push_str(&rest[..start]);
        let after = start + "<system-reminder>".len();
        rest = match rest[after..].find("</system-reminder>") {
            Some(close) => &rest[after + close + "</system-reminder>".len()..],
            // Unterminated block (truncated write): drop the tail.
            None => "",
        };
    }
    out.push_str(rest);
    out.trim().to_string()
}

/// Collect into a vector; the sync path uses [`drain_messages`] so a source's
/// messages are absorbed one at a time instead of all living at once.
pub fn collect_messages() -> Vec<crate::context::ContextMessage> {
    let mut msgs = Vec::new();
    drain_messages(&mut |m| msgs.push(m));
    msgs
}

#[cfg(test)]
mod tests {
    use super::{collect_message_files, content_text, parse_usage_records, strip_injected_blocks};
    use std::fs;
    use std::path::PathBuf;

    fn write_session(dir: &PathBuf, messages: &str) {
        fs::create_dir_all(dir).expect("session dir creatable");
        fs::write(
            dir.join("manifest.json"),
            r#"{"schemaVersion":1,"sessionId":"mvs_test"}"#,
        )
        .expect("manifest writable");
        fs::write(dir.join("messages.jsonl"), messages).expect("messages writable");
    }

    #[test]
    fn assistant_usage_maps_directly_without_uncaching() {
        let dir = std::env::temp_dir().join(format!("tokenbuddy-mx-{}", std::process::id()));
        let session = dir.join("v2/sessions/2026/09/27/12-00-00-000-session_a");
        write_session(
            &session,
            concat!(
                r#"{"message_id":"u1","message":{"role":"user","content":[{"type":"text","text":"hi"}],"timestamp":1790507900000}}"#,
                "\n",
                r#"{"message_id":"a1","message":{"role":"assistant","model":"MiniMax-M3.1-Flash-Preview","usage":{"input":21404,"output":238,"cacheRead":2877,"cacheWrite":5,"totalTokens":24524},"stopReason":"toolUse","timestamp":1790507900470,"responseId":"resp-1"}}"#,
                "\n",
                // tool results and usage-less assistant lines are not model calls
                r#"{"message_id":"t1","message":{"role":"toolResult","content":[{"type":"text","text":"out"}]}}"#,
                "\n",
                r#"{"message_id":"a2","message":{"role":"assistant","content":[{"type":"text","text":"err"}]}}"#,
                "\n",
            ),
        );

        let records = parse_usage_records(&session.join("messages.jsonl")).expect("parses");
        assert_eq!(records.len(), 1);
        let r = &records[0];
        // input/output/cacheRead/cacheWrite are disjoint components of
        // totalTokens — input is kept as-is, no cached share subtracted.
        assert_eq!(r.input_tokens, 21404);
        assert_eq!(r.output_tokens, 238);
        assert_eq!(r.cache_read_tokens, 2877);
        assert_eq!(r.cache_creation_tokens, 5);
        assert_eq!(r.timestamp, 1790507900);
        assert_eq!(r.session_id.as_deref(), Some("mvs_test"));
        assert_eq!(r.record_id.as_deref(), Some("resp-1"));
        assert_eq!(r.model, "MiniMax-M3.1-Flash-Preview");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn injected_reminder_blocks_are_stripped_from_user_prompts() {
        assert_eq!(
            strip_injected_blocks("<system-reminder><x>1</x></system-reminder>\n真正的问题"),
            "真正的问题"
        );
        assert_eq!(
            strip_injected_blocks(
                "<system-reminder>a</system-reminder><system-reminder>b</system-reminder>问题二"
            ),
            "问题二"
        );
        // No tag → untouched.
        assert_eq!(strip_injected_blocks("plain"), "plain");
        // Unterminated block: the tail is dropped rather than indexing boilerplate.
        assert_eq!(strip_injected_blocks("<system-reminder>cut"), "");
    }

    #[test]
    fn content_text_joins_only_text_parts() {
        let value: serde_json::Value = serde_json::from_str(
            r#"[{"type":"thinking","thinking":"hmm"},{"type":"text","text":"答"},{"type":"toolCall","id":"c1","name":"bash"}]"#,
        )
        .unwrap();
        assert_eq!(content_text(Some(&value)), "答");
    }

    #[test]
    fn walk_finds_messages_jsonl_at_dated_depth() {
        let dir = std::env::temp_dir().join(format!("tokenbuddy-mx-walk-{}", std::process::id()));
        let session = dir.join("v2/sessions/2026/09/27/12-00-00-000-session_a");
        write_session(&session, "{}\n");
        let found = collect_message_files(&dir.join("v2/sessions"));
        assert_eq!(found, vec![session.join("messages.jsonl")]);
        let _ = fs::remove_dir_all(&dir);
    }
}
