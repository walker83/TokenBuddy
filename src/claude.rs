use crate::file_mtime;
use crate::{FileCacheMap, Source, TokenRecord};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
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

struct ParsedAssistantUsage {
    #[allow(dead_code)]
    message_id: String,
    request_id: Option<String>,
    sidechain: bool,
    cwd: Option<String>,
    model: String,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
    stop_reason: Option<String>,
    timestamp: Option<String>,
    session_id: Option<String>,
}

impl ParsedAssistantUsage {
    fn token_sum(&self) -> u64 {
        self.input_tokens + self.output_tokens + self.cache_read_tokens + self.cache_creation_tokens
    }

    /// Cross-file merge identity, mirroring the reference implementation's
    /// dedupe design (ccusage `usage_dedupe_hash`): one API response is
    /// identified by `(message.id, requestId)`; a missing requestId (API
    /// gateways reuse one message.id across calls) degrades to
    /// `(message.id, sessionId, timestamp)` so gateway calls keep counting
    /// separately. Sidechain copies normalise to `sc:<message.id>` so they
    /// can be folded into their parent row wherever it lives.
    fn merge_identity(&self) -> String {
        let prefix = if self.sidechain { "sc:" } else { "" };
        match (&self.request_id, &self.session_id, &self.timestamp) {
            (Some(req), _, _) => format!("{prefix}{}|{req}", self.message_id),
            (None, Some(sid), Some(ts)) => format!("{prefix}{}|{sid}|{ts}", self.message_id),
            _ => format!("{prefix}{}", self.message_id),
        }
    }
}

/// Fold rows that share one merge identity into a single billable row:
/// non-sidechain beats sidechain, then the larger token sum wins (the
/// streaming final line). Returns records in first-seen group order.
fn merge_by_identity(records: Vec<TokenRecord>) -> Vec<TokenRecord> {
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, TokenRecord> = HashMap::new();
    // message_id → identity of the winning non-sidechain row, so a sidechain
    // replay finds its parent wherever it was parsed.
    let mut mainline_by_msg: HashMap<String, String> = HashMap::new();

    let is_sc = |k: &str| k.starts_with("sc:");
    let msg_of = |k: &str| {
        k.trim_start_matches("sc:")
            .split('|')
            .next()
            .unwrap_or(k)
            .to_string()
    };
    let sum = |r: &TokenRecord| {
        r.input_tokens + r.output_tokens + r.cache_read_tokens + r.cache_creation_tokens
    };

    for r in records {
        let identity = match &r.merge_key {
            Some(k) => k.clone(),
            // Rows from an older cache generation predate merge keys: pass
            // them through untouched, they heal on the file's next change.
            None => {
                let lone = format!("\x00lone{}", order.len());
                order.push(lone.clone());
                groups.insert(lone, r);
                continue;
            }
        };
        let sidechain = is_sc(&identity);
        let msg = msg_of(&identity);

        // A sidechain copy of a message we already count on the mainline is
        // a replay, not new work.
        let target = if sidechain {
            mainline_by_msg.get(&msg).cloned()
        } else {
            None
        };
        let key = target.unwrap_or_else(|| identity.clone());

        if !order.contains(&key) {
            order.push(key.clone());
        }
        match groups.get(&key) {
            None => {
                groups.insert(key.clone(), r);
                if !sidechain {
                    mainline_by_msg.entry(msg).or_insert(key);
                }
            }
            Some(existing) => {
                let existing_sc = groups
                    .get(&key)
                    .map(|e| e.merge_key.as_deref().is_some_and(is_sc))
                    .unwrap_or(false);
                let new_wins = if existing_sc && !sidechain {
                    true
                } else if existing_sc == sidechain {
                    sum(&r) > sum(existing)
                } else {
                    false
                };
                if new_wins {
                    groups.insert(key.clone(), r);
                }
                if !sidechain {
                    mainline_by_msg.entry(msg).or_insert(key);
                }
            }
        }
    }

    order
        .into_iter()
        .filter_map(|k| groups.remove(&k))
        .collect()
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let claude_dir = get_claude_dir();
    let projects_dir = claude_dir.join("projects");

    if !projects_dir.exists() {
        return Ok(vec![]);
    }

    let jsonl_files = collect_jsonl_files(&projects_dir);
    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let cache_map = cache.get_or_insert_with(HashMap::new);

    let mut all_records = Vec::new();
    let mut current_paths: HashSet<String> = HashSet::new();

    for file_path in &jsonl_files {
        let path_str = file_path.to_string_lossy().to_string();
        current_paths.insert(path_str.clone());

        let mtime = std::fs::metadata(file_path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let needs_reparse = match cache_map.get(&path_str) {
            Some((cached_mtime, _)) => mtime > *cached_mtime,
            None => true,
        };

        if needs_reparse {
            let records = parse_single_file(file_path).unwrap_or_default();
            cache_map.insert(path_str.clone(), (mtime, records));
        }

        if let Some((_, records)) = cache_map.get(&path_str) {
            all_records.extend(records.iter().cloned());
        }
    }

    // Clean up deleted files from cache
    cache_map.retain(|path, _| current_paths.contains(path));

    // One API response can surface in several files (resume copies the
    // transcript into a new session file; sidechain replays land in
    // subagents/*). Fold by merge identity so the bill counts the response
    // once, keeping the most complete line.
    Ok(merge_by_identity(all_records))
}

/// Candidate log locations, for `tokenbuddy doctor`: presence is optional,
/// the doctor reports what exists and what does not.
pub fn log_paths() -> Vec<PathBuf> {
    vec![get_claude_dir().join("projects")]
}

fn get_claude_dir() -> PathBuf {
    if let Ok(custom) = std::env::var("CLAUDE_CONFIG_DIR") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".claude")
}

/// Walk `projects/` recursively for *.jsonl: layouts evolved from
/// `<project>/<session>.jsonl` to `<project>/<session>/chat.jsonl` plus
/// `subagents/agent-*.jsonl`, and a shallow walk silently misses whole
/// conversations. Depth-capped so a pathological tree cannot hang a sync.
fn collect_jsonl_files(projects_dir: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, depth: u8, files: &mut Vec<PathBuf>) {
        if depth > 4 {
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
    walk(projects_dir, 0, &mut files);
    files.sort();
    files
}

fn parse_single_file(file_path: &Path) -> Result<Vec<TokenRecord>> {
    let file = fs::File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut messages: HashMap<String, ParsedAssistantUsage> = HashMap::new();
    let mut current_session_id: Option<String> = None;
    // Every line carries the session cwd; the latest wins. Project
    // attribution rides on it.
    let mut current_cwd: Option<String> = None;

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };

        if line.trim().is_empty() {
            continue;
        }

        let value: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        if current_session_id.is_none() {
            if let Some(sid) = value.get("sessionId").and_then(|v| v.as_str()) {
                current_session_id = Some(sid.to_string());
            }
        }

        if value.get("type").and_then(|t| t.as_str()) != Some("assistant") {
            continue;
        }

        let message = match value.get("message") {
            Some(m) => m,
            None => continue,
        };

        let msg_id = match message.get("id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => continue,
        };
        let request_id = value
            .get("requestId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let sidechain = value
            .get("isSidechain")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(cwd) = value.get("cwd").and_then(|v| v.as_str()) {
            current_cwd = Some(cwd.to_string());
        }

        let usage = match message.get("usage") {
            Some(u) => u,
            None => continue,
        };

        let parsed = ParsedAssistantUsage {
            message_id: msg_id.clone(),
            request_id,
            sidechain,
            cwd: current_cwd.clone(),
            model: message
                .get("model")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            input_tokens: usage
                .get("input_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            output_tokens: usage
                .get("output_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            cache_read_tokens: usage
                .get("cache_read_input_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            cache_creation_tokens: usage
                .get("cache_creation_input_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            stop_reason: message
                .get("stop_reason")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            timestamp: value
                .get("timestamp")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            session_id: current_session_id.clone(),
        };

        let identity = parsed.merge_identity();
        let should_replace = match messages.get(&identity) {
            None => true,
            Some(existing) => {
                // Same response seen again: the more complete line wins —
                // a stop_reason marks the final line of a stream, otherwise
                // the larger token sum is the later state. A mainline row
                // always beats a sidechain replay.
                match (existing.sidechain, parsed.sidechain) {
                    (true, false) => true,
                    (a, b) if a == b => {
                        match (parsed.stop_reason.is_some(), existing.stop_reason.is_some()) {
                            (true, false) => true,
                            (a, b) if a == b => parsed.token_sum() > existing.token_sum(),
                            _ => false,
                        }
                    }
                    _ => false,
                }
            }
        };

        if should_replace {
            messages.insert(identity, parsed);
        }
    }

    let mut records = Vec::new();
    for (msg_identity, msg) in messages.iter() {
        if msg.stop_reason.is_none() || msg.output_tokens == 0 {
            continue;
        }

        let timestamp = msg
            .timestamp
            .as_ref()
            .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
            .map(|dt| dt.timestamp())
            .or_else(|| file_mtime(file_path))
            .unwrap_or(0);

        records.push(TokenRecord {
            source: Source::Claude,
            model: msg.model.clone(),
            merge_key: Some(msg_identity.clone()),
            project: msg.cwd.clone().unwrap_or_default(),
            input_tokens: msg.input_tokens,
            output_tokens: msg.output_tokens,
            cache_read_tokens: msg.cache_read_tokens,
            cache_creation_tokens: msg.cache_creation_tokens,
            timestamp,
            session_id: msg.session_id.clone(),
            duration_ms: None,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: None,
        });
    }

    Ok(records)
}

/// Conversational text for context search: user prompts and assistant replies
/// as they appear in the session JSONL. Tool results, tool_use blocks and
/// sidechain (subagent) transcripts are skipped — they are the re-sent cached
/// context that drowns real content.
pub fn drain_messages(sink: &mut dyn FnMut(crate::context::ContextMessage)) {
    let projects_dir = get_claude_dir().join("projects");
    if !projects_dir.exists() {
        return;
    }

    for file_path in collect_jsonl_files(&projects_dir) {
        for m in extract_messages_from_file(&file_path) {
            sink(m);
        }
    }
}

fn extract_messages_from_file(file_path: &Path) -> Vec<crate::context::ContextMessage> {
    use crate::context::ContextMessage;

    let mut msgs = Vec::new();
    let file = match fs::File::open(file_path) {
        Ok(f) => f,
        Err(_) => return msgs,
    };
    // projects/<munged-cwd>/<session>.jsonl — the directory is the project.
    let project = file_path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();
    let mut session_id = String::new();
    // Claude writes `{"type":"summary","summary":"…"}` lines naming the
    // session; that is the display title.
    let mut title = String::new();

    for line_result in BufReader::new(file).lines() {
        let Ok(line) = line_result else { continue };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };

        if session_id.is_empty() {
            if let Some(sid) = value.get("sessionId").and_then(|v| v.as_str()) {
                session_id = sid.to_string();
            }
        }
        if title.is_empty() && value.get("type").and_then(|t| t.as_str()) == Some("summary") {
            if let Some(s) = value.get("summary").and_then(|v| v.as_str()) {
                title = s.to_string();
            }
        }
        if value.get("isSidechain").and_then(|v| v.as_bool()) == Some(true) {
            continue;
        }
        let role = match value.get("type").and_then(|t| t.as_str()) {
            Some("user") => "user",
            Some("assistant") => "assistant",
            _ => continue,
        };
        let message = match value.get("message") {
            Some(m) => m,
            None => continue,
        };
        let timestamp = value
            .get("timestamp")
            .and_then(|v| v.as_str())
            .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
            .map(|dt| dt.timestamp())
            .or_else(|| file_mtime(file_path))
            .unwrap_or(0);

        let text = content_text(message.get("content"));
        if !text.trim().is_empty() {
            msgs.push(ContextMessage {
                source: Source::Claude,
                session_id: session_id.clone(),
                role,
                timestamp,
                text,
                project: project.clone(),
                title: title.clone(),
            });
        }
    }
    msgs
}

/// Claude `message.content` is either a plain string or an array of typed
/// parts; only the text parts carry conversational words.
fn content_text(content: Option<&serde_json::Value>) -> String {
    match content {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Array(parts)) => parts
            .iter()
            .filter(|p| p.get("type").and_then(|t| t.as_str()) == Some("text"))
            .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

// ============================================================
// Tool events (R1 of the context-search blueprint)
// ============================================================

/// Collect into a vector; the sync path uses [`drain_messages`] so a source's
/// messages are absorbed one at a time instead of all living at once.
pub fn collect_messages() -> Vec<crate::context::ContextMessage> {
    let mut msgs = Vec::new();
    drain_messages(&mut |m| msgs.push(m));
    msgs
}

#[cfg(test)]
mod tests {
    use super::{merge_by_identity, parse_single_file};
    use std::io::Write;
    use std::path::PathBuf;

    /// A streamed Claude Code response appears as several assistant lines
    /// sharing one `message.id`: token counts grow line by line until the
    /// final line carries `stop_reason`. The billable row must be exactly
    /// one, with the final line's counts — this is the dedupe that keeps the
    /// raw sum from over-counting 2-3x (ccusage#1288-class defect).
    #[test]
    fn streamed_duplicates_collapse_to_the_final_complete_line() {
        let dir = std::env::temp_dir().join(format!("tb-claude-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        let lines = [
            r#"{"type":"user","sessionId":"s1","timestamp":"2026-09-28T10:00:00Z"}"#,
            r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-09-28T10:00:00Z","message":{"id":"msg_1","model":"m","stop_reason":null,"usage":{"input_tokens":100,"output_tokens":10}}}"#,
            r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-09-28T10:00:01Z","message":{"id":"msg_1","model":"m","stop_reason":null,"usage":{"input_tokens":100,"output_tokens":25}}}"#,
            r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-09-28T10:00:02Z","message":{"id":"msg_1","model":"m","stop_reason":"end_turn","usage":{"input_tokens":150,"output_tokens":42,"cache_read_input_tokens":7,"cache_creation_input_tokens":3}}}"#,
            r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-09-28T10:00:03Z","message":{"id":"msg_2","model":"m","stop_reason":"end_turn","usage":{"input_tokens":80,"output_tokens":5}}}"#,
        ];
        f.write_all(lines.join("\n").as_bytes()).unwrap();
        drop(f);

        let records = parse_single_file(&path).expect("parses");
        assert_eq!(records.len(), 2, "one row per message id, not per line");
        let msg1 = records.iter().find(|r| r.input_tokens == 150).unwrap();
        assert_eq!(msg1.output_tokens, 42);
        assert_eq!(msg1.cache_read_tokens, 7);
        assert_eq!(msg1.cache_creation_tokens, 3);
        assert_eq!(msg1.session_id.as_deref(), Some("s1"));
        let msg2 = records.iter().find(|r| r.input_tokens == 80).unwrap();
        assert_eq!(msg2.output_tokens, 5);

        // A resume/compact rewrite re-parses to the same records — and with
        // the frozen fallback key (timestamp+input) the store dedupes them.
        let again = parse_single_file(&path).unwrap();
        let key = |r: &crate::TokenRecord| (r.timestamp, r.input_tokens, r.output_tokens);
        let mut a: Vec<_> = records.iter().map(&key).collect();
        let mut b: Vec<_> = again.iter().map(key).collect();
        a.sort();
        b.sort();
        assert_eq!(a, b, "re-parse stability: same rows, same store keys");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// R9 fixture A+B (ccusage official matrix): streamed lines with the same
    /// (message.id, requestId) and growing usage collapse into ONE row with
    /// the final counts; a gateway that reuses one message.id with no
    /// requestId but different timestamps counts every call.
    #[test]
    fn composite_identity_matches_the_reference_semantics() {
        let dir = std::env::temp_dir().join(format!("tb-claude-c-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s.jsonl");
        let lines = [
            // A: same req+msg, three lines, usage grows → 1 row.
            r#"{"type":"assistant","sessionId":"sa","timestamp":"2026-09-15T12:00:00Z","requestId":"req-a","message":{"id":"msg-a","model":"m","usage":{"input_tokens":10,"output_tokens":2}}}"#,
            r#"{"type":"assistant","sessionId":"sa","timestamp":"2026-09-15T12:00:01Z","requestId":"req-a","message":{"id":"msg-a","model":"m","stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":5}}}"#,
            // B: no requestId, same ts, same msg — partial update → take 250.
            r#"{"type":"assistant","sessionId":"sb","timestamp":"2026-05-22T02:34:40Z","message":{"id":"msg-b","model":"m","stop_reason":"end_turn","usage":{"input_tokens":100,"output_tokens":25}}}"#,
            r#"{"type":"assistant","sessionId":"sb","timestamp":"2026-05-22T02:34:40Z","message":{"id":"msg-b","model":"m","stop_reason":"end_turn","usage":{"input_tokens":100,"output_tokens":250}}}"#,
            // C: gateway reuses msg id, no requestId, different ts → both count.
            r#"{"type":"assistant","sessionId":"sc","timestamp":"2026-09-11T12:00:00Z","message":{"id":"ocgo","model":"m","stop_reason":"end_turn","usage":{"input_tokens":7,"output_tokens":1}}}"#,
            r#"{"type":"assistant","sessionId":"sc","timestamp":"2026-09-11T12:01:00Z","message":{"id":"ocgo","model":"m","stop_reason":"end_turn","usage":{"input_tokens":7,"output_tokens":1}}}"#,
        ];
        std::fs::write(&path, lines.join("\n")).unwrap();
        let records = parse_single_file(&path).unwrap();
        // A(1) + B(1) + C(2) = 4
        assert_eq!(records.len(), 4, "A and B collapse, C stays separate");
        let b = records
            .iter()
            .find(|r| r.input_tokens == 100)
            .expect("msg-b row");
        assert_eq!(b.output_tokens, 250, "partial updates keep the largest");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// R9 fixture D: `claude --resume` copies transcript lines into a new
    /// session file. The same response must be billed once across files,
    /// keeping the larger (later, more complete) variant.
    #[test]
    fn resume_copied_lines_are_billed_once_across_files() {
        let dir = std::env::temp_dir().join(format!("tb-claude-d-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let shared = r#"{"type":"assistant","sessionId":"s-old","timestamp":"2026-05-22T02:34:40Z","requestId":"req-shared","message":{"id":"msg-shared","model":"m","stop_reason":"max_tokens","usage":{"input_tokens":100,"output_tokens":9}}}"#;
        let a = dir.join("a.jsonl");
        std::fs::write(
            &a,
            format!(
                "{shared}\n{}",
                r#"{"type":"assistant","sessionId":"s-old","timestamp":"2026-05-22T02:35:40Z","requestId":"req-a2","message":{"id":"msg-own-a","model":"m","stop_reason":"end_turn","usage":{"input_tokens":30,"output_tokens":3}}}"#
            ),
        )
        .unwrap();
        let b = dir.join("b.jsonl");
        std::fs::write(
            &b,
            format!(
                "{}\n{}",
                // The copy reports the completed usage: same response, larger sum.
                r#"{"type":"assistant","sessionId":"s-old","timestamp":"2026-05-22T02:34:40Z","requestId":"req-shared","message":{"id":"msg-shared","model":"m","stop_reason":"end_turn","usage":{"input_tokens":200,"output_tokens":9}}}"#,
                r#"{"type":"assistant","sessionId":"s-b","timestamp":"2026-05-22T03:00:00Z","requestId":"req-b1","message":{"id":"msg-own-b","model":"m","stop_reason":"end_turn","usage":{"input_tokens":11,"output_tokens":1}}}"#
            ),
        )
        .unwrap();

        let merged = merge_by_identity(
            vec![
                parse_single_file(&a).unwrap(),
                parse_single_file(&b).unwrap(),
            ]
            .into_iter()
            .flatten()
            .collect(),
        );

        assert_eq!(
            merged.iter().filter(|r| r.input_tokens == 200).count(),
            1,
            "the shared response is billed once"
        );
        assert!(
            !merged.iter().any(|r| r.input_tokens == 100),
            "the smaller copy is folded away"
        );
        assert_eq!(merged.len(), 3, "shared + own-a + own-b");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// R9 fixture E: a sidechain file replays a parent message (same
    /// message.id, different requestId). The replay folds into the parent's
    /// row; genuinely new sidechain work keeps counting.
    #[test]
    fn sidechain_replays_fold_into_the_mainline_row() {
        let dir = std::env::temp_dir().join(format!("tb-claude-e-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let main = dir.join("main.jsonl");
        std::fs::write(
            &main,
            r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-09-01T12:00:00Z","requestId":"req-parent","message":{"id":"msg-p","model":"m","stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":10}}}"#,
        )
        .unwrap();
        let sub = dir.join("sub.jsonl");
        std::fs::write(
            &sub,
            format!(
                "{}\n{}",
                // Replay of the parent message inside the sidechain.
                r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-09-11T12:00:00Z","requestId":"req-replay","isSidechain":true,"message":{"id":"msg-p","model":"m","stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":10,"cache_read_input_tokens":50000}}}"#,
                // The sidechain's own new message still counts.
                r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-09-11T12:00:30Z","requestId":"req-sub","isSidechain":true,"message":{"id":"msg-sub-own","model":"m","stop_reason":"end_turn","usage":{"input_tokens":4,"output_tokens":4}}}"#
            ),
        )
        .unwrap();

        let merged = merge_by_identity(
            vec![
                parse_single_file(&main).unwrap(),
                parse_single_file(&sub).unwrap(),
            ]
            .into_iter()
            .flatten()
            .collect(),
        );

        assert_eq!(merged.len(), 2, "replay folded, own work kept");
        let parent = merged.iter().find(|r| r.input_tokens == 10).unwrap();
        assert_eq!(parent.output_tokens, 10, "mainline row wins over replay");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Incomplete lines (no stop_reason yet) are never billable — a session
    /// file captured mid-stream contributes zero rows rather than partial
    /// ones.
    #[test]
    fn incomplete_streamed_lines_are_not_billable() {
        let dir = std::env::temp_dir().join(format!("tb-claude-i-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path: PathBuf = dir.join("session.jsonl");
        std::fs::write(
            &path,
            r#"{"type":"assistant","sessionId":"s1","timestamp":"2026-09-28T10:00:00Z","message":{"id":"msg_1","model":"m","stop_reason":null,"usage":{"input_tokens":100,"output_tokens":10}}}"#,
        )
        .unwrap();
        let records = parse_single_file(&path).unwrap();
        assert!(records.is_empty(), "no stop_reason → not yet billable");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
