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
//!
//! ## Generation timing
//!
//! The conversation log carries no timing — one completion timestamp per call,
//! not a span — so speed used to be unrecoverable for this source. The runtime
//! keeps its own SQLite store, and the assistant rows there do carry the pair
//! the TUI's speed readout is built from: `decode_duration_ms` (first streamed
//! token → generation end, prefill excluded) and `request_duration_ms` (the
//! whole call). They are joined in on `message_id`, which is the store's
//! `canonical_message_id`, and stored as `duration_ms = request` /
//! `ttft_ms = request − decode` so that `duration_ms − ttft_ms` is the decode
//! window for every source alike.
//!
//! Tokens still come from the log, and the dedupe key is untouched, so this
//! adds a column's worth of facts to rows that are already in the ledger
//! without re-importing a single one. A message the store has not caught up
//! with simply arrives untimed and is re-timed on a later sync.

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
    // Loaded once per sync and shared by every session file. The parse cache
    // below is dropped after each sync, so a message that lands in the store
    // after its log line is picked up on the next pass rather than staying
    // permanently untimed.
    let timing = load_timing_index();

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
            let records = parse_usage_records(file_path, &timing).unwrap_or_default();
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
    vec![sessions_dir(), sqlite_path()]
}

fn minimax_home() -> PathBuf {
    if let Ok(custom) = std::env::var("MINIMAX_HOME") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".minimax")
}

fn sessions_dir() -> PathBuf {
    minimax_home().join("v2/sessions")
}

/// The runtime's own SQLite store. Not a token source — the conversation log
/// above is — but the only place the per-call generation timing is written.
fn sqlite_path() -> PathBuf {
    minimax_home().join("v2/sqlite/runtime-state.sqlite")
}

/// Per-call generation timing, as the runtime measured it.
#[derive(Clone, Copy, Debug)]
struct GenerationTiming {
    /// Whole-request wall clock.
    request_ms: u64,
    /// First streamed token → generation end. The prefill wait is excluded,
    /// which is what makes this the denominator of a decode-speed figure.
    decode_ms: u64,
}

/// `canonical_message_id` → timing, read from the runtime store.
///
/// The TUI's own speed readout (`packages/tui/…/turn-output-rate.ts`) is
/// `Σ outputTokens / Σ decodeDurationMs` over a turn. Reproducing that number
/// needs this pair, and the conversation log does not carry it: its `usage`
/// object is `{input, output, cacheRead, cacheWrite, totalTokens, cost}` on
/// every assistant line, with no timing at all.
///
/// The store is opened read-only and every failure degrades to an empty index
/// — a missing, locked, or schema-drifted store costs the speed column and
/// nothing else. Mirrors the zcode receipt path.
fn load_timing_index() -> HashMap<String, GenerationTiming> {
    let mut index = HashMap::new();
    let db = sqlite_path();
    if !db.is_file() {
        return index;
    }
    let Ok(conn) =
        rusqlite::Connection::open_with_flags(&db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
    else {
        return index;
    };
    let Ok(mut stmt) =
        conn.prepare("SELECT data_json FROM local_runtime_message_rows WHERE role = 'assistant'")
    else {
        return index;
    };
    let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) else {
        return index;
    };
    for row in rows.flatten() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&row) else {
            continue;
        };
        let Some(key) = value.get("canonical_message_id").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(usage) = value.get("usage") else {
            continue;
        };
        // The store spells usage in snake_case, unlike the conversation log's
        // camelCase — same numbers, different serializer.
        let Some(decode_ms) = usage.get("decode_duration_ms").and_then(|v| v.as_u64()) else {
            continue;
        };
        let request_ms = usage
            .get("request_duration_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(decode_ms);
        index.insert(
            key.to_string(),
            GenerationTiming {
                request_ms,
                decode_ms,
            },
        );
    }
    index
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

fn parse_usage_records(
    messages_path: &Path,
    timing: &HashMap<String, GenerationTiming>,
) -> Result<Vec<TokenRecord>> {
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

        // Timing joins on the log line's own `message_id`, which is the same
        // `canonical_message_id` the store keys on. The pair is stored so that
        // `duration_ms - ttft_ms` reproduces the runtime's decode window
        // exactly, which is the denominator every source's speed figure uses.
        // No entry (older message, store absent) leaves both null: the session
        // then reports no speed rather than a fabricated one.
        let (duration_ms, ttft_ms) = value
            .get("message_id")
            .and_then(|v| v.as_str())
            .and_then(|id| timing.get(id))
            .map(|t| {
                let request = t.request_ms;
                // A decode window at or past the whole request means the two
                // clocks disagree; the prefill wait cannot be negative.
                let prefill = request.saturating_sub(t.decode_ms);
                (Some(request), Some(prefill))
            })
            .unwrap_or((None, None));

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
            duration_ms,
            ttft_ms,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: message
                .get("responseId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            sidechain: false,
            merge_key: None,
            request_count: 1,
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
    use super::{
        collect_message_files, content_text, load_timing_index, parse_usage_records,
        strip_injected_blocks, GenerationTiming,
    };
    use std::collections::HashMap;
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

        let records =
            parse_usage_records(&session.join("messages.jsonl"), &HashMap::new()).expect("parses");
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
        // The log alone carries no span, so the session reports no speed.
        assert_eq!(r.duration_ms, None);
        assert_eq!(r.ttft_ms, None);

        let _ = fs::remove_dir_all(&dir);
    }

    /// The decode window the store reports must survive the round trip
    /// through `duration_ms - ttft_ms` unchanged, because that subtraction is
    /// what every source's speed figure divides by.
    #[test]
    fn store_timing_reproduces_the_decode_window() {
        let dir = std::env::temp_dir().join(format!("tokenbuddy-mx-timing-{}", std::process::id()));
        let session = dir.join("v2/sessions/2026/10/04/09-00-00-000-session_a");
        write_session(
            &session,
            concat!(
                r#"{"message_id":"a1","message":{"role":"assistant","model":"m","usage":{"input":100,"output":900,"cacheRead":0,"cacheWrite":0,"totalTokens":1000},"timestamp":1791000000000,"responseId":"resp-1"}}"#,
                "\n",
                r#"{"message_id":"a2","message":{"role":"assistant","model":"m","usage":{"input":10,"output":50,"cacheRead":0,"cacheWrite":0,"totalTokens":60},"timestamp":1791000001000,"responseId":"resp-2"}}"#,
                "\n",
            ),
        );

        let mut timing = HashMap::new();
        timing.insert(
            "a1".to_string(),
            GenerationTiming {
                request_ms: 3_000,
                decode_ms: 1_200,
            },
        );
        // A decode window at or past the whole request is clock skew, not a
        // negative prefill wait.
        timing.insert(
            "a2".to_string(),
            GenerationTiming {
                request_ms: 500,
                decode_ms: 900,
            },
        );

        let records =
            parse_usage_records(&session.join("messages.jsonl"), &timing).expect("parses");
        assert_eq!(records.len(), 2);

        let timed = &records[0];
        assert_eq!(timed.duration_ms, Some(3_000));
        assert_eq!(timed.ttft_ms, Some(1_800));
        let decode = timed.duration_ms.unwrap() - timed.ttft_ms.unwrap();
        assert_eq!(decode, 1_200, "decode window must round-trip exactly");
        // Same arithmetic the TUI does, at the same place.
        assert!((timed.output_tokens as f64 / (decode as f64 / 1000.0) - 750.0).abs() < 1e-9);

        let skewed = &records[1];
        assert_eq!(skewed.duration_ms, Some(500));
        assert_eq!(skewed.ttft_ms, Some(0));

        let _ = fs::remove_dir_all(&dir);
    }

    /// A log line the store has not caught up with stays untimed rather than
    /// being dropped or guessed at.
    #[test]
    fn untimed_messages_stay_in_the_ledger_with_null_spans() {
        let dir =
            std::env::temp_dir().join(format!("tokenbuddy-mx-untimed-{}", std::process::id()));
        let session = dir.join("v2/sessions/2026/10/04/09-00-00-000-session_a");
        write_session(
            &session,
            concat!(
                r#"{"message_id":"a1","message":{"role":"assistant","model":"m","usage":{"input":1,"output":2,"cacheRead":0,"cacheWrite":0,"totalTokens":3},"timestamp":1791000000000,"responseId":"r1"}}"#,
                "\n",
                r#"{"message_id":"a2","message":{"role":"assistant","model":"m","usage":{"input":1,"output":2,"cacheRead":0,"cacheWrite":0,"totalTokens":3},"timestamp":1791000001000,"responseId":"r2"}}"#,
                "\n",
            ),
        );

        // Only a1 is known; a2 has no entry at all.
        let mut timing = HashMap::new();
        timing.insert(
            "a1".to_string(),
            GenerationTiming {
                request_ms: 2_000,
                decode_ms: 1_000,
            },
        );
        let records =
            parse_usage_records(&session.join("messages.jsonl"), &timing).expect("parses");
        assert_eq!(records.len(), 2, "an untimed message is still collected");
        assert_eq!(records[0].duration_ms, Some(2_000));
        assert_eq!(records[1].duration_ms, None);
        assert_eq!(records[1].ttft_ms, None);

        let _ = fs::remove_dir_all(&dir);
    }

    /// The store is optional infrastructure: absent, unreadable, or missing
    /// the table, the collector still returns its tokens.
    #[test]
    fn a_missing_runtime_store_costs_only_the_speed_column() {
        use crate::TEST_ENV_LOCK;
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("mx-nostore");
        fs::create_dir_all(&dir).expect("tmp creatable");
        std::env::set_var("MINIMAX_HOME", &dir);
        let index = load_timing_index();
        std::env::remove_var("MINIMAX_HOME");
        assert!(index.is_empty(), "no store means no timing, not an error");

        // A store that exists but has no such table must degrade the same way.
        let db = dir.join("v2/sqlite/runtime-state.sqlite");
        fs::create_dir_all(db.parent().unwrap()).expect("sqlite dir creatable");
        let conn = rusqlite::Connection::open(&db).expect("store creatable");
        conn.execute_batch("CREATE TABLE unrelated (x INTEGER);")
            .expect("schema creatable");
        drop(conn);
        std::env::set_var("MINIMAX_HOME", &dir);
        let index = load_timing_index();
        std::env::remove_var("MINIMAX_HOME");
        assert!(index.is_empty(), "schema drift must not be fatal");

        let _ = fs::remove_dir_all(&dir);
    }

    /// End to end over a real store: the row the runtime writes is what the
    /// conversation log ends up carrying, with the decode window intact.
    #[test]
    fn runtime_store_rows_reach_the_record_as_timing() {
        use crate::TEST_ENV_LOCK;
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("mx-store");
        let session = dir.join("v2/sessions/2026/10/04/09-00-00-000-session_a");
        write_session(
            &session,
            concat!(
                r#"{"message_id":"msg-a1","message":{"role":"assistant","model":"m","usage":{"input":10,"output":300,"cacheRead":0,"cacheWrite":0,"totalTokens":310},"timestamp":1791000000000,"responseId":"r1"}}"#,
                "\n",
            ),
        );

        let db = dir.join("v2/sqlite/runtime-state.sqlite");
        fs::create_dir_all(db.parent().unwrap()).expect("sqlite dir creatable");
        let conn = rusqlite::Connection::open(&db).expect("store creatable");
        conn.execute_batch(
            "CREATE TABLE local_runtime_message_rows (
                 session_id TEXT, role TEXT, data_json TEXT
             );
             INSERT INTO local_runtime_message_rows VALUES
                 ('mvs_test','assistant','{\"canonical_message_id\":\"msg-a1\",\"usage\":{\"output_tokens\":300,\"decode_duration_ms\":2000,\"request_duration_ms\":5000}}');",
        )
        .expect("store writable");

        std::env::set_var("MINIMAX_HOME", &dir);
        let index = load_timing_index();
        let records = parse_usage_records(&session.join("messages.jsonl"), &index).expect("parses");
        std::env::remove_var("MINIMAX_HOME");
        drop(conn);

        assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r.output_tokens, 300, "tokens still come from the log");
        assert_eq!(r.record_id.as_deref(), Some("r1"), "dedupe key untouched");
        assert_eq!(r.duration_ms, Some(5_000));
        assert_eq!(r.ttft_ms, Some(3_000));
        let decode = r.duration_ms.unwrap() - r.ttft_ms.unwrap();
        assert_eq!(decode, 2_000);
        assert!((r.output_tokens as f64 / (decode as f64 / 1000.0) - 150.0).abs() < 1e-9);

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
