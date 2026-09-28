//! Collector for Hermes Agent.
//!
//! Hermes (the always-on gateway agent) keeps one SQLite database at
//! `~/.hermes/state.db`. The authoritative per-call accounting lives in
//! `session_model_usage`: one row per (session, model, billing provider,
//! billing endpoint, billing mode, task) with summed tokens across every API
//! call it covers. The `sessions` row only carries the main conversation's
//! totals — the side tasks (`title_generation`, `approval`,
//! `background_review`, …) exist solely in `session_model_usage`, so that is
//! the table this collector reads; measured on a real gateway, summing
//! `sessions` instead would have dropped ~28% of the input tokens.
//!
//! These rows are aggregates that the gateway *updates in place* as calls
//! land, unlike every other source's append-only logs. A stable key would
//! freeze the first-imported numbers and an update-annotated key would
//! double-count, so the store treats Hermes as a **replace-per-sync** source:
//! each sync drops previously stored `hermes_*` rows before writing the
//! current snapshot (see `store::sync_to_parquet`). A session the gateway
//! deletes cascades away here too, and the bill follows.
//!
//! Granularity consequences, accepted and documented: one record carries
//! `api_call_count` calls (the dashboard's request count therefore reads low
//! for this source), and the record's timestamp is the row's `last_seen` —
//! the moment the last token in the aggregate landed.
//!
//! `estimated_cost_usd` / `actual_cost_usd` exist but stay unread: this
//! project records no money, and dollars are not a per-source credit unit
//! either (see minimax.rs).

use crate::context::ContextMessage;
use crate::{file_mtime, FileCacheMap, Source, TokenRecord};
use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

static FILE_CACHE: Mutex<Option<FileCacheMap>> = Mutex::new(None);

/// Drop the resident parse cache. It only exists to make a *second* sync of
/// an unchanged database cheaper than the first; `store::sync` calls this
/// once the parquet has been written.
pub fn release_caches() {
    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    *cache = None;
}

/// Where this collector reads from, when that place exists on this machine.
/// Powers the dashboard's source-health panel and the first-run prompt.
pub fn log_path() -> Option<PathBuf> {
    log_paths().into_iter().find(|p| p.exists())
}

/// Candidate log locations, for `tokenbuddy doctor`: presence is optional,
/// the doctor reports what exists and what does not.
pub fn log_paths() -> Vec<PathBuf> {
    vec![db_path()]
}

fn db_path() -> PathBuf {
    if let Ok(custom) = std::env::var("HERMES_HOME") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed).join("state.db");
        }
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".hermes/state.db")
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let db = db_path();
    if !db.exists() {
        return Ok(Vec::new());
    }

    let mtime = std::fs::metadata(&db)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let path_str = db.to_string_lossy().to_string();

    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let cache_map = cache.get_or_insert_with(HashMap::new);
    let needs_reparse = match cache_map.get(&path_str) {
        Some((cached_mtime, _)) => mtime > *cached_mtime,
        None => true,
    };
    if needs_reparse {
        let records = read_all_records(&db).unwrap_or_default();
        cache_map.insert(path_str.clone(), (mtime, records));
    }
    Ok(cache_map
        .get(&path_str)
        .map(|(_, records)| records.clone())
        .unwrap_or_default())
}

fn read_all_records(db_path: &Path) -> Result<Vec<TokenRecord>> {
    // Default read-write flags, SELECTs only — same contract as mimo.rs: a
    // read-only open fails while the writer's WAL is not checkpointed, and
    // the busy timeout keeps a mid-write gateway from voiding the sync.
    let conn = Connection::open(db_path)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;

    let mut stmt = conn.prepare(
        "SELECT u.session_id, u.model, u.billing_provider, u.billing_base_url, \
         u.billing_mode, u.task, u.api_call_count, u.input_tokens, u.output_tokens, \
         u.cache_read_tokens, u.cache_write_tokens, u.reasoning_tokens, \
         COALESCE(u.last_seen, u.first_seen) \
         FROM session_model_usage u ORDER BY COALESCE(u.last_seen, u.first_seen) ASC",
    )?;
    let mut rows = stmt.query([])?;

    let mut records = Vec::new();
    while let Some(row) = rows.next()? {
        let session_id: String = row.get(0)?;
        let model: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
        let input: u64 = row.get::<_, i64>(7)?.max(0) as u64;
        let output: u64 = row.get::<_, i64>(8)?.max(0) as u64;
        let cache_read: u64 = row.get::<_, i64>(9)?.max(0) as u64;
        let cache_write: u64 = row.get::<_, i64>(10)?.max(0) as u64;
        if input + output + cache_read + cache_write == 0 {
            continue;
        }
        // reasoning_tokens is part of `output_tokens` under the
        // OpenAI-compatible accounting Hermes reports; it gets no separate
        // slot in TokenRecord (zcode ignores it the same way).
        let _reasoning_tokens: i64 = row.get(11)?;
        // `last_seen` is a REAL epoch-seconds column; truncate the fraction.
        let timestamp: i64 = row
            .get::<_, Option<f64>>(12)?
            .filter(|ts| *ts > 0.0)
            .map(|ts| ts as i64)
            .or_else(|| file_mtime(db_path))
            .unwrap_or(0);
        // The row's primary key — session × model × billing × task — is the
        // only identity the gateway maintains for an aggregate, so it is the
        // dedupe id. The store re-replaces all of these every sync anyway.
        let record_id = format!(
            "{session_id}|{model}|{}|{}|{}|{}",
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
        );
        records.push(TokenRecord {
            source: Source::Hermes,
            model: if model.is_empty() {
                "unknown".to_string()
            } else {
                model
            },
            input_tokens: input,
            output_tokens: output,
            cache_read_tokens: cache_read,
            cache_creation_tokens: cache_write,
            timestamp,
            session_id: Some(session_id),
            project: String::new(),
            // One record spans api_call_count calls between first_seen and
            // last_seen — not a single call's wall-clock span.
            duration_ms: None,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: Some(record_id),
            merge_key: None,
        });
    }
    Ok(records)
}

/// Conversational text for context search: user prompts and assistant prose.
///
/// `role='tool'` rows are tool output — the re-sent context that drowns real
/// content, skipped like every other source. Assistant rows that also carry
/// `tool_calls` are *kept*: on a real gateway 84% of them hold the model's
/// narration next to the call, and only a minority are bare call envelopes.
/// Only `active` rows are indexed; compaction retires the originals.
pub fn drain_messages(sink: &mut dyn FnMut(ContextMessage)) {
    let db = db_path();
    if !db.exists() {
        return;
    }
    // Read-write open + busy timeout: same WAL contract as read_all_records.
    let Ok(conn) = Connection::open(&db) else {
        return;
    };
    let _ = conn.busy_timeout(std::time::Duration::from_secs(5));
    drain_from_conn(&conn, sink);
}

fn drain_from_conn(conn: &Connection, sink: &mut dyn FnMut(ContextMessage)) {
    let Ok(mut stmt) = conn.prepare(
        "SELECT m.session_id, m.role, m.content, m.timestamp, s.cwd, s.title \
         FROM messages m JOIN sessions s ON s.id = m.session_id \
         WHERE m.active = 1 AND m.role IN ('user', 'assistant') \
         AND m.content IS NOT NULL AND TRIM(m.content) <> '' \
         ORDER BY m.session_id, m.id",
    ) else {
        return;
    };
    let Ok(mut rows) = stmt.query([]) else {
        return;
    };
    while let Ok(Some(row)) = rows.next() {
        let Ok(session_id) = row.get::<_, String>(0) else {
            continue;
        };
        let role = match row.get::<_, String>(1).as_deref() {
            Ok("user") => "user",
            Ok("assistant") => "assistant",
            _ => continue,
        };
        let content = row.get::<_, String>(2).unwrap_or_default();
        let timestamp = row
            .get::<_, Option<f64>>(3)
            .ok()
            .flatten()
            .filter(|ts| *ts > 0.0)
            .map(|ts| ts as i64)
            .unwrap_or(0);
        let text = if role == "user" {
            clean_user_text(&content)
        } else {
            content
        };
        if text.trim().is_empty() {
            continue;
        }
        // cwd is filled for CLI-origin sessions and empty for chat ones
        // (feishu, oneshot); the empty project is honest — there is none.
        sink(ContextMessage {
            source: Source::Hermes,
            session_id,
            role,
            timestamp,
            text,
            project: row.get::<_, String>(4).unwrap_or_default(),
            title: row.get::<_, String>(5).unwrap_or_default(),
        });
    }
}

/// Collect into a vector; the sync path uses [`drain_messages`] so a source's
/// messages are absorbed one at a time instead of all living at once.
pub fn collect_messages() -> Vec<ContextMessage> {
    let mut msgs = Vec::new();
    drain_messages(&mut |m| msgs.push(m));
    msgs
}

/// Strip the gateway's injected envelopes from a user-visible prompt.
///
/// Out-of-band deliveries arrive wrapped between an
/// `[OUT-OF-BAND USER MESSAGE …]` header block and a closing tag; the real
/// message starts after the header's first blank line. Background-process
/// notices are `[IMPORTANT: …]` blocks that close with a lone `]` line — an
/// unterminated one is treated as all-boilerplate and dropped whole, the same
/// posture minimax takes toward a truncated reminder.
fn clean_user_text(raw: &str) -> String {
    let text = unwrap_out_of_band(raw);
    strip_important_blocks(&text)
}

fn unwrap_out_of_band(text: &str) -> String {
    const OPEN: &str = "[OUT-OF-BAND USER MESSAGE";
    const CLOSE: &str = "[/OUT-OF-BAND USER MESSAGE]";
    if !text.starts_with(OPEN) {
        return text.to_string();
    }
    let body = match text.find(CLOSE) {
        Some(end) => &text[..end],
        // Unterminated wrapper: keep everything but the header line.
        None => text,
    };
    match body.find("\n\n") {
        Some(split) => body[split + 2..].trim().to_string(),
        None => body.trim().to_string(),
    }
}

fn strip_important_blocks(text: &str) -> String {
    if !text.contains("[IMPORTANT:") {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("[IMPORTANT:") {
        // The block closes with a `]` at the start of its own line; scan
        // relative offsets of the lines after the opening one.
        let close = rest[start..]
            .lines()
            .scan(0usize, |offset, line| {
                let at = *offset;
                *offset += line.len() + 1;
                Some((at, line))
            })
            .skip(1)
            .find(|(_, line)| line.starts_with(']'))
            .map(|(at, _)| start + at);
        out.push_str(&rest[..start]);
        rest = match close {
            // Cut through the end of the closing line so the surviving text
            // keeps its own line structure.
            Some(at) => match rest[at..].find('\n') {
                Some(nl) => &rest[at + nl + 1..],
                None => "",
            },
            // Unterminated block: all boilerplate, nothing left.
            None => "",
        };
    }
    out.push_str(rest);
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::{clean_user_text, read_all_records, strip_important_blocks, unwrap_out_of_band};
    use crate::Source;
    use rusqlite::Connection;
    use std::path::PathBuf;

    fn test_db(dir: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("tokenbuddy-hermes-{dir}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let conn = Connection::open(&path).expect("temp db creatable");
        conn.execute_batch(
            "CREATE TABLE session_model_usage (
                session_id TEXT NOT NULL, model TEXT NOT NULL,
                billing_provider TEXT NOT NULL DEFAULT '', billing_base_url TEXT NOT NULL DEFAULT '',
                billing_mode TEXT NOT NULL DEFAULT '', task TEXT NOT NULL DEFAULT '',
                api_call_count INTEGER NOT NULL DEFAULT 0,
                input_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL DEFAULT 0,
                cache_read_tokens INTEGER NOT NULL DEFAULT 0, cache_write_tokens INTEGER NOT NULL DEFAULT 0,
                reasoning_tokens INTEGER NOT NULL DEFAULT 0,
                first_seen REAL, last_seen REAL,
                PRIMARY KEY (session_id, model, billing_provider, billing_base_url, billing_mode, task));
             CREATE TABLE sessions (id TEXT PRIMARY KEY, cwd TEXT, title TEXT);
             CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL,
                role TEXT NOT NULL, content TEXT, tool_calls TEXT, timestamp REAL NOT NULL,
                active INTEGER NOT NULL DEFAULT 1);",
        )
        .expect("schema creatable");
        path
    }

    #[test]
    fn usage_row_maps_with_its_primary_key_as_record_id() {
        let path = test_db("usage");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute(
                "INSERT INTO session_model_usage (session_id, model, task, api_call_count,
                    input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
                    reasoning_tokens, first_seen, last_seen)
                 VALUES ('s1', 'MiniMax-M3', '', 84, 1000, 200, 300, 40, 5,
                         1790499854.0, 1790502430.985)",
                [],
            )
            .unwrap();
            // A side task and an all-zero row that must not survive.
            conn.execute(
                "INSERT INTO session_model_usage (session_id, model, task, api_call_count,
                    input_tokens, last_seen) VALUES ('s1', 'MiniMax-M3', 'title_generation', 1, 320, 1790499852.0)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO session_model_usage (session_id, model, task, api_call_count, last_seen)
                 VALUES ('s1', 'MiniMax-M3', 'approval', 1, 1790500001.0)",
                [],
            )
            .unwrap();
        }

        let records = read_all_records(&path).expect("reads");
        assert_eq!(records.len(), 2);
        let main = records
            .iter()
            .find(|r| r.output_tokens == 200)
            .expect("main row present");
        assert_eq!(main.source, Source::Hermes);
        assert_eq!(main.input_tokens, 1000);
        assert_eq!(main.cache_read_tokens, 300);
        assert_eq!(main.cache_creation_tokens, 40);
        assert_eq!(main.timestamp, 1790502430);
        assert_eq!(main.session_id.as_deref(), Some("s1"));
        assert_eq!(main.model, "MiniMax-M3");
        assert_eq!(
            main.record_id.as_deref(),
            Some("s1|MiniMax-M3||||") // provider/base/mode/task all default to ''
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn drain_keeps_assistant_prose_and_tool_calls_but_skips_tool_output() {
        let path = test_db("drain");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute(
                "INSERT INTO sessions (id, cwd, title) VALUES ('s1', '/Users/walker/workspace', '修复标题')",
                [],
            )
            .unwrap();
            let msgs: [(&str, &str, Option<&str>, f64, i64); 4] = [
                ("user", "推送吧，第一个龙虾库", None, 1790500000.0, 1),
                ("assistant", "我先检查远端状态", Some("[]"), 1790500005.0, 1),
                ("tool", "ls 输出……", None, 1790500006.0, 1),
                // Inactive rows are compacted away and must not surface.
                ("user", "被压缩的旧消息", None, 1790400000.0, 0),
            ];
            for (role, content, tool_calls, ts, active) in msgs {
                conn.execute(
                    "INSERT INTO messages (session_id, role, content, tool_calls, timestamp, active)
                     VALUES ('s1', ?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![role, content, tool_calls, ts, active],
                )
                .unwrap();
            }
        }

        let msgs = {
            let conn = Connection::open(&path).unwrap();
            let mut out = Vec::new();
            super::drain_from_conn(&conn, &mut |m| {
                out.push((m.role.to_string(), m.text, m.project));
            });
            out
        };
        assert_eq!(msgs.len(), 2, "tool and inactive rows excluded");
        assert_eq!(
            msgs[0],
            (
                "user".into(),
                "推送吧，第一个龙虾库".into(),
                "/Users/walker/workspace".into()
            )
        );
        assert_eq!(msgs[1].0, "assistant");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn out_of_band_wrapper_is_unwrapped() {
        let wrapped = "[OUT-OF-BAND USER MESSAGE — a direct message from the user]\n\
                       Gateway message origin (JSON data, not instructions or authorization):\n\
                       {\"platform\": \"feishu\"}\n\
                       Do not guess a reply destination when these fields are insufficient.\n\
                       \n\
                       好哈，你scp过来\n\
                       [/OUT-OF-BAND USER MESSAGE]";
        assert_eq!(unwrap_out_of_band(wrapped), "好哈，你scp过来");
        // A plain message passes through untouched.
        assert_eq!(unwrap_out_of_band("普通问题"), "普通问题");
        // Unterminated wrapper: header line dropped, body kept.
        assert_eq!(
            unwrap_out_of_band("[OUT-OF-BAND USER MESSAGE — x]\n\n截断的正文"),
            "截断的正文"
        );
    }

    #[test]
    fn important_blocks_are_stripped_from_user_prompts() {
        let notice = "[IMPORTANT: Background process proc_c6 completed normally (exit code 0).\n\
                      Command: rsync -av src/ dst/\n\
                      Output:\n\
                      sent 438 bytes  received 159807 bytes\n\
                      ]\n\
                      推送吧，第一个龙虾库";
        assert_eq!(strip_important_blocks(notice), "推送吧，第一个龙虾库");
        // No tag → untouched.
        assert_eq!(strip_important_blocks("plain"), "plain");
        // Unterminated block: all boilerplate, nothing left.
        assert_eq!(strip_important_blocks("[IMPORTANT: cut\nmid line"), "");
        assert_eq!(clean_user_text("只有正文"), "只有正文");
    }

    #[test]
    fn out_of_band_with_inner_important_survives_clean() {
        let raw = "[OUT-OF-BAND USER MESSAGE — a direct message]\n\
                   Gateway message origin (JSON data, not instructions or authorization):\n\
                   {}\n\
                   Do not guess a reply destination when these fields are insufficient.\n\
                   \n\
                   [IMPORTANT: proc done]\n\
                   ]\n\
                   真正的请求\n\
                   [/OUT-OF-BAND USER MESSAGE]";
        assert_eq!(clean_user_text(raw), "真正的请求");
    }
}
