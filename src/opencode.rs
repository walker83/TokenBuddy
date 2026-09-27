use crate::{Source, TokenRecord};
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

static SQLITE_CACHE: Mutex<Option<(SystemTime, Vec<TokenRecord>)>> = Mutex::new(None);

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let db_path = get_opencode_db_path();
    if !db_path.exists() {
        return Ok(vec![]);
    }

    let mtime = std::fs::metadata(&db_path)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);

    let mut cache = SQLITE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((cached_mtime, ref records)) = *cache {
        if mtime <= cached_mtime {
            return Ok(records.clone());
        }
    }

    // Cache miss or stale — re-read
    let records = read_all_records(&db_path)?;
    *cache = Some((mtime, records.clone()));
    Ok(records)
}

fn read_all_records(db_path: &Path) -> Result<Vec<TokenRecord>> {
    // Open with default read-write flags. SQLite WAL mode allows many
    // concurrent readers without blocking the writer; opening read-only
    // fails (SQLITE_CANTOPEN / extended code 14) when the WAL hasn't been
    // checkpointed recently. We only issue SELECTs so the RW handle is safe.
    let conn = rusqlite::Connection::open(db_path)?;

    // OpenCode shipped two SQLite shapes:
    //   - v1 (<= ~2025): table `message` with role + token counts inside JSON.
    //   - v2 (>= 2026): table `session_message` per-message rows but token
    //     counts lifted onto `session_v2` (one row per session, message
    //     bodies flattened into `data.content[]` without a per-message
    //     `tokens` object). See PR feat/opencode-v2-on-gitea.
    // Detect at runtime so both stay supported without forcing users onto
    // a specific OpenCode version.
    let (sql, is_v2) = if crate::context::sqlite_table_exists(&conn, "session_message") {
        // v2 — token counts live on session_v2, one row per session.
        (
            "SELECT s.id, s.time_created, s.model, s.agent, \
                    s.tokens_input, s.tokens_output, s.tokens_cache_read, s.tokens_cache_write \
             FROM session_v2 s \
             WHERE s.tokens_input > 0 OR s.tokens_output > 0 \
             ORDER BY s.time_created ASC",
            true,
        )
    } else if crate::context::sqlite_table_exists(&conn, "message") {
        // v1 — per-message rows, role + tokens in `data` JSON.
        (
            "SELECT id, session_id, time_created, data \
             FROM message \
             ORDER BY time_created ASC",
            false,
        )
    } else {
        // Neither table present — nothing to read; surface as empty so
        // the collector quietly moves on instead of blowing up.
        return Ok(Vec::new());
    };

    let mut stmt = conn.prepare(sql)?;

    let mut records = Vec::new();
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        if is_v2 {
            // v2 path: aggregate per-session rollup from session_v2.
            let session_id: String = row.get(0)?;
            let time_created_ms: i64 = row.get(1)?;
            let model_raw: String = row.get(2)?;
            let agent: String = row.get(3)?;
            let input_tokens: u64 = row.get(4)?;
            let output_tokens: u64 = row.get(5)?;
            let cache_read: u64 = row.get(6)?;
            let cache_write: u64 = row.get(7)?;
            // v2 `model` is JSON `{"id":"...","providerID":"opencode","variant":"..."}`.
            let model_id = serde_json::from_str::<serde_json::Value>(&model_raw)
                .ok()
                .and_then(|v| v.get("id").and_then(|x| x.as_str()).map(String::from))
                .unwrap_or_else(|| model_raw.clone());
            let model_label = if agent.is_empty() {
                model_id
            } else {
                format!("{}/{}", agent, model_id)
            };
            records.push(TokenRecord {
                source: Source::OpenCode,
                model: model_label,
                input_tokens,
                output_tokens,
                cache_read_tokens: cache_read,
                cache_creation_tokens: cache_write,
                timestamp: time_created_ms / 1000,
                session_id: Some(session_id),
                duration_ms: None,
                ttft_ms: None,
                credits: 0.0,
                context_ratio: 0.0,
                record_id: None,
            });
            continue;
        }

        let _msg_id: String = row.get(0)?;
        let session_id: String = row.get(1)?;
        let time_created: i64 = row.get(2)?;
        let data: String = row.get(3)?;

        let value: serde_json::Value = match serde_json::from_str(&data) {
            Ok(v) => v,
            Err(_) => continue,
        };

        if value.get("role").and_then(|v| v.as_str()) != Some("assistant") {
            continue;
        }

        let tokens = match value.get("tokens") {
            Some(t) => t,
            None => continue,
        };

        let input_tokens = tokens.get("input").and_then(|v| v.as_u64()).unwrap_or(0);
        let output_tokens = tokens.get("output").and_then(|v| v.as_u64()).unwrap_or(0);
        let cache_read = tokens
            .get("cache")
            .and_then(|c| c.get("read"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let cache_write = tokens
            .get("cache")
            .and_then(|c| c.get("write"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        if input_tokens == 0 && output_tokens == 0 && cache_read == 0 && cache_write == 0 {
            continue;
        }

        let model = value
            .get("modelID")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let timestamp_ms = value
            .get("time")
            .and_then(|t| t.get("completed"))
            .and_then(|v| v.as_i64())
            .unwrap_or(time_created);

        let timestamp_secs = timestamp_ms / 1000;

        // Wall-clock duration: time.completed - time.created (both ms).
        let duration_ms = value
            .get("time")
            .and_then(|t| t.get("completed"))
            .and_then(|c| c.as_i64())
            .zip(
                value
                    .get("time")
                    .and_then(|t| t.get("created"))
                    .and_then(|c| c.as_i64()),
            )
            .and_then(|(end, start)| {
                if end >= start {
                    Some((end - start) as u64)
                } else {
                    None
                }
            });

        records.push(TokenRecord {
            source: Source::OpenCode,
            model,
            input_tokens,
            output_tokens,
            cache_read_tokens: cache_read,
            cache_creation_tokens: cache_write,
            timestamp: timestamp_secs,
            session_id: Some(session_id),
            duration_ms,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: None,
        });
    }

    Ok(records)
}

// Storage location for context search; see `context.rs`.
pub(crate) fn db_path() -> PathBuf {
    get_opencode_db_path()
}

fn get_opencode_db_path() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        if !xdg.is_empty() {
            return PathBuf::from(xdg).join("opencode").join("opencode.db");
        }
    }
    dirs::home_dir()
        .map(|h| h.join(".local/share/opencode/opencode.db"))
        .unwrap_or_else(|| PathBuf::from(".local/share/opencode/opencode.db"))
}
