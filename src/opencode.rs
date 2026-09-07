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

    let mut stmt = conn.prepare(
        "SELECT id, session_id, time_created, data FROM message ORDER BY time_created ASC",
    )?;

    let mut records = Vec::new();
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
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
            .zip(value.get("time").and_then(|t| t.get("created")).and_then(|c| c.as_i64()))
            .and_then(|(end, start)| if end >= start { Some((end - start) as u64) } else { None });

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
