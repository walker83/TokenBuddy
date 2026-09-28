use crate::{Source, TokenRecord};
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

static SQLITE_CACHE: Mutex<Option<(SystemTime, Vec<TokenRecord>)>> = Mutex::new(None);

/// Drop the resident parse cache. The cache only exists to make a *second*
/// sync cheaper than the first; left in place it pins every record of every
/// session log in the heap for the life of the process, growing with total
/// history and eating the resident-memory budget the dashboard is measured
/// against. `store::sync` calls this once the parquet has been written, so
/// the saving is paid back only by whoever asks for the next sync.
pub fn release_caches() {
    let mut cache = SQLITE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
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
    let db_path = get_zcode_db_path();
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
    // Default read-write flags. WAL mode allows concurrent readers; opening
    // read-only fails (SQLITE_CANTOPEN / code 14) when the WAL hasn't been
    // checkpointed. We only issue SELECTs so this is safe.
    let conn = rusqlite::Connection::open(db_path)?;

    // A tool that is mid-write holds an exclusive lock on its own database.
    // Without a busy timeout the SELECT fails instantly with SQLITE_BUSY —
    // and before sync became per-collector fault-tolerant, that single
    // locked database voided every other source's import as well.
    conn.busy_timeout(std::time::Duration::from_secs(5))?;

    // model_usage row id is unique — keep only completed rows so we don't
    // double-count in-flight or retried requests.
    let mut stmt = conn.prepare(
        "SELECT m.id, m.session_id, m.model_id, m.started_at, m.completed_at,
                m.input_tokens, m.output_tokens, m.reasoning_tokens,
                m.cache_creation_input_tokens, m.cache_read_input_tokens,
                m.duration_ms, m.time_to_first_token_ms, s.directory
         FROM model_usage m
         JOIN session s ON s.id = m.session_id
         WHERE m.status='completed'
         ORDER BY COALESCE(m.completed_at, m.started_at) ASC",
    )?;

    let mut records = Vec::new();
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let session_id: String = row.get(1)?;
        let model_id: String = row.get(2)?;
        let started_at: i64 = row.get(3)?;
        let completed_at: Option<i64> = row.get(4)?;
        let input_tokens: i64 = row.get(5)?;
        let output_tokens: i64 = row.get(6)?;
        let _reasoning_tokens: i64 = row.get(7)?;
        let cache_creation_input_tokens: i64 = row.get(8)?;
        let cache_read_input_tokens: i64 = row.get(9)?;
        let duration_ms_raw: Option<i64> = row.get(10).ok();
        let ttft_ms_raw: Option<i64> = row.get(11).ok();
        let directory: Option<String> = row.get(12).ok();

        if input_tokens == 0
            && output_tokens == 0
            && cache_read_input_tokens == 0
            && cache_creation_input_tokens == 0
        {
            continue;
        }

        let ts_ms = completed_at.unwrap_or(started_at);
        let timestamp_secs = ts_ms / 1000;

        // zcode reports duration_ms directly; fall back to completed_at - started_at.
        let duration_ms = duration_ms_raw
            .filter(|d| *d >= 0)
            .map(|d| d as u64)
            .or_else(|| {
                completed_at
                    .zip(Some(started_at))
                    .filter(|(e, s)| e >= s)
                    .map(|(e, s)| (e - s) as u64)
            });
        let ttft_ms = ttft_ms_raw.filter(|d| *d >= 0).map(|d| d as u64);

        // `input_tokens` from zcode already contains the cached prefix, so the
        // cached share is split out here to keep the same meaning as the other
        // sources: input + cache_read + cache_creation is the full prompt.
        let uncached_input = input_tokens
            .saturating_sub(cache_read_input_tokens.max(0))
            .saturating_sub(cache_creation_input_tokens.max(0))
            .max(0) as u64;

        records.push(TokenRecord {
            source: Source::Zcode,
            model: model_id,
            input_tokens: uncached_input,
            output_tokens: output_tokens.max(0) as u64,
            cache_read_tokens: cache_read_input_tokens.max(0) as u64,
            cache_creation_tokens: cache_creation_input_tokens.max(0) as u64,
            timestamp: timestamp_secs,
            session_id: Some(session_id),
            project: directory.unwrap_or_default(),
            duration_ms,
            ttft_ms,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: Some(id),
            merge_key: None,
        });
    }

    Ok(records)
}

// Storage location for context search; see `context.rs`.
pub(crate) fn db_path() -> PathBuf {
    get_zcode_db_path()
}

/// Candidate log locations, for `tokenbuddy doctor`: presence is optional,
/// the doctor reports what exists and what does not.
pub fn log_paths() -> Vec<PathBuf> {
    vec![get_zcode_db_path()]
}

fn get_zcode_db_path() -> PathBuf {
    if let Ok(custom) = std::env::var("ZCODE_CONFIG_DIR") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed)
                .join("cli")
                .join("db")
                .join("db.sqlite");
        }
    }
    dirs::home_dir()
        .map(|h| h.join(".zcode/cli/db/db.sqlite"))
        .unwrap_or_else(|| PathBuf::from(".zcode/cli/db/db.sqlite"))
}
