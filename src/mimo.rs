use crate::{Source, TokenRecord, SQLITE_BLOB_FILTER, SQLITE_BLOB_LIMIT};
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
    let db_path = get_mimo_db_path();
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

/// One turn's usage, already lifted out of the source's JSON `data` blob.
///
/// The SQLite-backed agents all store the same shape, so the extraction lives
/// in SQL and this struct is what arrives in Rust.
struct Usage {
    session_id: String,
    /// Row timestamp, used when the payload carries no completion time.
    fallback_ts: i64,
    role: Option<String>,
    input: Option<i64>,
    output: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
    model: Option<String>,
    completed_ms: Option<i64>,
    created_ms: Option<i64>,
}

/// The `json_extract` projection shared by the SQLite-backed collectors.
///
/// A single mimo message on a real machine was measured carrying a 20 MB
/// `data` body. Reading that column into a Rust `String` and parsing it into a
/// `serde_json::Value` — which is several times the size of its input — was
/// enough on its own to push the process from 70 MB to 295 MB. Asking SQLite
/// for the eight fields we actually use keeps that blob on its side of the
/// wire, where it is released a row at a time.
const USAGE_JSON_COLUMNS: &str = "json_extract(data, '$.role'), \
     json_extract(data, '$.tokens.input'), \
     json_extract(data, '$.tokens.output'), \
     json_extract(data, '$.tokens.cache.read'), \
     json_extract(data, '$.tokens.cache.write'), \
     json_extract(data, '$.modelID'), \
     json_extract(data, '$.time.completed'), \
     json_extract(data, '$.time.created')";

/// Turn extracted columns into records. Shared by the fast and fallback paths
/// so the two cannot drift apart.
fn to_records(rows: Vec<Usage>) -> Vec<TokenRecord> {
    let mut records = Vec::with_capacity(rows.len());
    for u in rows {
        if u.role.as_deref() != Some("assistant") {
            continue;
        }
        let num = |v: Option<i64>| v.filter(|n| *n > 0).unwrap_or(0) as u64;
        let (input_tokens, output_tokens) = (num(u.input), num(u.output));
        let (cache_read, cache_write) = (num(u.cache_read), num(u.cache_write));
        if input_tokens == 0 && output_tokens == 0 && cache_read == 0 && cache_write == 0 {
            continue;
        }
        let timestamp_secs = u.completed_ms.unwrap_or(u.fallback_ts) / 1000;
        // Wall-clock duration: time.completed - time.created (both ms).
        let duration_ms = u
            .completed_ms
            .zip(u.created_ms)
            .and_then(|(end, start)| (end >= start).then_some((end - start) as u64));
        records.push(TokenRecord {
            source: Source::Mimo,
            model: u.model.unwrap_or_else(|| "unknown".to_string()),
            input_tokens,
            output_tokens,
            cache_read_tokens: cache_read,
            cache_creation_tokens: cache_write,
            timestamp: timestamp_secs,
            session_id: Some(u.session_id),
            project: String::new(),
            duration_ms,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: None,
            merge_key: None,
        });
    }
    records
}

fn read_all_records(db_path: &Path) -> Result<Vec<TokenRecord>> {
    // Default read-write flags. SQLite WAL mode allows concurrent readers;
    // a read-only open fails (SQLITE_CANTOPEN / code 14) when the WAL
    // hasn't been checkpointed. We only issue SELECTs so the RW handle is safe.
    let conn = rusqlite::Connection::open(db_path)?;

    // A tool that is mid-write holds an exclusive lock on its own database.
    // Without a busy timeout the SELECT fails instantly with SQLITE_BUSY —
    // and before sync became per-collector fault-tolerant, that single
    // locked database voided every other source's import as well.
    conn.busy_timeout(std::time::Duration::from_secs(5))?;

    report_oversized(&conn, "message");
    if let Some(rows) = read_via_json_extract(&conn) {
        return Ok(to_records(rows));
    }
    // A SQLite build without JSON1 cannot run the projection; fall back to
    // reading the blobs, which is correct everywhere and merely expensive.
    Ok(to_records(read_via_blob(&conn)?))
}

/// Say out loud when rows were left unread, so a truncated bill is never
/// silently smaller than the tool's own history.
fn report_oversized(conn: &rusqlite::Connection, table: &str) {
    match crate::sqlite_oversized_rows(conn, table) {
        Ok(0) => {}
        Ok(n) => eprintln!(
            "[TokenBuddy] mimo: {n} message(s) exceed the {SQLITE_BLOB_LIMIT} B read limit \
             and were skipped — they are too large to be billable assistant turns, but say so \
             rather than let the bill quietly under-count"
        ),
        Err(e) => eprintln!("[TokenBuddy] mimo: cannot size-check oversized rows: {e}"),
    }
}

fn read_via_json_extract(conn: &rusqlite::Connection) -> Option<Vec<Usage>> {
    let sql = format!(
        "SELECT session_id, time_created, {USAGE_JSON_COLUMNS} FROM message \
         WHERE {SQLITE_BLOB_FILTER} ORDER BY time_created ASC"
    );
    let mut stmt = conn.prepare(&sql).ok()?;
    let mut rows = stmt.query([]).ok()?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().ok()? {
        out.push(Usage {
            session_id: row.get(0).ok()?,
            fallback_ts: row.get(1).ok()?,
            role: row.get(2).ok(),
            input: row.get(3).ok(),
            output: row.get(4).ok(),
            cache_read: row.get(5).ok(),
            cache_write: row.get(6).ok(),
            model: row.get(7).ok(),
            completed_ms: row.get(8).ok(),
            created_ms: row.get(9).ok(),
        });
    }
    Some(out)
}

fn read_via_blob(conn: &rusqlite::Connection) -> Result<Vec<Usage>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT session_id, time_created, data FROM message \
         WHERE {SQLITE_BLOB_FILTER} ORDER BY time_created ASC"
    ))?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let data: String = row.get(2)?;
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&data) else {
            continue;
        };
        let tokens = value.get("tokens");
        let time = value.get("time");
        out.push(Usage {
            session_id: row.get(0)?,
            fallback_ts: row.get(1)?,
            role: value.get("role").and_then(|v| v.as_str()).map(String::from),
            input: tokens.and_then(|t| t.get("input")).and_then(|v| v.as_i64()),
            output: tokens
                .and_then(|t| t.get("output"))
                .and_then(|v| v.as_i64()),
            cache_read: tokens
                .and_then(|t| t.get("cache"))
                .and_then(|c| c.get("read"))
                .and_then(|v| v.as_i64()),
            cache_write: tokens
                .and_then(|t| t.get("cache"))
                .and_then(|c| c.get("write"))
                .and_then(|v| v.as_i64()),
            model: value
                .get("modelID")
                .and_then(|v| v.as_str())
                .map(String::from),
            completed_ms: time
                .and_then(|t| t.get("completed"))
                .and_then(|v| v.as_i64()),
            created_ms: time.and_then(|t| t.get("created")).and_then(|v| v.as_i64()),
        });
    }
    Ok(out)
}

/// Public path accessor: the search index reads conversations from the same
/// database the token collector reads.
pub fn db_path() -> PathBuf {
    get_mimo_db_path()
}

/// Candidate log locations, for `tokenbuddy doctor`: presence is optional,
/// the doctor reports what exists and what does not.
pub fn log_paths() -> Vec<PathBuf> {
    vec![get_mimo_db_path()]
}

fn get_mimo_db_path() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        if !xdg.is_empty() {
            return PathBuf::from(xdg).join("mimocode").join("mimocode.db");
        }
    }
    dirs::home_dir()
        .map(|h| h.join(".local/share/mimocode/mimocode.db"))
        .unwrap_or_else(|| PathBuf::from(".local/share/mimocode/mimocode.db"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two extraction paths must agree exactly. The SQL projection is an
    /// optimization, never a different reading of the same rows: if it ever
    /// disagrees with the blob walk, every mimo number on the dashboard is
    /// quietly wrong.
    #[test]
    fn json_extract_path_matches_the_blob_path() {
        let dir = std::env::temp_dir().join(format!("tokenbuddy_mimo_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("mimocode.db");

        // The shapes that matter: a plain assistant turn, one with cache
        // reads and writes, a user turn, a turn with no tokens, one that fell
        // back to the row timestamp, a negative token, and a body far larger
        // than any real turn — the case that made the blob path expensive.
        let rows: Vec<(String, i64, String)> = vec![
            (
                "s1".into(),
                1_700_000_000_000,
                r#"{"role":"assistant","modelID":"mimo-x","tokens":{"input":10,"output":20,"cache":{"read":30,"write":40}},"time":{"created":1700000000000,"completed":1700000002500}}"#.into(),
            ),
            (
                "s2".into(),
                1_700_000_001_000,
                r#"{"role":"assistant","modelID":"mimo-y","tokens":{"input":1,"output":0,"cache":{"read":0,"write":0}},"time":{"created":1700000001000,"completed":1700000001000}}"#.into(),
            ),
            (
                "s3".into(),
                1_700_000_002_000,
                r#"{"role":"user","modelID":"mimo-z","tokens":{"input":99,"output":99}}"#.into(),
            ),
            (
                "s4".into(),
                1_700_000_003_000,
                r#"{"role":"assistant","modelID":"mimo-w","tokens":{"input":0,"output":0}}"#.into(),
            ),
            (
                "s5".into(),
                1_700_000_004_500,
                r#"{"role":"assistant","modelID":"mimo-v","tokens":{"input":7,"output":8}}"#.into(),
            ),
            (
                "s6".into(),
                1_700_000_005_000,
                format!(
                    r#"{{"role":"assistant","modelID":"mimo-big","tokens":{{"input":5,"output":6}},"filler":"{}"}}"#,
                    "x".repeat(900_000)
                ),
            ),
            // Malformed payload. The SQL path must skip this row exactly the
            // way the blob walk does — it must not fail the whole query and
            // take the entire source dark.
            (
                "s7".into(),
                1_700_000_006_000,
                r#"{"role":"assistant" BROKEN"#.into(),
            ),
        ];

        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);",
            )
            .unwrap();
            for (i, (sid, ts, data)) in rows.iter().enumerate() {
                conn.execute(
                    "INSERT INTO message VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![format!("m{i}"), sid, ts, data],
                )
                .unwrap();
            }
        }

        let conn = rusqlite::Connection::open(&path).unwrap();
        let fast = read_via_json_extract(&conn)
            .expect("json1 must be available, and one malformed row must not abort the query");
        let slow = read_via_blob(&conn).unwrap();
        assert_eq!(fast.len(), slow.len(), "row counts differ");
        assert_eq!(fast.len(), 6, "the malformed row is dropped by both paths");
        for (a, b) in fast.iter().zip(slow.iter()) {
            assert_eq!(a.session_id, b.session_id);
            assert_eq!(a.fallback_ts, b.fallback_ts);
            assert_eq!(a.role, b.role);
            assert_eq!(a.input, b.input, "input for {}", a.session_id);
            assert_eq!(a.output, b.output, "output for {}", a.session_id);
            assert_eq!(
                a.cache_read, b.cache_read,
                "cache_read for {}",
                a.session_id
            );
            assert_eq!(
                a.cache_write, b.cache_write,
                "cache_write for {}",
                a.session_id
            );
            assert_eq!(a.model, b.model, "model for {}", a.session_id);
            assert_eq!(
                a.completed_ms, b.completed_ms,
                "completed for {}",
                a.session_id
            );
            assert_eq!(a.created_ms, b.created_ms, "created for {}", a.session_id);
        }
        // And the record mapping itself, not just the extraction.
        let ra = to_records(fast);
        let rb = to_records(slow);
        assert_eq!(ra.len(), 4, "only assistant turns with tokens are kept");
        assert_eq!(ra.len(), rb.len());
        for (a, b) in ra.iter().zip(rb.iter()) {
            assert_eq!(a.model, b.model);
            assert_eq!(a.input_tokens, b.input_tokens);
            assert_eq!(a.output_tokens, b.output_tokens);
            assert_eq!(a.cache_read_tokens, b.cache_read_tokens);
            assert_eq!(a.cache_creation_tokens, b.cache_creation_tokens);
            assert_eq!(a.timestamp, b.timestamp);
            assert_eq!(a.duration_ms, b.duration_ms);
            assert_eq!(a.session_id, b.session_id);
        }
        // A large-but-readable body is a normal turn as far as the numbers go.
        let big = ra
            .iter()
            .find(|r| r.model == "mimo-big")
            .expect("big row kept");
        assert_eq!(big.input_tokens, 5);
        assert_eq!(big.output_tokens, 6);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The blob bound is a correctness claim, not just an optimization: it must
    /// exclude exactly the rows that cannot be billable assistant turns, count
    /// what it excluded, and leave everything below the bound untouched.
    #[test]
    fn oversized_rows_are_skipped_and_counted() {
        let dir = std::env::temp_dir().join(format!("tokenbuddy_mimo_big_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("mimocode.db");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);",
            )
            .unwrap();
            let filler = |n: usize| "x".repeat(n);
            let rows = vec![
                // Under the limit — an assistant turn, must be read.
                (
                    "small",
                    format!(
                        r#"{{"role":"assistant","modelID":"a","tokens":{{"input":3,"output":4}},"filler":"{}"}}"#,
                        filler(1000)
                    ),
                ),
                // Over the limit — a user turn with an inline screenshot. A
                // real one measured 19.5 MB; the shape is what matters here.
                (
                    "huge",
                    format!(
                        r#"{{"role":"user","modelID":"a","tokens":{{"input":3}},"filler":"{}"}}"#,
                        filler(SQLITE_BLOB_LIMIT as usize + 1000)
                    ),
                ),
            ];
            for (i, (id, data)) in rows.into_iter().enumerate() {
                conn.execute(
                    "INSERT INTO message VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![id, "s", 1_700_000_000_000i64 + i as i64, data],
                )
                .unwrap();
            }
        }

        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            crate::sqlite_oversized_rows(&conn, "message").unwrap(),
            1,
            "the oversized row must be counted"
        );
        let fast = read_via_json_extract(&conn).expect("json1 available");
        let slow = read_via_blob(&conn).unwrap();
        assert_eq!(fast.len(), 1, "only the readable row survives");
        assert_eq!(fast[0].model.as_deref(), Some("a"));
        assert_eq!(to_records(fast).len(), 1, "and it is the billable one");
        assert_eq!(slow.len(), 1, "the fallback path bounds the same way");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
