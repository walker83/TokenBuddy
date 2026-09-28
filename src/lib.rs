pub mod claude;
pub mod codex;
pub mod context;
pub mod crypt;
pub mod doctor;
pub mod export;
pub mod fleet;
pub mod gemini;
pub mod hermes;
pub mod mcp;
pub mod mimo;
pub mod minimax;
pub mod opencode;
pub mod pi;
pub mod qoder;
pub mod qwen;
pub mod report;
pub mod store;
pub mod workbuddy;
pub mod zcode;

use chrono::Datelike;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::SystemTime;

/// Root data directory: `~/.tokenbuddy`. The historical name was `~/.ltc`;
/// the one-time rename below carries existing data across the rebrand so an
/// upgrade never starts from an empty store.
pub fn data_dir() -> PathBuf {
    // TOKENBUDDY_HOME relocates the whole store (tests, multi-instance, MCP
    // fixtures). Migration below only runs for the default location.
    if let Ok(home) = std::env::var("TOKENBUDDY_HOME") {
        if !home.is_empty() {
            return PathBuf::from(home);
        }
    }
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    let dir = home.join(".tokenbuddy");
    if !dir.exists() {
        let old = home.join(".ltc");
        if old.is_dir() {
            if let Err(e) = std::fs::rename(&old, &dir) {
                eprintln!(
                    "[TokenBuddy] cannot migrate {} -> {}: {e}",
                    old.display(),
                    dir.display()
                );
            }
        }
    }
    dir
}

/// All day/week/month bucketing and every `timeRange` filter uses China
/// Standard Time. Source logs store true UTC epoch seconds, so the offset is
/// applied at read time; nothing in the parquet is timezone-shifted.
/// China has observed no DST since 1991, hence a fixed offset.
pub const CN_OFFSET_SECS: i64 = 8 * 3600;

fn cn_tz() -> chrono::FixedOffset {
    chrono::FixedOffset::east_opt(CN_OFFSET_SECS as i32).expect("UTC+8 is a valid offset")
}

/// Epoch seconds of China-local midnight, `days_ago` days back from today.
pub fn cn_midnight(days_ago: i64) -> i64 {
    let tz = cn_tz();
    let naive = chrono::Utc::now()
        .with_timezone(&tz)
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight is always valid");
    naive
        .and_local_timezone(tz)
        .single()
        .expect("China has no DST, so local midnight is unambiguous")
        .timestamp()
        - days_ago * 86_400
}

/// Epoch seconds of 00:00 on the first day of the current China month.
pub fn cn_month_start() -> i64 {
    let tz = cn_tz();
    let first = chrono::Utc::now()
        .with_timezone(&tz)
        .date_naive()
        .with_day(1)
        .expect("every month has a day 1")
        .and_hms_opt(0, 0, 0)
        .expect("midnight is always valid");
    first
        .and_local_timezone(tz)
        .single()
        .expect("China has no DST, so local midnight is unambiguous")
        .timestamp()
}

/// `%Y-%m-%d` calendar day of an epoch reading, in China time.
pub fn cn_day_label(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.with_timezone(&cn_tz()).format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

/// Current instant as epoch seconds.
pub fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Last-modified time of a log file, in epoch seconds.
///
/// Collectors use this as a last resort when a record carries no parseable
/// timestamp. Defaulting to 0 instead would file the row under 1970, which
/// sits outside every `timeRange` filter — the record gets imported and
/// counted in the sync result, yet is visible nowhere in the dashboard.
pub fn file_mtime(path: &std::path::Path) -> Option<i64> {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
}

/// Normalize a model name into a comparable family label, e.g.
/// `claude-sonnet-4-5-20250929` → `sonnet`, `glm-4.6` → `glm`,
/// `Qwen/Qwen3-Coder-480B` → `qwen`.
pub fn model_family(model: &str) -> String {
    let m = model.to_lowercase();
    let known: &[&str] = &[
        "opus", "sonnet", "haiku", "glm", "kimi", "deepseek", "qwen", "gemini", "minimax", "mimo",
        "grok", "llama", "mistral", "ernie",
    ];
    for k in known {
        if m.contains(*k) {
            return k.to_string();
        }
    }
    // gpt-5-mini → gpt-5 ; o4-mini → o4 ; claude-3-7-sonnet is caught above.
    for prefix in ["gpt-5", "gpt-4", "gpt-3", "o3", "o4"] {
        if m.contains(prefix) {
            return prefix.to_string();
        }
    }
    if m.contains("gpt") {
        return "gpt".to_string();
    }
    // Fall back to the segment before the first digit, trimmed.
    let cut = m.find(|c: char| c.is_ascii_digit()).unwrap_or(m.len());
    let fam = m[..cut].trim_matches(|c: char| c == '-' || c == '/' || c == '_' || c == '.');
    if fam.is_empty() {
        m.chars().take(24).collect()
    } else {
        fam.chars().take(24).collect()
    }
}

/// Upper bound on a SQLite-backed collector's `data` blob, in bytes.
///
/// Measured on a real mimo database: assistant turns are at most 1.7 KB, while
/// *user* turns carry inline `data:image` screenshots — one measured 19.5 MB,
/// and user rows hold 99% of the blob mass in a 262 MB file. Every collector
/// already discards non-assistant rows, so that mass was being read for
/// nothing.
///
/// SQLite must materialize a value before `json_extract` can walk it, and
/// filtering on the extracted role does not help: measured, the peak is
/// unchanged (73 MB either way). It does honour a `length()` bound without
/// assembling the value — with this filter the same scan peaks at the
/// "no extraction at all" floor (46 MB, against 45.6 MB for a bare scan).
///
/// 1 MB is ~600× the largest real assistant turn, so nothing billable is lost.
/// Rows above it are counted and reported rather than dropped in silence.
pub const SQLITE_BLOB_LIMIT: i64 = 1_000_000;

/// The `WHERE` bound that keeps [`SQLITE_BLOB_LIMIT`] honest in a query.
///
/// The order of the two bounds is the whole trick, and it is measured, not
/// guessed:
///
/// * `LENGTH` first. It is cheap — a bare `SELECT LENGTH(data)` over the whole
///   table already peaks at the no-extraction floor (45.6 MB) — and it rejects
///   the multi-megabyte rows before anything tries to look inside them.
/// * `json_valid` second. SQLite's JSON functions *error* on malformed input
///   rather than yielding NULL, so without it a single corrupt row aborts the
///   whole query and silently takes the entire source dark, where the blob walk
///   it replaced merely skipped that row. It sits before every `json_extract`
///   so an invalid row is rejected first; SQLite evaluates `AND` terms left to
///   right and short-circuits.
///
/// Swapping them costs 24 MB: `json_valid` alone must parse a 19.5 MB body to
/// answer, so it has to materialize the very value the length bound exists to
/// skip.
pub const SQLITE_BLOB_FILTER: &str = "LENGTH(data) < 1000000 AND json_valid(data)";

/// How many rows the blob bound skipped, for the caller to report. Uses only
/// `length()`, so it costs a scan that never assembles a value.
pub fn sqlite_oversized_rows(conn: &rusqlite::Connection, table: &str) -> rusqlite::Result<u64> {
    let sql = format!(
        "SELECT COUNT(*) FROM {table} WHERE LENGTH(data) >= {}",
        SQLITE_BLOB_LIMIT
    );
    let n: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
    Ok(n.max(0) as u64)
}

/// Per-source on-disk cache shared by the collectors: log path → (mtime when
/// parsed, records). Unchanged files skip re-parsing on the next sync.
pub(crate) type FileCacheMap = HashMap<String, (SystemTime, Vec<TokenRecord>)>;

#[derive(Debug, Clone)]
pub struct TokenRecord {
    pub source: Source,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub timestamp: i64,
    pub session_id: Option<String>,
    /// Project attribution when the source log provides one (cwd, workspace
    /// directory, project slug). Empty string = the source does not say —
    /// pivot tables must not guess (the WakaTime lesson: bad attribution is
    /// worse than none).
    pub project: String,
    /// Total wall-clock duration of the model call in milliseconds.
    /// `None` when the source log doesn't expose this (e.g. Claude jsonl).
    pub duration_ms: Option<u64>,
    /// Time-to-first-token in milliseconds. Only zcode populates this directly.
    pub ttft_ms: Option<u64>,
    /// Spend reported by the tool itself, in its own credit unit. Qoder masks
    /// token counts but gives exact credits, so it is recorded as-is and never
    /// converted to currency.
    pub credits: f64,
    /// Fraction of the model context window one request consumed, as reported
    /// by the source (Qoder's `context_usage_ratio`, a number in 0..=1).
    /// `0.0` means the source does not report it. For masked sources this is
    /// the only token-scale signal that survives, so it is recorded raw and
    /// never multiplied by an assumed window size.
    pub context_ratio: f64,
    /// Stable id from the source log, used as the sync dedupe key. Sources
    /// whose token counts are masked would otherwise collide on the
    /// timestamp+input_tokens key the other collectors fall back to.
    pub record_id: Option<String>,
    /// In-memory cross-file merge identity (never written to parquet, never
    /// used for store keys — those stay frozen). A collector whose messages
    /// can appear in several files (resume copies, sidechain replays) sets
    /// this so its own collect pass can fold the copies into one billable
    /// row regardless of which file they sat in.
    pub merge_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Source {
    Claude,
    Codex,
    Gemini,
    Qwen,
    OpenCode,
    Mimo,
    Zcode,
    Pi,
    Qoder,
    WorkBuddy,
    MiniMax,
    Hermes,
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Source::Claude => "Claude",
            Source::Codex => "Codex",
            Source::Gemini => "Gemini",
            Source::Qwen => "Qwen",
            Source::OpenCode => "OpenCode",
            Source::Mimo => "Mimo",
            Source::Zcode => "Zcode",
            Source::Pi => "Pi",
            Source::Qoder => "Qoder",
            Source::WorkBuddy => "WorkBuddy",
            Source::MiniMax => "MiniMax",
            Source::Hermes => "Hermes",
        })
    }
}

impl Source {
    pub fn as_str(&self) -> &str {
        match self {
            Source::Claude => "claude",
            Source::Codex => "codex",
            Source::Gemini => "gemini",
            Source::Qwen => "qwen",
            Source::OpenCode => "opencode",
            Source::Mimo => "mimo",
            Source::Zcode => "zcode",
            Source::Pi => "pi",
            Source::Qoder => "qoder",
            Source::WorkBuddy => "workbuddy",
            Source::MiniMax => "minimax",
            Source::Hermes => "hermes",
        }
    }
}

pub fn format_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{cn_day_label, cn_midnight, file_mtime, model_family, now_ts};

    #[test]
    fn family_normalization() {
        assert_eq!(model_family("claude-sonnet-4-5-20250929"), "sonnet");
        assert_eq!(model_family("glm-4.6"), "glm");
        assert_eq!(model_family("Qwen/Qwen3-Coder-480B-A35B"), "qwen");
        assert_eq!(model_family("kimi-k2-0905-preview"), "kimi");
        assert_eq!(model_family("gpt-5-mini"), "gpt-5");
    }

    #[test]
    fn cn_day_boundary_is_local_midnight() {
        // A CST midnight epoch must label as that CST day, not the UTC calendar
        // day eight hours earlier.
        let midnight = cn_midnight(0);
        let expected = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::hours(8))
            .expect("now + 8h is representable")
            .format("%Y-%m-%d")
            .to_string();
        assert_eq!(cn_day_label(midnight), expected);
        // cn_midnight steps in whole days, so yesterday is exactly 86400 back.
        assert_eq!(cn_midnight(1), midnight - 86_400);
        // One second before CST midnight still belongs to the previous day.
        assert_ne!(cn_day_label(midnight - 1), expected);
    }

    /// The collectors fall back to `file_mtime` when a record has no parseable
    /// timestamp, so it has to yield real epoch seconds and not panic on a
    /// missing file.
    #[test]
    fn file_mtime_is_epoch_seconds_and_none_when_absent() {
        let dir = std::env::temp_dir().join(format!("tokenbuddy-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir should be creatable");
        let path = dir.join("sample.jsonl");
        std::fs::write(&path, "{}").expect("temp file should be writable");

        let mtime = file_mtime(&path).expect("an existing file has an mtime");
        assert!(
            (mtime - now_ts()).abs() < 86_400,
            "mtime {mtime} is not near now {}",
            now_ts()
        );
        assert!(file_mtime(&dir.join("missing.jsonl")).is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
