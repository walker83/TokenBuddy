pub mod amp;
pub mod artifacts;
pub mod claude;
pub mod cline;
pub mod codex;
pub mod context;
pub mod crypt;
pub mod doctor;
pub mod export;
pub mod fleet;
#[cfg(feature = "gateway")]
pub mod gateway;
pub mod gemini;
pub mod hermes;
pub mod kimi;
pub mod mcp;
pub mod mimo;
pub mod minimax;
pub mod mirasim;
pub mod opencode;
pub mod pi;
pub mod qoder;
pub mod quota;
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

/// Boundaries of the digest comparison: `(cur_start, now, prev_start)`.
///
/// The previous window is shifted back by the *same span* the current one
/// covers, so the two are exactly equal. Deriving it as
/// `[today - 2*days, today - days)` left the current window longer by however
/// much of today has already elapsed, inflating every delta by roughly
/// `1/days` — at 7 days, about 13% of phantom growth. `cur_start` stays on a
/// China-local midnight so the daily buckets still align to whole days, and
/// the current window still runs right up to now. Lives in the lib so the
/// HTTP handler and the MCP digest tool share one definition (issue #27).
pub fn digest_windows(days: i64) -> (i64, i64, i64) {
    let now = now_ts();
    let cur_start = cn_midnight(0) - days * 86_400;
    let span = (now - cur_start).max(1);
    (cur_start, now, cur_start - span)
}

/// Epoch seconds of China-local midnight **on the day containing `ts`**.
///
/// [`cn_midnight`] answers "midnight of the current wall-clock day, N days
/// back", which is the right question for a dashboard being read right now.
/// Aggregators that take an explicit `now` (so tests can pin time) need the
/// other question: given a timestamp, where does its day start. Both live here
/// so day boundaries stay in one place — `timestamp + CN_OFFSET_SECS` is
/// floored, then shifted **back** into UTC epochs; forgetting the second
/// step returns "08:00 today" and silently drops the 00:00–08:00 traffic
/// from every "today" bucket (2026-10-06 凌晨实测炸出,白天跑测试恰好无感).
pub fn cn_midnight_of(ts: i64) -> i64 {
    let local = ts + CN_OFFSET_SECS;
    let floored = local - local.rem_euclid(86_400);
    floored - CN_OFFSET_SECS
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

/// R68 — normalize a tool name (spellings differ across agents:
/// `Bash`/`shell`/`execute`, `Edit`/`edit`, …) into a small category
/// vocabulary for the tool-mix view. Unknown tools are 其他, not dropped —
/// a new tool showing up IS signal.
pub fn tool_category(name: &str) -> &'static str {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "bash" | "shell" | "execute" | "command" => "命令",
        "edit" | "write" | "multiedit" | "patch" | "apply_patch" => "编辑",
        "read" | "view" | "read_file" | "open" => "读取",
        "webfetch" | "websearch" | "fetch" | "search_web" => "网络",
        "todowrite" | "task" | "todo" => "任务",
        "grep" | "glob" | "find" | "list" => "检索",
        _ => "其他",
    }
}

/// Current instant as epoch seconds.
pub fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Marker for caller-caused failures; travels in the anyhow context chain so
/// route arms can keep a single `Err => error_response` path. Lives here (not
/// in the binary) because library modules reject caller input too — an
/// artifact path that is not on the whitelist is a 400, not a broken server.
#[derive(Debug)]
pub struct BadRequest;

impl std::fmt::Display for BadRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bad request")
    }
}
impl std::error::Error for BadRequest {}

/// A 400-shaped error carrying a message the caller can act on.
pub fn client_error(msg: impl std::fmt::Display) -> anyhow::Error {
    anyhow::Error::new(BadRequest).context(msg.to_string())
}

/// Tests that relocate TOKENBUDDY_HOME all take this one lock. The env var
/// is process-global and `data_dir()` reads it live, so two test modules
/// each holding their *own* mutex would still stomp each other's data dirs
/// mid-test — the lock must be crate-wide, not per-module. Public (not
/// cfg(test)) so the integration tests in tests/ — a separate crate the
/// cfg(test) items are invisible to — can take the same lock.
pub static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Unique per-call scratch dir for tests. Naming by pid alone collides when
/// the OS recycles pids across CI runs (Android recycles aggressively, and a
/// runner that hosts a live TokenBuddy generates plenty) — a leftover
/// quota.jsonl from a previous run then poisons every assertion that counts
/// lines or keys. The nanos suffix makes the dir unique per call.
/// Not `#[cfg(test)]`: the bin's test harness links the lib as a normal
/// dependency, so cfg(test) items are invisible there (TEST_ENV_LOCK has a
/// bin-local twin for the same reason).
pub fn unique_test_dir(name: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!("tb_{name}_{}_{}", std::process::id(), nanos))
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
    /// R64 — true when this response came from a subagent/sidechain run
    /// rather than the main thread (Claude's `isSidechain`). Provenance
    /// only: dedupe folds a sidechain copy into its mainline twin exactly
    /// as before, so totals never change — this column just lets analytics
    /// split 主线 vs 子代理. False for every other source (unknown).
    pub sidechain: bool,
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
    /// How many upstream API calls this ledger row aggregates (issue #42).
    /// 1 for every per-call source; hermes writes the source's
    /// `api_call_count` because one of its rows spans a whole
    /// session×model×billing aggregate. Analytics count *calls*, not ledger
    /// rows — `avg_input_per_req` divides by this, not by 1.
    pub request_count: u64,
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
    /// Cline 任务日志(R83;Roo/Kilo 为其 fork,同格式不同安装位)。
    Cline,
    RooCode,
    Kilo,
    /// Kimi CLI wire.jsonl(R85;StatusUpdate 滚动累计,同 message_id
    /// 末值为准)。
    Kimi,
    /// Amp(Sourcegraph)usageLedger(R87;账本事件即权威计费)。
    Amp,
    /// 本机网关经手的请求(R108;meter 直接落盘 usage.jsonl,记的是
    /// 上游 usage 直报或显式标注的估算,工具侧落盘读不到的流量从此有账)。
    Gateway,
    /// Mirasim(issue #18;本地 ndjson 计账,`input` 是净新增,真实输入要
    /// 加上 cacheRead/cacheWrite——与 zcode 的减法口径相反,见 mirasim.rs
    /// 文件头)。
    Mirasim,
}

/// Every collector name, lowercase — the vocabulary of the per-source
/// switch (`disabled_sources` in quota.json) and of `/api/*?source=`.
pub const SOURCE_NAMES: &[&str] = &[
    "claude",
    "codex",
    "gemini",
    "qwen",
    "opencode",
    "mimo",
    "zcode",
    "pi",
    "qoder",
    "workbuddy",
    "minimax",
    "hermes",
    "cline",
    "roocode",
    "kilo",
    "kimi",
    "amp",
    "gateway",
    "mirasim",
];

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
            Source::Cline => "Cline",
            Source::RooCode => "Roo Code",
            Source::Kilo => "Kilo Code",
            Source::Kimi => "Kimi CLI",
            Source::Amp => "Amp",
            Source::Gateway => "Gateway",
            Source::Mirasim => "Mirasim",
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
            Source::Cline => "cline",
            Source::RooCode => "roocode",
            Source::Kilo => "kilo",
            Source::Kimi => "kimi",
            Source::Amp => "amp",
            Source::Gateway => "gateway",
            Source::Mirasim => "mirasim",
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
    use super::{
        cn_day_label, cn_midnight, cn_midnight_of, file_mtime, model_family, now_ts, tool_category,
    };

    /// R68:方言归一的六类目;未知工具进"其他"(新工具出现即信号)。
    #[test]
    fn tool_category_normalizes_dialects() {
        assert_eq!(tool_category("Bash"), "命令");
        assert_eq!(tool_category("shell"), "命令");
        assert_eq!(tool_category("execute"), "命令");
        assert_eq!(tool_category("Edit"), "编辑");
        assert_eq!(tool_category("write"), "编辑");
        assert_eq!(tool_category("MultiEdit"), "编辑");
        assert_eq!(tool_category("Read"), "读取");
        assert_eq!(tool_category("WebFetch"), "网络");
        assert_eq!(tool_category("websearch"), "网络");
        assert_eq!(tool_category("TodoWrite"), "任务");
        assert_eq!(tool_category("Grep"), "检索");
        assert_eq!(tool_category("mcp__node_repl__js"), "其他");
    }

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

    /// cn_midnight_of must agree with the chrono-based cn_midnight on "now",
    /// and a reading one second past CST midnight must land in the new day —
    /// the shifted-floor version that forgot to shift back answered
    /// "08:00 today" instead, so anything between 00:00 and 08:00 CST was
    /// bucketed into yesterday (only visible when tests run after midnight).
    #[test]
    fn cn_midnight_of_matches_cn_midnight_and_covers_early_morning() {
        let now = now_ts();
        assert_eq!(cn_midnight_of(now), cn_midnight(0));
        // 00:00:01 CST on 2026-10-06 = 2026-10-05 16:00:01 UTC, pinned so the
        // boundary is exercised every day of the year, not just at 00-08 CST.
        let just_past = 1_791_216_001;
        assert_eq!(cn_midnight_of(just_past), just_past - 1);
        assert!(just_past >= cn_midnight_of(just_past));
        // ...and 23:59:59 CST still floors to the same day's midnight.
        assert_eq!(cn_midnight_of(just_past + 86_398), just_past - 1);
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
