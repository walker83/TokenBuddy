pub mod claude;
pub mod opencode;
pub mod mimo;
pub mod zcode;
pub mod pi;
pub mod qoder;
pub mod workbuddy;
pub mod store;
pub mod context;
pub mod tools;

use chrono::Datelike;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Root data directory: `~/.tokenbuddy`. The historical name was `~/.ltc`;
/// the one-time rename below carries existing data across the rebrand so an
/// upgrade never starts from an empty store.
pub fn data_dir() -> PathBuf {
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
        "opus", "sonnet", "haiku", "glm", "kimi", "deepseek", "qwen", "gemini",
        "minimax", "mimo", "grok", "llama", "mistral", "ernie",
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
    /// Total wall-clock duration of the model call in milliseconds.
    /// `None` when the source log doesn't expose this (e.g. Claude jsonl).
    pub duration_ms: Option<u64>,
    /// Time-to-first-token in milliseconds. Only zcode populates this directly.
    pub ttft_ms: Option<u64>,
    /// Spend reported by the tool itself, in its own credit unit. Qoder masks
    /// token counts but gives exact credits, so cost is derived from this
    /// rather than the pricing table whenever it is non-zero.
    pub credits: f64,
    /// Stable id from the source log, used as the sync dedupe key. Sources
    /// whose token counts are masked would otherwise collide on the
    /// timestamp+input_tokens key the other collectors fall back to.
    pub record_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Source {
    Claude,
    OpenCode,
    Mimo,
    Zcode,
    Pi,
    Qoder,
    WorkBuddy,
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Source::Claude => "Claude",
            Source::OpenCode => "OpenCode",
            Source::Mimo => "Mimo",
            Source::Zcode => "Zcode",
            Source::Pi => "Pi",
            Source::Qoder => "Qoder",
            Source::WorkBuddy => "WorkBuddy",
        })
    }
}

impl Source {
    pub fn as_str(&self) -> &str {
        match self {
            Source::Claude => "claude",
            Source::OpenCode => "opencode",
            Source::Mimo => "mimo",
            Source::Zcode => "zcode",
            Source::Pi => "pi",
            Source::Qoder => "qoder",
            Source::WorkBuddy => "workbuddy",
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
