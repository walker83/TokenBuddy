//! Quota snapshots (套餐余量) — what the *provider* says about your plan
//! capacity, collected without TokenBuddy ever dialing out.
//!
//! Three ingestion paths, all honoring the zero-outbound positioning:
//!
//! 1. **File readers (built-in, always on).** Agents persist their provider's
//!    rate-limit state in local logs they already write. Codex CLI piggybacks
//!    a `rate_limits` object on every `token_count` event of its rollout
//!    files: `primary` (5h window) and `secondary` (7d), each with
//!    `used_percent`, `resets_at` (unix seconds) and `window_minutes`, plus a
//!    `plan_type` — verified against three independent implementations
//!    (splashboard's fetcher docs, codex-ratelimit, Codex Plus monitoring
//!    write-ups). Claude Code caches `utilization` + an OAuth tier in
//!    `~/.claude.json`. ZCode logs every billing/balance poll it makes to
//!    `~/.zcode/v2/logs` — `balances[]` with `total_units` / `used_units` and
//!    a `period_start` / `period_end` window, `plans[].name` for the plan.
//!    Reading files the agent already wrote costs nothing.
//! 2. **Command collectors (opt-in).** The user lists a local command in
//!    `~/.tokenbuddy/quota.json` whose stdout carries quota JSON — e.g.
//!    `mmx quota show --output json` for MiniMax plans. TokenBuddy runs it
//!    *only* on an explicit refresh (`tokenbuddy quota --refresh`,
//!    `POST /api/quota/refresh`), never on a timer. If that command dials
//!    out, it is the user's own CLI doing what the user asked, the same way
//!    Fleet only leaves the machine because the user configured it.
//!
//! Snapshots land in `quota.jsonl` (one JSON object per line, append-only,
//! compacted when it grows past 2 MiB). Codex file reads are live at query
//! time — a dashboard load never rewrites the log; only explicit refreshes
//! append. Timezone discipline: `resets_at` is an absolute epoch second;
//! humans see a relative countdown, so no local-time formatting lives here.

use crate::codex;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// One provider-reported capacity reading.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QuotaSnapshot {
    /// Collector identity: `"codex"` for the file reader, the configured
    /// collector `name` for command ones.
    pub source: String,
    /// Plan or model the window applies to (`plan_type`, `model_name`, …).
    pub plan: String,
    /// Human window label derived from the provider's own window length —
    /// never hardcoded semantics (`"5h"`, `"7d"`, `"interval"`, `"weekly"`).
    pub window: String,
    /// Used share, 0–100+ (a spend limit can exceed 100).
    pub used_percent: f64,
    /// Unix seconds when the window resets, when the provider says.
    pub resets_at: Option<i64>,
    /// Unix seconds when TokenBuddy observed the reading.
    pub collected_at: i64,
    /// `"file"` (local log read) or `"command"` (opt-in collector run).
    /// String rather than an enum so the jsonl line round-trips with serde.
    pub origin: String,
}

impl QuotaSnapshot {
    /// Dedup/display identity: newest reading per key wins.
    fn key(&self) -> String {
        format!("{}\u{1f}{}\u{1f}{}", self.source, self.plan, self.window)
    }
}

/// Label a window length in minutes the way a human would say it. The
/// provider owns the semantics (don't hardcode "primary == 5h"); we only
/// render the duration it reports.
pub(crate) fn window_label(window_minutes: Option<f64>) -> String {
    match window_minutes {
        None => "window".into(),
        Some(m) if m <= 0.0 => "window".into(),
        Some(m) if m < 60.0 => format!("{}m", m as i64),
        Some(m) if (m % 1440.0).abs() < f64::EPSILON => {
            let d = (m / 1440.0) as i64;
            if d == 7 {
                "7d".into()
            } else if d == 1 {
                "1d".into()
            } else {
                format!("{d}d")
            }
        }
        Some(m) => {
            let h = m / 60.0;
            if (h.fract()).abs() < f64::EPSILON {
                format!("{}h", h as i64)
            } else {
                format!("{h:.1}h")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Codex rollout file reader
// ---------------------------------------------------------------------------

/// Live-read the newest provider rate-limit state Codex wrote locally. Scans
/// the newest rollout files first (mtime order, capped — old sessions cannot
/// hold fresher state). Lines stream forward with constant memory — a rollout
/// can be tens of MB and the memory discipline forbids slurping one — and
/// the *last* `rate_limits` in a file wins; across newest-first files the
/// first file to mention a window wins. `collected_at` is the source file's
/// mtime, not the read time: a rate-limit line from last week must present
/// itself as last week's. Missing files, absent `rate_limits`
/// (non-subscription Codex, older CLI) and poison lines all degrade to
/// "nothing new here", never an error.
pub fn codex_snapshots() -> Vec<QuotaSnapshot> {
    let mut files: Vec<(std::time::SystemTime, PathBuf)> = codex::log_paths()
        .iter()
        .flat_map(|root| newest_rollouts(root))
        .collect();
    files.sort_by_key(|a| std::cmp::Reverse(a.0));
    files.truncate(24);

    let mut best: BTreeMap<String, QuotaSnapshot> = BTreeMap::new();
    for (mtime, path) in files {
        let Ok(file) = std::fs::File::open(&path) else {
            continue;
        };
        let collected_at = mtime
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let mut in_file: BTreeMap<String, QuotaSnapshot> = BTreeMap::new();
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            if let Some(snapshots) = rate_limits_from_line(&line, collected_at) {
                for s in snapshots {
                    in_file.insert(s.key(), s);
                }
            }
        }
        for (key, snap) in in_file {
            best.entry(key).or_insert(snap);
        }
        // Files are newest-first: once a file carried rate-limit state, no
        // older file can beat what we already hold — except for a window the
        // newer files never mentioned, so keep walking for those.
        if best.len() >= 2 {
            break;
        }
    }
    best.into_values().collect()
}

/// Newest-first rollout file list under one sessions root.
fn newest_rollouts(root: &std::path::Path) -> Vec<(std::time::SystemTime, PathBuf)> {
    fn walk(dir: &std::path::Path, depth: u8, out: &mut Vec<(std::time::SystemTime, PathBuf)>) {
        if depth > 5 {
            return;
        }
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, depth + 1, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                let mtime = entry
                    .metadata()
                    .and_then(|m| m.modified())
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                out.push((mtime, path));
            }
        }
    }
    let mut out = Vec::new();
    if root.is_dir() {
        walk(root, 0, &mut out);
    }
    out
}

/// Extract quota snapshots from one rollout JSONL line, if it carries a
/// `token_count` event with `rate_limits`. Poison lines return `None`.
pub(crate) fn rate_limits_from_line(line: &str, now: i64) -> Option<Vec<QuotaSnapshot>> {
    let value: serde_json::Value = serde_json::from_str(line.trim()).ok()?;
    if value.get("type").and_then(|v| v.as_str()) != Some("event_msg") {
        return None;
    }
    let payload = value.get("payload")?;
    if payload.get("type").and_then(|v| v.as_str()) != Some("token_count") {
        return None;
    }
    let limits = payload.get("rate_limits")?;
    if !limits.is_object() {
        return None;
    }
    let plan = limits
        .get("plan_type")
        .and_then(|v| v.as_str())
        .unwrap_or("plan")
        .to_string();

    let mut out = Vec::new();
    for key in ["primary", "secondary"] {
        let Some(w) = limits.get(key) else { continue };
        let Some(used) = w.get("used_percent").and_then(|v| v.as_f64()) else {
            continue;
        };
        if !used.is_finite() {
            continue;
        }
        out.push(QuotaSnapshot {
            source: "codex".into(),
            plan: plan.clone(),
            window: window_label(w.get("window_minutes").and_then(|v| v.as_f64())),
            used_percent: used,
            resets_at: w.get("resets_at").and_then(|v| v.as_i64()),
            collected_at: now,
            origin: "file".to_string(),
        });
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

// ---------------------------------------------------------------------------
// Claude Code `.claude.json` cache reader
// ---------------------------------------------------------------------------

/// Claude Code caches the rate-limit response it receives into `.claude.json`
/// (keys verified against cc-switch's quota.js, which reads the same cache
/// instead of spending quota on API calls):
///
/// ```json
/// { "utilization": {
///     "five_hour": { "utilization": 23.5, "resets_at": "2026-09-29T12:00:00Z" },
///     "seven_day": { "utilization": 41.2, "resets_at": "..." },
///     "extra_usage": { "is_enabled": false, ... }
///   },
///   "oauthAccount": { "userRateLimitTier": "default_claude_max_5x", ... } }
/// ```
///
/// Differences from Codex that matter: percent lives in `utilization`,
/// `resets_at` is an ISO-8601 *string*, and a window past its reset is
/// **dropped** — the server already granted a fresh allowance the cache never
/// saw, so the stale percent would overstate usage (cc-switch's "expired"
/// semantics). The plan name comes from the OAuth account's limit tier.
pub fn claude_cache_paths() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(dir) = std::env::var("CLAUDE_CONFIG_DIR") {
        if !dir.trim().is_empty() {
            out.push(PathBuf::from(dir.trim()).join(".claude.json"));
        }
    }
    if let Some(home) = dirs::home_dir() {
        out.push(home.join(".claude.json"));
    }
    out
}

pub fn claude_snapshots() -> Vec<QuotaSnapshot> {
    for path in claude_cache_paths() {
        if !path.is_file() {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let collected_at = crate::file_mtime(&path).unwrap_or(0);
        return claude_snapshots_from(&text, collected_at);
    }
    Vec::new()
}

/// Parse one cache payload. The first existing candidate file wins — Claude
/// Code keeps exactly one live config per config dir.
pub(crate) fn claude_snapshots_from(text: &str, collected_at: i64) -> Vec<QuotaSnapshot> {
    let value: serde_json::Value = match serde_json::from_str(text.trim()) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let Some(utilization) = value.get("utilization") else {
        return Vec::new();
    };
    if !utilization.is_object() {
        return Vec::new();
    }
    let plan = value
        .get("oauthAccount")
        .and_then(|a| {
            a.get("userRateLimitTier")
                .or_else(|| a.get("seatTier"))
                .and_then(|t| t.as_str())
        })
        .unwrap_or("plan")
        .to_string();
    let now = crate::now_ts();

    let mut out = Vec::new();
    for (key, label) in [("five_hour", "5h"), ("seven_day", "7d")] {
        let Some(w) = utilization.get(key) else {
            continue;
        };
        let Some(used) = w.get("utilization").and_then(|v| v.as_f64()) else {
            continue;
        };
        if !used.is_finite() {
            continue;
        }
        let resets_at = w
            .get("resets_at")
            .and_then(|v| v.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.timestamp());
        // Past its reset the cache describes an allowance the server already
        // replaced — drop it rather than show a stale percent (cc-switch
        // semantics). A missing/unparseable reset time stays visible but the
        // view marks it "重置时间未知".
        if let Some(at) = resets_at {
            if at <= now {
                continue;
            }
        }
        out.push(QuotaSnapshot {
            source: "claude".into(),
            plan: plan.clone(),
            window: label.into(),
            used_percent: used,
            resets_at,
            collected_at,
            origin: "file".to_string(),
        });
    }
    out
}

// ---------------------------------------------------------------------------
// ZCode billing log reader
// ---------------------------------------------------------------------------

/// Marker ZCode prefixes onto the billing/balance response it logs. The JSON
/// body follows it verbatim on the same line.
const ZCODE_BALANCE_MARKER: &str = "billing/balance 请求完成";

/// Roots that may hold ZCode's day logs. `ZCODE_CONFIG_DIR` wins (same
/// override the ZCode token collector honors, so tests and multi-instance
/// setups stay isolated); otherwise two layouts are probed because the v2 app
/// moved them (`~/.zcode/v2/logs`, and `~/.zcode/logs` on older installs).
fn zcode_log_roots() -> Vec<PathBuf> {
    if let Ok(custom) = std::env::var("ZCODE_CONFIG_DIR") {
        let base = PathBuf::from(custom.trim());
        if !custom.trim().is_empty() {
            return vec![base.join("v2").join("logs"), base.join("logs")];
        }
    }
    let home = dirs::home_dir().unwrap_or_default();
    let base = home.join(".zcode");
    vec![base.join("v2").join("logs"), base.join("logs")]
}

/// Newest-first list of ZCode's local log files. Only `.log` files; capped at
/// a week of daily files — a balance reading older than that has certainly
/// been superseded.
fn zcode_log_files() -> Vec<PathBuf> {
    fn day_logs(dir: &std::path::Path) -> Vec<PathBuf> {
        if !dir.is_dir() {
            return Vec::new();
        }
        let mut out: Vec<PathBuf> = Vec::new();
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("log") {
                out.push(path);
            }
        }
        out
    }
    let mut files: Vec<PathBuf> = zcode_log_roots().iter().flat_map(|r| day_logs(r)).collect();
    files.sort_by_key(|p| std::cmp::Reverse(crate::file_mtime(p).unwrap_or(0)));
    files.dedup();
    files.truncate(7);
    files
}

/// Live-read the freshest ZCode plan balance from the logs ZCode already
/// wrote. ZCode makes the `billing/balance` call itself and logs the response;
/// TokenBuddy never dials out — this is the same "read what the agent already
/// persisted" bargain as the Codex and Claude readers, and it is why a ZCode
/// user gets real numbers with zero configuration.
///
/// Per `(source, plan, window)` the newest `collected_at` wins. A window whose
/// `period_end` is already past is dropped rather than shown: ZCode granted a
/// fresh period and logged a newer line we may not have parsed, so the stale
/// percent would understate usage. Missing logs, lines without a balance, and
/// zero-size grants all degrade to "nothing new here", never an error.
pub fn zcode_snapshots() -> Vec<QuotaSnapshot> {
    let now = crate::now_ts();
    let mut best: BTreeMap<String, QuotaSnapshot> = BTreeMap::new();
    for path in zcode_log_files() {
        let Ok(file) = std::fs::File::open(&path) else {
            continue;
        };
        let mtime = crate::file_mtime(&path).unwrap_or(0);
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            if let Some(snap) = zcode_snapshot_from_line(&line, now, mtime) {
                match best.get_mut(&snap.key()) {
                    Some(cur) if snap.collected_at > cur.collected_at => *cur = snap,
                    Some(_) => {}
                    None => {
                        best.insert(snap.key(), snap);
                    }
                }
            }
        }
    }
    best.into_values().collect()
}

/// Pull the balance JSON out of one log line and turn it into a snapshot.
/// `file_mtime` is only the fallback `collected_at`; the payload's own
/// `server_time` (when ZCode made the poll) is what we prefer.
pub(crate) fn zcode_snapshot_from_line(
    line: &str,
    now: i64,
    file_mtime: i64,
) -> Option<QuotaSnapshot> {
    let after = line.split_once(ZCODE_BALANCE_MARKER)?.1;
    let json_source = after.trim_start();
    if !json_source.starts_with('{') {
        return None;
    }
    // Scan from the opening brace: the object is nested, so stopping at the
    // first *inner* `}` would hand the parser a fragment with no balances.
    let end = balanced_object_end(json_source)?;
    let value: serde_json::Value = serde_json::from_str(&json_source[..=end]).ok()?;
    zcode_snapshot_from_value(&value, now, file_mtime)
}

/// Byte index of the `}` closing the object that starts at index 0. String
/// literals are tracked so a brace inside `"…"` can't end the object early.
fn balanced_object_end(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_str = false;
    let mut escaped = false;
    for (i, c) in s.char_indices() {
        if in_str {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_str = false;
            }
            continue;
        }
        match c {
            '"' => in_str = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Turn one billing/balance payload into a single snapshot. The response
/// nests the useful half under `payload.data`; a future ZCode that logs the
/// bare response still parses via the flat fallback.
pub(crate) fn zcode_snapshot_from_value(
    value: &serde_json::Value,
    now: i64,
    file_mtime: i64,
) -> Option<QuotaSnapshot> {
    let data = value
        .get("payload")
        .and_then(|p| p.get("data"))
        .unwrap_or(value);
    let balances = data.get("balances").and_then(|b| b.as_array())?;
    let plan = data
        .get("plans")
        .and_then(|p| p.as_array())
        .and_then(|plans| plans.iter().find_map(|p| p.get("name")))
        .and_then(|n| n.as_str())
        .unwrap_or("plan")
        .to_string();
    let collected_at = data
        .get("server_time")
        .and_then(|t| t.as_i64())
        .unwrap_or(file_mtime);

    // One plan can hold several per-model buckets; the binding window is the
    // most drained of them, so pick the highest used share.
    let mut best: Option<QuotaSnapshot> = None;
    for b in balances {
        let (Some(total), Some(used)) = (
            b.get("total_units").and_then(|v| v.as_f64()),
            b.get("used_units").and_then(|v| v.as_f64()),
        ) else {
            continue;
        };
        if !total.is_finite() || !used.is_finite() || total <= 0.0 {
            continue;
        }
        let used_percent = (used / total * 100.0).clamp(0.0, 100.0);
        // 上游时间戳口径自适应（cc-switch 同类教训：成员接口混发秒/毫秒，
        // 按量级归一——`> 1e12` 只能是毫秒）。归一后再做过期丢弃，否则一条
        // 毫秒值会让过期窗口被当成"还有 5 万年"永远展示。
        let norm_ts = |v: i64| if v > 1_000_000_000_000 { v / 1000 } else { v };
        let period_end = b.get("period_end").and_then(|v| v.as_i64()).map(norm_ts);
        if let Some(at) = period_end {
            if at <= now {
                continue;
            }
        }
        let period_start = b.get("period_start").and_then(|v| v.as_i64()).map(norm_ts);
        let window_minutes = match (period_start, period_end) {
            (Some(start), Some(end)) => Some((end - start) as f64 / 60.0),
            _ => None,
        };
        let candidate = QuotaSnapshot {
            source: "zcode".into(),
            plan: plan.clone(),
            window: window_label(window_minutes),
            used_percent,
            resets_at: period_end,
            collected_at,
            origin: "file".to_string(),
        };
        // `Option::is_none_or` would read nicer but is stable only since 1.82
        // while this crate's MSRV is 1.75 (clippy flags it as incompatible_msrv),
        // and `map_or(true, ..)` is what newer clippy nudges back to is_none_or.
        // A match is also the form the neighbouring collectors already use.
        let replace = match best.as_ref() {
            Some(current) => candidate.used_percent >= current.used_percent,
            None => true,
        };
        if replace {
            best = Some(candidate);
        }
    }
    best
}

// ---------------------------------------------------------------------------
// Opt-in command collectors
// ---------------------------------------------------------------------------

/// `~/.tokenbuddy/quota.json`. Absent file = feature off, zero processes run.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct QuotaConfig {
    #[serde(default)]
    pub collectors: Vec<CollectorConfig>,
    /// Collectors (token sources) the user switched off (R102): a sync does
    /// not walk their logs at all, and the context index drops them too.
    /// Absent/empty = every source on. Data already imported stays in the
    /// ledger — off means "stop collecting", not "delete".
    #[serde(default)]
    pub disabled_sources: Vec<String>,
    /// Optional threshold alert: when any window's usage crosses the
    /// threshold, POST a short message to the user's own webhook (Feishu
    /// bot, LAN ntfy, …). Opt-in like everything that leaves the machine —
    /// and HTTP-only: this build deliberately ships no TLS stack.
    #[serde(default)]
    pub alert: Option<AlertConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlertConfig {
    /// HTTP webhook endpoint (no TLS in this build by design).
    pub webhook_url: String,
    /// Used-percent trigger line (default 80).
    #[serde(default = "default_alert_threshold")]
    pub threshold_percent: f64,
    /// `"generic"` (default, `{"text": …}`) or `"feishu"` (custom bot
    /// `{"msg_type":"text","content":{"text":…}}`).
    #[serde(default)]
    pub format: String,
    /// Minimum hours between two alerts for the same window (default 12).
    #[serde(default = "default_alert_cooldown")]
    pub cooldown_hours: i64,
    /// Also fire the rollover notice (窗口临近滚动且余量足——「现在用掉
    /// 不浪费」) to the same webhook. Default off; the dashboard banner
    /// covers people who do not want pushes.
    #[serde(default)]
    pub rollover: bool,
    /// Push yesterday's report (markdown digest) once per day, on the first
    /// sync after CN midnight. Default off.
    #[serde(default)]
    pub daily_digest: bool,
    /// R82 — also push a runaway-agent notice when TODAY's running total
    /// already clears the same-weekday full-day median at the store's
    /// robust-z line. Same webhook, once per flagged date. Default off.
    #[serde(default)]
    pub anomaly: bool,
    /// R88 — push a "session went quiet" knock when a session that worked
    /// today has been silent for [`Self::idle_minutes`]. The local answer
    /// to "know the moment an agent needs you": no screen scraping, just
    /// the ledger's own timestamps. Once per session per day. Default off.
    #[serde(default)]
    pub session_idle: bool,
    /// Silence threshold for the idle knock (minutes, default 15).
    #[serde(default = "default_idle_minutes")]
    pub idle_minutes: u16,
}

fn default_idle_minutes() -> u16 {
    15
}

fn default_alert_threshold() -> f64 {
    80.0
}

fn default_alert_cooldown() -> i64 {
    12
}

impl AlertConfig {
    pub fn is_feishu(&self) -> bool {
        self.format == "feishu"
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CollectorConfig {
    /// Display + storage identity.
    pub name: String,
    /// `"command"` (default — run a local program) or `"ledger"` (count
    /// today's requests for a source against a known daily cap; the tier
    /// limit is public knowledge for some plans, e.g. Gemini CLI's
    /// 1000/day OAuth / 250/day free key — the provider never needs to be
    /// asked). Absent = command, so existing configs read unchanged.
    #[serde(default)]
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub command: String,
    /// Built-in stdout parser id (`command` kind). `"minimax"` today.
    #[serde(default)]
    pub parser: String,
    /// Per-run budget in seconds (default 15, capped 1–120).
    pub timeout_secs: Option<u64>,
    /// `ledger` kind: which source's request count to use.
    #[serde(default)]
    pub ledger_source: String,
    /// `ledger` kind: the tier's documented daily request cap.
    #[serde(default)]
    pub daily_request_limit: Option<u64>,
}

impl CollectorConfig {
    pub fn is_ledger(&self) -> bool {
        self.kind == "ledger"
    }
}

pub fn config_path() -> PathBuf {
    crate::data_dir().join("quota.json")
}

/// Parser ids this build can actually interpret. The config panel offers
/// exactly these; a config naming an unknown parser degrades to a recorded
/// per-collector error at run time (visible, not silent).
pub const PARSERS: &[&str] = &["minimax"];

// ---------------------------------------------------------------------------
// Vendor auto-import
// ---------------------------------------------------------------------------

/// A vendor the panel can wire up without the user hand-writing a command,
/// plus the one sentence that says where the number actually comes from. The
/// config panel renders this table verbatim, so "which vendors can I track"
/// stops being a question only answered by reading quota.json's schema.
///
/// Adding a vendor is a row here — never a dashboard edit. The empty command
/// table stays as the escape hatch for anything not in the roster.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Vendor {
    pub id: &'static str,
    pub name: &'static str,
    /// `file` — TokenBuddy reads a local file the tool already wrote; zero
    /// config, nothing to import. `command` — one `CollectorConfig` row,
    /// which 导入 writes for you. `ledger` — self-measured against a
    /// published daily cap, so the provider is never asked.
    pub kind: &'static str,
    /// The row 导入 writes. Empty for `file` vendors, which have nothing to
    /// configure.
    pub command: &'static str,
    pub parser: &'static str,
    pub ledger_source: &'static str,
    pub daily_limit: u64,
    /// In the user's words: why this number is trustworthy, and what (if
    /// anything) leaves the machine.
    pub how: &'static str,
}

pub const VENDORS: &[Vendor] = &[
    Vendor {
        id: "zcode",
        name: "ZCode",
        kind: "file",
        command: "",
        parser: "",
        ledger_source: "",
        daily_limit: 0,
        how: "读 ZCode 自己写进本机日志的账单余额（billing/balance）——零外呼，装好即生效，不用配置",
    },
    Vendor {
        id: "codex",
        name: "Codex",
        kind: "file",
        command: "",
        parser: "",
        ledger_source: "",
        daily_limit: 0,
        how: "实时读 Codex rollout 文件里的 rate_limits——零外呼，装好即生效，不用配置",
    },
    Vendor {
        id: "claude",
        name: "Claude",
        kind: "file",
        command: "",
        parser: "",
        ledger_source: "",
        daily_limit: 0,
        how: "读 ~/.claude.json 的 utilization 缓存——零外呼，装好即生效，不用配置",
    },
    Vendor {
        id: "minimax",
        name: "MiniMax",
        kind: "command",
        command: "mmx quota show --output json",
        parser: "minimax",
        ledger_source: "",
        daily_limit: 0,
        how:
            "运行你本机的 mmx CLI 取套餐余量——命令是你自己的，外不外呼由它决定，且只在显式刷新时跑",
    },
    Vendor {
        id: "gemini",
        name: "Gemini",
        kind: "ledger",
        command: "",
        parser: "",
        ledger_source: "gemini",
        daily_limit: 1000,
        how: "用账本里已记的请求数对公开日限（1000 次/天）自测比例——不跑命令，不问厂商",
    },
];

pub fn vendor(id: &str) -> Option<&'static Vendor> {
    VENDORS.iter().find(|v| v.id == id)
}

/// The `CollectorConfig` 导入 writes for this vendor. `file` vendors return
/// `None`: there is no row to write, which is the whole point of them.
pub fn vendor_collector(v: &Vendor) -> Option<CollectorConfig> {
    match v.kind {
        "command" => Some(CollectorConfig {
            name: v.id.to_string(),
            kind: "command".into(),
            command: v.command.to_string(),
            parser: v.parser.to_string(),
            timeout_secs: Some(30),
            ledger_source: String::new(),
            daily_request_limit: None,
        }),
        "ledger" => Some(CollectorConfig {
            name: v.id.to_string(),
            kind: "ledger".into(),
            command: String::new(),
            parser: String::new(),
            timeout_secs: None,
            ledger_source: v.ledger_source.to_string(),
            daily_request_limit: Some(v.daily_limit),
        }),
        _ => None,
    }
}

/// Is this vendor actually installed here? A cheap existence probe — a
/// config dir for `file` vendors, the binary on `PATH` for `command` ones.
/// Never a network call, and never a claim about the provider's account.
pub fn vendor_detected(v: &Vendor) -> bool {
    match v.kind {
        "file" => match v.id {
            "zcode" => zcode_log_roots().iter().any(|p| p.exists()),
            "codex" => codex::log_paths().iter().any(|p| p.exists()),
            "claude" => claude_cache_paths().iter().any(|p| p.exists()),
            _ => false,
        },
        "command" => {
            let program = v.command.split_whitespace().next().unwrap_or("");
            !program.is_empty() && program_in_path(program)
        }
        // A ledger row only counts our own imported requests, so it is always
        // available; "detected" would claim the user has Gemini installed.
        "ledger" => true,
        _ => false,
    }
}

fn program_in_path(program: &str) -> bool {
    let Ok(path) = std::env::var("PATH") else {
        return false;
    };
    path.split(':')
        .filter(|d| !d.is_empty())
        .any(|dir| PathBuf::from(dir).join(program).exists())
}

/// The panel's vendor roster, each entry carrying its collector row so the
/// browser renders 导入 straight from the same table Rust validates against.
pub fn vendor_status() -> Vec<serde_json::Value> {
    VENDORS
        .iter()
        .map(|v| {
            serde_json::json!({
                "id": v.id,
                "name": v.name,
                "kind": v.kind,
                "how": v.how,
                "detected": vendor_detected(v),
                "importable": vendor_collector(v).is_some(),
                "collector": vendor_collector(v),
            })
        })
        .collect()
}

/// 导入: add this vendor's collector to quota.json and write it, atomically.
/// Idempotent — a second press is reported, not duplicated, so the button can
/// stay enabled. The return is a human sentence the panel shows verbatim.
pub fn import_vendor(id: &str) -> Result<String> {
    let Some(v) = vendor(id) else {
        anyhow::bail!(
            "未知厂商:{id}(可选:{})",
            VENDORS.iter().map(|v| v.id).collect::<Vec<_>>().join(", ")
        );
    };
    let Some(mut collector) = vendor_collector(v) else {
        anyhow::bail!("{}({})不需要配置——{}", v.name, v.id, v.how);
    };
    let mut cfg = load_config();
    let same_name = cfg
        .collectors
        .iter()
        .position(|c| c.name.trim() == collector.name);
    if let Some(idx) = same_name {
        cfg.collectors[idx] = collector;
        save_config(&cfg)?;
        return Ok(format!(
            "{} 已更新（{}）",
            v.name,
            describe(&cfg.collectors[idx])
        ));
    }
    if collector.kind == "ledger" {
        collector.name = format!("{id}-day");
    }
    cfg.collectors.push(collector.clone());
    save_config(&cfg)?;
    Ok(format!(
        "{} 已导入（{}），点「测试运行」或显式刷新取一次数",
        v.name,
        describe(&collector)
    ))
}

fn describe(c: &CollectorConfig) -> String {
    if c.is_ledger() {
        format!(
            "账本自测 {} 对 {}/天",
            c.ledger_source,
            c.daily_request_limit.unwrap_or(0)
        )
    } else {
        format!("{} + {} 解析器", c.command, c.parser)
    }
}

pub fn validate_config(cfg: &QuotaConfig) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for c in &cfg.collectors {
        let name = c.name.trim();
        if name.is_empty() {
            anyhow::bail!("采集器 name 不能为空");
        }
        if !seen.insert(name.to_string()) {
            anyhow::bail!("采集器 name 重复:{name}");
        }
        if c.name.len() > 64 {
            anyhow::bail!("采集器 name 过长(>64):{}", c.name);
        }
        if c.is_ledger() {
            if c.ledger_source.trim().is_empty() {
                anyhow::bail!("ledger 采集器 {name} 需要 ledger_source");
            }
            if c.daily_request_limit.unwrap_or(0) == 0 {
                anyhow::bail!("ledger 采集器 {name} 需要 daily_request_limit > 0");
            }
            continue;
        }
        if c.command.trim().is_empty() {
            anyhow::bail!("采集器 {name} 的 command 不能为空");
        }
        if !PARSERS.contains(&c.parser.as_str()) {
            anyhow::bail!(
                "采集器 {name} 的 parser 无效:{}(可用:{})",
                c.parser,
                PARSERS.join(", ")
            );
        }
    }
    let known: std::collections::HashSet<&str> = crate::SOURCE_NAMES.iter().copied().collect();
    let mut seen_sources = std::collections::HashSet::new();
    for s in &cfg.disabled_sources {
        let name = s.trim();
        if !known.contains(name) {
            anyhow::bail!("停用源无效:{name}(可用:{})", crate::SOURCE_NAMES.join(", "));
        }
        if !seen_sources.insert(name) {
            anyhow::bail!("停用源重复:{name}");
        }
    }
    Ok(())
}

/// Atomically write quota.json. The file holds no secrets by design — but
/// commands may embed keys the user chose to inline, so match fleet.toml's
/// 0600 posture anyway.
pub fn save_config(cfg: &QuotaConfig) -> Result<()> {
    validate_config(cfg)?;
    let path = config_path();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let text = serde_json::to_string_pretty(cfg)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, text)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

pub fn load_config() -> QuotaConfig {
    load_config_strict().unwrap_or_default()
}

/// Whether a token source's collector should run. The one gate every
/// collection path (ledger sync, context sync, quota file readers) asks —
/// a disabled source must cost nothing per sync, which is the point of
/// the switch (R102). Malformed config reads as "everything on": the
/// strict variant surfaces the file problem in the panel instead.
pub fn source_enabled(name: &str) -> bool {
    !load_config()
        .disabled_sources
        .iter()
        .any(|s| s.trim() == name)
}

/// Strict variant for the config panel: a malformed file surfaces as an
/// error there instead of silently reading as "nothing configured".
pub fn load_config_strict() -> Result<QuotaConfig> {
    let text = match std::fs::read_to_string(config_path()) {
        Ok(t) => t,
        Err(_) if !config_path().exists() => return Ok(QuotaConfig::default()),
        Err(e) => return Err(e.into()),
    };
    Ok(serde_json::from_str(&text)?)
}

/// What one collector run produced, including how it failed — a collector
/// that errors must still be *visible* (doctor, dashboard), not silent.
#[derive(Debug, Clone, Serialize)]
pub struct CollectorOutcome {
    pub name: String,
    pub snapshots: Vec<QuotaSnapshot>,
    pub error: Option<String>,
    pub duration_ms: u64,
}

/// Run one collector: spawn without a shell, drain both pipes concurrently
/// (a child blocking on a full stdout pipe would otherwise never exit and
/// look like a hang), wait with a deadline, SIGKILL on expiry. A failing
/// collector is a recorded outcome, never a panic.
pub fn run_collector(cfg: &CollectorConfig) -> CollectorOutcome {
    let started = Instant::now();
    let now = crate::now_ts();
    let mk_err = |msg: String| CollectorOutcome {
        name: cfg.name.clone(),
        snapshots: Vec::new(),
        error: Some(msg),
        duration_ms: started.elapsed().as_millis() as u64,
    };

    let mut parts = cfg.command.split_whitespace();
    let Some(program) = parts.next() else {
        return mk_err("command 为空".into());
    };
    let timeout = Duration::from_secs(cfg.timeout_secs.unwrap_or(15).clamp(1, 120));
    let mut child = match Command::new(program)
        .args(parts)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return mk_err(format!("无法启动 {program}: {e}")),
    };

    // Drain pipes in their own threads: wait_with_output alone cannot enforce
    // a timeout, and polling without draining deadlocks a chatty child.
    let mut stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();
    let drain = |pipe: &mut Option<std::process::ChildStdout>| {
        let mut buf = Vec::new();
        if let Some(p) = pipe.as_mut() {
            let _ = std::io::Read::read_to_end(p, &mut buf);
        }
        buf
    };
    let out_handle = std::thread::spawn(move || drain(&mut stdout_pipe));
    let err_handle = std::thread::spawn(move || drain_stderr(&mut stderr_pipe));

    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => {
                if Instant::now() >= deadline {
                    kill_process(child.id());
                    let _ = child.wait();
                    break None;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return mk_err(format!("等待进程失败: {e}")),
        }
    };

    let Some(status) = status else {
        // Timed out: the handles end when the killed process's pipes close.
        let _ = out_handle.join();
        let _ = err_handle.join();
        return mk_err(format!("超时(>{timeout:?})已终止"));
    };
    let stdout = out_handle.join().unwrap_or_default();
    let _stderr = err_handle.join().unwrap_or_default();
    let duration_ms = started.elapsed().as_millis() as u64;

    if !status.success() {
        return CollectorOutcome {
            name: cfg.name.clone(),
            snapshots: Vec::new(),
            error: Some(format!("退出码非零: {status}")),
            duration_ms,
        };
    }
    let text = String::from_utf8_lossy(&stdout).to_string();
    let snapshots = parse_collector_output(&cfg.parser, &text, now);
    if snapshots.is_empty() {
        CollectorOutcome {
            name: cfg.name.clone(),
            snapshots,
            error: Some(format!(
                "解析得到 0 条(parser={}),stdout 前 200 字节: {:.200}",
                cfg.parser,
                text.trim()
            )),
            duration_ms,
        }
    } else {
        CollectorOutcome {
            name: cfg.name.clone(),
            snapshots,
            error: None,
            duration_ms,
        }
    }
}

/// Kill a child by pid: libc::kill on unix (libc is already a Fleet-crypto
/// dependency), `Child::kill` needs ownership we don't have here. The
/// project's platforms are macOS/Linux only.
fn kill_process(pid: u32) {
    #[cfg(unix)]
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
    }
}

/// stderr twin of the stdout drainer (separate fn for the distinct type).
fn drain_stderr(pipe: &mut Option<std::process::ChildStderr>) -> Vec<u8> {
    let mut buf = Vec::new();
    if let Some(p) = pipe.as_mut() {
        let _ = std::io::Read::read_to_end(p, &mut buf);
    }
    buf
}

fn parse_collector_output(parser: &str, text: &str, now: i64) -> Vec<QuotaSnapshot> {
    match parser {
        "minimax" => parse_minimax(text, now),
        other => {
            let _ = other;
            Vec::new()
        }
    }
}

/// Parser for `mmx quota show --output json`: `model_remains[]` entries carry
/// an interval window and a weekly window per model, both as *remaining*
/// percent plus millisecond epoch bounds. Missing pieces skip that window —
/// unknown is not zero.
pub(crate) fn parse_minimax(text: &str, now: i64) -> Vec<QuotaSnapshot> {
    let value: serde_json::Value = match serde_json::from_str(text.trim()) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let Some(entries) = value.get("model_remains").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries {
        let plan = entry
            .get("model_name")
            .and_then(|v| v.as_str())
            .unwrap_or("plan")
            .to_string();
        let interval_remaining = entry
            .get("current_interval_remaining_percent")
            .and_then(|v| v.as_f64());
        let interval_end = entry.get("end_time").and_then(|v| v.as_f64());
        let weekly_remaining = entry
            .get("current_weekly_remaining_percent")
            .and_then(|v| v.as_f64());
        let weekly_end = entry.get("weekly_end_time").and_then(|v| v.as_f64());
        if let (Some(remaining), Some(end_ms)) = (interval_remaining, interval_end) {
            if remaining.is_finite() && (0.0..=100.0).contains(&remaining) && end_ms.is_finite() {
                out.push(QuotaSnapshot {
                    source: String::new(), // filled by the caller
                    plan: plan.clone(),
                    window: "interval".into(),
                    used_percent: 100.0 - remaining,
                    resets_at: Some((end_ms / 1000.0) as i64),
                    collected_at: now,
                    origin: "command".to_string(),
                });
            }
        }
        if let (Some(remaining), Some(end_ms)) = (weekly_remaining, weekly_end) {
            if remaining.is_finite() && (0.0..=100.0).contains(&remaining) && end_ms.is_finite() {
                out.push(QuotaSnapshot {
                    source: String::new(),
                    plan,
                    window: "weekly".into(),
                    used_percent: 100.0 - remaining,
                    resets_at: Some((end_ms / 1000.0) as i64),
                    collected_at: now,
                    origin: "command".to_string(),
                });
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// quota.jsonl store
// ---------------------------------------------------------------------------

/// One persisted log line: either a snapshot or a collector error. Errors are
/// data too — "collector X last failed with Y at T" is what doctor shows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRecord {
    /// "snap" | "err" — String so the record deserializes owned (no borrow
    /// of the log line, which dies at end of read).
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<QuotaSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub collected_at: i64,
}

pub fn log_path() -> PathBuf {
    crate::data_dir().join("quota.jsonl")
}

const LOG_COMPACT_BYTES: u64 = 2 * 1024 * 1024;

/// Append refresh outcomes. Best-effort durability: a failed append returns
/// the error but callers decide whether it matters (a dashboard refresh does
/// not fail because history could not be recorded).
pub fn append_outcomes(outcomes: &[CollectorOutcome]) -> Result<()> {
    let now = crate::now_ts();
    let mut text = String::new();
    for outcome in outcomes {
        for snap in &outcome.snapshots {
            let mut s = snap.clone();
            s.source = outcome.name.clone();
            text.push_str(
                &serde_json::json!({
                    "kind": "snap",
                    "snapshot": s,
                    "collected_at": now,
                })
                .to_string(),
            );
            text.push('\n');
        }
        if let Some(err) = &outcome.error {
            text.push_str(
                &serde_json::json!({
                    "kind": "err",
                    "source": outcome.name,
                    "error": err,
                    "collected_at": now,
                })
                .to_string(),
            );
            text.push('\n');
        }
    }
    append_text(&text)
}

/// Sample the file readers (Codex rollout + Claude cache) into the history
/// log. Called at the end of a successful sync: the ledger write is the
/// natural sampling bell, and fleet auto-push makes it hourly for free. The
/// line's `collected_at` is the *sample* time (the drain curve's x-axis);
/// the snapshot inside keeps the source file's mtime (honest freshness).
/// Never lets a sampling failure break the sync that triggered it.
pub fn sample_file_readers() {
    // The switches gate quota probes too: a source turned off has no business
    // being read at sync cadence for its drain curve either.
    let mut snaps = Vec::new();
    if source_enabled("codex") {
        snaps.extend(codex_snapshots());
    }
    if source_enabled("claude") {
        snaps.extend(claude_snapshots());
    }
    if source_enabled("zcode") {
        snaps.extend(zcode_snapshots());
    }
    if !snaps.is_empty() {
        let now = crate::now_ts();
        let mut text = String::new();
        for s in &snaps {
            text.push_str(
                &serde_json::json!({
                    "kind": "snap",
                    "snapshot": s,
                    "collected_at": now,
                })
                .to_string(),
            );
            text.push('\n');
        }
        if let Err(e) = append_text(&text) {
            eprintln!("[TokenBuddy] quota sample failed: {e}");
        }
    }
    // 采样点也是告警检查点:sync 后文件源的新读数可能刚过阈值。
    // 必须在 detached 线程里发——sync 持账本写锁,webhook 超时(10s)会
    // 把整个仪表盘卡在锁上。快照克隆进线程,锁内只做收集。
    let cfg = load_config();
    if let Some(_alert) = cfg.alert.clone() {
        let snapshots = snaps.clone();
        std::thread::spawn(move || {
            let ledger: Vec<QuotaSnapshot> = load_config()
                .collectors
                .iter()
                .filter(|c| c.is_ledger())
                .filter_map(ledger_snapshot)
                .collect();
            let all: Vec<QuotaSnapshot> = snapshots.into_iter().chain(ledger).collect();
            maybe_alert(&all, &load_config());
        });
    }
}

fn append_text(text: &str) -> Result<()> {
    if text.is_empty() {
        return Ok(());
    }
    let path = log_path();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        f.write_all(text.as_bytes())?;
    }
    compact_if_large(&path)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Threshold alert — the one place TokenBuddy may knock on the network
// outside Fleet, and only because the user wrote the URL themselves.
// ---------------------------------------------------------------------------

pub fn alert_state_path() -> PathBuf {
    crate::data_dir().join("quota-alert-state.json")
}

/// Per-window last-sent bookkeeping, tolerant parse (the file is ours).
fn read_alert_state() -> BTreeMap<String, i64> {
    std::fs::read_to_string(alert_state_path())
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

fn write_alert_state(state: &BTreeMap<String, i64>) -> Result<()> {
    let path = alert_state_path();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(state)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// Which windows should fire now: over threshold AND outside their cooldown.
/// Pure so the decision is testable without a network.
fn due_alerts(
    snapshots: &[QuotaSnapshot],
    alert: &AlertConfig,
    state: &BTreeMap<String, i64>,
    now: i64,
) -> Vec<(String, String, f64)> {
    let cooldown = alert.cooldown_hours.max(1) * 3600;
    let mut out = Vec::new();
    for s in snapshots {
        // 滚动提醒:45 分钟内重置、余量 ≥20%(即用量 ≤80% 但不看阈值线,
        // 看的是"还有得用");阈值告警:越过阈值线。两者互斥的语义由
        // used_percent 与 resets_at 各自把关。
        // 滚动提醒只对「余量足」的窗口响:用量 >80% 时阈值告警语义更
        // 准确(那是告急,不是机会)。
        let rollover_due = alert.rollover
            && s.used_percent <= 80.0
            && s.resets_at
                .map(|r| r > now && r - now <= 45 * 60)
                .unwrap_or(false);
        if !rollover_due && s.used_percent < alert.threshold_percent {
            continue;
        }
        let key = if rollover_due {
            format!("ro\u{1f}{}", s.key())
        } else {
            s.key()
        };
        if let Some(last) = state.get(&key) {
            if now - *last < cooldown {
                continue;
            }
        }
        out.push((
            key,
            format!("{} {} {}", s.source, s.plan, s.window),
            s.used_percent,
        ));
    }
    out
}

/// Evaluate + send. Called where snapshots are freshest (explicit refresh
/// and the sync sampling bell). Every failure is stderr noise — alerts must
/// never break the sync or the refresh that triggered them.
/// Send one message in the configured format. Returns success; failures
/// are caller-visible (stderr there), never panics.
pub fn send_webhook_message(alert: &AlertConfig, text: &str) -> bool {
    send_webhook_message_result(alert, text).is_ok()
}

/// The same with the transport error surfaced, so a failed push says WHY
/// (refused / timeout / non-2xx) instead of a bare "failed" line.
pub fn send_webhook_message_result(alert: &AlertConfig, text: &str) -> Result<(), String> {
    let body = if alert.is_feishu() {
        serde_json::json!({"msg_type": "text", "content": {"text": text}}).to_string()
    } else {
        serde_json::json!({"text": text}).to_string()
    };
    ureq::post(&alert.webhook_url)
        .timeout(std::time::Duration::from_secs(10))
        .send_string(&body)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Fire-once primitive: cooldown-checked state, then send, then mark. True
/// when a message actually went out. The shared gate for every push channel
/// (threshold / rollover / daily digest) — one cooldown store, one send path.
pub fn fire_once(key: &str, alert: &AlertConfig, text: &str, now: i64) -> bool {
    let cooldown = alert.cooldown_hours.max(1) * 3600;
    let mut state = read_alert_state();
    if let Some(last) = state.get(key) {
        if now - *last < cooldown {
            return false;
        }
    }
    if let Err(why) = send_webhook_message_result(alert, text) {
        // 带上传输层错误类别,偶发失败才诊断得动。
        eprintln!("[TokenBuddy] alert send failed: {key}: {why}");
        return false;
    }
    state.insert(key.to_string(), now);
    if let Err(e) = write_alert_state(&state) {
        eprintln!("[TokenBuddy] alert state write failed: {e}");
    }
    true
}

pub fn maybe_alert(snapshots: &[QuotaSnapshot], cfg: &QuotaConfig) {
    let Some(alert) = cfg.alert.clone() else {
        return;
    };
    if !alert.webhook_url.starts_with("http://") {
        // No TLS in this build: an https URL here would silently do nothing,
        // so it refuses at config-validation time — this is the belt to that
        // suspenders.
        return;
    }
    let now = crate::now_ts();
    let mut state = read_alert_state();
    let due = due_alerts(snapshots, &alert, &state, now);
    for (key, label, used) in due {
        let text = format!(
            "TokenBuddy 套餐告警:{label} 已用 {used:.0}%(阈值 {:.0}%)",
            alert.threshold_percent
        );
        let body = if alert.is_feishu() {
            serde_json::json!({"msg_type": "text", "content": {"text": text}}).to_string()
        } else {
            serde_json::json!({"text": text}).to_string()
        };
        let sent = ureq::post(&alert.webhook_url)
            .timeout(std::time::Duration::from_secs(10))
            .send_string(&body)
            .is_ok();
        if sent {
            state.insert(key, now);
            eprintln!("[TokenBuddy] quota alert sent: {label} {used:.0}%");
        } else {
            eprintln!("[TokenBuddy] quota alert failed: {label}");
        }
    }
    if let Err(e) = write_alert_state(&state) {
        eprintln!("[TokenBuddy] alert state write failed: {e}");
    }
}

/// R82 — runaway-agent push. The store's daily anomaly detector (same-weekday
/// median/MAD, the exact report the dashboard banner shows) flags a day only
/// in hindsight; this turns TODAY's flag into a knock: if today's *running*
/// total already clears the same-weekday full-day median, an agent is very
/// likely looping while nobody watches. Opt-in (`alert.anomaly`), same
/// webhook/format as every other channel, once per flagged date via
/// `fire_once` — a partial day can only trip this by exceeding a full-day
/// baseline, so a healthy morning never fires it.
pub fn maybe_anomaly_alert(report: &crate::store::AnomalyReport, cfg: &QuotaConfig) {
    let Some(alert) = cfg.alert.clone() else {
        return;
    };
    if !alert.anomaly || !alert.webhook_url.starts_with("http://") {
        return;
    }
    let today = crate::cn_day_label(crate::now_ts());
    let Some(day) = report.flagged.iter().find(|d| d.date == today) else {
        return;
    };
    let ratio = if day.baseline_median > 0 {
        day.tokens as f64 / day.baseline_median as f64
    } else {
        0.0
    };
    let text = format!(
        "TokenBuddy 异常用量:{} 已用 {} tokens,已达同星期整天中位数({}) 的 {:.1} 倍(z={:.1})——查一下有 agent 在空转?",
        day.date,
        fmt_thousands(day.tokens),
        fmt_thousands(day.baseline_median),
        ratio,
        day.modified_z
    );
    if fire_once(
        &format!("anomaly|{}", day.date),
        &alert,
        &text,
        crate::now_ts(),
    ) {
        eprintln!("[TokenBuddy] anomaly alert sent: {}", day.date);
    }
}

/// R88 — "session went quiet" knock. The caller (sync path, detached)
/// supplies today's sessions from the ledger — (source, session_id,
/// last activity, requests) — and this picks the ones that worked hard
/// enough to matter (≥3 requests) and have been silent past the threshold.
/// One knock per session per day (`fire_once` key carries the date); a
/// fresh-of-ideas session never rings, an ancient one can't (today's rows
/// only), and nothing claims to know WHY it went quiet — done, stuck or
/// waiting are indistinguishable from timestamps, so the message just says
/// how long it has been.
pub fn maybe_session_idle_alert(sessions: &[crate::store::SessionRow], cfg: &QuotaConfig) {
    let Some(alert) = cfg.alert.clone() else {
        return;
    };
    if !alert.session_idle || !alert.webhook_url.starts_with("http://") {
        return;
    }
    let threshold = (alert.idle_minutes.max(1) as i64) * 60;
    let now = crate::now_ts();
    let today = crate::cn_day_label(now);
    for s in sessions {
        if s.requests < 3 || s.last_ts <= 0 {
            continue;
        }
        let quiet_for = now - s.last_ts;
        if quiet_for < threshold {
            continue;
        }
        let text = format!(
            "TokenBuddy 会话静默:{} 会话 {}… 已 {} 分钟无活动(今日 {} 请求)——可能收工或在等你",
            s.source,
            &s.session_id[..s.session_id.len().min(8)],
            quiet_for / 60,
            s.requests
        );
        let key = format!("idle|{}|{}|{}", s.source, s.session_id, today);
        if fire_once(&key, &alert, &text, now) {
            eprintln!("[TokenBuddy] session idle notice sent: {}", s.session_id);
        }
    }
}

/// R95 — 一键测试告警通道:向配置的 webhook 发一条带时间戳的测试
/// 消息,传输层错误原样返回(保存后按钮一点,链路通不通当场知道;
/// 不用等到真的越线那天才发现 webhook 填错了)。
pub fn send_test_alert(cfg: &QuotaConfig, now: i64) -> Result<(), String> {
    let Some(alert) = cfg.alert.as_ref() else {
        return Err("未配置告警:先填 webhook URL 并勾选阈值告警".into());
    };
    if !alert.webhook_url.starts_with("http://") {
        return Err("webhook 必须是 http://(本构建无 TLS,https 会静默不动)".into());
    }
    let text = format!(
        "TokenBuddy 告警测试 ✓ 链路通({});这是测试消息,阈值告警/滚动提醒/日报/异常/静默五条通道都走这里。",
        crate::cn_day_label(now)
    );
    send_webhook_message_result(alert, &text)
}

/// 1_234_567 → "1,234,567":push 文案里的 token 数按千分位读得快。
fn fmt_thousands(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Past 2 MiB rewrite the log keeping: the newest line per
/// (source, plan, window) and per source-error — plus the last
/// [`HISTORY_TAIL_LINES`] snapshot lines as the drain-curve tail. Bounded:
/// a compaction cycle sheds everything older than the tail, so frequent
/// refreshes trade granularity for a hard size ceiling, not growth.
fn compact_if_large(path: &std::path::Path) -> Result<()> {
    const HISTORY_TAIL_LINES: usize = 300;
    let size = match std::fs::metadata(path) {
        Ok(m) => m.len(),
        Err(_) => return Ok(()),
    };
    if size < LOG_COMPACT_BYTES {
        return Ok(());
    }
    let records = read_records(path);
    let mut latest: BTreeMap<String, usize> = BTreeMap::new();
    for (i, r) in records.iter().enumerate() {
        let key = match (r.kind.as_str(), r.source.as_deref()) {
            ("snap", _) => r
                .snapshot
                .as_ref()
                .map(|s| format!("s\u{1f}{}", s.key()))
                .unwrap_or_default(),
            ("err", Some(src)) => format!("e\u{1f}{src}"),
            _ => continue,
        };
        latest.insert(key, i);
    }
    let tail_start = records.len().saturating_sub(HISTORY_TAIL_LINES);
    let mut text = String::new();
    for (i, r) in records.iter().enumerate() {
        if i >= tail_start || latest.values().any(|&j| j == i) {
            text.push_str(&serde_json::to_string(r).unwrap_or_default());
            text.push('\n');
        }
    }
    let tmp = path.with_extension("jsonl.tmp");
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Parse the whole log, tolerating garbage lines (partial writes, manual
/// edits) — unknown lines are skipped, never fatal.
fn read_records(path: &std::path::Path) -> Vec<StoredRecord> {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(_) => return Vec::new(),
    };
    text.lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

/// Newest stored record per key: snapshots by (source, plan, window), errors
/// by source.
fn latest_stored() -> (
    BTreeMap<String, QuotaSnapshot>,
    BTreeMap<String, (String, i64)>,
) {
    let records = read_records(&log_path());
    let mut snaps: BTreeMap<String, QuotaSnapshot> = BTreeMap::new();
    let mut errs: BTreeMap<String, (String, i64)> = BTreeMap::new();
    for r in records {
        match (r.kind.as_str(), r.snapshot, r.source, r.error) {
            ("snap", Some(s), _, _) => {
                snaps.insert(s.key(), s);
            }
            ("err", _, Some(src), Some(err)) => {
                errs.insert(src, (err, r.collected_at));
            }
            _ => {}
        }
    }
    (snaps, errs)
}

// ---------------------------------------------------------------------------
// Merged view — what the API, the CLI and the dashboard all consume
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct CollectorHealth {
    pub name: String,
    pub configured: bool,
    pub last_ok_at: Option<i64>,
    pub last_error: Option<String>,
    pub last_error_at: Option<i64>,
}

/// One point of a plan's drain curve: when it was sampled and how much of
/// the window was used. `t` is the sample time (line collected_at), not the
/// source file's mtime.
#[derive(Debug, Clone, Serialize)]
pub struct QuotaHistoryPoint {
    pub t: i64,
    pub used_percent: f64,
}

#[derive(Debug, Serialize)]
pub struct QuotaView {
    pub snapshots: Vec<QuotaSnapshot>,
    pub collectors: Vec<CollectorHealth>,
    /// Per "source|plan|window" drain curves, oldest → newest, capped —
    /// sparse by nature: they grow on sync (file readers) and explicit
    /// refreshes (command collectors), never on a timer.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub history: BTreeMap<String, Vec<QuotaHistoryPoint>>,
    pub generated_at: i64,
}

/// Drain curves from the log: newest [`HISTORY_POINTS_PER_KEY`] samples per
/// key, oldest first so a polyline draws without sorting.
pub fn history() -> BTreeMap<String, Vec<QuotaHistoryPoint>> {
    const HISTORY_POINTS_PER_KEY: usize = 200;
    let mut all: BTreeMap<String, Vec<QuotaHistoryPoint>> = BTreeMap::new();
    for r in read_records(&log_path()) {
        if r.kind != "snap" {
            continue;
        }
        let Some(s) = r.snapshot else { continue };
        all.entry(format!("{}|{}|{}", s.source, s.plan, s.window))
            .or_default()
            .push(QuotaHistoryPoint {
                t: r.collected_at,
                used_percent: s.used_percent,
            });
    }
    for series in all.values_mut() {
        if series.len() > HISTORY_POINTS_PER_KEY {
            series.drain(..series.len() - HISTORY_POINTS_PER_KEY);
        }
    }
    all.retain(|_, v| !v.is_empty());
    all
}

/// The binding constraint per (source, plan): whichever window is closest to
/// exhausting is the one a statusline, a brief or an agent should hear about.
/// One snapshot per key, stable order by source then plan.
pub fn binding_windows(snapshots: &[QuotaSnapshot]) -> Vec<QuotaSnapshot> {
    let mut best: BTreeMap<(String, String), QuotaSnapshot> = BTreeMap::new();
    for s in snapshots {
        let key = (s.source.clone(), s.plan.clone());
        match best.get_mut(&key) {
            Some(cur) if cur.used_percent >= s.used_percent => {}
            _ => {
                best.insert(key, s.clone());
            }
        }
    }
    best.into_values().collect()
}

/// Compact countdown for statuslines: `3h5m` / `2d3h` / `45m` / `已重置`.
/// Same semantics as [`format_countdown`], statusline budget.
pub fn format_countdown_short(resets_at: Option<i64>, now: i64) -> String {
    let Some(at) = resets_at else {
        return "—".into();
    };
    let left = at - now;
    if left <= 0 {
        return "已重置".into();
    }
    if left >= 86_400 {
        format!("{}d{}h", left / 86_400, (left % 86_400) / 3_600)
    } else if left >= 3_600 {
        format!("{}h{}m", left / 3_600, (left % 3_600) / 60)
    } else {
        format!("{}m", left / 60)
    }
}

/// Relative countdown, zh, timezone-free (differences of epoch seconds).
/// `None`/non-positive renders as "已重置" — the caller decides whether to
/// show the row at all.
pub fn format_countdown(resets_at: Option<i64>, now: i64) -> String {
    let Some(at) = resets_at else {
        return "重置时间未知".into();
    };
    let left = at - now;
    if left <= 0 {
        return "已重置".into();
    }
    if left >= 86_400 {
        format!("{}天{}小时后重置", left / 86_400, (left % 86_400) / 3_600)
    } else if left >= 3_600 {
        format!("{}小时{}分后重置", left / 3_600, (left % 3_600) / 60)
    } else {
        format!("{}分后重置", left / 60)
    }
}

/// Ledger-derived quota: today's request count for one source against the
/// tier's documented daily cap. This is the third quota provenance — not
/// provider-reported (codex/claude caches) and not user-scripted (command
/// collectors), but *self-measured against public knowledge*: the count is
/// exactly what the ledger imported, the limit is the plan's published
/// number. Window ends at the next UTC+8 midnight, where such daily caps
/// actually reset for this user's clock.
fn ledger_snapshot(cfg: &CollectorConfig) -> Option<QuotaSnapshot> {
    let limit = cfg.daily_request_limit?;
    let source = cfg.ledger_source.trim();
    if limit == 0 || source.is_empty() {
        return None;
    }
    let store = crate::store::Store::open().ok()?;
    let start = crate::cn_midnight(0);
    let summary = store
        .query_summary(Some(source), None, Some(start), None)
        .ok()?;
    let used = summary.total_requests as f64;
    Some(QuotaSnapshot {
        source: cfg.name.clone(),
        plan: format!("{source}/day"),
        window: "day".into(),
        used_percent: (used / limit as f64 * 100.0).clamp(0.0, 100.0),
        resets_at: Some(crate::cn_midnight(-1)),
        collected_at: crate::now_ts(),
        origin: "ledger".into(),
    })
}

/// The one quota view everything renders. `run_commands = false` is the
/// read-only path (dashboard load, `tokenbuddy quota`): file readers go live,
/// command collectors report their *stored* last state. `true` is the
/// explicit refresh path: collectors run first, results append to the log.
pub fn collect_view(run_commands: bool) -> QuotaView {
    let now = crate::now_ts();
    let config = load_config();

    let ledger_snapshots: Vec<QuotaSnapshot> = config
        .collectors
        .iter()
        .filter(|c| c.is_ledger())
        .filter_map(ledger_snapshot)
        .collect();
    let outcomes: Vec<CollectorOutcome> = if run_commands {
        let runs: Vec<CollectorOutcome> = config
            .collectors
            .iter()
            .filter(|c| !c.is_ledger())
            .map(run_collector)
            .collect();
        if let Err(e) = append_outcomes(&runs) {
            eprintln!("[TokenBuddy] quota log append failed: {e}");
        }
        runs
    } else {
        Vec::new()
    };
    // 告警在合并视图算完后再评(能看见 file+command+ledger 全部窗口)。
    // run_commands=false 的只读路径不评——仪表盘浏览不该有网络副作用。

    let (stored_snaps, stored_errs) = latest_stored();
    // Per-collector latest stored OK time, read out before the merge below
    // consumes the map.
    let last_ok: BTreeMap<String, i64> = stored_snaps
        .values()
        .map(|s| (s.source.clone(), s.collected_at))
        .collect();

    // Live file reads are the freshest thing we have; they win their keys,
    // stored command readings fill everything the file readers don't see.
    let mut merged: BTreeMap<String, QuotaSnapshot> = BTreeMap::new();
    for snap in codex_snapshots()
        .into_iter()
        .chain(claude_snapshots())
        .chain(zcode_snapshots())
        .chain(ledger_snapshots)
    {
        merged.insert(snap.key(), snap);
    }
    for (key, snap) in &stored_snaps {
        merged.entry(key.clone()).or_insert_with(|| snap.clone());
    }

    let mut collectors: Vec<CollectorHealth> = config
        .collectors
        .iter()
        .map(|c| {
            let ran = outcomes.iter().find(|o| o.name == c.name);
            let ran_ok = matches!(ran, Some(o) if o.error.is_none());
            let ran_err = ran.and_then(|o| o.error.clone());
            let stored_err = stored_errs.get(&c.name);
            CollectorHealth {
                name: c.name.clone(),
                configured: true,
                last_ok_at: if ran_ok {
                    Some(now)
                } else {
                    last_ok.get(&c.name).copied()
                },
                // A fresh run's result (ok or error) speaks for itself; only
                // without one does the stored error state show — and only
                // while it is newer than the last stored success, so a stale
                // failure cannot outlive the refresh that recovered from it.
                last_error: ran_err.or_else(|| {
                    stored_err
                        .filter(|(_, at)| {
                            ran.is_none() && last_ok.get(&c.name).map_or(true, |ok| *at >= *ok)
                        })
                        .map(|(e, _)| e.clone())
                }),
                last_error_at: ran.filter(|o| o.error.is_some()).map(|_| now).or_else(|| {
                    stored_err
                        .filter(|(_, at)| {
                            ran.is_none() && last_ok.get(&c.name).map_or(true, |ok| *at >= *ok)
                        })
                        .map(|(_, at)| *at)
                }),
            }
        })
        .collect();
    collectors.sort_by(|a, b| a.name.cmp(&b.name));

    let view = QuotaView {
        snapshots: merged.into_values().collect(),
        collectors,
        history: history(),
        generated_at: now,
    };
    if run_commands {
        maybe_alert(&view.snapshots, &config);
    }
    view
}

// ---------------------------------------------------------------------------
// Tests — fixtures shaped like the real formats, poison for the fuzzer mindset
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    /// 读完整个 HTTP 请求(头+按 Content-Length 的体)再返回——单次 read
    /// 就回包的写法在负载下会抢跑:客户端还在发就被 RST,ureq 报
    /// BadHeader(EINVAL)。这是 R76「要回完整响应」的另一半。
    pub(super) fn read_full_request(stream: &mut std::net::TcpStream) -> String {
        use std::io::Read;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 2048];
        loop {
            let n = stream.read(&mut chunk).unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            let text = String::from_utf8_lossy(&buf);
            if let Some(idx) = text.find("\r\n\r\n") {
                let len = text[..idx]
                    .to_ascii_lowercase()
                    .split("\r\n")
                    .find_map(|l| {
                        l.strip_prefix("content-length:")
                            .and_then(|v| v.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if buf.len() >= idx + 4 + len {
                    break;
                }
            }
        }
        String::from_utf8_lossy(&buf).to_string()
    }

    use super::*;

    fn now() -> i64 {
        1_800_000_000
    }

    const CODEX_LINE: &str = r#"{"timestamp":"2026-05-01T10:00:02Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":100}},"rate_limits":{"primary":{"used_percent":72.3,"resets_at":1745000000,"window_minutes":300},"secondary":{"used_percent":18.1,"resets_at":1745604800,"window_minutes":10080},"plan_type":"pro"}}}"#;

    #[test]
    fn codex_rate_limits_parse_both_windows() {
        let snaps = rate_limits_from_line(CODEX_LINE, now()).expect("line carries limits");
        assert_eq!(snaps.len(), 2);
        let primary = snaps.iter().find(|s| s.window == "5h").unwrap();
        assert_eq!(primary.source, "codex");
        assert_eq!(primary.plan, "pro");
        assert!((primary.used_percent - 72.3).abs() < 1e-9);
        assert_eq!(primary.resets_at, Some(1_745_000_000));
        assert_eq!(primary.origin, "file");
        let secondary = snaps.iter().find(|s| s.window == "7d").unwrap();
        assert!((secondary.used_percent - 18.1).abs() < 1e-9);
    }

    #[test]
    fn codex_line_without_rate_limits_is_none() {
        let line = r#"{"type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"total_tokens":5}}}}"#;
        assert!(rate_limits_from_line(line, now()).is_none());
        assert!(rate_limits_from_line("not json at all", now()).is_none());
        assert!(rate_limits_from_line("", now()).is_none());
    }

    #[test]
    fn codex_unknown_window_minutes_get_derived_labels() {
        let line = r#"{"type":"event_msg","payload":{"type":"token_count","rate_limits":{"primary":{"used_percent":10,"resets_at":5,"window_minutes":90},"secondary":{"used_percent":20,"resets_at":6,"window_minutes":10080}}}}"#;
        let snaps = rate_limits_from_line(line, now()).unwrap();
        let w = snaps
            .iter()
            .find(|s| s.window == "1.5h")
            .expect("90m → 1.5h");
        assert_eq!(w.resets_at, Some(5));
        // plan_type absent → neutral label, not a crash
        assert_eq!(w.plan, "plan");
    }

    #[test]
    fn codex_non_finite_percent_is_rejected() {
        let line = r#"{"type":"event_msg","payload":{"type":"token_count","rate_limits":{"primary":{"used_percent":"72","window_minutes":300}}}}"#;
        // string percent: as_f64 fails → window dropped → None
        assert!(rate_limits_from_line(line, now()).is_none());
    }

    #[test]
    fn window_labels_follow_provider_durations() {
        assert_eq!(window_label(Some(300.0)), "5h");
        assert_eq!(window_label(Some(10080.0)), "7d");
        assert_eq!(window_label(Some(1440.0)), "1d");
        assert_eq!(window_label(Some(2880.0)), "2d");
        assert_eq!(window_label(Some(90.0)), "1.5h");
        assert_eq!(window_label(Some(45.0)), "45m");
        assert_eq!(window_label(None), "window");
        assert_eq!(window_label(Some(0.0)), "window");
    }

    /// One real-shaped ZCode log line: the client logs its own
    /// `billing/balance` response verbatim after this marker. Values are the
    /// raw units the API uses (tokens), not percents.
    const ZCODE_LINE: &str = r#"[2026-09-29 18:08:05.123] [info] [pid:17489] [main] [host-log] (local-1) [zcode-host] [usage-stats] billing/balance 请求完成 {"balanceCount":1,"balances":[{"entitlement_id":"zcode-v3-start-plan-trust-0929-1","show_name":"GLM-5.3-Flash","total_units":100000000,"used_units":40000000,"remaining_units":60000000,"available_units":60000000,"period_start":1790611345,"period_end":1790697600,"expires_at":1790697600}],"code":0,"payload":{"code":0,"data":{"server_time":1790676485,"plans":[{"name":"ZCode Trust Build","plan_id":"zcode-v3-start-plan-trust-0929","status":"active","starts_at":1790611345,"ends_at":1790697600}],"balances":[{"total_units":100000000,"used_units":40000000,"remaining_units":60000000,"period_start":1790611345,"period_end":1790697600}]}},"success":true,"url":"https://zcode.z.ai/api/v1/zcode-plan/billing/balance?app_version=3.14.3"}"#;

    #[test]
    fn zcode_balance_line_becomes_a_quota_snapshot() {
        let now = 1_790_678_485; // inside the period (server_time)
        let snap = zcode_snapshot_from_line(ZCODE_LINE, now, 0).expect("line carries a balance");
        assert_eq!(snap.source, "zcode");
        assert_eq!(snap.plan, "ZCode Trust Build");
        assert_eq!(snap.window, "24.0h"); // the period is 86255s, not a round 86400
        assert!((snap.used_percent - 40.0).abs() < 1e-9);
        assert_eq!(snap.resets_at, Some(1_790_697_600));
        // server_time is the honest observation stamp, not the file's mtime.
        assert_eq!(snap.collected_at, 1_790_676_485);
        assert_eq!(snap.origin, "file");
    }

    #[test]
    fn zcode_window_past_its_reset_is_dropped() {
        // ZCode already rolled the period over and logged a fresher line; the
        // stale window must not be shown as if its percent were current.
        let stale = ZCODE_LINE.replace("1790697600", "1000000000");
        assert!(zcode_snapshot_from_line(&stale, 1_790_678_485, 0).is_none());
    }

    #[test]
    fn zcode_millisecond_timestamps_are_normalized() {
        // 上游成员接口混发秒/毫秒（cc-switch 在 MiniMax 上踩到同款）:一条
        // 毫秒值曾让过期窗口被当成"还有 5 万年",永不淘汰也永不重置。
        let ms = ZCODE_LINE.replace("1790697600", "1790697600000");
        let snap = zcode_snapshot_from_line(&ms, 1_790_678_485, 0).expect("balance still parses");
        assert_eq!(snap.resets_at, Some(1_790_697_600), "millis → seconds");
        assert_eq!(snap.window, "24.0h", "period span normalizes too");
    }

    #[test]
    fn zcode_line_without_a_balance_is_none() {
        assert!(zcode_snapshot_from_line("[info] [usage-stats] heartbeat OK", 0, 0).is_none());
        assert!(zcode_snapshot_from_line("billing/balance 请求完成 not json", 0, 0).is_none());
        // Present marker, empty balances array → nothing to report.
        let empty =
            r#"billing/balance 请求完成 {"balances":[],"payload":{"data":{"balances":[]}}}"#;
        assert!(zcode_snapshot_from_line(empty, 0, 0).is_none());
    }

    #[test]
    fn zcode_most_drained_bucket_wins() {
        // One plan, several per-model buckets: the binding window is the most
        // drained of them, not the first one listed.
        let value: serde_json::Value = serde_json::from_str(
            r#"{"payload":{"data":{"server_time":1790676485,"plans":[{"name":"P"}],
            "balances":[
              {"total_units":100,"used_units":10,"period_start":1790611345,"period_end":1790697600},
              {"total_units":100,"used_units":90,"period_start":1790611345,"period_end":1790697600}
            ]}}}"#,
        )
        .unwrap();
        let snap = zcode_snapshot_from_value(&value, 1_790_678_485, 0).expect("buckets present");
        assert!((snap.used_percent - 90.0).abs() < 1e-9);
    }

    #[test]
    fn vendor_roster_never_hands_out_an_unimportable_row() {
        for v in VENDORS {
            let row = vendor_collector(v);
            match v.kind {
                // File readers have nothing to configure — that is the point.
                "file" => assert!(row.is_none(), "{} should be config-free", v.id),
                _ => {
                    let row = row.unwrap_or_else(|| panic!("{} needs a row", v.id));
                    let mut cfg = QuotaConfig {
                        collectors: vec![row.clone()],
                        disabled_sources: Vec::new(),
                        alert: None,
                    };
                    // Every preset must survive the same validation the
                    // 导入 path runs, or the button would only fail on press.
                    validate_config(&cfg).unwrap_or_else(|e| panic!("{}: {e}", v.id));
                    cfg.collectors[0].name = String::new();
                    assert!(validate_config(&cfg).is_err());
                }
            }
            if v.parser.is_empty() && v.kind == "command" {
                panic!("{} names no parser", v.id);
            }
        }
    }

    #[test]
    fn vendor_detection_never_needs_the_vendor() {
        // Detection is a local existence probe. Assert it answers without a
        // network call by checking the command probe is PATH-only.
        assert!(!program_in_path("definitely-not-installed-tb"));
    }

    const MINIMAX_JSON: &str = r#"{"model_remains":[
      {"start_time":1790611200000,"end_time":1790629200000,"remains_time":17339699,
       "current_interval_total_count":0,"current_interval_usage_count":0,
       "model_name":"general","current_weekly_total_count":0,"current_weekly_usage_count":0,
       "weekly_start_time":1790524800000,"weekly_end_time":1791129600000,
       "current_interval_status":1,"current_interval_remaining_percent":98,
       "current_weekly_status":3,"current_weekly_remaining_percent":100},
      {"start_time":1790611200000,"end_time":1790697600000,"model_name":"video",
       "current_interval_remaining_percent":55.5,"weekly_end_time":1791129600000,
       "current_weekly_remaining_percent":80}
    ]}"#;

    #[test]
    fn minimax_parser_reads_interval_and_weekly() {
        let snaps = parse_minimax(MINIMAX_JSON, now());
        assert_eq!(snaps.len(), 4);
        let general_interval = snaps
            .iter()
            .find(|s| s.plan == "general" && s.window == "interval")
            .unwrap();
        assert!((general_interval.used_percent - 2.0).abs() < 1e-9);
        assert_eq!(general_interval.resets_at, Some(1_790_629_200));
        assert_eq!(general_interval.origin, "command");
        let video_weekly = snaps
            .iter()
            .find(|s| s.plan == "video" && s.window == "weekly")
            .unwrap();
        assert!((video_weekly.used_percent - 20.0).abs() < 1e-9);
    }

    #[test]
    fn minimax_parser_skips_unknown_not_zero() {
        // A missing remaining percent must skip the window — unknown ≠ 0.
        let text = r#"{"model_remains":[{"model_name":"audio","end_time":1790629200000}]}"#;
        assert!(parse_minimax(text, now()).is_empty());
        // Out-of-range percents are garbage from a schema change, not usage.
        let bad = r#"{"model_remains":[{"model_name":"audio","end_time":1790629200000,"current_interval_remaining_percent":180}]}"#;
        assert!(parse_minimax(bad, now()).is_empty());
        assert!(parse_minimax("garbage", now()).is_empty());
        assert!(parse_minimax("{}", now()).is_empty());
    }

    #[test]
    fn store_roundtrip_latest_wins_and_tolerates_garbage() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_quota_test");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);

        let snap_a = QuotaSnapshot {
            source: "minimax".into(),
            plan: "general".into(),
            window: "interval".into(),
            used_percent: 2.0,
            resets_at: Some(1_790_629_200),
            collected_at: 100,
            origin: "command".to_string(),
        };
        let snap_b = QuotaSnapshot {
            used_percent: 40.0,
            collected_at: 200,
            ..snap_a.clone()
        };
        let outcome = CollectorOutcome {
            name: "minimax".into(),
            snapshots: vec![snap_a, snap_b],
            error: None,
            duration_ms: 5,
        };
        let failed = CollectorOutcome {
            name: "broken".into(),
            snapshots: vec![],
            error: Some("boom".into()),
            duration_ms: 1,
        };
        append_outcomes(&[outcome, failed]).unwrap();

        let (snaps, errs) = latest_stored();
        let general = snaps
            .get("minimax\u{1f}general\u{1f}interval")
            .expect("latest snapshot stored");
        assert_eq!(general.used_percent, 40.0, "newest reading wins");
        assert_eq!(errs.get("broken").map(|(e, _)| e.as_str()), Some("boom"));

        // Garbage lines (truncated write) must not break reads.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(log_path())
                .unwrap();
            f.write_all(b"{truncated\n").unwrap();
        }
        let (snaps2, errs2) = latest_stored();
        assert_eq!(snaps2.len(), snaps.len());
        assert_eq!(errs2.len(), errs.len());

        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn command_collector_runs_parses_and_fails_safely() {
        let dir = crate::unique_test_dir("tb_quota_cmd");
        std::fs::create_dir_all(&dir).unwrap();
        let fixture = dir.join("quota_fixture.json");
        std::fs::write(&fixture, MINIMAX_JSON).unwrap();

        // Success path: a real local program (cat) emits the fixture.
        let ok_cfg = CollectorConfig {
            name: "minimax".into(),
            command: format!("cat {}", fixture.display()),
            parser: "minimax".into(),
            timeout_secs: Some(10),
            kind: "command".into(),
            ledger_source: String::new(),
            daily_request_limit: None,
        };
        let outcome = run_collector(&ok_cfg);
        assert!(
            outcome.error.is_none(),
            "unexpected error: {:?}",
            outcome.error
        );
        assert_eq!(outcome.snapshots.len(), 4);

        // Failing program: recorded as an error outcome, never a panic.
        let fail_cfg = CollectorConfig {
            name: "minimax".into(),
            command: format!("cat {}", dir.join("missing.json").display()),
            parser: "minimax".into(),
            timeout_secs: Some(10),
            kind: "command".into(),
            ledger_source: String::new(),
            daily_request_limit: None,
        };
        let failed = run_collector(&fail_cfg);
        assert!(failed.error.is_some());
        assert!(failed.snapshots.is_empty());

        // Unspawnable program name.
        let bad_cfg = CollectorConfig {
            name: "nope".into(),
            command: "tb_no_such_binary_9x7".into(),
            parser: "minimax".into(),
            timeout_secs: Some(5),
            kind: "command".into(),
            ledger_source: String::new(),
            daily_request_limit: None,
        };
        assert!(run_collector(&bad_cfg).error.is_some());

        // Empty command.
        let empty_cfg = CollectorConfig {
            name: "blank".into(),
            command: "   ".into(),
            parser: "minimax".into(),
            timeout_secs: None,
            kind: "command".into(),
            ledger_source: String::new(),
            daily_request_limit: None,
        };
        assert!(run_collector(&empty_cfg).error.is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn command_collector_timeout_kills_hang() {
        let cfg = CollectorConfig {
            name: "slow".into(),
            command: if cfg!(windows) {
                "ping -n 30 127.0.0.1".into()
            } else {
                "sleep 30".into()
            },
            parser: "minimax".into(),
            timeout_secs: Some(1),
            kind: "command".into(),
            ledger_source: String::new(),
            daily_request_limit: None,
        };
        let started = Instant::now();
        let outcome = run_collector(&cfg);
        assert!(outcome.error.unwrap().contains("超时"));
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "kill must not wait 30s"
        );
    }

    #[test]
    fn collect_view_merges_file_over_command_without_commands_run() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Read-only path with no quota.json and no agent quota state: an
        // empty view with zero collectors — and crucially no processes
        // spawned. The file readers' env roots are pointed at empty dirs so
        // the assertion holds on a machine where Codex/Claude ARE installed.
        let dir = crate::unique_test_dir("tb_quota_view");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        std::env::set_var("CODEX_HOME", &dir);
        std::env::set_var("CLAUDE_CONFIG_DIR", &dir);
        std::env::set_var("ZCODE_CONFIG_DIR", &dir);
        let view = collect_view(false);
        assert!(view.snapshots.is_empty());
        assert!(view.collectors.is_empty());
        std::env::remove_var("TOKENBUDDY_HOME");
        std::env::remove_var("CODEX_HOME");
        std::env::remove_var("CLAUDE_CONFIG_DIR");
        std::env::remove_var("ZCODE_CONFIG_DIR");
        let _ = std::fs::remove_dir_all(&dir);
    }

    const CLAUDE_CACHE: &str = r#"{
      "utilization": {
        "five_hour": { "utilization": 23.5, "resets_at": "2030-01-01T00:00:00Z" },
        "seven_day": { "utilization": 41.2, "resets_at": "2030-01-05T00:00:00Z" }
      },
      "oauthAccount": { "emailAddress": "x@example.com", "userRateLimitTier": "default_claude_max_5x" }
    }"#;

    #[test]
    fn claude_cache_parses_windows_and_tier() {
        let snaps = claude_snapshots_from(CLAUDE_CACHE, 1_800_000_000);
        assert_eq!(snaps.len(), 2);
        let five = snaps.iter().find(|s| s.window == "5h").unwrap();
        assert_eq!(five.source, "claude");
        assert_eq!(five.plan, "default_claude_max_5x");
        assert!((five.used_percent - 23.5).abs() < 1e-9);
        assert_eq!(five.resets_at, Some(1_893_456_000));
        assert_eq!(
            five.collected_at, 1_800_000_000,
            "collected_at rides the file mtime"
        );
        let seven = snaps.iter().find(|s| s.window == "7d").unwrap();
        assert!((seven.used_percent - 41.2).abs() < 1e-9);
    }

    #[test]
    fn claude_expired_window_is_dropped_not_shown_stale() {
        // resets_at in the past: the server already granted a fresh allowance
        // this cache never saw — showing the old percent would overstate.
        let past = "2020-01-01T00:00:00Z";
        let text = format!(
            r#"{{"utilization":{{"five_hour":{{"utilization":99.0,"resets_at":"{past}"}},"seven_day":{{"utilization":5.0}}}}}}"#
        );
        let snaps = claude_snapshots_from(&text, 0);
        assert!(snaps.iter().all(|s| s.window != "5h"), "expired 5h dropped");
        // seven_day has no resets_at: kept, reset marked unknown.
        let seven = snaps.iter().find(|s| s.window == "7d").unwrap();
        assert_eq!(seven.resets_at, None);
    }

    #[test]
    fn claude_garbage_and_missing_fields_degrade_to_empty() {
        assert!(claude_snapshots_from("not json", 0).is_empty());
        assert!(claude_snapshots_from("{}", 0).is_empty());
        assert!(claude_snapshots_from(r#"{"utilization":[]}"#, 0).is_empty());
        assert!(
            claude_snapshots_from(r#"{"utilization":{"five_hour":{"utilization":"23"}}}"#, 0)
                .is_empty()
        );
    }

    #[test]
    fn claude_cache_reader_reads_config_dir_env() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_quota_claude");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(".claude.json"), CLAUDE_CACHE).unwrap();
        std::env::set_var("CLAUDE_CONFIG_DIR", &dir);
        let snaps = claude_snapshots();
        std::env::remove_var("CLAUDE_CONFIG_DIR");
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(snaps.len(), 2);
        let mtime = snaps[0].collected_at;
        let now = crate::now_ts();
        assert!(
            mtime > now - 60 && mtime <= now,
            "collected_at must be the file mtime ({mtime} vs now {now})"
        );
    }

    /// FEATURES #9 承诺的毒化防线,补齐到 R31 以来的新解析器:确定性 LCG
    /// 变异(截断/字节翻转/垃圾注入)喂三个解析面,断言只有"拒绝"没有
    /// "崩溃"——最坏情况是少统计一条,不是服务挂掉。
    #[test]
    fn fuzz_corpus_never_panics_quota_parsers() {
        // 真实形状的种子,变异才有意义。
        let seeds = [
            MINIMAX_JSON.to_string(),
            CLAUDE_CACHE.to_string(),
            CODEX_LINE.to_string(),
        ];
        let parse = |i: usize, text: &str| -> usize {
            match i {
                0 => parse_minimax(text, now()).len(),
                1 => claude_snapshots_from(text, now()).len(),
                _ => rate_limits_from_line(text, now())
                    .map(|v| v.len())
                    .unwrap_or(0),
            }
        };
        let mut lcg: u64 = 0x5EED_2026_0929;
        let mut next = move || {
            lcg = lcg
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            lcg
        };
        for round in 0..600 {
            let seed = &seeds[round % seeds.len()];
            let mut bytes = seed.clone().into_bytes();
            match next() % 3 {
                0 => {
                    // 截断到随机位置(含 0 = 空串)。
                    let cut = (next() as usize) % (bytes.len() + 1);
                    bytes.truncate(cut);
                }
                1 => {
                    // 字节翻转:随机位置换成随机字节。
                    let pos = (next() as usize) % bytes.len();
                    bytes[pos] = (next() % 256) as u8;
                }
                _ => {
                    // 垃圾注入:随机位置插 1-8 个随机字节。
                    let pos = (next() as usize) % (bytes.len() + 1);
                    let n = (next() % 8 + 1) as usize;
                    let junk: Vec<u8> = (0..n).map(|_| (next() % 256) as u8).collect();
                    bytes.splice(pos..pos, junk);
                }
            }
            let text = String::from_utf8_lossy(&bytes).to_string();
            let _ = parse(round % 3, &text); // 只要不 panic,返回什么都可以
        }
    }

    #[test]
    fn due_alerts_pure_decision_with_cooldown() {
        let alert = AlertConfig {
            webhook_url: "http://127.0.0.1:1/hook".into(),
            threshold_percent: 80.0,
            format: String::new(),
            cooldown_hours: 12,
            rollover: false,
            daily_digest: false,
            anomaly: false,
            session_idle: false,
            idle_minutes: 15,
        };
        let mk = |used: f64| QuotaSnapshot {
            source: "minimax".into(),
            plan: "general".into(),
            window: "interval".into(),
            used_percent: used,
            resets_at: None,
            collected_at: 0,
            origin: "command".into(),
        };
        let now = 1_800_000_000;

        // 低于阈值:不响。
        let mut state = BTreeMap::new();
        assert!(due_alerts(&[mk(79.9)], &alert, &state, now).is_empty());
        // 越线且无历史:响,带 label。
        let due = due_alerts(&[mk(85.0)], &alert, &state, now);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].1, "minimax general interval");
        // 刚响过(冷却内):不响。
        state.insert(due[0].0.clone(), now);
        assert!(due_alerts(&[mk(85.0)], &alert, &state, now).is_empty());
        // 冷却过了:再响。
        state.insert(due[0].0.clone(), now - 12 * 3600 - 1);
        assert_eq!(due_alerts(&[mk(85.0)], &alert, &state, now).len(), 1);
    }

    #[test]
    fn fire_once_is_cooldown_gated_and_marks_state() {
        // TOKENBUDDY_HOME 在此被改写——必须拿 crate 级锁,否则与其它
        // env 测试并发互踩(R84 补:此前裸奔,全量并行时偶发发送竞态)。
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let alert = AlertConfig {
            webhook_url: "http://127.0.0.1:2/hook".into(),
            threshold_percent: 80.0,
            format: String::new(),
            cooldown_hours: 24,
            rollover: false,
            daily_digest: false,
            anomaly: false,
            session_idle: false,
            idle_minutes: 15,
        };
        let dir = crate::unique_test_dir("tb_fire");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        let now = crate::now_ts();

        // 无 webhook 服务的地址:发送失败 → 状态不标记 → 下次仍会尝试。
        let first = fire_once("digest|t", &alert, "hello", now);
        assert!(!first, "send fails without a listener");

        // 换一个真实监听:起一个一次性本地 TCP 接收端。
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            use std::io::Write;
            let _ = read_full_request(&mut stream);
            let resp = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(resp);
            let _ = stream.flush();
        });
        let alert2 = AlertConfig {
            webhook_url: format!("http://{addr}/hook"),
            threshold_percent: 80.0,
            format: String::new(),
            cooldown_hours: 24,
            rollover: false,
            daily_digest: false,
            anomaly: false,
            session_idle: false,
            idle_minutes: 15,
        };
        let sent = fire_once("digest|t2", &alert2, "hello", now);
        let _ = server.join();
        assert!(sent, "first fire sends");
        // 冷却内:不再发送(即使 listener 已关,fire_once 也应先被冷却挡住)。
        let again = fire_once("digest|t2", &alert2, "hello", now + 60);
        assert!(!again, "cooldown gates the second call");

        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rollover_alerts_fire_on_time_window_not_threshold() {
        let mut alert = AlertConfig {
            webhook_url: "http://127.0.0.1:1/hook".into(),
            threshold_percent: 80.0,
            format: String::new(),
            cooldown_hours: 12,
            rollover: true,
            daily_digest: false,
            anomaly: false,
            session_idle: false,
            idle_minutes: 15,
        };
        let now = 1_800_000_000;
        let mk = |used: f64, resets_in: i64| QuotaSnapshot {
            source: "codex".into(),
            plan: "pro".into(),
            window: "5h".into(),
            used_percent: used,
            resets_at: Some(now + resets_in),
            collected_at: now,
            origin: "file".into(),
        };
        let state = BTreeMap::new();
        // 40 分钟后重置、仅用 60%:rollover 触发(不看 80% 阈值)。
        assert_eq!(
            due_alerts(&[mk(60.0, 40 * 60)], &alert, &state, now).len(),
            1
        );
        // rollover 未开:同数据不触发(60% < 80% 阈值)。
        alert.rollover = false;
        assert!(due_alerts(&[mk(60.0, 40 * 60)], &alert, &state, now).is_empty());
        alert.rollover = true;
        // 已过重置点:不触发。
        assert!(due_alerts(&[mk(60.0, -60)], &alert, &state, now).is_empty());
        // >45 分钟:不触发。
        assert!(due_alerts(&[mk(60.0, 46 * 60)], &alert, &state, now).is_empty());
        // 用量 >80%:阈值告警接管(同轮触发,但走阈值语义)。
        let due = due_alerts(&[mk(95.0, 40 * 60)], &alert, &state, now);
        assert_eq!(due.len(), 1);
        assert!(
            !due[0].0.starts_with("ro\u{1f}"),
            "high usage = threshold semantics"
        );
    }

    #[test]
    fn alert_state_roundtrips() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_alert");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        let mut state = BTreeMap::new();
        state.insert("k".to_string(), 1_800_000_000);
        write_alert_state(&state).unwrap();
        let back = read_alert_state();
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(back.get("k"), Some(&1_800_000_000));
    }

    #[test]
    fn ledger_collector_counts_today_against_known_cap() {
        use crate::{Source, TokenRecord};
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_quota_ledger");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);

        // 种账本:今天 25 条 gemini 请求。
        let ts = crate::cn_midnight(0) + 3600;
        let records: Vec<(String, TokenRecord)> = (0..25)
            .map(|i| {
                (
                    format!("g{i}"),
                    TokenRecord {
                        source: Source::Gemini,
                        model: "gemini-2.5-pro".into(),
                        input_tokens: 10,
                        output_tokens: 0,
                        cache_read_tokens: 0,
                        cache_creation_tokens: 0,
                        timestamp: ts,
                        session_id: None,
                        project: String::new(),
                        duration_ms: None,
                        ttft_ms: None,
                        credits: 0.0,
                        context_ratio: 0.0,
                        record_id: None,
                        sidechain: false,
                        merge_key: None,
                        request_count: 1,
                    },
                )
            })
            .collect();
        let batch = crate::store::records_to_batch(&records);
        crate::store::write_parquet(&dir.join("data.parquet"), &batch).unwrap();

        std::fs::write(
            config_path(),
            r#"{"collectors":[{"name":"gemini","type":"ledger","ledger_source":"gemini","daily_request_limit":1000}]}"#,
        )
        .unwrap();

        let view = collect_view(false);
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);

        let snap = view
            .snapshots
            .iter()
            .find(|s| s.source == "gemini" && s.window == "day")
            .expect("ledger snapshot present");
        assert_eq!(snap.plan, "gemini/day");
        assert!((snap.used_percent - 2.5).abs() < 1e-9, "25/1000 = 2.5%");
        assert_eq!(snap.origin, "ledger");
        assert_eq!(snap.resets_at, Some(crate::cn_midnight(-1)));
    }

    #[test]
    fn ledger_collector_validation_requires_source_and_limit() {
        let c = |json: &str| {
            validate_config(&QuotaConfig {
                collectors: serde_json::from_str::<Vec<CollectorConfig>>(json).unwrap(),
                alert: None,
                disabled_sources: vec![],
            })
        };
        assert!(c(
            r#"[{"name":"g","type":"ledger","ledger_source":"gemini","daily_request_limit":1000}]"#
        )
        .is_ok());
        assert!(c(r#"[{"name":"g","type":"ledger","daily_request_limit":1000}]"#).is_err());
        assert!(c(r#"[{"name":"g","type":"ledger","ledger_source":"gemini"}]"#).is_err());
        // command 采集器保持原语义:无 type 字段 = command。
        assert!(c(r#"[{"name":"m","command":"x","parser":"minimax"}]"#).is_ok());
    }

    #[test]
    fn short_countdown_is_statusline_budget() {
        let now = 1_800_000_000;
        assert_eq!(format_countdown_short(None, now), "—");
        assert_eq!(format_countdown_short(Some(now - 5), now), "已重置");
        assert_eq!(format_countdown_short(Some(now + 45 * 60), now), "45m");
        assert_eq!(
            format_countdown_short(Some(now + 3 * 3600 + 5 * 60), now),
            "3h5m"
        );
        assert_eq!(
            format_countdown_short(Some(now + 2 * 86_400 + 3 * 3_600), now),
            "2d3h"
        );
    }

    #[test]
    fn binding_windows_picks_the_tightest_window_per_source_plan() {
        let mk = |source: &str, plan: &str, window: &str, used: f64| QuotaSnapshot {
            source: source.into(),
            plan: plan.into(),
            window: window.into(),
            used_percent: used,
            resets_at: Some(1),
            collected_at: 0,
            origin: "file".into(),
        };
        let snaps = vec![
            mk("codex", "pro", "5h", 72.0),
            mk("codex", "pro", "7d", 18.0),
            mk("codex", "pro", "5h", 90.0), // later reading of same window wins on max
            mk("minimax", "general", "interval", 3.0),
            mk("minimax", "general", "weekly", 41.0),
        ];
        let binding = binding_windows(&snaps);
        assert_eq!(binding.len(), 2);
        let codex = binding.iter().find(|s| s.source == "codex").unwrap();
        assert_eq!(codex.window, "5h");
        assert!((codex.used_percent - 90.0).abs() < 1e-9);
        let minimax = binding.iter().find(|s| s.source == "minimax").unwrap();
        assert_eq!(minimax.window, "weekly");
        // Stable order: source then plan.
        assert!(binding[0].source <= binding[1].source);
    }

    #[test]
    fn countdown_format_is_relative_and_timezone_free() {
        let now = 1_800_000_000;
        assert_eq!(format_countdown(None, now), "重置时间未知");
        assert_eq!(format_countdown(Some(now - 1), now), "已重置");
        assert_eq!(format_countdown(Some(now + 45 * 60), now), "45分后重置");
        assert_eq!(
            format_countdown(Some(now + 3 * 3600 + 5 * 60), now),
            "3小时5分后重置"
        );
        assert_eq!(
            format_countdown(Some(now + 2 * 86_400 + 3 * 3_600), now),
            "2天3小时后重置"
        );
    }

    #[test]
    fn config_validation_rejects_bad_collectors() {
        let ok = |cs: Vec<CollectorConfig>| {
            validate_config(&QuotaConfig {
                collectors: cs,
                alert: None,
                disabled_sources: vec![],
            })
        };
        assert!(ok(vec![CollectorConfig {
            name: "minimax".into(),
            kind: "command".into(),
            command: "mmx quota show".into(),
            parser: "minimax".into(),
            timeout_secs: None,
            ledger_source: String::new(),
            daily_request_limit: None,
        }])
        .is_ok());
        // 空名/重复名/空命令/未知 parser 全部拒绝。
        let c = |name: &str, command: &str, parser: &str| CollectorConfig {
            name: name.into(),
            kind: "command".into(),
            command: command.into(),
            parser: parser.into(),
            timeout_secs: None,
            ledger_source: String::new(),
            daily_request_limit: None,
        };
        assert!(ok(vec![c("", "x", "minimax")]).is_err());
        assert!(ok(vec![c("a", "x", "minimax"), c("a", "y", "minimax")]).is_err());
        assert!(ok(vec![c("a", "  ", "minimax")]).is_err());
        assert!(ok(vec![c("a", "x", "no-such-parser")]).is_err());
    }

    #[test]
    fn config_save_is_atomic_and_strict_load_roundtrips() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_quota_save");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);

        let cfg = QuotaConfig {
            alert: None,
            collectors: vec![CollectorConfig {
                name: "minimax".into(),
                kind: "command".into(),
                command: "mmx quota show --output json".into(),
                parser: "minimax".into(),
                timeout_secs: Some(20),
                ledger_source: String::new(),
                daily_request_limit: None,
            }],
            disabled_sources: vec![],
        };
        save_config(&cfg).unwrap();
        // 0600 落盘(fleet.toml 同款姿态;文件本身无密钥,但命令可能内嵌)。
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(config_path())
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let loaded = load_config_strict().unwrap();
        assert_eq!(loaded.collectors.len(), 1);
        assert_eq!(loaded.collectors[0].timeout_secs, Some(20));

        // 损坏的文件在严格读取下报错(面板可见),宽松读取降级为空。
        std::fs::write(config_path(), "{broken").unwrap();
        assert!(load_config_strict().is_err());
        assert!(load_config().collectors.is_empty());

        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn history_series_is_per_key_and_chronological() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_quota_hist");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);

        let mk = |used: f64| CollectorOutcome {
            name: "minimax".into(),
            snapshots: vec![QuotaSnapshot {
                source: "minimax".into(),
                plan: "general".into(),
                window: "interval".into(),
                used_percent: used,
                resets_at: None,
                collected_at: 0,
                origin: "command".into(),
            }],
            error: None,
            duration_ms: 1,
        };
        append_outcomes(&[mk(5.0)]).unwrap();
        append_outcomes(&[mk(9.0)]).unwrap();
        append_outcomes(&[mk(20.0)]).unwrap();

        let hist = history();
        let series = &hist["minimax|general|interval"];
        assert_eq!(series.len(), 3);
        assert_eq!(series[0].used_percent, 5.0, "oldest first");
        assert_eq!(series[2].used_percent, 20.0);
        assert!(
            series[0].t <= series[1].t && series[1].t <= series[2].t,
            "sample time order"
        );

        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sample_file_readers_persists_claude_state() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_quota_smp");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        std::env::set_var("CLAUDE_CONFIG_DIR", &dir);
        std::fs::write(dir.join(".claude.json"), CLAUDE_CACHE).unwrap();

        sample_file_readers();

        let hist = history();
        std::env::remove_var("TOKENBUDDY_HOME");
        std::env::remove_var("CLAUDE_CONFIG_DIR");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(hist.contains_key("claude|default_claude_max_5x|5h"));
        assert_eq!(hist["claude|default_claude_max_5x|5h"].len(), 1);
    }

    #[test]
    fn compaction_keeps_latest_per_key_plus_recent_tail() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_quota_cmp");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);

        // One giant append past the 2 MiB trip-wire: 11000 distinct-ish
        // lines for one key (history) + a final reading of another key.
        let mut text = String::new();
        for i in 0..11_000 {
            let used = (i % 100) as f64;
            text.push_str(&format!(
                "{{\"kind\":\"snap\",\"snapshot\":{{\"source\":\"minimax\",\"plan\":\"general\",\"window\":\"interval\",\"used_percent\":{used},\"resets_at\":null,\"collected_at\":{i},\"origin\":\"command\"}},\"collected_at\":{i}}}\n"
            ));
        }
        text.push_str(
            "{\"kind\":\"snap\",\"snapshot\":{\"source\":\"codex\",\"plan\":\"pro\",\"window\":\"5h\",\"used_percent\":77.0,\"resets_at\":null,\"collected_at\":1,\"origin\":\"file\"},\"collected_at\":99999}\n",
        );
        append_text(&text).unwrap();

        let size = std::fs::metadata(log_path()).unwrap().len();
        let (snaps, _) = latest_stored();
        let hist = history();
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);

        assert!(
            size < 2 * 1024 * 1024,
            "compaction must shed below the trip-wire, got {size}"
        );
        assert_eq!(
            snaps
                .get("minimax\u{1f}general\u{1f}interval")
                .map(|s| s.used_percent),
            Some(99.0),
            "newest line per key survives (10999 % 100 == 99)"
        );
        assert_eq!(
            snaps.get("codex\u{1f}pro\u{1f}5h").map(|s| s.used_percent),
            Some(77.0)
        );
        // Tail kept, then capped at 200 points per key by history().
        assert_eq!(hist["minimax|general|interval"].len(), 200);
        assert_eq!(hist["codex|pro|5h"].len(), 1);
    }

    /// R102: the per-source switch — strict roster validation, roundtrip,
    /// and `source_enabled` reading the file back.
    #[test]
    fn disabled_sources_validate_roundtrip_and_gate() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_quota_srcsw");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);

        // Unknown source names are rejected, not silently kept.
        let bad = QuotaConfig {
            alert: None,
            collectors: vec![],
            disabled_sources: vec!["claude".into(), "not_a_source".into()],
        };
        assert!(
            validate_config(&bad).is_err(),
            "unknown source must be refused"
        );
        // Duplicates are rejected too.
        let dup = QuotaConfig {
            alert: None,
            collectors: vec![],
            disabled_sources: vec!["kimi".into(), "kimi".into()],
        };
        assert!(
            validate_config(&dup).is_err(),
            "duplicate source must be refused"
        );

        // Real names roundtrip through the file and gate the helper.
        let cfg = QuotaConfig {
            alert: None,
            collectors: vec![],
            disabled_sources: vec!["kimi".into(), "amp".into()],
        };
        save_config(&cfg).unwrap();
        assert!(source_enabled("claude"));
        assert!(
            !source_enabled("kimi"),
            "disabled source must read back off"
        );
        assert!(!source_enabled("amp"));

        // An empty roster means everything on.
        std::fs::remove_file(config_path()).unwrap();
        assert!(source_enabled("kimi"));

        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn collector_config_missing_file_is_feature_off() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_quota_cfg");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        let cfg = load_config();
        assert!(cfg.collectors.is_empty());
        // Malformed config degrades to off, never a crash.
        std::fs::write(config_path(), "{not json").unwrap();
        assert!(load_config().collectors.is_empty());
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Refresh path end to end with a real collector configured: the run
    /// appends to quota.jsonl, the read-only view afterwards still shows the
    /// stored reading, and a recovered-from error does not outlive its fix.
    #[test]
    fn refresh_appends_then_readonly_view_shows_stored_state() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_quota_e2e");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        // 文件读取器的根一并隔离:zcode/claude/codex 读的是各自工具目录,
        // 本机在跑这些工具时,泄漏的实时快照会击穿 len()==4 断言。
        std::env::set_var("CODEX_HOME", &dir);
        std::env::set_var("CLAUDE_CONFIG_DIR", &dir);
        std::env::set_var("ZCODE_CONFIG_DIR", &dir);

        // Collector 1 fails; collector 2 succeeds via a real local program
        // (no shell involved, so the fixture rides a file, not quotes).
        let fixture = dir.join("quota_fixture.json");
        std::fs::write(&fixture, MINIMAX_JSON).unwrap();
        std::fs::write(
            config_path(),
            format!(
                r#"{{"collectors":[
                    {{"name":"broken","command":"cat {}","parser":"minimax","timeout_secs":5}},
                    {{"name":"minimax","command":"cat {}","parser":"minimax","timeout_secs":10}}
                ]}}"#,
                dir.join("missing.json").display(),
                fixture.display()
            ),
        )
        .unwrap();

        let view = collect_view(true);
        assert_eq!(view.snapshots.len(), 4, "minimax interval+weekly x2 models");
        let broken = view.collectors.iter().find(|c| c.name == "broken").unwrap();
        assert!(broken.last_error.is_some());
        let okc = view
            .collectors
            .iter()
            .find(|c| c.name == "minimax")
            .unwrap();
        assert!(okc.last_error.is_none());
        assert!(okc.last_ok_at.is_some());

        // Read-only path after restart semantics: no commands run, stored
        // state still renders. The genuinely-broken collector keeps showing
        // its error (it never recovered); the healthy one stays clean.
        let again = collect_view(false);
        assert_eq!(again.snapshots.len(), 4);
        let broken2 = again
            .collectors
            .iter()
            .find(|c| c.name == "broken")
            .unwrap();
        assert!(broken2.last_error.is_some());
        let ok2 = again
            .collectors
            .iter()
            .find(|c| c.name == "minimax")
            .unwrap();
        assert!(ok2.last_error.is_none());
        assert!(ok2.last_ok_at.is_some());

        // And the log file exists with snap + err lines.
        let log = std::fs::read_to_string(log_path()).unwrap();
        assert!(log.contains("\"kind\":\"snap\""));
        assert!(log.contains("\"kind\":\"err\""));

        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// R82 异常推送:flagged 里只有非今天 → 静默;今天入榜 → 恰好一封,
    /// 正文带中位数与倍数,fire_once 状态落 `anomaly|<日期>` 键;
    /// `anomaly:false` 或 https URL 一律静默(opt-in 纪律)。
    #[test]
    fn anomaly_alert_pushes_only_today_and_marks_state() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_anomaly");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let req = read_full_request(&mut stream);
            let resp = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(resp);
            let _ = stream.flush();
            req
        });
        let alert = AlertConfig {
            webhook_url: format!("http://{addr}/hook"),
            threshold_percent: 80.0,
            format: String::new(),
            cooldown_hours: 24,
            rollover: false,
            daily_digest: false,
            anomaly: true,
            session_idle: false,
            idle_minutes: 15,
        };
        let cfg = QuotaConfig {
            collectors: vec![],
            alert: Some(alert),
            disabled_sources: vec![],
        };
        let today = crate::cn_day_label(crate::now_ts());
        let mkday = |date: &str| crate::store::AnomalyDay {
            date: date.to_string(),
            tokens: 2_000_000,
            baseline_median: 300_000,
            modified_z: 5.1,
            raw_z: 5.1,
            ratio: Some(6.7),
            baseline_days: 4,
            mad: 220_000.0,
            severity: "robust",
        };

        // 非今天的异常日:不推(历史异常是看板的事,推送只管正在发生)。
        let past = crate::store::AnomalyReport {
            checked_days: 10,
            window_days: 56,
            today_excluded: true,
            today_tokens: 0,
            today: today.clone(),
            flagged: vec![mkday("2026-01-01")],
        };
        maybe_anomaly_alert(&past, &cfg);

        // 今天入榜:恰好一封。
        let now_report = crate::store::AnomalyReport {
            checked_days: 10,
            window_days: 56,
            today_excluded: true,
            today_tokens: 0,
            today: String::new(),
            flagged: vec![mkday(&today)],
        };
        maybe_anomaly_alert(&now_report, &cfg);
        let req = server.join().unwrap();
        assert!(req.contains("异常用量"), "正文应可读: {req}");
        assert!(req.contains("300,000"));
        assert!(req.contains("6.7"), "2M/300k≈6.7 倍: {req}");

        let state = read_alert_state();
        assert!(state.contains_key(&format!("anomaly|{today}")));

        // opt-in 关闭(或 https)时静默:换一个不可能有服务的地址也不会
        // 尝试连接——标志位直接短路,连状态都不该新增。
        let mut off = cfg.clone();
        off.alert.as_mut().unwrap().anomaly = false;
        off.alert.as_mut().unwrap().webhook_url = "http://127.0.0.1:1/hook".into();
        maybe_anomaly_alert(&now_report, &off);

        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 千分位格式化:仅分隔,不取整不科学计数。
    #[test]
    fn fmt_thousands_groups_by_three() {
        assert_eq!(fmt_thousands(0), "0");
        assert_eq!(fmt_thousands(999), "999");
        assert_eq!(fmt_thousands(1_000), "1,000");
        assert_eq!(fmt_thousands(1_940_000_000), "1,940,000,000");
    }
}

#[cfg(test)]
mod idle_tests {
    use super::*;
    use crate::TEST_ENV_LOCK;

    fn row(source: &str, sid: &str, last_ts: i64, requests: u64) -> crate::store::SessionRow {
        crate::store::SessionRow {
            archetype: "标准".into(),
            source: source.into(),
            session_id: sid.into(),
            requests,
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            total_tokens: 0,
            credits: 0.0,
            avg_context_ratio: None,
            first_ts: last_ts - 600,
            last_ts,
            user_prompts: None,
            turns: None,
            tool_calls: None,
            from_runtime: false,
            output_tok_per_s: None,
            speed_requests: 0,
        }
    }

    /// 静默提醒:够活(≥3 请求)且静默过阈值才响;1 请求/刚活动/开关关
    /// 都不响;fire_once 键带日期,每会话每天一封。
    #[test]
    fn idle_knock_fires_once_per_session_and_filters_noise() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-idle-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let req = super::tests::read_full_request(&mut stream);
            let resp = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(resp);
            let _ = stream.flush();
            req
        });
        let cfg = QuotaConfig {
            collectors: vec![],
            disabled_sources: vec![],
            alert: Some(AlertConfig {
                webhook_url: format!("http://{addr}/hook"),
                threshold_percent: 80.0,
                format: String::new(),
                cooldown_hours: 24,
                rollover: false,
                daily_digest: false,
                anomaly: false,
                session_idle: true,
                idle_minutes: 15,
            }),
        };
        let now = crate::now_ts();
        let sessions = vec![
            row("zcode", "sess-aaa", now - 30 * 60, 5), // 命中:静默 30m,5 请求
            row("zcode", "sess-bbb", now - 60, 5),      // 刚活动:不响
            row("claude", "sess-ccc", now - 60 * 60, 1), // 1 请求:噪声,不响
            row("claude", "sess-ddd", 0, 50),           // 无活动:不响
        ];
        maybe_session_idle_alert(&sessions, &cfg);
        let req = server.join().unwrap();
        assert!(req.contains("会话静默"), "{req}");
        assert!(req.contains("sess-aaa"));
        assert!(req.contains("30 分钟"));

        let today = crate::cn_day_label(now);
        let state = read_alert_state();
        assert!(state.contains_key(&format!("idle|zcode|sess-aaa|{today}")));
        // 同一会话再调:冷却挡住,不再发送(监听已关也不尝试)。
        maybe_session_idle_alert(&sessions, &cfg);

        // 开关关:静默。
        let mut off = cfg.clone();
        off.alert.as_mut().unwrap().session_idle = false;
        off.alert.as_mut().unwrap().webhook_url = "http://127.0.0.1:1/hook".into();
        maybe_session_idle_alert(&sessions, &off);

        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod alert_test_tests {
    use super::*;
    use crate::TEST_ENV_LOCK;

    /// 测试告警:未配置/https 拒绝并给出人话;真监听收到消息体。
    #[test]
    fn test_alert_validates_and_sends() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-alert-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        let now = crate::now_ts();

        // 未配置:人话报错。
        let empty = QuotaConfig {
            collectors: vec![],
            alert: None,
            disabled_sources: vec![],
        };
        let err = send_test_alert(&empty, now).unwrap_err();
        assert!(err.contains("未配置"), "{err}");

        // https:拒绝并说明原因。
        let https = QuotaConfig {
            collectors: vec![],
            disabled_sources: vec![],
            alert: Some(AlertConfig {
                webhook_url: "https://example.com/hook".into(),
                threshold_percent: 80.0,
                format: String::new(),
                cooldown_hours: 12,
                rollover: false,
                daily_digest: false,
                anomaly: false,
                session_idle: false,
                idle_minutes: 15,
            }),
        };
        let err = send_test_alert(&https, now).unwrap_err();
        assert!(err.contains("http://"), "{err}");

        // 真监听:发出且正文可读。
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            use std::io::Write;
            let (mut stream, _) = listener.accept().unwrap();
            let req = super::tests::read_full_request(&mut stream);
            let resp = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(resp);
            let _ = stream.flush();
            req
        });
        let ok_cfg = QuotaConfig {
            collectors: vec![],
            disabled_sources: vec![],
            alert: Some(AlertConfig {
                webhook_url: format!("http://{addr}/hook"),
                threshold_percent: 80.0,
                format: String::new(),
                cooldown_hours: 12,
                rollover: false,
                daily_digest: false,
                anomaly: false,
                session_idle: false,
                idle_minutes: 15,
            }),
        };
        send_test_alert(&ok_cfg, now).unwrap();
        let req = server.join().unwrap();
        assert!(req.contains("告警测试"), "{req}");

        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
