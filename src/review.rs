//! 复盘产物层 — 日 / 周 / 月三档，**固定模板**，零模型调用。
//!
//! TokenBuddy 不接大模型，所以「复盘」不是让模型写一段感想，而是**规则把账本
//! 算成结论**：分母、集中度、返工热点、异常日、套餐余量，每条都能指回账本里的
//! 原始行。模板固定 → 同样的账本永远得到同样的复盘，可复查、可回归。
//!
//! 这一层只管「周期 / 产出 / 留存 / 读取」，渲染交给 [`crate::report`]：
//!
//! - **周期**按中国时区切：日=昨天，周=上一个完整自然周（周一起），月=上一个
//!   完整自然月。日界只在 [`crate::CN_OFFSET_SECS`] 这一个常量上算，别处不重写。
//! - **键即幂等键**：`reviews/<kind>/<key>.md` 在就不重算；唯一例外是账本比复盘
//!   新（午夜后的迟到 sync 补了旧行），此时按快照语义重算一次（[`stale`]）。
//! - 写入 tmp + rename，进程被杀不留半截；[`prune`] 只删本目录自己命名的文件。
//! - **不 spawn 采集器**：复盘是账本此刻的真。某天没同步过，那段复盘就是空的
//!   ——这是口径不是 bug，缺口由 [`gaps`] 如实报出。
//! - 产物 0700 / 0600，与账本同姿态；隐私是这产品的卖点，不交给 umask 决定。

use crate::store::Store;
use chrono::{Datelike, Duration, NaiveDate};
use std::path::PathBuf;

/// Which cadence a review closes over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Daily,
    Weekly,
    Monthly,
}

impl Kind {
    pub const ALL: [Kind; 3] = [Kind::Daily, Kind::Weekly, Kind::Monthly];

    /// Directory name and API value.
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Daily => "daily",
            Kind::Weekly => "weekly",
            Kind::Monthly => "monthly",
        }
    }

    /// Single character for the dashboard's switcher.
    pub fn title_cn(self) -> &'static str {
        match self {
            Kind::Daily => "日",
            Kind::Weekly => "周",
            Kind::Monthly => "月",
        }
    }

    /// 「前一日 / 前一周 / 前一月」——环比句的主语。
    pub fn prev_unit_cn(self) -> &'static str {
        match self {
            Kind::Daily => "前一日",
            Kind::Weekly => "上一周",
            Kind::Monthly => "上一月",
        }
    }

    /// Retention per cadence: daily is what you read, monthly is what you
    /// compare quarters against. A flat cap would keep 4 weeks of "weeks".
    pub fn cap(self) -> usize {
        match self {
            Kind::Daily => 30,
            Kind::Weekly => 52,
            Kind::Monthly => 24,
        }
    }

    pub fn parse(s: &str) -> Option<Kind> {
        Kind::ALL.into_iter().find(|k| k.as_str() == s)
    }
}

/// One closed period: where its artifact is filed, what it is called, and the
/// exact China-time window it covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Period {
    pub kind: Kind,
    /// Filename key — always sorts as text and steps by a fixed amount:
    /// daily `2026-10-06`, weekly `2026-10-11`（该周周日）, monthly `2026-09`.
    pub key: String,
    /// Human title: `2026-10-06（日）`, `2026-10-05~10-11（周）`, `2026-09（月）`.
    pub title: String,
    /// `[start, end)` in epoch seconds, China-local boundaries.
    pub start: i64,
    pub end: i64,
}

impl Period {
    pub fn days(&self) -> i64 {
        (self.end - self.start) / 86_400
    }
}

/// China-local calendar day of an epoch reading. The offset comes from the one
/// exported constant, so there is no second definition of the day boundary.
fn cn_date(ts: i64) -> NaiveDate {
    let off = chrono::FixedOffset::east_opt(crate::CN_OFFSET_SECS as i32)
        .expect("UTC+8 is a valid offset");
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.with_timezone(&off).date_naive())
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"))
}

/// China-local midnight of a calendar day.
fn midnight(day: NaiveDate) -> i64 {
    let off = chrono::FixedOffset::east_opt(crate::CN_OFFSET_SECS as i32)
        .expect("UTC+8 is a valid offset");
    day.and_hms_opt(0, 0, 0)
        .expect("midnight is always valid")
        .and_local_timezone(off)
        .single()
        .expect("China has no DST")
        .timestamp()
}

/// The most recent **closed** period of this cadence, as of `now`.
///
/// Weekly and monthly reviews never cover the period in progress: a week that
/// is still running would be re-rendered every tick and its "环比" would compare
/// a half week against a whole one. Closing the period is what makes the
/// conclusion stable.
pub fn last_closed(kind: Kind, now: i64) -> Period {
    let today = cn_date(now);
    match kind {
        Kind::Daily => {
            let day = today - Duration::days(1);
            Period {
                kind,
                key: day.format("%Y-%m-%d").to_string(),
                title: format!("{}（日）", day.format("%Y-%m-%d")),
                start: midnight(day),
                end: midnight(day + Duration::days(1)),
            }
        }
        Kind::Weekly => {
            // 周一起；本周起点往前一周 = 上一个完整自然周。
            let this_monday = today - Duration::days(today.weekday().num_days_from_monday() as i64);
            let last_monday = this_monday - Duration::days(7);
            let last_sunday = this_monday - Duration::days(1);
            Period {
                kind,
                key: last_sunday.format("%Y-%m-%d").to_string(),
                title: format!(
                    "{}~{}（周）",
                    last_monday.format("%m-%d"),
                    last_sunday.format("%m-%d")
                ),
                start: midnight(last_monday),
                end: midnight(this_monday),
            }
        }
        Kind::Monthly => {
            let first_this = today.with_day(1).expect("month always has day 1");
            let last_month_first = first_this - Duration::days(1);
            let last_month_first = last_month_first
                .with_day(1)
                .expect("month always has day 1");
            Period {
                kind,
                key: last_month_first.format("%Y-%m").to_string(),
                title: format!("{}（月）", last_month_first.format("%Y-%m")),
                start: midnight(last_month_first),
                end: midnight(first_this),
            }
        }
    }
}

// --------------------------------------------------------------- 产物存放

pub fn dir(kind: Kind) -> PathBuf {
    crate::data_dir().join("reviews").join(kind.as_str())
}

pub fn path_for(kind: Kind, key: &str) -> PathBuf {
    dir(kind).join(format!("{key}.md"))
}

/// Filename keys we are willing to delete or list: `YYYY-MM-DD` or `YYYY-MM`,
/// and nothing else — so a stray note in the directory survives retention.
pub fn is_key(kind: Kind, key: &str) -> bool {
    let b = key.as_bytes();
    let dash_count = b.iter().filter(|c| **c == b'-').count();
    match kind {
        // YYYY-MM-DD
        Kind::Daily | Kind::Weekly => {
            b.len() == 10 && dash_count == 2 && b.iter().all(|c| c.is_ascii_digit() || *c == b'-')
        }
        // YYYY-MM
        Kind::Monthly => {
            b.len() == 7 && dash_count == 1 && b.iter().all(|c| c.is_ascii_digit() || *c == b'-')
        }
    }
}

/// Snapshot staleness, as a pure rule.
///
/// A missing file always needs producing. An existing one is re-rendered only
/// when the ledger is newer — a late sync backfills the period's rows, and a
/// snapshot taken before that would understate it. When either mtime is
/// unreadable we keep the old snapshot rather than guess.
pub fn stale(
    file_exists: bool,
    file_mtime: Option<std::time::SystemTime>,
    ledger_mtime: Option<std::time::SystemTime>,
) -> bool {
    if !file_exists {
        return true;
    }
    match (file_mtime, ledger_mtime) {
        (Some(f), Some(l)) => l > f,
        _ => false,
    }
}

fn mtime(path: &std::path::Path) -> Option<std::time::SystemTime> {
    std::fs::metadata(path).ok().and_then(|m| m.modified().ok())
}

/// Does this period's review need (re)producing right now?
pub fn needs_produce(kind: Kind, key: &str) -> bool {
    let path = path_for(kind, key);
    let ledger = crate::data_dir().join("data.parquet");
    stale(path.exists(), mtime(&path), mtime(&ledger))
}

/// Keys present on disk, newest first.
fn keys(kind: Kind) -> Vec<String> {
    let mut out: Vec<String> = match std::fs::read_dir(dir(kind)) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter_map(|n| n.strip_suffix(".md").map(str::to_string))
            .filter(|k| is_key(kind, k))
            .collect(),
        Err(_) => Vec::new(),
    };
    out.sort_unstable_by(|a, b| b.cmp(a));
    out
}

/// One rendered review as the dashboard and `/api/reviews` serve it.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Entry {
    pub kind: String,
    pub key: String,
    pub title: String,
    pub bytes: u64,
    /// The markdown document, verbatim — `report::render` output, unedited.
    pub markdown: String,
}

fn read(kind: Kind, key: &str, title: &str) -> Option<Entry> {
    let text = std::fs::read_to_string(path_for(kind, key)).ok()?;
    Some(Entry {
        kind: kind.as_str().to_string(),
        key: key.to_string(),
        title: title.to_string(),
        bytes: text.len() as u64,
        markdown: text,
    })
}

/// The newest `limit` reviews of one cadence, each with its markdown.
pub fn list(kind: Kind, limit: usize) -> Vec<Entry> {
    keys(kind)
        .into_iter()
        .take(limit)
        .filter_map(|k| read(kind, &k, &title_of(kind, &k)))
        .collect()
}

/// Title for a stored key — recomputed from the key so a file written by an
/// older build still shows the right label.
fn title_of(kind: Kind, key: &str) -> String {
    match kind {
        Kind::Daily => format!("{key}（日）"),
        Kind::Weekly => {
            let Some(sunday) = NaiveDate::parse_from_str(key, "%Y-%m-%d").ok() else {
                return key.to_string();
            };
            let monday = sunday - Duration::days(6);
            format!(
                "{}~{}（周）",
                monday.format("%m-%d"),
                sunday.format("%m-%d")
            )
        }
        Kind::Monthly => format!("{key}（月）"),
    }
}

/// Write one review atomically, then trim retention.
pub fn write(kind: Kind, key: &str, markdown: &str) -> std::io::Result<()> {
    let d = dir(kind);
    std::fs::create_dir_all(&d)?;
    // A review is a period of your own usage: same privacy posture as the
    // ledger and quota.json (0700 / 0600), not the umask default.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o700))?;
    }
    let tmp = d.join(format!(".{key}.md.tmp"));
    std::fs::write(&tmp, markdown)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, path_for(kind, key))?;
    let _ = prune(kind);
    Ok(())
}

/// Keep the newest [`Kind::cap`] reviews of one cadence; returns how many went.
pub fn prune(kind: Kind) -> usize {
    let all = keys(kind);
    if all.len() <= kind.cap() {
        return 0;
    }
    let mut removed = 0;
    for key in &all[kind.cap()..] {
        if std::fs::remove_file(path_for(kind, key)).is_ok() {
            removed += 1;
        }
    }
    removed
}

/// Period keys walked back from `upto`, newest first. Shared by [`gaps`] and
/// the tests, so "one step back" is defined exactly once.
fn step_back(kind: Kind, key: &str) -> Option<String> {
    let date = |k: &str| NaiveDate::parse_from_str(k, "%Y-%m-%d").ok();
    match kind {
        Kind::Daily => date(key).map(|d| (d - Duration::days(1)).format("%Y-%m-%d").to_string()),
        Kind::Weekly => date(key).map(|d| (d - Duration::days(7)).format("%Y-%m-%d").to_string()),
        Kind::Monthly => {
            let first = NaiveDate::parse_from_str(&format!("{key}-01"), "%Y-%m-%d").ok()?;
            let prev = (first - Duration::days(1))
                .with_day(1)
                .expect("month always has day 1");
            Some(prev.format("%Y-%m").to_string())
        }
    }
}

/// How many periods inside the trailing `window` have no review on disk.
///
/// The dashboard shows this instead of presenting a gappy history as a
/// continuous one — a missed period is a fact, not a glitch.
pub fn gaps(kind: Kind, upto: &str, window: usize) -> usize {
    let have: std::collections::HashSet<String> = keys(kind).into_iter().collect();
    let mut cursor = Some(upto.to_string());
    let mut missing = 0;
    for _ in 0..window {
        let Some(key) = cursor else { break };
        if !have.contains(&key) {
            missing += 1;
        }
        cursor = step_back(kind, &key);
    }
    missing
}

// --------------------------------------------------------------- 固定结论区

/// A percentage rendered for humans, or an honest dash when the denominator is
/// zero. `None` never becomes `0%`.
fn pct(part: u64, whole: u64) -> String {
    if whole == 0 {
        "—".to_string()
    } else {
        format!("{:.1}%", part as f64 / whole as f64 * 100.0)
    }
}

fn delta(cur: u64, prev: u64) -> String {
    if prev == 0 {
        // 句子前缀已经是「较上一期」，这里只补结论，别再重复一遍主语。
        return if cur == 0 {
            "两期都无数据".to_string()
        } else {
            "无可比基数".to_string()
        };
    }
    let d = (cur as f64 - prev as f64) / prev as f64 * 100.0;
    if d.abs() < 0.5 {
        "基本持平".to_string()
    } else if d > 0.0 {
        format!("+{d:.1}%")
    } else {
        format!("{d:.1}%")
    }
}

fn hours(secs: u64) -> String {
    format!("{:.1}h", secs as f64 / 3600.0)
}

/// Everything the conclusion block reads. Kept as a struct so a rule change is
/// a field, not a signature migration across the render and its tests.
#[derive(Debug, Default)]
pub struct Conclusion {
    pub title: String,
    /// 「前一日 / 上一周 / 前一月」——由周期决定，不从标题反推。
    pub prev_unit: &'static str,
    pub window_desc: String,
    pub empty: bool,
    pub total_tokens: u64,
    pub requests: u64,
    pub delta: String,
    pub net_new: u64,
    pub net_new_share: String,
    pub output: u64,
    pub cache_share: String,
    pub subagent_share: String,
    pub top_source: Option<(String, u64, String)>,
    pub second_source: Option<String>,
    pub top_model: Option<(String, u64, String)>,
    pub top_project: Option<(String, u64, String)>,
    pub concentration: String,
    pub active: String,
    pub files_touched: u64,
    pub commands: u64,
    pub tests: u64,
    pub sessions: usize,
    pub rework_top: Option<(String, u64, u64)>,
    pub anomaly_days: Vec<(String, u64)>,
    pub quota_worst: Option<(String, String, f64)>,
    pub burn_rate: Option<String>,
}

/// Rule 1: totals **and** the two honest denominators. `total_tokens` is
/// 93–97% `cache_read` — it measures re-reading, not output, so a conclusion
/// built on it alone misleads (R-review discipline: 双分母必须并列).
/// Rule 2: concentration and leaders, so "who moved the number" is one read.
/// Rule 3: only what the ledger proves — receipts, anomaly days, quota. Never
/// a judgement the data cannot carry.
impl Conclusion {
    fn compute(store: &Store, period: &Period, input: &crate::report::ReportInput) -> Self {
        let s = &input.summary;
        let span = period.end - period.start;
        let prev_start = period.start - span;
        let prev = store.query_summary(None, None, Some(prev_start), Some(period.start));
        let prev_total = prev.map(|p| p.total_tokens).unwrap_or(0);

        let net_new = s.total_input_tokens + s.total_output_tokens;
        let cache_base = s.total_input_tokens + s.total_cache_read_tokens;

        // 来源/模型按 total 排序取头名；SourceRow 没有 total 列，用四项还原。
        let mut sources: Vec<(&str, u64)> = s
            .by_source
            .iter()
            .map(|r| {
                (
                    r.source.as_str(),
                    r.input_tokens
                        + r.output_tokens
                        + r.cache_read_tokens
                        + r.cache_creation_tokens,
                )
            })
            .collect();
        sources.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
        let mut models: Vec<(&str, u64)> = s
            .by_model
            .iter()
            .map(|r| (r.model.as_str(), r.total_tokens))
            .collect();
        models.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
        // 透视是 project × model，先按 project 合并再取头名。
        let mut projects: std::collections::BTreeMap<&str, u64> = std::collections::BTreeMap::new();
        for row in &input.pivot.rows {
            *projects.entry(row.project.as_str()).or_insert(0) += row.tokens;
        }
        let mut projects: Vec<(&str, u64)> = projects.into_iter().collect();
        projects.sort_by_key(|(_, n)| std::cmp::Reverse(*n));

        let top2: u64 = sources.iter().take(2).map(|(_, n)| *n).sum();
        let total_active: u64 = input.active.days.iter().map(|d| d.active_secs).sum();
        let bursts: u64 = input.active.days.iter().map(|d| d.bursts as u64).sum();

        let hot = crate::zcode::rework_hotspots(&input.receipts, 1)
            .into_iter()
            .next();

        // 异常日只取落在本周期内的：审计窗固定 56 天，不按周期收窄。
        let anomalies: Vec<(String, u64)> = input
            .anomalies
            .flagged
            .iter()
            .filter(|a| {
                let day = day_epoch(&a.date);
                day >= period.start && day < period.end
            })
            .map(|a| (a.date.clone(), a.tokens))
            .collect();

        let quota_worst = input
            .quota_snapshots
            .iter()
            .filter(|x| x.used_percent.is_finite())
            .max_by(|a, b| {
                a.used_percent
                    .partial_cmp(&b.used_percent)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|x| (x.source.clone(), x.window.clone(), x.used_percent));

        Self {
            title: period.title.clone(),
            prev_unit: period.kind.prev_unit_cn(),
            window_desc: {
                let from = crate::cn_day_label(period.start);
                // 日档的 end-1 仍落在同一天，写成「A → A」读起来像 bug。
                let to = crate::cn_day_label(period.end - 1);
                if from == to {
                    format!("{from}（UTC+8）")
                } else {
                    format!("{from} → {to}（UTC+8）")
                }
            },
            empty: s.total_requests == 0,
            total_tokens: s.total_tokens,
            requests: s.total_requests,
            delta: delta(s.total_tokens, prev_total),
            net_new,
            net_new_share: pct(net_new, s.total_tokens),
            output: s.total_output_tokens,
            cache_share: pct(s.total_cache_read_tokens, cache_base),
            subagent_share: pct(s.subagent_tokens, s.total_tokens),
            top_source: sources
                .first()
                .map(|(n, v)| (n.to_string(), *v, pct(*v, s.total_tokens))),
            second_source: sources.get(1).map(|(n, v)| {
                format!(
                    "{} {}（{}）",
                    n,
                    crate::format_tokens(*v),
                    pct(*v, s.total_tokens)
                )
            }),
            top_model: models
                .first()
                .map(|(n, v)| (n.to_string(), *v, pct(*v, s.total_tokens))),
            top_project: projects
                .first()
                .map(|(n, v)| (n.to_string(), *v, pct(*v, s.total_tokens))),
            concentration: pct(top2, s.total_tokens),
            active: format!("{}（{} 个工作段）", hours(total_active), bursts),
            files_touched: input.receipts.iter().map(|r| r.files.len() as u64).sum(),
            commands: input.receipts.iter().map(|r| r.bash_count).sum(),
            tests: input.receipts.iter().map(|r| r.test_count).sum(),
            sessions: input.receipts.len(),
            rework_top: hot.map(|h| (h.path, h.sessions, h.edits)),
            anomaly_days: anomalies,
            quota_worst,
            burn_rate: input
                .windows
                .open_window
                .as_ref()
                .map(|w| format!("{}/h", crate::format_tokens(w.burn_per_hour as u64))),
        }
    }

    /// The fixed template. Every line is a fact with a number; anything the
    /// ledger cannot answer is left out rather than smoothed over.
    pub fn render(&self) -> String {
        let mut o = String::new();
        o.push_str(&format!(
            "> **固定模板** · 全部由规则从账本算出，不调用任何模型。窗口 {}\n",
            self.window_desc
        ));
        if self.empty {
            o.push_str("\n## 结论\n\n这一段没有任何请求。可能是没同步，也可能是真的没用——复盘只说账本里有的东西。\n");
            return o;
        }
        o.push_str("\n## 结论\n\n");
        o.push_str(&format!(
            "{} 共 **{}** tokens / **{}** 请求 / 投入 {}，较{} {}\n",
            self.title,
            crate::format_tokens(self.total_tokens),
            self.requests,
            self.active,
            self.prev_unit,
            self.delta,
        ));
        o.push_str(&format!(
            "- **真实新增（输入+输出）** {} —— 只占总 tokens 的 {}。其余是缓存重读，衡量重读不是产出\n",
            crate::format_tokens(self.net_new),
            self.net_new_share
        ));
        o.push_str(&format!("- **输出** {}", crate::format_tokens(self.output)));
        o.push_str(&format!(
            " · **缓存承接** 输入侧 {} 来自缓存\n",
            self.cache_share
        ));
        o.push_str(&format!(
            "- **子代理占比** {}（只在有 sidechain 归属的行上统计）",
            self.subagent_share
        ));
        if let Some(r) = &self.burn_rate {
            o.push_str(&format!(" · **当前燃速** {r}"));
        }
        o.push('\n');

        o.push_str("\n## 谁动的这个数\n\n");
        match &self.top_source {
            Some((name, v, share)) => o.push_str(&format!(
                "- 最大来源 **{}** {}（{}）",
                name,
                crate::format_tokens(*v),
                share
            )),
            None => o.push_str("- 账本里没有按来源拆分的记录"),
        };
        if let Some(second) = &self.second_source {
            o.push_str(&format!("，第二 {second}"));
        }
        o.push_str(&format!(
            "。Top2 合计 **{}**——越集中，波动越由它们决定\n",
            self.concentration
        ));
        if let Some((m, v, share)) = &self.top_model {
            o.push_str(&format!(
                "- 最大模型 **{}** {}（{}）\n",
                m,
                crate::format_tokens(*v),
                share
            ));
        }
        if let Some((p, v, share)) = &self.top_project {
            o.push_str(&format!(
                "- 最大工程 **{}** {}（{}）\n",
                p,
                crate::format_tokens(*v),
                share
            ));
        }

        o.push_str("\n## 工作产出\n\n");
        if self.sessions == 0 {
            o.push_str("没有工作收据（仅 ZCode / Claude / OpenCode 提供这条通道）。\n");
        } else {
            o.push_str(&format!(
                "- 改动文件 **{}** 个 · 命令 **{}** 次 · 测试 **{}** 次（{} 个会话）\n",
                self.files_touched, self.commands, self.tests, self.sessions
            ));
            if let Some((path, sessions, edits)) = &self.rework_top {
                o.push_str(&format!(
                    "- 返工热点：**{}**（{} 个会话 / {} 次改动）—— 被 ≥2 个会话反复编辑\n",
                    path, sessions, edits
                ));
            }
        }

        o.push_str("\n## 值得注意\n\n");
        if self.anomaly_days.is_empty() {
            o.push_str("- 本周期没有异常日\n");
        } else {
            for (date, tokens) in &self.anomaly_days {
                o.push_str(&format!(
                    "- 异常日 **{}**：{}（详见下方异常表）\n",
                    date,
                    crate::format_tokens(*tokens)
                ));
            }
        }
        if let Some((src, win, used)) = &self.quota_worst {
            o.push_str(&format!(
                "- 套餐余量最高的一档：**{src}** {win} 窗口 {used:.1}%（采集于复盘产出时刻）\n"
            ));
        }
        o
    }
}

fn day_epoch(date: &str) -> i64 {
    NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .map(midnight)
        .unwrap_or(0)
}

// ------------------------------------------------------------------- 产出

/// Everything one period's report needs, **already bounded on both ends**.
///
/// Same queries as `tokenbuddy report --days N`, so the numbers are the CLI's
/// by construction — but the window is the closed period, not "start → now".
/// Doing it in one pass is also the reason the conclusion and the ledger
/// detail agree: an earlier version built the detail with an open end and
/// narrowed it afterwards, so the summary above the line counted today while
/// the table below it did not.
fn build_input(
    store: &Store,
    start: i64,
    end: i64,
    days: i64,
) -> anyhow::Result<crate::report::ReportInput> {
    // Read-only quota view: collectors' stored readings, no process spawned.
    let quota = crate::quota::collect_view(false);
    Ok(crate::report::ReportInput {
        days,
        period_label: None,
        conclusion: None,
        summary: store.query_summary(None, None, Some(start), Some(end))?,
        metrics: store.query_metrics(None, None, Some(start), Some(end))?,
        windows: store.query_windows()?,
        anomalies: store.query_anomalies()?,
        pivot: store.query_pivot(Some(start), Some(end))?,
        active: store.query_active_time(Some(start), Some(end))?,
        quota_snapshots: quota.snapshots,
        receipts: crate::zcode::merged_receipts(start),
        forecast: store.query_week_forecast()?,
    })
}

/// Rebuild one period from its filename key — the backfill walk (issue #77)
/// goes backwards through `step_back` keys and needs the same `[start, end)`
/// window `last_closed` would have produced for that period. Weekly keys are
/// that week's Sunday; monthly keys are `YYYY-MM`.
fn period_from_key(kind: Kind, key: &str) -> Option<Period> {
    let date = |k: &str| NaiveDate::parse_from_str(k, "%Y-%m-%d").ok();
    match kind {
        Kind::Daily => date(key).map(|day| Period {
            kind,
            key: key.to_string(),
            title: format!("{key}（日）"),
            start: midnight(day),
            end: midnight(day + Duration::days(1)),
        }),
        Kind::Weekly => date(key).map(|sunday| {
            let monday = sunday - Duration::days(6);
            Period {
                kind,
                key: key.to_string(),
                title: format!(
                    "{}~{}（周）",
                    monday.format("%m-%d"),
                    sunday.format("%m-%d")
                ),
                start: midnight(monday),
                end: midnight(sunday + Duration::days(1)),
            }
        }),
        Kind::Monthly => {
            let first = NaiveDate::parse_from_str(&format!("{key}-01"), "%Y-%m-%d").ok()?;
            let next_first = (first + Duration::days(31))
                .with_day(1)
                .expect("month always has day 1");
            Some(Period {
                kind,
                key: key.to_string(),
                title: format!("{key}（月）"),
                start: midnight(first),
                end: midnight(next_first),
            })
        }
    }
}

/// Render one closed period to disk. `Ok(None)` = already there and current.
pub fn produce(store: &Store, kind: Kind, now: i64) -> anyhow::Result<Option<String>> {
    let period = last_closed(kind, now);
    if !needs_produce(kind, &period.key) {
        return Ok(None);
    }
    produce_key(store, &period)
}

/// Render a specific (already resolved) period. Shared by the current-period
/// pass and the backfill walk.
fn produce_key(store: &Store, period: &Period) -> anyhow::Result<Option<String>> {
    let mut input = build_input(store, period.start, period.end, period.days())?;
    // 结论区必须读封口后的那份 input——先算结论再收窄窗口会让两个数字打架。
    let conclusion = Conclusion::compute(store, period, &input);
    input.period_label = Some(period.title.clone());
    input.conclusion = Some(conclusion.render());
    write(period.kind, &period.key, &crate::report::render(&input))?;
    Ok(Some(period.key.clone()))
}

/// How far the backfill walk may reach in one tick. Purely a runaway guard:
/// the walk stops at the ledger's first record long before this on a young
/// install.
const BACKFILL_MAX_PERIODS: usize = 400;
/// 每个 tick 最多补几个周期。一次长停机后的首次 catch_up 若把积压全部
/// 渲完,每个周期 7 次账本扫描会把 serve 卡上几分钟——留给 5 分钟巡检
/// 线程逐 tick 消化,正常停机跨度(几天)一个 tick 就能补完。
const BACKFILL_PER_TICK: usize = 8;

/// Produce every cadence that is due, logging and swallowing failures.
///
/// A missed review must not become a crash loop: it is a nice-to-have the next
/// tick retries, and the failure shows up honestly as a gap.
pub fn catch_up(store: &Store, now: i64) {
    for kind in Kind::ALL {
        match produce(store, kind, now) {
            Ok(Some(key)) => eprintln!("[TokenBuddy] 复盘已产出：{} {key}", kind.as_str()),
            Ok(None) => {}
            Err(e) => eprintln!("[TokenBuddy] 复盘产出失败（{}）：{e:#}", kind.as_str()),
        }
        backfill(store, kind, now);
    }
}

/// Backfill closed periods the serve was down for (issue #77).
///
/// `produce` only ever looks at `last_closed` — one period. A stretch of
/// downtime (the host asleep, a long trip) leaves every period it spanned
/// permanently unproduced while `gaps()` keeps counting them: 10-09 had
/// 4.74M real tokens and never got a review. The walk goes backwards from
/// the last closed period down to the ledger's first record, producing
/// every missing file; a hole can sit behind an existing file (10-08
/// present, 10-09 missing), so "stop at the first existing period" would
/// re-create exactly the bug being fixed. Bounds: the ledger's first record
/// (nothing earlier can have content) and the runaway cap. The ledger-mtime
/// freshness rule stays out of the walk on purpose: applying it to history
/// would re-render every old file on every sync, forever.
fn backfill(store: &Store, kind: Kind, now: i64) {
    let first_ts = match store.query_first_ts() {
        Ok(ts) => ts,
        Err(_) => return, // 账本读不了就不回填,下个 tick 再试
    };
    let Some(first_ts) = first_ts else { return }; // 空账本:没有任何周期可能有内容
    let mut key = last_closed(kind, now).key;
    let mut produced = 0;
    for _ in 0..BACKFILL_MAX_PERIODS {
        if produced >= BACKFILL_PER_TICK {
            return; // 余下的下个 tick 继续
        }
        let Some(prev) = step_back(kind, &key) else {
            break;
        };
        key = prev;
        let Some(period) = period_from_key(kind, &key) else {
            break;
        };
        if period.end <= first_ts {
            return; // 整个周期都在第一笔数据之前,更早的也不可能有了
        }
        if path_for(kind, &key).exists() {
            continue; // 已补过/本来就有:继续往前找洞
        }
        match produce_key(store, &period) {
            Ok(_) => {
                produced += 1;
                eprintln!("[TokenBuddy] 复盘回填：{} {}", kind.as_str(), period.key)
            }
            Err(e) => {
                eprintln!(
                    "[TokenBuddy] 复盘回填失败（{} {}）：{e:#}",
                    kind.as_str(),
                    period.key
                );
                return; // 同一根因会连续失败,停止走避免刷屏
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Scratch {
        _guard: std::sync::MutexGuard<'static, ()>,
        home: PathBuf,
    }

    impl Scratch {
        fn new(name: &str) -> Self {
            let guard = crate::TEST_ENV_LOCK
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let home = crate::unique_test_dir(name);
            std::fs::create_dir_all(&home).unwrap();
            std::env::set_var("TOKENBUDDY_HOME", &home);
            Self {
                _guard: guard,
                home,
            }
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            std::env::remove_var("TOKENBUDDY_HOME");
        }
    }

    /// 测试锚点：把「今天」钉在 2026-10-07（周三）22:00 CST，周/月的边界才有
    /// 确定答案。日期由 chrono 算出来而不是手填 epoch —— 手填过一次，差了 9 天，
    /// 三个周期测试一起红，看起来像周期逻辑坏了。
    fn wed() -> i64 {
        let off = chrono::FixedOffset::east_opt(crate::CN_OFFSET_SECS as i32).unwrap();
        NaiveDate::from_ymd_opt(2026, 10, 7)
            .unwrap()
            .and_hms_opt(22, 0, 0)
            .unwrap()
            .and_local_timezone(off)
            .unwrap()
            .timestamp()
    }

    /// A China-local timestamp on a given date, for pinning period edges.
    fn at(y: i32, m: u32, d: u32) -> i64 {
        let off = chrono::FixedOffset::east_opt(crate::CN_OFFSET_SECS as i32).unwrap();
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_local_timezone(off)
            .unwrap()
            .timestamp()
    }

    #[test]
    fn daily_period_is_yesterday_cn() {
        let p = last_closed(Kind::Daily, wed());
        assert_eq!(p.key, "2026-10-06");
        assert_eq!(p.title, "2026-10-06（日）");
        assert_eq!(p.end - p.start, 86_400);
        assert_eq!(crate::cn_day_label(p.start), "2026-10-06");
        assert_eq!(crate::cn_day_label(p.end), "2026-10-07");
    }

    #[test]
    fn weekly_period_is_the_last_closed_monday_to_sunday() {
        let p = last_closed(Kind::Weekly, wed());
        // 本周周一 = 10-05，所以上一个完整周是 09-28(一) ~ 10-04(日)。
        assert_eq!(crate::cn_day_label(p.start), "2026-09-28");
        assert_eq!(crate::cn_day_label(p.end), "2026-10-05");
        assert_eq!(p.key, "2026-10-04", "key is that week's Sunday");
        assert_eq!(p.title, "09-28~10-04（周）");
        assert_eq!(p.days(), 7);
    }

    #[test]
    fn monthly_period_is_the_last_closed_month() {
        let p = last_closed(Kind::Monthly, wed());
        assert_eq!(p.key, "2026-09");
        assert_eq!(p.title, "2026-09（月）");
        assert_eq!(crate::cn_day_label(p.start), "2026-09-01");
        assert_eq!(crate::cn_day_label(p.end), "2026-10-01");
        assert_eq!(p.days(), 30);
    }

    #[test]
    fn january_rolls_back_into_the_previous_year() {
        // 2027-01-01 是周五：上一个完整月必须是 2026-12（不是 2027-01 本身）。
        let edge = at(2027, 1, 1);
        let p = last_closed(Kind::Monthly, edge);
        assert_eq!(p.key, "2026-12");
        assert_eq!(crate::cn_day_label(p.start), "2026-12-01");
        assert_eq!(crate::cn_day_label(p.end), "2027-01-01");
        assert_eq!(p.days(), 31, "2026-12 is 31 days");
        // 周档同样跨年：这一周的周日属于 2026，键也必须是 2026 的日期。
        let w = last_closed(Kind::Weekly, edge);
        assert_eq!(
            w.key, "2026-12-27",
            "the Sunday closing 2026-12-28..2027-01-03"
        );
    }

    /// issue #77:停机跨过的封口周期必须回填——洞可以藏在已有文件后面
    /// (10-08 在,10-09 不在),所以「走到第一个已存在的就停」会原样复发。
    /// 下界:第一笔数据之前的周期没有内容,不渲染。
    #[test]
    fn backfill_fills_holes_behind_existing_files_and_stops_at_first_record() {
        let s = Scratch::new("review-backfill");
        let now = wed(); // 2026-10-07 22:00 CST → 当前封口日 = 10-06
                         // 账本第一笔钉在 2026-10-01:回填到它为止,更早的周期不再渲染。
        let record = crate::TokenRecord {
            source: crate::Source::Claude,
            model: "m".into(),
            input_tokens: 10,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            timestamp: at(2026, 10, 1),
            session_id: Some("s".into()),
            project: String::new(),
            sidechain: false,
            duration_ms: None,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: None,
            merge_key: None,
            request_count: 1,
        };
        let batch = crate::store::records_to_batch(&[("k".into(), record)]);
        crate::store::write_parquet(&s.home.join("data.parquet"), &batch).unwrap();
        let store = crate::store::Store::open().unwrap();

        catch_up(&store, now);
        let days = [
            "2026-10-06",
            "2026-10-05",
            "2026-10-04",
            "2026-10-03",
            "2026-10-02",
            "2026-10-01",
        ];
        for day in days {
            assert!(path_for(Kind::Daily, day).exists(), "{day} 应被产出/回填");
        }
        assert!(
            !path_for(Kind::Daily, "2026-09-30").exists(),
            "第一笔数据之前的周期不回填"
        );
        assert_eq!(
            gaps(Kind::Daily, "2026-10-06", 6),
            0,
            "回填后缺口归零(#77 的验收口径)"
        );
        // 周回填:含 10-01 的那一周(09-28~10-04,key 10-04)有内容;再往前
        // 的周整段都在第一笔之前,不渲染。
        assert!(path_for(Kind::Weekly, "2026-10-04").exists());
        assert!(!path_for(Kind::Weekly, "2026-09-27").exists());

        // 已有文件后面的洞:删掉 10-04,再跑一次只补它(其他文件原样保留)。
        std::fs::remove_file(path_for(Kind::Daily, "2026-10-04")).unwrap();
        catch_up(&store, now);
        assert!(path_for(Kind::Daily, "2026-10-04").exists(), "洞要能补上");
        assert_eq!(gaps(Kind::Daily, "2026-10-06", 6), 0);
    }

    #[test]
    fn periods_never_cover_the_period_in_progress() {
        for kind in Kind::ALL {
            let p = last_closed(kind, wed());
            assert!(
                p.end <= crate::cn_midnight(0) + 86_400,
                "{} leaks into today",
                kind.as_str()
            );
            assert_eq!(
                crate::cn_day_label(p.end),
                crate::cn_day_label(last_closed(kind, wed()).end),
                "{} end is stable",
                kind.as_str()
            );
        }
    }

    #[test]
    fn key_gate_only_admits_its_own_shape() {
        assert!(is_key(Kind::Daily, "2026-10-06"));
        assert!(is_key(Kind::Weekly, "2026-10-04"));
        assert!(is_key(Kind::Monthly, "2026-09"));
        assert!(!is_key(Kind::Monthly, "2026-10-06"), "monthly is YYYY-MM");
        assert!(!is_key(Kind::Daily, "2026-09"), "daily needs the day");
        assert!(!is_key(Kind::Daily, "../data"));
        assert!(!is_key(Kind::Daily, "notes"));
    }

    #[test]
    fn stepping_back_is_one_period_per_step() {
        assert_eq!(
            step_back(Kind::Daily, "2026-10-06").as_deref(),
            Some("2026-10-05")
        );
        assert_eq!(
            step_back(Kind::Weekly, "2026-10-04").as_deref(),
            Some("2026-09-27")
        );
        assert_eq!(
            step_back(Kind::Monthly, "2026-03").as_deref(),
            Some("2026-02")
        );
        // 跨年：1 月回退到 12 月。
        assert_eq!(
            step_back(Kind::Monthly, "2026-01").as_deref(),
            Some("2025-12")
        );
        assert_eq!(
            step_back(Kind::Daily, "2026-01-01").as_deref(),
            Some("2025-12-31")
        );
    }

    #[test]
    fn gaps_counts_missing_periods_per_cadence() {
        let _s = Scratch::new("review-gaps");
        write(Kind::Daily, "2026-10-05", "# x").unwrap();
        write(Kind::Daily, "2026-10-06", "# x").unwrap();
        // 10-06 往前 4 期：10-06/10-05 有，10-04/10-03 缺。
        assert_eq!(gaps(Kind::Daily, "2026-10-06", 4), 2);
        assert_eq!(gaps(Kind::Weekly, "2026-10-04", 3), 3);
        assert_eq!(gaps(Kind::Monthly, "2026-09", 2), 2);
    }

    #[test]
    fn retention_is_per_cadence_and_leaves_foreign_files_alone() {
        let _s = Scratch::new("review-prune");
        // write() 自己就带一次 prune，所以这里断言的是终态：写 32 篇、留 30 篇。
        for i in 0..32 {
            let d = NaiveDate::from_ymd_opt(2026, 9, 6).unwrap() + Duration::days(i);
            write(Kind::Daily, &d.format("%Y-%m-%d").to_string(), "# x").unwrap();
        }
        std::fs::write(dir(Kind::Daily).join("README.txt"), "keep me").unwrap();
        let left = list(Kind::Daily, 100);
        assert_eq!(left.len(), 30, "cap 30 keeps the newest 30");
        assert_eq!(left[0].key, "2026-10-07");
        assert_eq!(
            left[29].key, "2026-09-08",
            "oldest kept, not oldest written"
        );
        assert!(
            dir(Kind::Daily).join("README.txt").exists(),
            "non-review survives"
        );
        assert_eq!(prune(Kind::Daily), 0, "already at cap → no-op");

        // 周档的 cap 是 52：32 篇不动。
        for i in 0..32 {
            let d = NaiveDate::from_ymd_opt(2026, 1, 4).unwrap() + Duration::weeks(i);
            write(Kind::Weekly, &d.format("%Y-%m-%d").to_string(), "# x").unwrap();
        }
        assert_eq!(prune(Kind::Weekly), 0, "周档 cap 52，32 篇不动");
        assert_eq!(list(Kind::Weekly, 100).len(), 32);
    }

    #[test]
    fn listing_is_newest_first_with_titles_rebuilt_from_keys() {
        let _s = Scratch::new("review-list");
        write(Kind::Weekly, "2026-09-27", "# x").unwrap();
        write(Kind::Weekly, "2026-10-04", "# x").unwrap();
        let got = list(Kind::Weekly, 10);
        assert_eq!(got[0].key, "2026-10-04");
        assert_eq!(got[0].title, "09-28~10-04（周）");
        assert_eq!(got[1].key, "2026-09-27");
        assert_eq!(list(Kind::Weekly, 1).len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn reviews_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let _s = Scratch::new("review-perm");
        write(Kind::Daily, "2026-10-06", "# x").unwrap();
        let mode = |p: PathBuf| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(dir(Kind::Daily)), 0o700);
        assert_eq!(mode(path_for(Kind::Daily, "2026-10-06")), 0o600);
    }

    #[test]
    fn produce_writes_all_three_cadences_and_is_idempotent() {
        let _s = Scratch::new("review-produce");
        let store = Store::open().unwrap();

        for kind in Kind::ALL {
            let key = produce(&store, kind, wed()).unwrap().expect("produced");
            assert_eq!(key, last_closed(kind, wed()).key);
            let md = &list(kind, 1)[0].markdown;
            assert!(
                md.starts_with(&format!(
                    "# TokenBuddy 复盘 · {}",
                    last_closed(kind, wed()).title
                )),
                "{kind:?} titled with its own period: {:.70}",
                md
            );
            assert!(md.contains("## 结论"), "{kind:?} has the fixed conclusion");
            assert!(md.contains("固定模板"), "{kind:?} says it is rule-computed");
            assert!(
                md.contains("## 账本明细"),
                "{kind:?} keeps the ledger detail"
            );
        }
        // Second pass: nothing re-rendered.
        for kind in Kind::ALL {
            assert_eq!(
                produce(&store, kind, wed()).unwrap(),
                None,
                "{kind:?} idempotent"
            );
        }
    }

    #[test]
    fn needs_produce_follows_the_ledger_mtime() {
        let _s = Scratch::new("review-stale");
        let ledger = _s.home.join("data.parquet");
        std::fs::write(&ledger, b"rows").unwrap();
        assert!(needs_produce(Kind::Daily, "2026-10-06"));
        write(Kind::Daily, "2026-10-06", "# x").unwrap();
        assert!(!needs_produce(Kind::Daily, "2026-10-06"));
        std::thread::sleep(std::time::Duration::from_millis(1100));
        std::fs::write(&ledger, b"more rows").unwrap();
        assert!(
            needs_produce(Kind::Daily, "2026-10-06"),
            "ledger newer → re-render"
        );
    }

    #[test]
    fn conclusion_never_invents_a_denominator() {
        let c = Conclusion {
            empty: true,
            title: "2026-10-06（日）".to_string(),
            window_desc: "x".to_string(),
            ..Default::default()
        };
        let md = c.render();
        assert!(md.contains("没有任何请求"), "{md}");
        assert!(
            !md.contains("0.0%"),
            "no percentages invented on an empty period"
        );
        assert_eq!(pct(0, 0), "—");
        assert_eq!(pct(1, 4), "25.0%");
        assert_eq!(delta(0, 0), "两期都无数据");
        assert_eq!(delta(5, 0), "无可比基数", "前缀已说「较上一期」");
        assert_eq!(delta(110, 100), "+10.0%");
        assert_eq!(delta(99, 100), "-1.0%");
        assert_eq!(delta(100, 100), "基本持平");
    }
}
