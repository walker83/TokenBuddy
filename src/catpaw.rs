//! 妙手 CatPaw（issue #19）——本仓第一个**无拆分来源**。
//!
//! 云端只给**按天 token 总量**：没有输入/输出拆分、没有缓存维度、没有模型
//! 维度。这一族来源的难点从来不是解析，是**口径**——它正好撞上本仓
//! 「不编造数字」和「账本加列」两条纪律。
//!
//! 口径（B 方案，与 issue 同向）：总量进 `unsplit_tokens` 列，是**原始
//! 事实**——既不伪装成 input_tokens（那会污染按输入排序的榜），也不丢掉
//! （那是这个源唯一的价值）。模型维度记 `unsplit`，不假装知道；时间戳
//! 落在当天**本地正午**（云端只给日期，没有时刻）。
//!
//! 传输：本构建**无 TLS 栈**，且 TokenBuddy 永不主动出网（CLAUDE.md
//! 「零外呼与网络」）。所以云端那一段由用户配置的**命令**去取——命令负责
//! HTTPS 与凭据解密，本模块只负责口径。命令契约：stdout 打印
//! `{"daily":[{"date":"2026-10-18","totalTokens":12345}]}`（也接受裸数组）。
//! 命令只在显式调用时跑（GET 永不 spawn）。
//!
//! 重复计数（差量）：云端当天桶随用随涨，**整包重发会被当成新行记两次**。
//! 账本里该 (account, date) 已有的 unsplit 合计就是基线，只把
//! `cloud_total - baseline` 的增量写成新行；桶没长就不写。

use anyhow::{anyhow, Result};
use std::collections::HashMap;

use crate::{Source, TokenRecord};

/// 模型维度：云端不告诉我是哪个模型，如实记 `unsplit`。
pub const MODEL_LABEL: &str = "unsplit";

/// 云端按天 token 总量的一行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyPoint {
    /// `YYYY-MM-DD`
    pub date: String,
    pub total_tokens: u64,
}

/// 解析命令 stdout。接受两种形状：
/// `{"daily":[{"date":"2026-10-18","totalTokens":123}]}`（云端原样）
/// 与裸数组 `[{"date":…,"totalTokens":…}]`。
pub fn parse_daily_json(text: &str) -> Result<Vec<DailyPoint>> {
    let v: serde_json::Value =
        serde_json::from_str(text.trim()).map_err(|e| anyhow!("命令输出不是合法 JSON：{e}"))?;
    let arr = match &v {
        serde_json::Value::Array(a) => a.clone(),
        serde_json::Value::Object(_) => v
            .get("daily")
            .and_then(|d| d.as_array())
            .cloned()
            .ok_or_else(|| anyhow!("命令输出缺少 daily 数组"))?,
        _ => return Err(anyhow!("命令输出既不是对象也不是数组")),
    };
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let date = item
            .get("date")
            .and_then(|d| d.as_str())
            .ok_or_else(|| anyhow!("daily 条目缺 date 字段"))?
            .to_string();
        if parse_date(&date).is_none() {
            return Err(anyhow!("daily 条目的 date 不是 YYYY-MM-DD：{date}"));
        }
        // 字段名两种都收：云端是 totalTokens，脚本手写常是 total_tokens。
        let total = item
            .get("totalTokens")
            .or_else(|| item.get("total_tokens"))
            .and_then(|t| t.as_u64())
            .ok_or_else(|| anyhow!("daily 条目 {date} 缺 totalTokens"))?;
        out.push(DailyPoint {
            date,
            total_tokens: total,
        });
    }
    Ok(out)
}

/// 解析 `YYYY-MM-DD`，不做别的校验。
fn parse_date(date: &str) -> Option<chrono::NaiveDate> {
    chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()
}

/// 当天**本地正午**的 UTC epoch 秒——任何时区读它都落在那一天。
pub fn noon_ts(date: &str) -> Result<i64> {
    let d = parse_date(date).ok_or_else(|| anyhow!("date 不是 YYYY-MM-DD：{date}"))?;
    let naive = d.and_hms_opt(12, 0, 0).expect("12:00 is a valid time");
    // 本地(CN, UTC+8)正午 = UTC 04:00
    Ok(naive.and_utc().timestamp() - crate::CN_OFFSET_SECS)
}

/// 账本里某个 (account, date) 的基线：已记的 unsplit 合计与行数。
/// 行数用来给同日的新增量段编一个不撞号的 seq。
pub type Baselines = HashMap<String, (u64, u64)>;

/// 一个计划写进账本的行：`(dedupe key, record)`。
pub type PlannedRow = (String, TokenRecord);

/// 按云端每日总量与账本基线，算出**该写哪些增量行**。
///
/// 规则（issue #19 的验收）：
/// - 云端总量 ≤ 基线 → 不写（桶没长，或云端回退了——回退不删账本里的历史）
/// - 云端总量 > 基线 → 写一行差量，dedupe key = `catpaw_<account>_<date>_<seq>`
/// - seq = 该 (account, date) 已有行数，保证同日多段差量键不撞
pub fn plan_rows(
    points: &[DailyPoint],
    account: &str,
    baselines: &Baselines,
) -> Result<Vec<PlannedRow>> {
    let mut rows = Vec::new();
    for p in points {
        let ts = noon_ts(&p.date)?;
        let key = format!("{account}_{}", p.date);
        let (base_sum, base_rows) = baselines.get(&key).copied().unwrap_or((0, 0));
        if p.total_tokens <= base_sum {
            continue; // 桶没长（或云端回退）：不重复记账
        }
        let delta = p.total_tokens - base_sum;
        let seq = base_rows;
        rows.push((
            format!("catpaw_{account}_{}_{seq}", p.date),
            TokenRecord {
                source: Source::CatPaw,
                model: MODEL_LABEL.to_string(),
                input_tokens: 0,
                output_tokens: 0,
                cache_read_tokens: 0,
                cache_creation_tokens: 0,
                unsplit_tokens: delta,
                timestamp: ts,
                session_id: Some(key),
                project: String::new(),
                sidechain: false,
                duration_ms: None,
                ttft_ms: None,
                credits: 0.0,
                context_ratio: 0.0,
                record_id: None, // key 由调用方在落盘时定死
                merge_key: None,
                request_count: 1,
            },
        ));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_cloud_shape_and_a_bare_array() {
        let cloud = r#"{"daily":[{"date":"2026-10-18","totalTokens":13755212}]}"#;
        let pts = parse_daily_json(cloud).unwrap();
        assert_eq!(pts.len(), 1);
        assert_eq!(pts[0].date, "2026-10-18");
        assert_eq!(pts[0].total_tokens, 13_755_212);

        let bare = r#"[{"date":"2026-10-18","total_tokens":42}]"#;
        let pts = parse_daily_json(bare).unwrap();
        assert_eq!(pts[0].total_tokens, 42);
    }

    #[test]
    fn bad_command_output_is_an_error_not_a_silent_zero() {
        assert!(parse_daily_json("not json").is_err());
        assert!(parse_daily_json("{}").is_err());
        assert!(parse_daily_json(r#"{"daily":[{"totalTokens":1}]}"#).is_err());
        assert!(parse_daily_json(r#"{"daily":[{"date":"2026/10/18","totalTokens":1}]}"#).is_err());
    }

    #[test]
    fn noon_anchor_lands_inside_the_day_in_cn() {
        let ts = noon_ts("2026-10-18").unwrap();
        let day = crate::cn_day_label(ts);
        assert_eq!(day, "2026-10-18", "本地正午必须落在当天");
        // 正午 = UTC 04:00
        assert_eq!((ts + crate::CN_OFFSET_SECS) % 86_400, 12 * 3600);
    }

    #[test]
    fn only_the_growth_is_written_never_the_whole_bucket_twice() {
        let points = vec![DailyPoint {
            date: "2026-10-18".into(),
            total_tokens: 1000,
        }];
        // 账本里还没有 → 整段写入
        let rows = plan_rows(&points, "acct", &Baselines::new()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.unsplit_tokens, 1000);
        assert_eq!(rows[0].0, "catpaw_acct_2026-10-18_0");
        assert_eq!(rows[0].1.input_tokens, 0, "总量不得冒充输入 token");
        assert_eq!(rows[0].1.model, MODEL_LABEL);

        // 账本里已有 1000 → 桶没长，不写
        let mut base = Baselines::new();
        base.insert("acct_2026-10-18".to_string(), (1000, 1));
        assert!(plan_rows(&points, "acct", &base).unwrap().is_empty());

        // 账本里 600 → 只补 400 的差量，seq 递增
        let mut base = Baselines::new();
        base.insert("acct_2026-10-18".to_string(), (600, 2));
        let rows = plan_rows(&points, "acct", &base).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.unsplit_tokens, 400);
        assert_eq!(rows[0].0, "catpaw_acct_2026-10-18_2");
    }

    #[test]
    fn a_cloud_rollback_never_deletes_ledger_history() {
        let mut base = Baselines::new();
        base.insert("acct_2026-10-18".to_string(), (5000, 1));
        let points = vec![DailyPoint {
            date: "2026-10-18".into(),
            total_tokens: 100, // 云端回退
        }];
        assert!(plan_rows(&points, "acct", &base).unwrap().is_empty());
    }

    #[test]
    fn multiple_days_each_get_their_own_delta() {
        let points = vec![
            DailyPoint {
                date: "2026-10-17".into(),
                total_tokens: 10,
            },
            DailyPoint {
                date: "2026-10-18".into(),
                total_tokens: 20,
            },
        ];
        let rows = plan_rows(&points, "acct", &Baselines::new()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "catpaw_acct_2026-10-17_0");
        assert_eq!(rows[1].0, "catpaw_acct_2026-10-18_0");
        assert_eq!(rows[1].1.unsplit_tokens, 20);
    }
}
