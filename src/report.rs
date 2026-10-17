//! `tokenbuddy report` — a markdown digest of one window, for humans and for
//! the tokenbuddy-analyze skill (an agent can read this instead of raw logs;
//! tiered summarization is what keeps self-review affordable).
//!
//! Pure rendering: the store's own queries feed it, so the numbers on this
//! page are the dashboard's numbers by construction.

use crate::store::{
    ActiveTime, AnomalyReport, Metrics, Pivot, Summary, WeekForecast, WindowsFacts,
};

/// R71 — one bag for everything the report renders. The parameter list hit
/// ten and kept growing with every data domain; a struct makes adding the
/// next domain a field, not a signature migration across three call sites.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct ReportInput {
    pub summary: Summary,
    pub metrics: Metrics,
    pub windows: WindowsFacts,
    pub anomalies: AnomalyReport,
    pub pivot: Pivot,
    pub active: ActiveTime,
    pub quota_snapshots: Vec<crate::quota::QuotaSnapshot>,
    pub receipts: Vec<crate::zcode::WorkReceipt>,
    pub forecast: WeekForecast,
    pub days: i64,
}

/// Render the markdown report for one window. `quota` is the read-only
/// quota view's snapshots (file readers live + collectors' stored readings)
/// — rendering never runs a collector command.
pub fn render(input: &ReportInput) -> String {
    let ReportInput {
        days,
        summary,
        metrics,
        windows,
        anomalies,
        pivot,
        active,
        quota_snapshots,
        receipts,
        forecast,
        ..
    } = input;
    let t = &metrics.totals;
    let title = if *days <= 1 {
        "日报（今日）".to_string()
    } else {
        format!("报告（近 {} 天）", days)
    };
    let mut out = String::new();
    out.push_str(&format!("# TokenBuddy {title}\n\n"));

    out.push_str(&format!(
        "| 指标 | 值 |\n|---|---|\n| 总 tokens | {} |\n| 输入 / 输出 | {} / {} |\n| 缓存读 / 写 | {} / {} |\n| 请求数 | {} |\n| Cache 命中率 | {} |\n| 平均耗时 | {} |\n\n",
        crate::format_tokens(summary.total_tokens),
        crate::format_tokens(summary.total_input_tokens),
        crate::format_tokens(summary.total_output_tokens),
        crate::format_tokens(summary.total_cache_read_tokens),
        crate::format_tokens(summary.total_cache_creation_tokens),
        summary.total_requests,
        fmt_pct(t.cache_hit_rate),
        fmt_ms(t.avg_duration_ms),
    ));

    if !summary.by_source.is_empty() {
        out.push_str("## 按工具\n\n| 工具 | tokens | 请求 |\n|---|---|---|\n");
        for s in &summary.by_source {
            out.push_str(&format!(
                "| {} | {} | {} |\n",
                s.source,
                crate::format_tokens(source_total(s)),
                s.requests
            ));
        }
        out.push('\n');
    }

    if !summary.by_model.is_empty() {
        out.push_str("## 按模型（前 5）\n\n| 模型 | tokens | 请求 |\n|---|---|---|\n");
        for m in summary.by_model.iter().take(5) {
            out.push_str(&format!(
                "| {} | {} | {} |\n",
                m.model,
                crate::format_tokens(m.total_tokens),
                m.requests
            ));
        }
        out.push('\n');
    }

    if !pivot.rows.is_empty() {
        out.push_str(
            "## 按项目 × 模型（前 10）\n\n| 项目 | 模型 | tokens | 请求 |\n|---|---|---|---|\n",
        );
        for r in pivot.rows.iter().take(10) {
            out.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                r.project,
                r.model,
                crate::format_tokens(r.tokens),
                r.requests
            ));
        }
        out.push('\n');
    }

    out.push_str(&format!(
        "## 窗口事实\n\n- 当前 5h 窗口：{}\n- 滚动 7 天：{}（{} 请求）\n- 28 天窗口 P90 参照：{}（峰值 {}）\n",
        match &windows.open_window {
            Some(w) => format!(
                "{} tokens（{} 请求，燃速 {}/h）",
                crate::format_tokens(w.window_tokens),
                w.window_requests,
                crate::format_tokens(w.burn_per_hour as u64)
            ),
            None => "无打开的窗口".into(),
        },
        crate::format_tokens(windows.week_tokens),
        windows.week_requests,
        crate::format_tokens(windows.p90_5h_tokens),
        crate::format_tokens(windows.max_5h_tokens),
    ));

    // R69:周终外推——双口径并列,分叉即洞察。
    if forecast.days_elapsed > 0 {
        out.push_str("\n## 周终外推\n\n");
        out.push_str(&format!(
                "- 本自然周({}/7 天)已:{}\n- 按前 28 天节奏(中位 {}/天)预计:{}\n- 按本期日均预计:{}\n\n两种口径分叉大 = 本周节奏与往常不同。\n",
                forecast.days_elapsed,
                crate::format_tokens(forecast.week_so_far),
                crate::format_tokens(forecast.prior_28d_median_daily),
                crate::format_tokens(forecast.projected_own_rhythm),
                crate::format_tokens(forecast.projected_actual),
            ));
    }

    // R39:投入时长——账本时间戳推导,只有日合计声称是墙钟时间。
    let total_active: u64 = active.days.iter().map(|d| d.active_secs).sum();
    out.push_str("\n## 投入时长\n\n");
    if active.days.is_empty() || total_active == 0 {
        out.push_str("当前窗口没有请求。\n");
    } else {
        out.push_str(&format!(
            "- 合计:{:.1} 小时({} 个工作段;间隔 ≤ {} 分钟视为同一段)\n",
            total_active as f64 / 3600.0,
            active.days.iter().map(|d| d.bursts as u64).sum::<u64>(),
            active.burst_gap_secs / 60
        ));
        out.push_str("\n| 日期 | 活跃 | 段 |\n|---|---|---|\n");
        for d in &active.days {
            out.push_str(&format!(
                "| {} | {:.1} h | {} |\n",
                d.day,
                d.active_secs as f64 / 3600.0,
                d.bursts
            ));
        }
        let top: Vec<&crate::store::ActiveSlice> = {
            let mut v: Vec<&crate::store::ActiveSlice> = active
                .by_source
                .iter()
                .filter(|s| s.active_secs > 0)
                .collect();
            v.sort_by_key(|s| std::cmp::Reverse(s.active_secs));
            v.truncate(3);
            v
        };
        if !top.is_empty() {
            let parts: Vec<String> = top
                .iter()
                .map(|s| format!("{} {:.1} h", s.key, s.active_secs as f64 / 3600.0))
                .collect();
            out.push_str(&format!(
                "\n按来源(并行各自计,会重叠):{}\n",
                parts.join(" · ")
            ));
        }
    }

    // R56-R58:工作收据——确定性的文件/命令/测试证据,不是自述。
    out.push_str("\n## 工作收据\n\n");
    let total_files: u64 = receipts.iter().map(|r| r.files.len() as u64).sum();
    let total_cmds: u64 = receipts.iter().map(|r| r.bash_count).sum();
    let total_tests: u64 = receipts.iter().map(|r| r.test_count).sum();
    if receipts.is_empty() {
        out.push_str("当前窗口没有工作收据(仅 ZCode/Claude/OpenCode 提供此通道)。\n");
    } else {
        out.push_str(&format!(
            "- 改动文件 {} 个 · 命令 {} 次 · 测试 {} 次(共 {} 个会话)\n",
            total_files,
            total_cmds,
            total_tests,
            receipts.len()
        ));
        // 全局最常改动的文件 top 5:跨会话合并计数。
        let mut merged: std::collections::BTreeMap<&str, u64> = std::collections::BTreeMap::new();
        for r in receipts {
            for (p, n) in &r.files {
                *merged.entry(p.as_str()).or_insert(0) += n;
            }
        }
        let mut top: Vec<(&str, u64)> = merged.into_iter().collect();
        top.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
        top.truncate(5);
        if !top.is_empty() {
            out.push_str("\n| 文件 | 改动 |\n|---|---|\n");
            for (p, n) in top {
                out.push_str(&format!("| {} | {} |\n", p, n));
            }
        }
        // R96:跨会话返工热点 top 3——会话内反复编辑是迭代不是返工,
        // 只认被 ≥2 个会话碰过的文件(与 /api/work-receipts 同一判定)。
        let hotspots = crate::zcode::rework_hotspots(receipts, 3);
        if !hotspots.is_empty() {
            out.push_str("\n**返工热点**(被 ≥2 个会话编辑):\n\n");
            for h in hotspots {
                out.push_str(&format!(
                    "- {} — {} 个会话 / {} 次\n",
                    h.path, h.sessions, h.edits
                ));
            }
        }
    }

    // R31-R34:套餐余量(供应商口径,读取即止,不运行任何采集命令)。
    out.push_str("\n## 套餐余量\n\n");
    if quota_snapshots.is_empty() {
        out.push_str(
                "无套餐数据。Codex / Claude 文件源自动读取;其他来源见 `tokenbuddy quota` 的配置提示。\n",
            );
    } else {
        out.push_str("| 来源 | 套餐 | 窗口 | 已用 | 重置 |\n|---|---|---|---|---|\n");
        for s in quota_snapshots {
            let reset = match s.resets_at {
                Some(at) if at <= crate::now_ts() => "已过重置点".to_string(),
                Some(at) => {
                    let left = at - crate::now_ts();
                    if left >= 86_400 {
                        format!("{}天{}小时后", left / 86_400, (left % 86_400) / 3_600)
                    } else if left >= 3_600 {
                        format!("{}小时{}分后", left / 3_600, (left % 3_600) / 60)
                    } else {
                        format!("{}分后", left / 60)
                    }
                }
                None => "未知".to_string(),
            };
            out.push_str(&format!(
                "| {} | {} | {} | {:.1}% | {} |\n",
                s.source, s.plan, s.window, s.used_percent, reset
            ));
        }
    }

    if anomalies.flagged.is_empty() {
        out.push_str("\n## 异常\n\n审计窗内无异常日。\n");
    } else {
        out.push_str(
            "\n## 异常\n\n| 日期 | tokens | 基线(同星期中位) | 倍数 | 稳健 z | 口径 |\n|---|---|---|---|---|---|\n",
        );
        for a in &anomalies.flagged {
            out.push_str(&format!(
                "| {} | {} | {} | {} | {:.1} | {} |\n",
                a.date,
                crate::format_tokens(a.tokens),
                crate::format_tokens(a.baseline_median),
                a.ratio
                    .map(|x| format!("{x:.1}×"))
                    .unwrap_or_else(|| "—".into()),
                a.modified_z,
                a.severity
            ));
        }
        if anomalies.today_excluded {
            out.push_str(&format!(
                "\n未闭合的 {} 未参与判定,截至当前 {} tokens。\n",
                anomalies.today,
                crate::format_tokens(anomalies.today_tokens)
            ));
        }
    }
    out
}

fn source_total(s: &crate::store::SourceRow) -> u64 {
    s.input_tokens + s.output_tokens + s.cache_read_tokens + s.cache_creation_tokens
}

fn fmt_ms(ms: Option<f64>) -> String {
    match ms {
        Some(v) if v >= 1000.0 => format!("{:.1}s", v / 1000.0),
        Some(v) => format!("{v:.0}ms"),
        None => "—".into(),
    }
}

fn fmt_pct(v: Option<f64>) -> String {
    match v {
        Some(v) => format!("{:.1}%", v * 100.0),
        None => "—".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::render;
    use super::ReportInput;
    use crate::store::{ActiveTime, Summary};
    use crate::store::{AnomalyDay, AnomalyReport, Metrics, Pivot, TotalsMetrics, WindowsFacts};
    use crate::Source;

    fn metrics_with(hit: Option<f64>) -> Metrics {
        Metrics {
            by_source: vec![],
            totals: TotalsMetrics {
                requests: 3,
                avg_duration_ms: Some(1200.0),
                avg_ttft_ms: None,
                cache_hit_rate: hit,
                output_input_ratio: None,
                avg_input_per_req: 0.0,
                avg_output_per_req: 0.0,
            },
        }
    }

    fn summary_with(rows: Vec<(Source, u64, u64)>) -> Summary {
        let mut s = crate::store::Summary {
            total_requests: 0,
            total_input_tokens: 0,
            total_output_tokens: 0,
            total_cache_read_tokens: 0,
            total_cache_creation_tokens: 0,
            total_tokens: 0,
            total_credits: 0.0,
            avg_context_ratio: None,
            by_source: vec![],
            by_model: vec![],
            subagent_tokens: 0,
            subagent_requests: 0,
        };
        for (source, tokens, requests) in rows {
            s.by_source.push(crate::store::SourceRow {
                source: source.as_str().to_string(),
                requests,
                input_tokens: tokens,
                output_tokens: 0,
                cache_read_tokens: 0,
                cache_creation_tokens: 0,
                credits: 0.0,
                avg_context_ratio: None,
            });
        }
        s
    }

    #[test]
    fn report_contains_all_sections_and_numbers() {
        let s = summary_with(vec![(Source::Claude, 1500, 3)]);
        let m = metrics_with(Some(0.9));
        let w = WindowsFacts {
            data_now: 0,
            open_window: None,
            week_tokens: 99_000,
            week_requests: 7,
            p90_5h_tokens: 1000,
            max_5h_tokens: 2000,
            p90_ratio: None,
            hours_to_p90: None,
        };
        let a = AnomalyReport {
            checked_days: 30,
            window_days: 56,
            today_excluded: true,
            today_tokens: 4_200,
            today: "2026-09-29".into(),
            flagged: vec![AnomalyDay {
                date: "2026-09-20".into(),
                tokens: 50_000,
                baseline_median: 1_200,
                modified_z: 7.9,
                raw_z: 7.9,
                ratio: Some(41.7),
                baseline_days: 4,
                mad: 950.0,
                severity: "robust",
            }],
        };
        let pivot = Pivot {
            rows: vec![crate::store::PivotRow {
                project: "/code/demo".into(),
                model: "m".into(),
                tokens: 900,
                requests: 2,
            }],
        };
        // R96:两个会话都改 a.rs(返工热点),单会话文件不进热点。
        let receipts = vec![
            crate::zcode::WorkReceipt {
                session_id: "s1".into(),
                files: vec![("a.rs".into(), 3), ("solo.rs".into(), 1)],
                bash_count: 2,
                test_count: 1,
                tool_calls: 6,
                tools: Default::default(),
                last_ts: 1_788_874_500,
            },
            crate::zcode::WorkReceipt {
                session_id: "s2".into(),
                files: vec![("a.rs".into(), 2)],
                bash_count: 0,
                test_count: 0,
                tool_calls: 2,
                tools: Default::default(),
                last_ts: 1_788_874_600,
            },
        ];
        let md = render(&ReportInput {
            days: 7,
            summary: s.clone(),
            metrics: m.clone(),
            windows: w.clone(),
            anomalies: a.clone(),
            pivot: pivot.clone(),
            active: ActiveTime::default(),
            quota_snapshots: vec![],
            receipts,
            forecast: crate::store::WeekForecast::default(),
        });
        for needle in [
            "# TokenBuddy 报告（近 7 天）",
            "| 总 tokens |",
            "Cache 命中率 | 90.0% |",
            "## 按工具",
            "claude | 1.5K | 3",
            "| /code/demo | m | 900 | 2 |",
            "## 窗口事实",
            "滚动 7 天：99.0K（7 请求）",
            "## 异常",
            "2026-09-20 | 50.0K | 1.2K | 41.7× | 7.9 | robust",
            "## 工作收据",
            "改动文件 3 个 · 命令 2 次 · 测试 1 次",
            "**返工热点**",
            "- a.rs — 2 个会话 / 5 次",
        ] {
            assert!(md.contains(needle), "report missing {needle}");
        }
        assert!(!md.contains("solo.rs —"), "单会话文件不得进热点");
    }

    #[test]
    fn report_renders_active_time_and_quota_sections() {
        let s = summary_with(vec![]);
        let m = metrics_with(None);
        let w = WindowsFacts {
            data_now: 0,
            open_window: None,
            week_tokens: 0,
            week_requests: 0,
            p90_5h_tokens: 0,
            max_5h_tokens: 0,
            p90_ratio: None,
            hours_to_p90: None,
        };
        let a = AnomalyReport {
            checked_days: 0,
            window_days: 56,
            today_excluded: false,
            today_tokens: 0,
            today: String::new(),
            flagged: vec![],
        };
        let pivot = Pivot::default();

        let mut active = ActiveTime::default();
        active.days.push(crate::store::ActiveDay {
            day: "2026-09-29".into(),
            active_secs: 32_842,
            bursts: 2,
        });
        active.by_source.push(crate::store::ActiveSlice {
            key: "zcode".into(),
            active_secs: 32_842,
            bursts: 2,
        });
        active.burst_gap_secs = 900;
        let quota = vec![crate::quota::QuotaSnapshot {
            source: "minimax".into(),
            plan: "general".into(),
            window: "interval".into(),
            used_percent: 4.0,
            resets_at: Some(crate::now_ts() + 3600),
            collected_at: crate::now_ts(),
            origin: "command".into(),
        }];

        let md = render(&ReportInput {
            days: 1,
            summary: s.clone(),
            metrics: m.clone(),
            windows: w.clone(),
            anomalies: a.clone(),
            pivot: pivot.clone(),
            active: active.clone(),
            quota_snapshots: quota.clone(),
            receipts: vec![],
            forecast: crate::store::WeekForecast::default(),
        });
        assert!(md.contains("## 投入时长"), "active section present");
        assert!(md.contains("9.1 小时"), "hours formatted: {md}");
        assert!(md.contains("2026-09-29 | 9.1 h | 2"));
        assert!(md.contains("zcode 9.1 h"), "per-source line");
        assert!(md.contains("## 套餐余量"), "quota section present");
        assert!(md.contains("| minimax | general | interval | 4.0% |"));
        assert!(md.contains("1小时0分后"));

        // 空数据:两节都给诚实文案,不出空表。
        let md = render(&ReportInput {
            days: 1,
            summary: s.clone(),
            metrics: m.clone(),
            windows: w.clone(),
            anomalies: a.clone(),
            pivot: pivot.clone(),
            active: ActiveTime::default(),
            quota_snapshots: vec![],
            receipts: vec![],
            forecast: crate::store::WeekForecast::default(),
        });
        assert!(md.contains("当前窗口没有请求"));
        assert!(md.contains("无套餐数据"));
        assert!(md.contains("当前窗口没有工作收据"));

        // 有收据:合计行 + 文件表。
        let receipts = vec![crate::zcode::WorkReceipt {
            session_id: "sess-x".into(),
            files: vec![("/tmp/a.rs".to_string(), 3), ("/tmp/b.rs".to_string(), 1)],
            bash_count: 12,
            test_count: 4,
            tool_calls: 20,
            tools: [
                ("Bash".to_string(), 12),
                ("Edit".to_string(), 6),
                ("Read".to_string(), 2),
            ]
            .into_iter()
            .collect(),
            last_ts: 0,
        }];
        let md = render(&ReportInput {
            days: 1,
            summary: s.clone(),
            metrics: m.clone(),
            windows: w.clone(),
            anomalies: a.clone(),
            pivot: pivot.clone(),
            active: ActiveTime::default(),
            quota_snapshots: vec![],
            receipts: receipts.clone(),
            forecast: crate::store::WeekForecast::default(),
        });
        assert!(md.contains("## 工作收据"));
        assert!(md.contains("改动文件 2 个 · 命令 12 次 · 测试 4 次(共 1 个会话)"));
        assert!(md.contains("| /tmp/a.rs | 3 |"), "top file count: {md}");
    }

    #[test]
    fn empty_report_says_so() {
        let s = summary_with(vec![]);
        let m = metrics_with(None);
        let w = WindowsFacts {
            data_now: 0,
            open_window: None,
            week_tokens: 0,
            week_requests: 0,
            p90_5h_tokens: 0,
            max_5h_tokens: 0,
            p90_ratio: None,
            hours_to_p90: None,
        };
        let a = AnomalyReport::default();
        let empty_pivot = Pivot::default();
        let md = render(&ReportInput {
            days: 1,
            summary: s.clone(),
            metrics: m.clone(),
            windows: w.clone(),
            anomalies: a.clone(),
            pivot: empty_pivot.clone(),
            active: ActiveTime::default(),
            quota_snapshots: vec![],
            receipts: vec![],
            forecast: crate::store::WeekForecast::default(),
        });
        assert!(md.contains("日报（今日）"));
        assert!(md.contains("审计窗内无异常日"));
    }
    /// R89 --json 出口:ReportInput 可序列化(与 markdown 同一结构),
    /// 缺省构造也要能出合法 JSON。
    #[test]
    fn report_input_serializes_for_json_outlet() {
        let input = ReportInput::default();
        let text = serde_json::to_string(&input).unwrap();
        assert!(text.contains("\"days\":0"));
        assert!(text.contains("summary"));
    }
}
