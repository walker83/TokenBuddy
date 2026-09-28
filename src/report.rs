//! `tokenbuddy report` — a markdown digest of one window, for humans and for
//! the tokenbuddy-analyze skill (an agent can read this instead of raw logs;
//! tiered summarization is what keeps self-review affordable).
//!
//! Pure rendering: the store's own queries feed it, so the numbers on this
//! page are the dashboard's numbers by construction.

use crate::store::{AnomalyReport, Metrics, Pivot, Summary, WindowsFacts};

/// Render the markdown report for one window.
pub fn render(
    days: i64,
    summary: &Summary,
    metrics: &Metrics,
    windows: &WindowsFacts,
    anomalies: &AnomalyReport,
    pivot: &Pivot,
) -> String {
    let t = &metrics.totals;
    let title = if days <= 1 {
        "日报（今日）".to_string()
    } else {
        format!("报告（近 {days} 天）")
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

    if anomalies.flagged.is_empty() {
        out.push_str("\n## 异常\n\n审计窗内无异常日。\n");
    } else {
        out.push_str(
            "\n## 异常\n\n| 日期 | tokens | 基线(同星期中位) | 稳健 z |\n|---|---|---|---|\n",
        );
        for a in &anomalies.flagged {
            out.push_str(&format!(
                "| {} | {} | {} | {:.1} |\n",
                a.date,
                crate::format_tokens(a.tokens),
                crate::format_tokens(a.baseline_median),
                a.modified_z
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
    use crate::store::Summary;
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
            flagged: vec![AnomalyDay {
                date: "2026-09-20".into(),
                tokens: 50_000,
                baseline_median: 1_200,
                modified_z: 7.9,
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
        let md = render(7, &s, &m, &w, &a, &pivot);
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
            "2026-09-20 | 50.0K | 1.2K | 7.9 |",
        ] {
            assert!(md.contains(needle), "report missing {needle}");
        }
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
        let md = render(1, &s, &m, &w, &a, &empty_pivot);
        assert!(md.contains("日报（今日）"));
        assert!(md.contains("审计窗内无异常日"));
    }
}
