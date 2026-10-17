//! `tokenbuddy mcp` — an MCP (Model Context Protocol) server over stdio.
//!
//! The ledger is exactly what a coding agent wants to query ("what did this
//! project cost me this week", "find the session where we debugged X"), and
//! MCP is the channel agents already speak: structured, discoverable, one
//! config line. Every tool is a thin wrapper over an existing store query —
//! same numbers as the dashboard by construction.
//!
//! Transport: newline-delimited JSON-RPC 2.0 on stdin/stdout (MCP stdio).
//! Nothing else may write to stdout in this mode. Zero new dependencies:
//! the protocol needs only serde_json and lines.

use crate::context::SearchFilter;
use crate::store::Store;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, Write};

/// Run the stdio server loop. Blocks until stdin closes.
pub fn run() -> anyhow::Result<()> {
    let store = Store::open()?;
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                // Unparseable frame: reply with a protocol error (id unknown).
                write_msg(
                    &mut stdout,
                    &json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": format!("parse error: {e}")}}),
                )?;
                continue;
            }
        };
        if let Some(reply) = handle_message(&msg, &store) {
            write_msg(&mut stdout, &reply)?;
        }
    }
    Ok(())
}

fn write_msg(stdout: &mut std::io::Stdout, msg: &Value) -> anyhow::Result<()> {
    writeln!(stdout, "{msg}")?;
    stdout.flush()?;
    Ok(())
}

/// Handle one JSON-RPC message. Returns `None` for notifications (no `id`;
/// "notifications/initialized" and "notifications/cancelled" need no state
/// on our side).
fn handle_message(msg: &Value, store: &Store) -> Option<Value> {
    let id = msg.get("id").cloned();
    id.as_ref()?;
    let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let result = match method {
        "initialize" => Ok(json!({
            "protocolVersion": msg
                .pointer("/params/protocolVersion")
                .cloned()
                .unwrap_or_else(|| json!("2024-11-05")),
            "capabilities": {"tools": {}},
            "serverInfo": {
                "name": "tokenbuddy",
                "version": env!("CARGO_PKG_VERSION")
            }
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tool_descriptors() })),
        "tools/call" => tools_call(msg, store),
        other => Err(format!("method not found: {other}")),
    };
    Some(match result {
        Ok(r) => json!({"jsonrpc": "2.0", "id": id, "result": r}),
        Err(e) => json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": e}}),
    })
}

/// tools/call: agent-facing errors (bad tool, bad args, query failure) come
/// back as an `isError` result — the agent can read and retry them — while
/// a malformed *request* (no name) is a protocol error.
fn tools_call(msg: &Value, store: &Store) -> Result<Value, String> {
    let name = msg
        .pointer("/params/name")
        .and_then(|n| n.as_str())
        .ok_or("tools/call requires params.name")?;
    let args = msg
        .pointer("/params/arguments")
        .cloned()
        .unwrap_or(json!({}));
    if !args.is_object() {
        return Err("params.arguments must be an object".into());
    }
    match call_tool(name, &args, store) {
        Ok(text) => Ok(json!({
            "content": [{"type": "text", "text": text}],
            "isError": false
        })),
        Err(e) => Ok(json!({
            "content": [{"type": "text", "text": format!("error: {e}")}],
            "isError": true
        })),
    }
}

fn arg_str(args: &Value, key: &str) -> Option<String> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn arg_i64(args: &Value, key: &str) -> Option<i64> {
    args.get(key).and_then(|v| v.as_i64())
}

/// `days` argument → absolute `date_start` (now - days*86400). days <= 0 or
/// absent means "everything".
fn arg_since(args: &Value, key: &str) -> Option<i64> {
    arg_i64(args, key)
        .filter(|d| *d > 0)
        .map(|d| crate::now_ts() - d * 86_400)
}

fn call_tool(name: &str, args: &Value, store: &Store) -> anyhow::Result<String> {
    match name {
        "usage_summary" => {
            let s = store.query_summary(
                arg_str(args, "source").as_deref(),
                arg_str(args, "model").as_deref(),
                arg_since(args, "days"),
                None,
            )?;
            Ok(serde_json::to_string_pretty(&s)?)
        }
        "usage_timeline" => {
            let mode = match arg_str(args, "mode").as_deref() {
                Some("hourly") => crate::store::TimelineMode::Hourly,
                Some("weekly") => crate::store::TimelineMode::Weekly,
                Some("monthly") => crate::store::TimelineMode::Monthly,
                _ => crate::store::TimelineMode::Daily,
            };
            let buckets = store.query_timeline(
                mode,
                arg_str(args, "source").as_deref(),
                arg_str(args, "model").as_deref(),
                arg_since(args, "days"),
                None,
            )?;
            Ok(serde_json::to_string_pretty(&buckets)?)
        }
        "daily_report" => {
            let days = arg_i64(args, "days").unwrap_or(1).max(1);
            let start = if days <= 1 {
                crate::cn_midnight(0)
            } else {
                crate::cn_midnight(days - 1)
            };
            let summary = store.query_summary(None, None, Some(start), None)?;
            let metrics = store.query_metrics(None, None, Some(start), None)?;
            let windows = store.query_windows()?;
            let anomalies = store.query_anomalies()?;
            let pivot = store.query_pivot(Some(start), None)?;
            let active =
                store.query_active_time(Some(crate::cn_midnight(days.saturating_sub(1))), None)?;
            let quota = crate::quota::collect_view(false);
            let receipts = crate::zcode::merged_receipts(start);
            let forecast = store.query_week_forecast()?;
            let input = crate::report::ReportInput {
                days,
                summary,
                metrics,
                windows,
                anomalies,
                pivot,
                active,
                quota_snapshots: quota.snapshots,
                receipts,
                forecast,
            };
            Ok(crate::report::render(&input))
        }
        "window_facts" => {
            let w = store.query_windows()?;
            Ok(serde_json::to_string_pretty(&w)?)
        }
        "quota" => {
            // Read-only path: file readers go live, configured collector
            // commands are never spawned from an agent session — an MCP
            // caller must not be able to silently launch processes.
            let view = crate::quota::collect_view(false);
            Ok(serde_json::to_string_pretty(&view)?)
        }
        "work_receipts" => {
            let days = arg_i64(args, "days").unwrap_or(7).clamp(1, 365);
            let since = crate::cn_midnight(days - 1);
            let mut receipts = crate::zcode::merged_receipts(since);
            receipts.truncate(200);
            // R92:sessions 工具给的 session_id 直接深挖单个会话的收据。
            let session = args
                .get("session")
                .and_then(|s| s.as_str())
                .filter(|s| !s.is_empty());
            if let Some(p) = session {
                receipts.retain(|r| r.session_id.starts_with(p));
            }
            // R81:跨会话返工热点,agent 直接拿结论不用自己聚合。
            let rework = crate::zcode::rework_hotspots(&receipts, 10);
            Ok(serde_json::to_string_pretty(&serde_json::json!({
                "receipts": receipts,
                "rework_top": rework,
                "days": days,
            }))?)
        }
        "anomalies" => {
            let a = store.query_anomalies()?;
            Ok(serde_json::to_string_pretty(&a)?)
        }
        "project_pivot" => {
            let p = store.query_pivot(arg_since(args, "days"), None)?;
            Ok(serde_json::to_string_pretty(&p)?)
        }
        "search_context" => {
            let query = arg_str(args, "query")
                .ok_or_else(|| anyhow::anyhow!("search_context requires a query string"))?;
            let limit = arg_i64(args, "limit").unwrap_or(10).clamp(1, 50) as usize;
            // The index is built on demand and dropped at the end of this
            // call — an MCP session sits idle most of the time, so holding
            // ~165 MB between calls would betray the memory budget. It reads
            // context.parquet (the conversation store), not data.parquet.
            let context_path = crate::data_dir().join("context.parquet");
            let index = crate::context::ContextIndex::build(&context_path)?;
            let src = arg_str(args, "source");
            let proj = arg_str(args, "project");
            // issue #52：MCP 侧与 HTTP 同口径翻页（作用于 top-up 后的最终序列）。
            let offset = arg_i64(args, "offset").unwrap_or(0).clamp(0, 10_000) as usize;
            let resp = index.search_with_clicks(
                &query,
                &SearchFilter {
                    limit,
                    offset,
                    source: src.as_deref(),
                    project: proj.as_deref(),
                    since: arg_since(args, "days"),
                    ..Default::default()
                },
                &HashMap::new(),
            );
            let mut out = serde_json::to_string_pretty(&resp)?;
            // Keep replies bounded for the agent: the full response carries
            // session headers and trace detail that bloat the context window.
            if out.len() > 64 * 1024 {
                out.truncate(64 * 1024);
                out.push_str("\n…(truncated)");
            }
            Ok(out)
        }
        "sessions" => {
            // R90 — 第 12 工具:最近会话清单。agent 拿到 session_id 后
            // 可接 work_receipts / search_context 深挖,构成闭环。
            let days = arg_i64(args, "days").unwrap_or(7).clamp(1, 365);
            let top = arg_i64(args, "top").unwrap_or(15).clamp(1, 50) as usize;
            let source = args
                .get("source")
                .and_then(|s| s.as_str())
                .filter(|s| !s.is_empty());
            let start = crate::cn_midnight(days - 1);
            let insights = store.query_insights(source, None, Some(start), None, usize::MAX)?;
            let total = insights.sessions.len();
            let mut rows = insights.sessions;
            rows.sort_by_key(|r| std::cmp::Reverse(r.last_ts));
            rows.truncate(top);
            Ok(serde_json::to_string_pretty(&serde_json::json!({
                "days": days,
                "total_in_range": total,
                "sessions": rows.iter().map(|r| serde_json::json!({
                    "source": r.source,
                    "session_id": r.session_id,
                    "archetype": r.archetype,
                    "requests": r.requests,
                    "total_tokens": r.total_tokens,
                    "last_ts": r.last_ts,
                })).collect::<Vec<_>>(),
            }))?)
        }
        "source_health" => {
            let report = crate::doctor::diagnose()?;
            Ok(serde_json::to_string_pretty(&report)?)
        }
        // issue #27:HTTP 侧分析套路的 MCP 对应物——模型分布 / 投入时长 /
        // 逐请求指标 / 本期 vs 上期 / 高频组合热力图,「同源同口径」在
        // 能力集合上也成立。
        "models" => {
            let m = store.query_models(
                arg_str(args, "source").as_deref(),
                arg_str(args, "model").as_deref(),
                arg_since(args, "days"),
                None,
            )?;
            Ok(serde_json::to_string_pretty(&m)?)
        }
        "metrics" => {
            let m = store.query_metrics(
                arg_str(args, "source").as_deref(),
                arg_str(args, "model").as_deref(),
                arg_since(args, "days"),
                None,
            )?;
            Ok(serde_json::to_string_pretty(&m)?)
        }
        "active_time" => {
            let days = arg_i64(args, "days").unwrap_or(7).clamp(1, 365);
            let a = store.query_active_time(Some(crate::cn_midnight(days - 1)), None)?;
            Ok(serde_json::to_string_pretty(&a)?)
        }
        "digest" => {
            let days = arg_i64(args, "days").unwrap_or(7).clamp(1, 365);
            let (cur_start, now, prev_start) = crate::digest_windows(days);
            let cur = store.query_summary(None, None, Some(cur_start), Some(now))?;
            let prev = store.query_summary(None, None, Some(prev_start), Some(cur_start))?;
            let delta = |a: u64, b: u64| -> Option<f64> {
                if b > 0 {
                    Some((a as f64 - b as f64) / b as f64)
                } else {
                    None
                }
            };
            Ok(serde_json::to_string_pretty(&serde_json::json!({
                "days": days,
                "window": {
                    "window_start": cur_start,
                    "window_end": now,
                    "previous_start": prev_start,
                    "window_kind": "cn_calendar",
                },
                "current": {
                    "tokens": cur.total_tokens,
                    "requests": cur.total_requests,
                    "cache_read": cur.total_cache_read_tokens,
                },
                "previous": {
                    "tokens": prev.total_tokens,
                    "requests": prev.total_requests,
                    "cache_read": prev.total_cache_read_tokens,
                },
                "delta": {
                    "tokens": delta(cur.total_tokens, prev.total_tokens),
                    "requests": delta(cur.total_requests, prev.total_requests),
                },
            }))?)
        }
        "heatmap" => {
            let mode = match arg_str(args, "mode").as_deref() {
                Some("model_x_day") => "model_x_day",
                _ => "model_x_source",
            };
            let metric = match arg_str(args, "metric").as_deref() {
                Some("requests") => "requests",
                Some("avg_duration_ms") => "avg_duration_ms",
                _ => "total_tokens",
            };
            let h = store.query_heatmap(
                mode,
                metric,
                arg_str(args, "source").as_deref(),
                arg_str(args, "model").as_deref(),
                arg_since(args, "days"),
                None,
            )?;
            Ok(serde_json::to_string_pretty(&h)?)
        }
        "gateway_status" => {
            #[cfg(feature = "gateway")]
            {
                let cfg = crate::gateway::load_config_strict()?;
                // MCP 是独立进程,看不到 serve 里的在线监听;报配置态 +
                // 账本事实,在线状态明确说去面板看。
                let rows = crate::gateway::collect_records().unwrap_or_default();
                let (input, output, cache): (u64, u64, u64) =
                    rows.iter().fold((0, 0, 0), |(i, o, c), r| {
                        (
                            i + r.input_tokens,
                            o + r.output_tokens,
                            c + r.cache_read_tokens,
                        )
                    });
                Ok(serde_json::to_string_pretty(&serde_json::json!({
                    "enabled": cfg.enabled,
                    "listen": cfg.listen,
                    "note": "在线状态与本进程无关——serve 进程的面板「网关」页或 GET /api/gateway 才是在线事实",
                    "providers": cfg.providers.iter().map(|p| serde_json::json!({
                        "id": p.id, "protocol": p.protocol, "base_url": p.base_url,
                        "models": p.models, "dialects": p.dialects,
                    })).collect::<Vec<_>>(),
                    "ledger": {
                        "rows": rows.len(),
                        "input_tokens": input,
                        "output_tokens": output,
                        "cache_read_tokens": cache,
                        "by_client": rows.iter().fold(std::collections::BTreeMap::new(), |mut m: std::collections::BTreeMap<String, u64>, r| {
                            *m.entry(r.session_id.clone().unwrap_or_default()).or_insert(0) += r.output_tokens;
                            m
                        }),
                    },
                }))?)
            }
            #[cfg(not(feature = "gateway"))]
            Ok(serde_json::json!({"enabled": false, "note": "本构建未编译网关"}).to_string())
        }
        other => Err(anyhow::anyhow!("unknown tool: {other}")),
    }
}

fn tool_descriptors() -> Value {
    let obj = |name: &str, description: &str, props: Value, required: Value| {
        json!({
            "name": name,
            "description": description,
            "inputSchema": {
                "type": "object",
                "properties": props,
                "required": required
            }
        })
    };
    json!([
        obj("usage_summary",
            "Token usage totals for the local machine: total tokens, input/output, cache read/write, requests, per-source and per-model rollups. Same numbers as the TokenBuddy dashboard.",
            json!({
                "days": {"type": "integer", "description": "Only include the last N days (UTC+8 day boundaries). Omit for all time."},
                "source": {"type": "string", "description": "Filter by tool id: claude, codex, gemini, qwen, zcode, opencode, qoder, mimo, pi, workbuddy, minimax, hermes, cline, roocode, kilo, kimi, amp."},
                "model": {"type": "string", "description": "Filter by model name (substring match on the ledger's model column)."}
            }),
            json!([])),
        obj("usage_timeline",
            "Token usage over time as buckets, for spotting trends and busy days.",
            json!({
                "mode": {"type": "string", "enum": ["daily", "weekly", "monthly", "hourly"], "description": "Bucket size. Default daily."},
                "days": {"type": "integer", "description": "Only include the last N days."},
                "source": {"type": "string", "description": "Filter by tool id."},
                "model": {"type": "string", "description": "Filter by model name."}
            }),
            json!([])),
        obj("daily_report",
            "Markdown report for one window: KPIs, per-tool, per-model top 5, 5-hour window facts, anomalies, project × model pivot. The fastest way to answer 'how much did I use recently'.",
            json!({
                "days": {"type": "integer", "description": "Window length in days. 1 (default) = today only (UTC+8 midnight)."}
            }),
            json!([])),
        obj("window_facts",
            "5-hour usage window segments (like Claude's weekly limits, self-referenced): open window, remaining-to-own-P90, 28-day P90 baseline. Local facts only, no official quota.",
            json!({}),
            json!([])),
        obj("quota",
            "Provider-reported plan capacity (套餐余量) read locally with zero outbound calls: Codex rollout rate_limits, the Claude .claude.json limit cache, plus any configured command collectors' last stored reading. Never runs collector commands.",
            json!({}),
            json!([])),
        obj("work_receipts",
            "Work receipts (工作收据): per-session files changed, shell commands, test runs and tool-call counts, extracted from ZCode part rows, Claude transcript tool_use blocks and OpenCode session_message — deterministic evidence, not self-report. Never spawns processes.",
            json!({
                "days": {"type": "integer", "description": "Only include the last N days (1-365, default 7)."},
                "session": {"type": "string", "description": "Filter by session-id prefix (from the sessions tool) to drill into one session."}
            }),
            json!([])),
        obj("anomalies",
            "Days whose usage deviated from the same-weekday baseline (weekday-stratified median/MAD robust z, |z|>3.5). Needs ~3 weeks of history before it reports anything.",
            json!({}),
            json!([])),
        obj("project_pivot",
            "Usage pivoted by project × model: which working directory burned the tokens.",
            json!({
                "days": {"type": "integer", "description": "Only include the last N days."}
            }),
            json!([])),
        obj("search_context",
            "Full-text search over every conversation turn the local agents ever logged (user + assistant text; tool output excluded). Supports query syntax: source:claude, project:foo, role:user, days:7, -excluded, \"exact phrase\". First call builds the index (a few seconds on large corpora).",
            json!({
                "query": {"type": "string", "description": "Search text; may include the field filters above."},
                "limit": {"type": "integer", "description": "Max hits to return (1-50, default 10)."},
                "offset": {"type": "integer", "description": "Skip the first N final hits to page (0-10000, default 0)."},
                "source": {"type": "string", "description": "Filter by tool id."},
                "project": {"type": "string", "description": "Filter by project path."},
                "session": {"type": "string", "description": "Filter by session-id prefix (from the sessions or work_receipts tools)."},
                "days": {"type": "integer", "description": "Only search the last N days."}
            }),
            json!(["query"])),
        obj("sessions",
            "Recent coding sessions, newest first, with archetype (单发/快问/标准/深度/马拉松), request count and tokens — a session index to cross-reference with work_receipts and search_context.",
            json!({
                "days": {"type": "integer", "description": "Only include the last N days (1-365, default 7)."},
                "source": {"type": "string", "description": "Filter by tool id."},
                "top": {"type": "integer", "description": "Max sessions to return (1-50, default 15)."}
            }),
            json!([])),
        obj("source_health",
            "Per-source diagnostics: where each tool's local logs live, whether they are readable, why a source might be missing from the totals.",
            json!({}),
            json!([])),
        obj("models",
            "Per-model comparison for a window: which model burned the tokens (HTTP twin: GET /api/models).",
            json!({
                "days": {"type": "integer", "description": "Only include the last N days."},
                "source": {"type": "string", "description": "Filter by tool id."},
                "model": {"type": "string", "description": "Filter by model name."}
            }),
            json!([])),
        obj("metrics",
            "Per-request aggregates for a window: duration, TTFT, cache efficiency (HTTP twin: GET /api/metrics).",
            json!({
                "days": {"type": "integer", "description": "Only include the last N days."},
                "source": {"type": "string", "description": "Filter by tool id."},
                "model": {"type": "string", "description": "Filter by model name."}
            }),
            json!([])),
        obj("active_time",
            "Active wall-clock time per day, split by source and project, with overlap/unattributed caveats (HTTP twin: GET /api/active-time).",
            json!({
                "days": {"type": "integer", "description": "Window length in days (1-365, default 7, UTC+8 midnights)."}
            }),
            json!([])),
        obj("digest",
            "This period vs the previous equal-length period: totals and change ratios (HTTP twin: GET /api/digest).",
            json!({
                "days": {"type": "integer", "description": "Window length in days (1-365, default 7)."}
            }),
            json!([])),
        obj("heatmap",
            "Model × source (or model × day) matrix of tokens/requests/duration — spot high-frequency low-value combos (HTTP twin: GET /api/heatmap).",
            json!({
                "mode": {"type": "string", "enum": ["model_x_source", "model_x_day"]},
                "metric": {"type": "string", "enum": ["total_tokens", "requests", "avg_duration_ms"]},
                "days": {"type": "integer", "description": "Only include the last N days."},
                "source": {"type": "string", "description": "Filter by tool id."},
                "model": {"type": "string", "description": "Filter by model name."}
            }),
            json!([])),
        obj("gateway_status",
            "Local TokenBuddy gateway (R108): config state (enabled/listen/providers), plus what the gateway has metered into the ledger so far — rows, tokens, per-client output split. Config facts only; live listener state lives in the serve process's dashboard.",
            json!({}),
            json!([])),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{records_to_batch, write_parquet};
    use crate::{Source, TokenRecord};

    /// Tests that touch TOKENBUDDY_HOME serialize on crate::TEST_ENV_LOCK:
    /// the env var is process-global and other tests in this binary must not
    /// see it.
    fn temp_home(tag: &str) -> PathBufGuard {
        let dir = std::env::temp_dir().join(format!(
            "tb-mcp-{}-{}-{}",
            tag,
            std::process::id(),
            crate::now_ts()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        PathBufGuard { dir }
    }

    /// Removes the temp store and unsets the override on drop.
    struct PathBufGuard {
        dir: std::path::PathBuf,
    }
    impl Drop for PathBufGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
            std::env::remove_var("TOKENBUDDY_HOME");
        }
    }

    fn seeded_records(n: usize) -> Vec<(String, TokenRecord)> {
        (0..n)
            .map(|i| {
                let r = TokenRecord {
                    source: if i % 2 == 0 {
                        Source::Claude
                    } else {
                        Source::Zcode
                    },
                    model: "test-model".into(),
                    input_tokens: 10,
                    output_tokens: 5,
                    cache_read_tokens: 2,
                    cache_creation_tokens: 0,
                    timestamp: 1_788_874_500 + i as i64,
                    session_id: Some(format!("s{i}")),
                    project: String::new(),
                    duration_ms: Some(100),
                    ttft_ms: None,
                    credits: 0.0,
                    context_ratio: 0.0,
                    record_id: Some(format!("r{i}")),
                    sidechain: false,
                    merge_key: None,
                    request_count: 1,
                };
                (format!("k{i}"), r)
            })
            .collect()
    }

    #[test]
    fn protocol_replies_follow_jsonrpc_shape() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let guard = temp_home("protocol");
        let store = Store::open().expect("store on empty home");
        let msg = |s: &str| serde_json::from_str(s).unwrap();

        // initialize → serverInfo + protocol version echo.
        let reply = handle_message(
            &msg(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26"}}"#),
            &store,
        )
        .unwrap();
        assert_eq!(reply["id"], 1);
        assert_eq!(reply["result"]["serverInfo"]["name"], "tokenbuddy");
        assert_eq!(reply["result"]["protocolVersion"], "2025-03-26");

        // tools/list → every tool carries name, description and a schema.
        let reply = handle_message(
            &msg(r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#),
            &store,
        )
        .unwrap();
        let tools = reply["result"]["tools"].as_array().unwrap();
        assert!(tools.len() >= 8, "eight tools advertised");
        for t in tools {
            assert!(t["name"].is_string(), "tool name");
            assert!(t["description"].is_string(), "tool description");
            assert_eq!(t["inputSchema"]["type"], "object");
        }

        // Notifications (no id) get no reply at all.
        assert!(handle_message(
            &msg(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#),
            &store
        )
        .is_none());

        // Unknown method → JSON-RPC error, not a crash.
        let reply = handle_message(
            &msg(r#"{"jsonrpc":"2.0","id":3,"method":"resources/list"}"#),
            &store,
        )
        .unwrap();
        assert_eq!(reply["error"]["code"], -32601);

        // Unknown tool → isError result the agent can read.
        let reply = handle_message(
            &msg(r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"nope","arguments":{}}}"#),
            &store,
        )
        .unwrap();
        assert_eq!(reply["result"]["isError"], true);
        drop(guard);
    }

    #[test]
    fn tools_read_a_synthetic_ledger() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let guard = temp_home("ledger");
        let dir = guard.dir.clone();
        let batch = records_to_batch(&seeded_records(6));
        write_parquet(&dir.join("data.parquet"), &batch).unwrap();
        let store = Store::open().expect("store");

        let call = |name: &str, args: &str| {
            let msg = serde_json::from_str::<Value>(&format!(
                r#"{{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{{"name":"{name}","arguments":{args}}}}}"#
            ))
            .unwrap();
            let reply = handle_message(&msg, &store).unwrap();
            assert!(reply["error"].is_null(), "no protocol error: {reply}");
            let r = &reply["result"];
            assert_eq!(r["isError"], false, "tool ok: {}", r["content"][0]["text"]);
            r["content"][0]["text"].as_str().unwrap().to_string()
        };

        // usage_summary: 6 records × (10 in + 5 out + 2 cache read) = 102;
        // the ledger's total_tokens includes cache reads by definition.
        let text = call("usage_summary", "{}");
        let s: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(s["total_tokens"], 102);
        assert_eq!(s["total_requests"], 6);

        // days filter excludes everything when the window cannot reach the seeds.
        let text = call("usage_summary", r#"{"days":1}"#);
        let s: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(s["total_tokens"], 0);

        // source filter narrows to the even (claude) records only.
        let text = call("usage_summary", r#"{"source":"claude"}"#);
        let s: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(s["total_requests"], 3);

        // R90 sessions:合成账本的会话清单(分型/请求数/token),top 截断。
        let text = call("sessions", r#"{"days":365,"top":3}"#);
        let s: Value = serde_json::from_str(&text).unwrap();
        let list = s["sessions"].as_array().unwrap();
        assert!(!list.is_empty() && list.len() <= 3, "{s}");
        let first = &list[0];
        assert!(first["session_id"].as_str().unwrap().starts_with('s'));
        assert!(first["archetype"].as_str().is_some());
        assert!(first["total_tokens"].as_u64().unwrap() > 0);
        assert!(s["total_in_range"].as_u64().unwrap() >= list.len() as u64);

        // daily_report renders the markdown with the same numbers by construction.
        // R62/R69/R70 的三个新节必须在:出口矩阵的回归锚点。
        let text = call("daily_report", r#"{"days":30}"#);
        for section in ["## 投入时长", "## 工作收据", "## 套餐余量", "## 周终外推"]
        {
            assert!(text.contains(section), "daily_report missing {section}");
        }
        assert!(text.contains("TokenBuddy"), "report title");

        // window_facts/anomalies/pivot answer on a tiny ledger without blowing up.
        for name in [
            "window_facts",
            "anomalies",
            "project_pivot",
            "quota",
            "work_receipts",
        ] {
            let text = call(name, "{}");
            serde_json::from_str::<Value>(&text).expect("valid JSON out");
        }
        drop(guard);
    }
}
