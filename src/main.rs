use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tiny_http::{Header, Response, Server};
use tokenbuddy::context::{self, ContextHandle};
use tokenbuddy::store::{Store, TimelineMode};

const HTML: &str = include_str!("dashboard.html");

/// Every response body this server writes is a `String`, so the tiny_http
/// response type is the same everywhere and handlers can share helpers.
type JsonResponse = Response<std::io::Cursor<Vec<u8>>>;

fn json_response(body: String) -> JsonResponse {
    Response::from_string(body).with_header(
        Header::from_bytes("Content-Type", "application/json")
            .expect("hardcoded header should be valid"),
    )
}

/// Errors go through serde rather than `format!` — DuckDB and IO messages
/// routinely contain double quotes, and interpolating one into a JSON string
/// literal produces a malformed body that the client reports as an opaque
/// parse failure, hiding the real cause.
fn error_response(e: &anyhow::Error) -> JsonResponse {
    let body = serde_json::json!({ "error": e.to_string() }).to_string();
    Response::from_string(body)
        .with_header(
            Header::from_bytes("Content-Type", "application/json")
                .expect("hardcoded header should be valid"),
        )
        .with_status_code(500)
}

/// Parse a request's query string into decoded key/value pairs.
///
/// The dashboard builds queries with `URLSearchParams`, so a model name like
/// `Qwen/Qwen3-Coder-480B` arrives as `Qwen%2FQwen3-Coder-480B` and a
/// WorkBuddy composite `a+b` as `a%2Bb`. Handing the raw text to the
/// `model LIKE` filter would match nothing for exactly the names users paste
/// into the box, so values are decoded here.
fn parse_params(path: &str) -> HashMap<String, String> {
    path.split('?')
        .nth(1)
        .unwrap_or("")
        .split('&')
        .filter(|s| !s.is_empty())
        .filter_map(|s| {
            // splitn keeps an '=' that belongs to the value instead of
            // truncating the pair at the first one.
            let mut parts = s.splitn(2, '=');
            Some((percent_decode(parts.next()?), percent_decode(parts.next()?)))
        })
        .collect()
}

/// Decode `%XX` escapes and turn `+` back into a space, per
/// `application/x-www-form-urlencoded`. Decoding works on bytes so multi-byte
/// UTF-8 (a `混元` filter, say) survives intact.
fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => match (hex_val(b[i + 1]), hex_val(b[i + 2])) {
                (Some(hi), Some(lo)) => {
                    out.push(hi * 16 + lo);
                    i += 3;
                }
                // A truncated or non-hex escape is passed through verbatim.
                _ => {
                    out.push(b[i]);
                    i += 1;
                }
            },
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn main() -> Result<()> {
    let store = Arc::new(Store::open()?);
    let server = Server::http("127.0.0.1:8080")
        .map_err(|e| anyhow::anyhow!("Failed to start server: {}", e))?;

    // Context index lives next to the token parquet. The first build runs in
    // the background so the dashboard is up immediately; until it lands the
    // search endpoint reports the in-progress phase.
    let context = Arc::new(ContextHandle::new(
        tokenbuddy::data_dir().join("context.parquet"),
    ));
    {
        let ctx = Arc::clone(&context);
        std::thread::spawn(move || {
            if let Err(e) = ctx.sync_and_build(false) {
                eprintln!("[TokenBuddy] context index build failed: {e}");
            }
        });
    }

    println!("TokenBuddy server running on http://127.0.0.1:8080");
    println!("Press Ctrl+C to stop");

    for request in server.incoming_requests() {
        let url = request.url();
        let method = request.method();

        let response: JsonResponse = match (method.as_str(), url) {
            ("GET", "/") => {
                Response::from_string(HTML)
                    .with_header(
                        Header::from_bytes("Content-Type", "text/html")
                            .expect("hardcoded header should be valid"),
                    )
                    // The page is rebuilt into the binary on every change;
                    // without this the browser serves a stale page and the
                    // change "does not land".
                    .with_header(
                        Header::from_bytes("Cache-Control", "no-cache")
                            .expect("hardcoded header should be valid"),
                    )
            }
            ("GET", path) if path.starts_with("/api/summary") => {
                match handle_summary(&store, path) {
                    Ok(json) => json_response(json),
                    Err(e) => error_response(&e),
                }
            }
            ("POST", path) if path.starts_with("/api/sync") => {
                let mode = parse_params(path)
                    .get("mode")
                    .map(|s| s.as_str())
                    .unwrap_or("incremental")
                    .to_string();
                let result = if mode == "full" {
                    store.sync_full()
                } else {
                    store.sync()
                };
                match result.and_then(|r| Ok(serde_json::to_string(&r)?)) {
                    Ok(json) => {
                        // Refresh the context index alongside the token sync;
                        // backgrounded so the sync response is not held up by
                        // a slow source log.
                        let ctx = Arc::clone(&context);
                        std::thread::spawn(move || {
                            if let Err(e) = ctx.sync_and_build(false) {
                                eprintln!("[TokenBuddy] context refresh failed: {e}");
                            }
                        });
                        json_response(json)
                    }
                    Err(e) => error_response(&e),
                }
            }
            ("GET", path) if path.starts_with("/api/timeline") => {
                match handle_timeline(&store, path) {
                    Ok(json) => json_response(json),
                    Err(e) => error_response(&e),
                }
            }
            ("GET", path) if path.starts_with("/api/metrics") => {
                match handle_metrics(&store, path) {
                    Ok(json) => json_response(json),
                    Err(e) => error_response(&e),
                }
            }
            ("GET", path) if path.starts_with("/api/heatmap") => {
                match handle_heatmap(&store, path) {
                    Ok(json) => json_response(json),
                    Err(e) => error_response(&e),
                }
            }
            ("GET", path) if path.starts_with("/api/insights") => {
                match handle_insights(&store, path) {
                    Ok(json) => json_response(json),
                    Err(e) => error_response(&e),
                }
            }
            ("GET", path) if path.starts_with("/api/models") => match handle_models(&store, path) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            },
            ("GET", path) if path.starts_with("/api/digest") => match handle_digest(&store, path) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            },
            ("GET", path) if path.starts_with("/api/context/search") => {
                match handle_context_search(&context, path) {
                    Ok(json) => json_response(json),
                    Err(e) => error_response(&e),
                }
            }
            ("GET", path) if path.starts_with("/api/context/session") => {
                match handle_context_session(&context, path) {
                    Ok(json) => json_response(json),
                    Err(e) => error_response(&e),
                }
            }
            ("GET", path) if path.starts_with("/api/context/stats") => {
                match handle_context_stats(&context) {
                    Ok(json) => json_response(json),
                    Err(e) => error_response(&e),
                }
            }
            ("POST", path) if path.starts_with("/api/context/click") => {
                let doc_id = parse_params(path)
                    .get("doc_id")
                    .and_then(|d| d.parse::<i64>().ok());
                match doc_id {
                    Some(id) => {
                        let n = context.record_click(id);
                        json_response(serde_json::json!({ "doc_id": id, "clicks": n }).to_string())
                    }
                    None => error_response(&anyhow::anyhow!("click 需要 doc_id 参数")),
                }
            }
            ("GET", path) if path.starts_with("/api/context/quality") => {
                json_response(serde_json::to_string(&context.quality()).unwrap_or_default())
            }
            ("POST", path) if path.starts_with("/api/context/rebuild") => {
                let ctx = Arc::clone(&context);
                std::thread::spawn(move || {
                    if let Err(e) = ctx.sync_and_build(true) {
                        eprintln!("[TokenBuddy] context rebuild failed: {e}");
                    }
                });
                json_response(serde_json::json!({ "started": true }).to_string())
            }
            _ => Response::from_string("Not Found").with_status_code(404),
        };

        let _ = request.respond(response);
    }

    Ok(())
}

/// Resolve a `timeRange` preset to its inclusive start, as China-local
/// midnight. `None` means "全部时间" and leaves the window unbounded.
fn time_range_start(time_range: Option<&str>) -> Option<i64> {
    let days_ago = match time_range {
        Some("today") => 0,
        Some("7d") => 7,
        Some("30d") => 30,
        Some("90d") => 90,
        _ => return None,
    };
    Some(tokenbuddy::cn_midnight(days_ago))
}

/// The `timeRange` / `source` / `model` triple every report endpoint accepts.
struct Filters {
    date_start: Option<i64>,
    date_end: Option<i64>,
    source: Option<String>,
    model: Option<String>,
}

/// No endpoint currently accepts an upper bound, but the field keeps the
/// call sites uniform and is what the digest's equal-length windows rely on.
fn filters_from(path: &str) -> Filters {
    let params = parse_params(path);
    let source = match params.get("source").map(|s| s.as_str()) {
        Some("all") | None => None,
        Some(s) => Some(s.to_string()),
    };
    let model = match params.get("model").map(|s| s.as_str()) {
        Some("") | None => None,
        Some(s) => Some(s.to_string()),
    };
    Filters {
        date_start: time_range_start(params.get("timeRange").map(|s| s.as_str())),
        date_end: None,
        source,
        model,
    }
}

fn handle_summary(store: &Store, path: &str) -> Result<String> {
    let f = filters_from(path);
    let summary = store.query_summary(
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    Ok(serde_json::to_string(&summary)?)
}

/// `GET /api/context/search?q=...&source=&role=&project=&days=&limit=` —
/// full-text search over the conversation index. `days` snaps to
/// China-local midnights like every other time filter; `project` takes a
/// `project_label` value as listed by `/api/context/stats`.
fn handle_context_search(context: &ContextHandle, path: &str) -> Result<String> {
    let params = parse_params(path);
    let q = params.get("q").cloned().unwrap_or_default();
    let source = params
        .get("source")
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty() && *s != "all")
        .map(|s| s.to_string());
    let role = params
        .get("role")
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty() && *s != "all")
        .map(|s| s.to_string());
    let project = params
        .get("project")
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty() && *s != "all")
        .map(|s| s.to_string());
    let days = params
        .get("days")
        .and_then(|d| d.parse::<i64>().ok())
        .filter(|d| (1..=365).contains(d));
    let limit = params
        .get("limit")
        .and_then(|l| l.parse::<usize>().ok())
        .unwrap_or(30)
        .min(100);

    let exclude_session = params
        .get("exclude_session")
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty());
    let exclude_sessions: Vec<&str> = exclude_session.into_iter().collect();
    let filter = context::SearchFilter {
        source: source.as_deref(),
        role: role.as_deref(),
        project: project.as_deref(),
        since: days.map(tokenbuddy::cn_midnight),
        limit,
        exclude_sessions: &exclude_sessions,
    };
    let resp = context.search(&q, &filter)?;
    Ok(serde_json::to_string(&resp)?)
}

/// `GET /api/context/session?source=&session_id=&doc_id=&around=` — the
/// conversation around one search hit, so a match can be read in place.
fn handle_context_session(context: &ContextHandle, path: &str) -> Result<String> {
    let params = parse_params(path);
    let source = params.get("source").cloned().unwrap_or_default();
    let session_id = params.get("session_id").cloned().unwrap_or_default();
    let doc_id: i64 = params
        .get("doc_id")
        .and_then(|d| d.parse().ok())
        .unwrap_or(i64::MIN);
    let around = params
        .get("around")
        .and_then(|a| a.parse().ok())
        .unwrap_or(10);
    anyhow::ensure!(
        !source.is_empty() && !session_id.is_empty(),
        "source 与 session_id 必填"
    );
    Ok(serde_json::to_string(&context.session_view(
        &source,
        &session_id,
        doc_id,
        around,
    )?)?)
}

/// `GET /api/context/stats` — index build phase plus corpus figures, so the
/// dashboard can show "indexing" instead of an empty result list.
fn handle_context_stats(context: &ContextHandle) -> Result<String> {
    let body = serde_json::json!({
        "status": context.status(),
        "index": context.index_stats(),
    });
    Ok(body.to_string())
}

fn handle_metrics(store: &Store, path: &str) -> Result<String> {
    let f = filters_from(path);
    let metrics = store.query_metrics(
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    Ok(serde_json::to_string(&metrics)?)
}

/// `GET /api/insights?timeRange=&source=&model=&limit=` — the deep-analysis
/// panels (hour-of-day rhythm, daily cache efficiency, session leaderboard,
/// context fill trend) in one call.
fn handle_insights(store: &Store, path: &str) -> Result<String> {
    let f = filters_from(path);
    let limit = parse_params(path)
        .get("limit")
        .and_then(|l| l.parse::<usize>().ok())
        .filter(|l| (1..=100).contains(l))
        .unwrap_or(20);
    let insights = store.query_insights(
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
        limit,
    )?;
    Ok(serde_json::to_string(&insights)?)
}

fn handle_models(store: &Store, path: &str) -> Result<String> {
    let f = filters_from(path);
    let comparison = store.query_models(
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    Ok(serde_json::to_string(&comparison)?)
}

fn handle_heatmap(store: &Store, path: &str) -> Result<String> {
    let params = parse_params(path);
    let mode = params
        .get("mode")
        .map(|s| s.as_str())
        .unwrap_or("model_x_source");
    let metric = params
        .get("metric")
        .map(|s| s.as_str())
        .unwrap_or("total_tokens");
    let f = filters_from(path);

    let heatmap = store.query_heatmap(
        mode,
        metric,
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    Ok(serde_json::to_string(&heatmap)?)
}

fn handle_timeline(store: &Store, path: &str) -> Result<String> {
    let params = parse_params(path);
    let mode = match params.get("mode").map(|s| s.as_str()).unwrap_or("daily") {
        "hourly" => TimelineMode::Hourly,
        "daily" => TimelineMode::Daily,
        "weekly" => TimelineMode::Weekly,
        "monthly" => TimelineMode::Monthly,
        _ => TimelineMode::Daily,
    };
    // The model filter has to be forwarded: the dashboard sends it, and with
    // it dropped the timeline was the one panel that kept showing unfiltered
    // numbers while every other one narrowed.
    let f = filters_from(path);

    let timeline = store.query_timeline(
        mode,
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    Ok(serde_json::to_string(&timeline)?)
}

/// Boundaries of the digest comparison: `(cur_start, now, prev_start)`.
///
/// The previous window is shifted back by the *same span* the current one
/// covers, so the two are exactly equal. Deriving it as
/// `[today - 2*days, today - days)` left the current window longer by however
/// much of today has already elapsed, inflating every delta by roughly
/// `1/days` — at 7 days, about 13% of phantom growth. `cur_start` stays on a
/// China-local midnight so the daily buckets still align to whole days, and
/// the current window still runs right up to now.
fn digest_windows(days: i64) -> (i64, i64, i64) {
    let now = tokenbuddy::now_ts();
    let cur_start = tokenbuddy::cn_midnight(0) - days * 86_400;
    let span = (now - cur_start).max(1);
    (cur_start, now, cur_start - span)
}

/// Consolidated at-a-glance report for a recent window (default 7 days):
/// current vs previous window totals with change ratios, per-day buckets,
/// per-source split and top models by tokens — one call for the dashboard
/// digest panel instead of stitching several filtered queries client-side.
fn handle_digest(store: &Store, path: &str) -> Result<String> {
    let params = parse_params(path);
    let days: i64 = params
        .get("days")
        .and_then(|d| d.parse::<i64>().ok())
        .filter(|d| (1..=365).contains(d))
        .unwrap_or(7);

    let (cur_start, now, prev_start) = digest_windows(days);

    let cur = store.query_summary(None, None, Some(cur_start), Some(now))?;
    let prev = store.query_summary(None, None, Some(prev_start), Some(cur_start))?;
    let daily =
        store.query_timeline(TimelineMode::Daily, None, None, Some(cur_start), Some(now))?;

    let cache_hit = |s: &tokenbuddy::store::Summary| {
        let input_side = s.total_input_tokens + s.total_cache_read_tokens;
        if input_side > 0 {
            Some(s.total_cache_read_tokens as f64 / input_side as f64)
        } else {
            None
        }
    };
    let window = |s: &tokenbuddy::store::Summary| {
        serde_json::json!({
            "tokens": s.total_tokens,
            "requests": s.total_requests,
            "input": s.total_input_tokens,
            "output": s.total_output_tokens,
            "cache_read": s.total_cache_read_tokens,
            "cache_hit": cache_hit(s),
        })
    };
    let delta = |a: f64, b: f64| {
        if b > 0.0 {
            Some((a - b) / b)
        } else {
            None
        }
    };

    let mut top_models: Vec<&tokenbuddy::store::ModelRow> = cur.by_model.iter().collect();
    top_models.sort_by_key(|m| std::cmp::Reverse(m.total_tokens));
    let top_models: Vec<serde_json::Value> = top_models
        .into_iter()
        .take(6)
        .map(|m| {
            serde_json::json!({
                "model": m.model,
                "tokens": m.total_tokens,
                "requests": m.requests,
            })
        })
        .collect();

    let mut by_source: Vec<serde_json::Value> = cur
        .by_source
        .iter()
        .map(|s| {
            serde_json::json!({
                "source": s.source,
                "tokens": s.input_tokens + s.output_tokens + s.cache_read_tokens + s.cache_creation_tokens,
                "requests": s.requests,
                // Masked sources (Qoder tokens are server-zeroed) would rank on
                // 0 forever; credits are their real consumption unit.
                "credits": s.credits,
            })
        })
        .collect();
    by_source.sort_by(|a, b| {
        let av = a["tokens"].as_u64().unwrap_or(0);
        let bv = b["tokens"].as_u64().unwrap_or(0);
        bv.cmp(&av)
    });

    let daily: Vec<serde_json::Value> = daily
        .into_iter()
        .map(|b| {
            serde_json::json!({
                "label": b.label,
                "tokens": b.total_tokens,
                "requests": b.requests,
            })
        })
        .collect();

    let report = serde_json::json!({
        "days": days,
        "current": window(&cur),
        "previous": window(&prev),
        "delta": {
            "tokens": delta(cur.total_tokens as f64, prev.total_tokens as f64),
            "requests": delta(cur.total_requests as f64, prev.total_requests as f64),
        },
        "daily": daily,
        "top_models": top_models,
        "by_source": by_source,
    });
    Ok(report.to_string())
}

#[cfg(test)]
mod tests {
    use super::{digest_windows, parse_params, percent_decode};

    #[test]
    fn decodes_the_model_names_the_dashboard_sends() {
        // URLSearchParams encodes '/' as %2F, '+' as %2B and a space as '+'.
        assert_eq!(
            percent_decode("Qwen%2FQwen3-Coder-480B"),
            "Qwen/Qwen3-Coder-480B"
        );
        assert_eq!(percent_decode("claude-x%2Bgpt-y"), "claude-x+gpt-y");
        assert_eq!(percent_decode("gpt-5+mini"), "gpt-5 mini");
        assert_eq!(percent_decode("claude-sonnet-4-5"), "claude-sonnet-4-5");
        // Decoding runs on bytes, so multi-byte UTF-8 survives.
        assert_eq!(percent_decode("%E6%B7%B7%E5%85%83"), "混元");
    }

    #[test]
    fn malformed_escapes_pass_through_verbatim() {
        assert_eq!(percent_decode("%2"), "%2");
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
    }

    #[test]
    fn params_are_decoded_and_split_on_the_first_equals_only() {
        let p = parse_params("/api/models?timeRange=30d&model=Qwen%2FQwen3-Coder-480B&source=all");
        assert_eq!(p.get("timeRange").map(String::as_str), Some("30d"));
        assert_eq!(
            p.get("model").map(String::as_str),
            Some("Qwen/Qwen3-Coder-480B")
        );
        assert_eq!(p.get("source").map(String::as_str), Some("all"));
        assert!(parse_params("/api/models").is_empty());
    }

    /// A digest delta is only meaningful if both windows span the same
    /// duration; this is the regression test for the equal-length fix.
    #[test]
    fn digest_windows_are_equal_length_and_end_now() {
        for days in [1, 7, 30, 365] {
            let (cur_start, now, prev_start) = digest_windows(days);
            assert_eq!(
                now - cur_start,
                cur_start - prev_start,
                "{days}-day digest windows are not the same length"
            );
            // cur_start sits on a China-local midnight, so the span is the
            // requested number of whole days plus however much of today has
            // elapsed — never a full extra day.
            assert!(now - cur_start >= days * 86_400, "{days}d window too short");
            assert!(
                now - cur_start < (days + 1) * 86_400,
                "{days}d window too long"
            );
        }
    }
}
