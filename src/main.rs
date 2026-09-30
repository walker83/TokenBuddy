use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tiny_http::{Header, Response, Server};
use tokenbuddy::context::{self, ContextHandle};
use tokenbuddy::fleet;
use tokenbuddy::store::{Store, TimelineMode};

// Dashboard asset, zstd-compressed at build time (see build.rs). Served
// as-is to zstd-capable clients; decompressed once for everyone else.
const HTML_ZSTD: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/dashboard.html.zstd"));
static HTML: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn dashboard_html() -> &'static str {
    HTML.get_or_init(|| {
        String::from_utf8(zstd::decode_all(HTML_ZSTD).expect("embedded asset decompresses"))
            .expect("dashboard asset is utf8")
    })
}

/// True when the request's Accept-Encoding includes zstd.
fn client_accepts_zstd(headers: &[Header]) -> bool {
    header_value(headers, "Accept-Encoding")
        .map(|v| {
            v.split(',')
                .any(|part| part.trim().to_ascii_lowercase().starts_with("zstd"))
        })
        .unwrap_or(false)
}

/// Every response body this server writes is a `String`, so the tiny_http
/// response type is the same everywhere and handlers can share helpers.
type JsonResponse = Response<std::io::Cursor<Vec<u8>>>;

/// Upper bound for JSON request bodies. fleet.toml is well under 1 KiB; the
/// cap exists so a request with a huge body is refused (413) instead of being
/// read into resident memory.
const MAX_CONFIG_BODY_BYTES: usize = 1024 * 1024;

/// Read at most `cap` bytes of the request body as UTF-8. Returns `Err` when
/// the body exceeds the cap — the remaining bytes are never read, so the
/// caller answers with a 413 and the request's memory never materializes.
fn read_capped_body(request: &mut tiny_http::Request, cap: usize) -> std::io::Result<String> {
    use std::io::Read;
    let mut buf = Vec::new();
    let mut reader = request.as_reader().take((cap + 1) as u64);
    reader.read_to_end(&mut buf)?;
    if buf.len() > cap {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "body exceeds cap",
        ));
    }
    String::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn json_response(body: String) -> JsonResponse {
    Response::from_string(body).with_header(
        Header::from_bytes("Content-Type", "application/json")
            .expect("hardcoded header should be valid"),
    )
}

/// Extra Host values accepted beside the loopback names, read once at
/// startup. This exists for reverse-proxy deployments (the Fleet aggregator
/// box serves the dashboard behind the homelab console proxy) — a proxy that
/// rewrites `Host` names itself in `TOKENBUDDY_ALLOWED_HOSTS`, comma
/// separated. Loopback names are always allowed; this list only ever widens.
fn extra_allowed_hosts() -> Vec<String> {
    std::env::var("TOKENBUDDY_ALLOWED_HOSTS")
        .map(|v| {
            v.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_ascii_lowercase())
                .collect()
        })
        .unwrap_or_default()
}

/// Split a `Host`/`Origin` authority into (host, port). Bracketed IPv6 is
/// unwrapped; a bare IPv6 literal without brackets cannot appear in a Host
/// header, so `rsplit_once` on ':' is safe for everything else.
fn split_host_port(authority: &str) -> (&str, Option<u16>) {
    if let Some(rest) = authority.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            let after = &rest[end + 1..];
            let port = after.strip_prefix(':').and_then(|p| p.parse().ok());
            return (&rest[..end], port);
        }
    }
    match authority.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
            (h, p.parse().ok())
        }
        _ => (authority, None),
    }
}

/// Is this host[:port] one the server answers on? The loopback names are
/// always allowed; a port must match the listener's when present (absent
/// port = default-port style, HTTP/1.0 clients).
fn host_allowed(hostport: &str, port: u16, extra: &[String]) -> bool {
    let lowered = hostport.trim().to_ascii_lowercase();
    if lowered.is_empty() {
        return false;
    }
    let (host, hport) = split_host_port(&lowered);
    if let Some(p) = hport {
        if p != port {
            return false;
        }
    }
    matches!(host, "127.0.0.1" | "localhost" | "::1") || extra.iter().any(|e| e == host)
}

/// Browsers attach `Origin` to every cross-site POST; `null` is what a
/// sandboxed or redirected context sends. Only our own http loopback origin
/// (or an explicitly allowed host) may mutate state.
fn origin_allowed(origin: &str, port: u16, extra: &[String]) -> bool {
    match origin.strip_prefix("http://") {
        Some(rest) => host_allowed(rest, port, extra),
        None => false,
    }
}

/// Case-insensitive header lookup (tiny_http's `equiv` only takes
/// `&'static str`, so the comparison is done by hand).
fn header_value<'a>(headers: &'a [Header], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|h| h.field.as_str().as_str().eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
}

/// F1/F2 of the 2026-09-28 security audit. Two independent defenses:
///
/// * `Host` (every method) must be a loopback name on this port. A rebinding
///   attack resolves its own domain to 127.0.0.1, so the browser's Host
///   header carries the attacker's domain and is refused here — the response
///   stays unreadable to any other origin.
/// * `Origin`/`Sec-Fetch-Site` (POST only) kills drive-by CSRF: any web page
///   can *send* POSTs at 127.0.0.1 (WICG LNA: responses are blocked, the
///   request still lands), so the server itself must reject cross-origin
///   writes. Non-browser clients (curl, statusline scripts) send neither
///   header and pass untouched.
fn cross_origin_check(
    method: &str,
    headers: &[Header],
    port: u16,
    extra: &[String],
) -> Result<(), &'static str> {
    if let Some(h) = header_value(headers, "Host") {
        if !host_allowed(h, port, extra) {
            return Err("Host 不在允许列表（rebinding 防护）");
        }
    }
    if method != "POST" {
        return Ok(());
    }
    if let Some(o) = header_value(headers, "Origin") {
        if !origin_allowed(o, port, extra) {
            return Err("Origin 跨源（CSRF 防护）");
        }
        return Ok(());
    }
    if let Some(s) = header_value(headers, "Sec-Fetch-Site") {
        let s = s.trim().to_ascii_lowercase();
        if s == "cross-site" || s == "same-site" {
            return Err("Sec-Fetch-Site 跨站（CSRF 防护）");
        }
    }
    Ok(())
}

/// Constant-time-enough token comparison for a LAN tool: reject length
/// mismatch, then XOR-fold the bytes so timing does not leak the prefix.
fn tokens_equal(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// R3 (audit F2): when `TOKENBUDDY_TOKEN` is set, every request must present
/// it — `Authorization: Bearer …` for scripts, or `?token=…` for a browser
/// navigating to the page (a top-level navigation cannot carry headers; the
/// dashboard then propagates the token into its own API calls).
fn request_has_token(headers: &[Header], url: &str, expected: &str) -> bool {
    if let Some(auth) = header_value(headers, "Authorization") {
        let bearer = auth
            .strip_prefix("Bearer ")
            .or_else(|| auth.strip_prefix("bearer "));
        if let Some(bearer) = bearer {
            return tokens_equal(bearer.trim(), expected);
        }
    }
    if let Some(query) = url.split('?').nth(1) {
        for pair in query.split('&') {
            let mut kv = pair.splitn(2, '=');
            if kv.next() == Some("token") {
                if let Some(v) = kv.next() {
                    return tokens_equal(&percent_decode(v), expected);
                }
            }
        }
    }
    false
}

/// Resolve the listen address: `--addr` / `TOKENBUDDY_ADDR` (`ip:port`, the
/// port optional), default loopback on `port`. Opening the port to the
/// network requires `TOKENBUDDY_TOKEN` — the refusal happens here, at
/// startup, so a remote bind without a lock never comes up at all.
fn resolve_bind_addr(
    addr_flag: Option<&str>,
    port: u16,
    token_set: bool,
) -> Result<(std::net::IpAddr, u16)> {
    let text = match addr_flag {
        Some(a) => Some(a.to_string()),
        None => std::env::var("TOKENBUDDY_ADDR").ok(),
    };
    let (ip, p) = match text {
        Some(t) => {
            let trimmed = t.trim();
            // A bare IPv6 literal ("::1") parses directly; only fall back to
            // host:port splitting (which cannot handle a bracketless "::1")
            // when that fails.
            match trimmed.parse::<std::net::IpAddr>() {
                Ok(ip) => (ip, port),
                Err(_) => {
                    let (host, hport) = split_host_port(trimmed);
                    let ip: std::net::IpAddr = host.parse().map_err(|_| {
                        anyhow::anyhow!(
                            "无法解析监听地址「{t}」——形如 127.0.0.1:8080、0.0.0.0 或 ::1"
                        )
                    })?;
                    (ip, hport.unwrap_or(port))
                }
            }
        }
        None => (std::net::IpAddr::from([127, 0, 0, 1]), port),
    };
    if !ip.is_loopback() && !token_set {
        anyhow::bail!(
            "绑定非环回地址 {ip} 会把仪表盘开放到网络——必须先设置 \
             TOKENBUDDY_TOKEN(访问令牌)再启动"
        );
    }
    Ok((ip, p))
}

/// Every response carries the baseline hardening headers: no sniffing, no
/// framing (clickjacking), no referrer leak to whatever a page links out to.
fn with_security_headers(mut response: JsonResponse) -> JsonResponse {
    for (name, value) in [
        ("X-Content-Type-Options", "nosniff"),
        ("X-Frame-Options", "DENY"),
        ("Referrer-Policy", "no-referrer"),
        ("Content-Security-Policy", "frame-ancestors 'none'"),
    ] {
        response
            .add_header(Header::from_bytes(name, value).expect("static header should be valid"));
    }
    response
}

/// Errors go through serde rather than `format!` — DuckDB and IO messages
/// routinely contain double quotes, and interpolating one into a JSON string
/// literal produces a malformed body that the client reports as an opaque
/// parse failure, hiding the real cause.
fn error_response(e: &anyhow::Error) -> JsonResponse {
    // A caller mistake (bad params) is a 400 — a 500 there reads as "our
    // server broke", which is wrong and unactionable for API consumers
    // building watchdogs and bots (issue #1). Param helpers tag their errors
    // with `BadRequest` in the context chain.
    let code = if e.chain().any(|c| c.downcast_ref::<BadRequest>().is_some()) {
        400
    } else {
        500
    };
    let body = serde_json::json!({ "error": e.to_string() }).to_string();
    Response::from_string(body)
        .with_header(
            Header::from_bytes("Content-Type", "application/json")
                .expect("hardcoded header should be valid"),
        )
        .with_status_code(code)
}

/// Marker for caller-caused failures; travels in the anyhow context chain so
/// route arms can keep a single `Err => error_response` path.
#[derive(Debug)]
struct BadRequest;

impl std::fmt::Display for BadRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bad request")
    }
}
impl std::error::Error for BadRequest {}

fn client_error(msg: impl std::fmt::Display) -> anyhow::Error {
    anyhow::Error::new(BadRequest).context(msg.to_string())
}

/// Prefixes of endpoints that ignore `days` (their window is `timeRange`).
/// The router turns any `?days=` on these into a 400 before dispatch.
const DAYS_UNSUPPORTED: &[&str] = &[
    "/api/summary",
    "/api/timeline",
    "/api/metrics",
    "/api/heatmap",
    "/api/insights",
    "/api/models",
    "/api/pivot",
    "/api/anomalies",
    "/api/windows",
    "/api/forecast",
    "/api/brief",
    "/api/context-health",
    "/api/fleet/summary",
    "/api/fleet/metrics",
    "/api/fleet/models",
    "/api/fleet/hosts",
    "/api/fleet/quota",
];

/// Route-arm matching for fixed endpoints: the path segment must equal
/// `endpoint` exactly; a query string (`?a=b`) still counts. Prefix arms
/// (`starts_with`) used to let `/api/status/extra` fall into the status
/// handler and answer 200 with the full body — a typo'd URL looked like
/// real data, proxies cached it, and debugging by 4xx was impossible
/// (issue #30 Bug 4).
fn route_is(path: &str, endpoint: &str) -> bool {
    match path.split_once('?') {
        Some((segment, _)) => segment == endpoint,
        None => path == endpoint,
    }
}

/// The one 404 every unmatched path gets. Points at `/api/docs` — the
/// machine-readable index — rather than `/api/`, which returns JSON a
/// human skimming for "what did I mistype" can't read (issue #30 Bug 6).
fn not_found_message(path: &str) -> String {
    format!("端点不存在：{path}（GET /api/docs 查看全部端点）")
}

fn not_found_response(path: &str) -> JsonResponse {
    let body = serde_json::json!({ "error": not_found_message(path) }).to_string();
    Response::from_string(body)
        .with_header(
            Header::from_bytes("Content-Type", "application/json")
                .expect("hardcoded header should be valid"),
        )
        .with_status_code(404)
}

/// Endpoints documented as timeRange-only (summary, timeline) refuse a
/// passed `days` outright with a 400 instead of silently ignoring it — a
/// silent no-op reads as "this IS the days-filtered result" (issue #12
/// Bug 1, a regression of the issue #1 family).
fn reject_unsupported_days(path: &str) -> Result<()> {
    if parse_params(path).contains_key("days") {
        return Err(client_error(
            "该端点不接受 days 参数，请用 timeRange（all|today|Nd，N 为 1–365 整数）",
        ));
    }
    Ok(())
}

/// Parse a request's query string into decoded key/value pairs.
///
/// The dashboard builds queries with `URLSearchParams`, so a model name like
/// `Qwen/Qwen3-Coder-480B` arrives as `Qwen%2FQwen3-Coder-480B` and a
/// WorkBuddy composite `a+b` as `a%2Bb`. Handing the raw text to the
/// `model LIKE` filter would match nothing for exactly the names users paste
/// into the box, so values are decoded here.
/// `GET /api/docs` — the machine-readable API index (issue #1 bug 3: the
/// endpoint list used to live only in this file's match arms). Keep in step
/// with the routes below; params shown are the documented ones.
fn api_endpoints() -> serde_json::Value {
    let e = |method: &str, path: &str, desc: &str| serde_json::json!({ "method": method, "path": path, "desc": desc });
    serde_json::json!([
        e("GET", "/", "仪表盘（单个内嵌 HTML）"),
        e("GET", "/api/health", "存活探针：不触数据层，看门狗用"),
        e("GET", "/api/docs", "本索引"),
        e("GET", "/api/sources", &format!("全部合法 source id（所有 ?source= 端点的白名单）：{}", tokenbuddy::SOURCE_NAMES.join("|"))),
        e("GET", "/api/skill", "内嵌 tokenbuddy-analyze SKILL.md(markdown + 安装位置),仪表盘 Skills 面板用"),
        e("POST", "/api/sync?mode=incremental|full", "导入新日志记录，刷新统计"),
        e("GET", "/api/summary?timeRange=&source=&model=", "总量 + 按来源/模型汇总（timeRange: all|today|Nd，N 为 1–365；传 days 返 400）"),
        e("GET", "/api/timeline?timeRange=&mode=daily|hourly|weekly|monthly&source=&model=", "分桶用量（timeRange: all|today|Nd；days 参数不接受，返 400）"),
        e("GET", "/api/metrics?timeRange=&source=&model=", "逐请求指标聚合（耗时/缓存/TTFT）"),
        e("GET", "/api/heatmap?mode=model_x_source|model_x_day&metric=&timeRange=", "热力图矩阵"),
        e("GET", "/api/models?timeRange=&source=&model=", "模型对比表"),
        e("GET", "/api/digest?days=7", "本期 vs 上期（days: 1–365）"),
        e("GET", "/api/insights?limit=20&timeRange=&source=&model=", "深度分析（limit: 1–100）"),
        e("GET", "/api/brief", "statusline 用：今日 + 近 7 天一行小 JSON"),
        e("GET", "/api/windows", "5 小时窗口分段事实 + 28 天 P90 自参考"),
        e("GET", "/api/quota", "套餐余量快照(Codex rollout 零外呼实时读 + 命令采集器最近一次结果)"),
        e("GET", "/api/quota/config", "读 quota.json 配置(parsers + sources 名册 + disabled_sources 停用开关)"),
        e("POST", "/api/quota/config", "校验并原子写 quota.json(0600;disabled_sources 停用源,下次 sync 生效)"),
        e("POST", "/api/quota/refresh", "显式运行 quota.json 里的命令采集器并落盘(不配置则无进程可跑)"),
        e("GET", "/api/anomalies", "日用量异常（审计窗内建 56 天，不接受参数）"),
        e("GET", "/api/pivot?start=&end=", "项目 × 模型透视（start/end: epoch 秒，可省略）"),
        e("GET", "/api/active-time?days=7", "投入时长:按日活跃小时+按来源/项目拆分(days: 1–365;15 分钟间隔会话化,日合计为真实墙钟时间)"),
        e("GET", "/api/context-health", "各来源最近一次上下文水位(>75% 建议收尾或重启会话);仅统计来源上报了水位的请求"),
        e("GET", "/api/forecast", "周终外推:本自然周(周一起,UTC+8)至今用量,按前 28 天中位日节奏与本期实际日均两种口径预计周末总量"),
        e("GET", "/api/work-receipts?days=7", "工作收据(三源):每会话改动的文件/命令数/测试数,来自 ZCode part 表/Claude transcript/OpenCode session_message 的 tool 块;rework_top 为跨会话返工热点(≥2 会话编辑同一文件)"),
        e("GET", "/api/context/search?q=&limit=&source=&project=&session=&days=", "全文搜索（limit: 1–100，days: 1–365；q 另支持 source: project: session: role: days: -排除 \"短语\" 语法）"),
        e("GET", "/api/context/session?source=&session_id=&doc_id=&around=", "命中处的上下文会话（doc_id，或 source+session_id 二选一）"),
        e("GET", "/api/context/stats", "索引构建状态 + 语料规模"),
        e("POST", "/api/context/click?doc_id=", "记录搜索结果点击（排序反馈）"),
        e("GET", "/api/context/quality", "搜索质量报告"),
        e("POST", "/api/context/rebuild", "强制全量重建索引"),
        e("GET", "/api/status", "行数/上次同步/各采集器状态"),
        e("GET", "/api/fleet/config", "读 Fleet 配置（secret_key 掩码）"),
        e("POST", "/api/fleet/config", "写 fleet.toml（0600）"),
        e("POST", "/api/fleet/push", "本地同步后整文件推送到 Fleet bucket"),
        e("POST", "/api/fleet/pull", "拉取全部主机 parquet（etag 增量）"),
        e("GET", "/api/fleet/hosts", "Fleet 可用主机列表"),
        e("GET", "/api/fleet/quota", "Fleet 各主机套餐余量(fleet-sync 拉回的 quota.json;本机未经推送的看不到)"),
        e("GET", "/api/fleet/summary?timeRange=&source=&model=&host=", "Fleet 总账 + 各机明细"),
        e("GET", "/api/fleet/metrics?…", "按主机分维的耗时/缓存面板"),
        e("GET", "/api/fleet/models?…", "跨主机的模型对比")
    ])
}

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

/// Strict integer params: "absent" and "malformed" must stay distinct — a
/// silently-ignored `days=abc` answers a different question than the caller
/// asked (issue #1) and is invisible from the response. Absent → None;
/// present but unparseable → 400 via the route's error path.
fn param_i64(params: &HashMap<String, String>, name: &str) -> Result<Option<i64>> {
    match params.get(name).map(|s| s.trim()).filter(|s| !s.is_empty()) {
        None => Ok(None),
        Some(raw) => raw
            .parse::<i64>()
            .map(Some)
            .map_err(|_| client_error(format!("参数 {name} 必须是整数，得到 “{raw}”"))),
    }
}

fn param_usize(params: &HashMap<String, String>, name: &str) -> Result<Option<usize>> {
    match params.get(name).map(|s| s.trim()).filter(|s| !s.is_empty()) {
        None => Ok(None),
        Some(raw) => raw
            .parse::<usize>()
            .map(Some)
            .map_err(|_| client_error(format!("参数 {name} 必须是非负整数，得到 “{raw}”"))),
    }
}

/// The common `days` window: 1–365, absent → None. Out of range is a caller
/// mistake (days=0 means "today" in some APIs and "everything" in others),
/// so it fails loudly instead of clamping to a guess.
fn days_param(params: &HashMap<String, String>) -> Result<Option<i64>> {
    match param_i64(params, "days")? {
        None => Ok(None),
        Some(d) if (1..=365).contains(&d) => Ok(Some(d)),
        Some(d) => Err(client_error(format!("参数 days 超出范围（1–365）：{d}"))),
    }
}

fn limit_param(params: &HashMap<String, String>, default: usize, max: usize) -> Result<usize> {
    match param_usize(params, "limit")? {
        None => Ok(default),
        Some(l) if (1..=max).contains(&l) => Ok(l),
        Some(l) => Err(client_error(format!("参数 limit 超出范围（1–{max}）：{l}"))),
    }
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

/// Command-line surface, hand-rolled: a flag parser is not worth a
/// dependency in a binary whose selling point is 4 MB and nothing else.
struct Options {
    port: u16,
    /// Open the dashboard in the default browser once the listener is up.
    open_browser: bool,
}

const DEFAULT_PORT: u16 = 8080;

fn usage() -> String {
    format!(
        "TokenBuddy {} — 本地 AI 编程工具的 token 账单与对话检索\n\
         \n\
         用法：\n\
         \x20 tokenbuddy [serve]        启动本地服务（默认 127.0.0.1:{DEFAULT_PORT}）\n\
         \x20 tokenbuddy status         在终端打印一行当前状态\n\
         \x20 tokenbuddy doctor [--json]  逐源体检:日志在哪/可读吗/为什么没统计到\n\
         \x20 tokenbuddy quota [--refresh] [--json]  套餐余量(--refresh 才会运行 quota.json 里的命令采集器)\n\
         \x20 tokenbuddy statusline        Claude Code statusline:stdin JSON → 一行 enriched 状态\n\\
         \x20 tokenbuddy today            一行今日/7日用量(statusline 用)\n\
         \x20 tokenbuddy sessions [--days N] [--top N] [--json]  最近会话清单(分型/跨度/来源)\n\
         \x20 tokenbuddy report [--days N] [--json]  markdown 日报/周报(stdout;--json 给脚本)\n\
         \x20 tokenbuddy export [--out 目录]   导出全部数据(带 sha256 清单)\n\
         \x20 tokenbuddy import 目录 [--force] 导入导出目录(覆盖现有账本)\n\
         \x20 tokenbuddy mcp             MCP server(stdio)——让 coding agent 直接查账/搜会话\n\
         \x20 tokenbuddy push           同步本地数据并推送到 Fleet\n\
         \x20 tokenbuddy fleet-sync    拉取全部主机的 Fleet 数据\n\
         \n\
         选项：\n\
         \x20 --port <端口>     监听端口，默认 {DEFAULT_PORT}（也可用环境变量 TOKENBUDDY_PORT）\n\
         \x20 --addr <ip:端口>  监听地址（也可用 TOKENBUDDY_ADDR）；非环回地址必须先设 TOKENBUDDY_TOKEN\n\
         \x20 --no-open         启动后不自动打开浏览器（也可用 TOKENBUDDY_NO_OPEN=1）\n\
         \x20 -h, --help        显示本帮助\n\
         \x20 -V, --version     显示版本号",
        env!("CARGO_PKG_VERSION")
    )
}

/// True when stdout is a terminal. A service manager (launchd, systemd)
/// redirects it to a file, and popping a browser window on every restart is
/// exactly the kind of surprise a background service must not spring.
fn stdout_is_terminal() -> bool {
    unsafe { libc::isatty(1) == 1 }
}

/// Environment first, explicit flag wins. Returns `Err` with the offending
/// text so `--port abc` says what is wrong instead of silently using 8080.
fn resolve_port(flag: Option<&str>) -> anyhow::Result<u16> {
    let raw = match flag {
        Some(v) => Some(v.to_string()),
        None => std::env::var("TOKENBUDDY_PORT").ok(),
    };
    match raw {
        None => Ok(DEFAULT_PORT),
        Some(v) => {
            let n: u16 = v
                .trim()
                .parse()
                .map_err(|_| anyhow::anyhow!("端口必须是 1-65535 之间的整数，收到的是「{v}」"))?;
            anyhow::ensure!(n > 0, "端口必须是 1-65535 之间的整数，收到的是「{v}」");
            Ok(n)
        }
    }
}

fn browser_should_open(no_open_flag: bool) -> bool {
    if no_open_flag {
        return false;
    }
    if std::env::var("TOKENBUDDY_NO_OPEN")
        .map(|v| !v.is_empty() && v != "0")
        .unwrap_or(false)
    {
        return false;
    }
    stdout_is_terminal()
}

/// Fire the platform's default browser at `url`, detached. A launcher that
/// is missing or refuses is not worth reporting: the address is on stdout.
fn open_browser(url: String) {
    std::thread::spawn(move || {
        // Long enough for the listener to accept, short enough that the user
        // does not think the launch failed.
        std::thread::sleep(std::time::Duration::from_millis(400));
        let opener = if cfg!(target_os = "macos") {
            "open"
        } else {
            "xdg-open"
        };
        let _ = std::process::Command::new(opener)
            .arg(&url)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    });
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut port_flag: Option<String> = None;
    let mut addr_flag: Option<String> = None;
    let mut no_open = false;
    let mut positional: Vec<String> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" => {
                println!("{}", usage());
                return Ok(());
            }
            "-V" | "--version" => {
                println!("tokenbuddy {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            "--port" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("--port 后面需要端口号"))?;
                port_flag = Some(v.clone());
            }
            "--addr" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("--addr 后面需要 ip:端口"))?;
                addr_flag = Some(v.clone());
            }
            "--no-open" => no_open = true,
            other if other.starts_with("--port=") => {
                port_flag = Some(other.trim_start_matches("--port=").to_string());
            }
            other if other.starts_with("--addr=") => {
                addr_flag = Some(other.trim_start_matches("--addr=").to_string());
            }
            other => positional.push(other.to_string()),
        }
        i += 1;
    }

    let options = Options {
        port: resolve_port(port_flag.as_deref())?,
        open_browser: browser_should_open(no_open),
    };

    match positional.first().map(String::as_str) {
        None | Some("serve") => {}
        Some("push") => return cmd_push(),
        Some("fleet-sync") => return cmd_fleet_sync(),
        Some("status") => return cmd_status(),
        Some("today") => return cmd_today(),
        Some("export") => {
            let out = positional
                .iter()
                .position(|a| a == "--out")
                .and_then(|i| positional.get(i + 1))
                .map(PathBuf::from)
                .unwrap_or_else(default_export_dir);
            return cmd_export(&out);
        }
        Some("import") => {
            let dir = positional
                .iter()
                .find(|a| !a.starts_with("--"))
                .map(PathBuf::from)
                .ok_or_else(|| anyhow::anyhow!("import 需要导出目录路径"))?;
            let force = positional.iter().any(|a| a == "--force");
            return cmd_import(&dir, force);
        }
        Some("report") => {
            let mut days = 1i64;
            if let Some(i) = positional.iter().position(|a| a == "--days") {
                let v = positional
                    .get(i + 1)
                    .ok_or_else(|| anyhow::anyhow!("--days 后面需要天数"))?;
                days = v.parse()?;
            }
            let json = positional.iter().any(|a| a == "--json");
            return cmd_report(days, json);
        }
        Some("mcp") => return tokenbuddy::mcp::run(),
        Some("doctor") => {
            let json = positional.iter().any(|a| a == "--json");
            return cmd_doctor(json);
        }
        Some("statusline") => return cmd_statusline(),
        Some("sessions") => {
            let mut days = 7i64;
            if let Some(i) = positional.iter().position(|a| a == "--days") {
                let v = positional
                    .get(i + 1)
                    .ok_or_else(|| anyhow::anyhow!("--days 后面需要天数"))?;
                days = v.parse()?;
            }
            let mut top = 15usize;
            if let Some(i) = positional.iter().position(|a| a == "--top") {
                let v = positional
                    .get(i + 1)
                    .ok_or_else(|| anyhow::anyhow!("--top 后面需要条数"))?;
                top = v.parse()?;
            }
            let json = positional.iter().any(|a| a == "--json");
            return cmd_sessions(days, top, json);
        }
        Some("quota") => {
            let refresh = positional.iter().any(|a| a == "--refresh");
            let json = positional.iter().any(|a| a == "--json");
            return cmd_quota(refresh, json);
        }
        Some(arg) => {
            eprintln!("未知参数：{arg}\n\n{}", usage());
            std::process::exit(2);
        }
    }

    // R3: a remote bind refuses to come up without a token (audit F2) —
    // the lock is checked before the port ever opens.
    let token = std::env::var("TOKENBUDDY_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    let (bind_ip, port) = resolve_bind_addr(addr_flag.as_deref(), options.port, token.is_some())?;
    let bind_is_loopback = bind_ip.is_loopback();

    let store = Arc::new(Store::open()?);
    // Held for the whole of any write: incremental sync, full rebuild and
    // Fleet push all rewrite the same parquet, and two of them at once would
    // interleave read-modify-write over the same rows.
    let write_lock = Arc::new(std::sync::Mutex::new(()));
    let addr = format!("{bind_ip}:{port}");
    let server = Arc::new(Server::http(&addr).map_err(|e| {
        // tiny_http hands back a boxed error, so the errno has to be read off
        // the text: EADDRINUSE is 48 on macOS and 98 on Linux, and the English
        // string is the one spelling both agree on. On this port the cause is
        // almost always a second instance, which is a message worth getting
        // right — it is also the first thing a user hits on a second launch.
        let text = e.to_string();
        let in_use = text.contains("Address already in use")
            || text.contains("os error 48")
            || text.contains("os error 98");
        if in_use {
            anyhow::anyhow!(
                "端口 {} 已被占用——多半是 TokenBuddy 已经在运行，直接打开 \
                 http://127.0.0.1:{} 即可；换端口用 --port <端口>",
                port,
                port
            )
        } else {
            anyhow::anyhow!("无法监听 {addr}：{text}")
        }
    })?);

    // Advertise the bound address so scripts and harness plugins can discover
    // the actual port without assuming the default (the plugin used to hardcode
    // 8080 and go dark on --port instances). Best-effort: its absence only
    // costs auto-discovery, and a stale file is rejected by the /api probe.
    let _ = std::fs::write(
        tokenbuddy::data_dir().join("server.json"),
        format!(
            "{{\"port\":{port},\"pid\":{},\"started_at\":{}}}\n",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default(),
        ),
    );

    // R104: the search index builds itself — once in the background at
    // startup, and again after every successful sync — so opening search is
    // instant instead of "indexing…". The 15-idle-minute unload below still
    // hands the memory back, and the post-sync warm runs detached: a sync
    // made for the numbers never blocks on the corpus.
    let context = Arc::new(ContextHandle::new(
        tokenbuddy::data_dir().join("context.parquet"),
    ));
    context.warm_if_needed();
    {
        let ctx = Arc::clone(&context);
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(60));
            ctx.unload_if_idle();
        });
    }

    let url = format!("http://{addr}");
    println!("TokenBuddy server running on {url}");
    println!("Press Ctrl+C to stop");
    if options.open_browser {
        open_browser(url.clone());
    }

    // Fleet auto-push: when the config enables it, a background thread
    // sync-then-pushes on a fixed interval (first round right away, so a
    // restarted server leaves a fresh object behind). Failures go to stderr
    // only — Fleet must never disturb local serving.
    if let Ok(Some(cfg)) = fleet::load_config() {
        // The push and pull threads each own a copy of the config.
        if cfg.enabled && cfg.auto_push {
            let cfg = cfg.clone();
            let store = Arc::clone(&store);
            let write_lock = Arc::clone(&write_lock);
            std::thread::spawn(move || {
                let interval = std::time::Duration::from_secs(cfg.push_interval_secs);
                loop {
                    // Fleet rewrites the same parquet the dashboard reads, so
                    // it takes the same write lock a manual sync does.
                    let _guard = write_lock.lock().unwrap_or_else(|e| e.into_inner());
                    match fleet::push_now(&store, &cfg) {
                        Ok((_, out)) => {
                            eprintln!("[TokenBuddy] fleet push: {} ({} bytes)", out.key, out.bytes)
                        }
                        Err(e) => eprintln!("[TokenBuddy] fleet push failed: {e}"),
                    }
                    drop(_guard);
                    std::thread::sleep(interval);
                }
            });
        }
        // Fleet auto-pull: the read-only twin. Brings every host's parquet +
        // quota.json home on the same cadence so the Fleet view answers
        // without a manual fleet-sync. Writes only under ~/.tokenbuddy/fleet
        // (tmp+rename per object), never the live ledger.
        if cfg.enabled && cfg.auto_pull {
            let cfg = cfg.clone();
            std::thread::spawn(move || {
                let interval = std::time::Duration::from_secs(cfg.push_interval_secs);
                loop {
                    match fleet::pull_all(&cfg) {
                        Ok(out) if !out.downloaded.is_empty() => eprintln!(
                            "[TokenBuddy] fleet pull: {} 台主机有更新",
                            out.downloaded.len()
                        ),
                        Ok(_) => {}
                        Err(e) => eprintln!("[TokenBuddy] fleet pull failed: {e}"),
                    }
                    std::thread::sleep(interval);
                }
            });
        }
    }

    // Requests are served by a small pool rather than one loop. A single
    // loop meant any long call froze the whole dashboard — most visibly
    // `POST /api/fleet/pull`, which downloads every host's parquet before it
    // can answer. Writes still serialize on `write_lock`: sync, full rebuild
    // and Fleet push all rewrite the same file.
    // A non-loopback bind IP is its own allowed Host/Origin name: a browser
    // on another machine reaches us as `http://{ip}:{port}` and that origin
    // is "same-origin" for the hardening above.
    let mut host_names = extra_allowed_hosts();
    if !bind_is_loopback {
        host_names.push(bind_ip.to_string());
    }
    let extra_hosts = Arc::new(host_names);
    let mut workers = Vec::new();
    for _ in 0..4 {
        let server = Arc::clone(&server);
        let store = Arc::clone(&store);
        let context = Arc::clone(&context);
        let write_lock = Arc::clone(&write_lock);
        let extra_hosts = Arc::clone(&extra_hosts);
        let token = token.clone();
        workers.push(std::thread::spawn(move || {
            while let Ok(request) = server.recv() {
                serve_one(
                    request,
                    &store,
                    &context,
                    &write_lock,
                    port,
                    &extra_hosts,
                    token.as_deref(),
                );
            }
        }));
    }
    for worker in workers {
        let _ = worker.join();
    }

    Ok(())
}

/// Route and answer one request.
fn serve_one(
    mut request: tiny_http::Request,
    store: &Arc<Store>,
    context: &Arc<ContextHandle>,
    write_lock: &Arc<std::sync::Mutex<()>>,
    port: u16,
    extra_hosts: &[String],
    token: Option<&str>,
) {
    let url = request.url().to_string();
    let method = request.method().as_str().to_string();

    // Cross-origin guard first: nothing below may run for a request whose
    // Host is not ours, and nothing state-changing may run cross-origin.
    if let Err(reason) = cross_origin_check(&method, request.headers(), port, extra_hosts) {
        eprintln!("[TokenBuddy] refused {} {}: {}", method, url, reason);
        let response =
            json_response(serde_json::json!({ "error": reason }).to_string()).with_status_code(403);
        let _ = request.respond(with_security_headers(response));
        return;
    }

    // R3: with TOKENBUDDY_TOKEN set, every route — page and API alike —
    // requires the token (Authorization: Bearer … or ?token=…).
    if let Some(expected) = token {
        if !request_has_token(request.headers(), &url, expected) {
            let response = json_response(
                serde_json::json!({ "error": "缺少或错误的访问令牌——用 ?token= 或 Authorization: Bearer 提供" })
                    .to_string(),
            )
            .with_status_code(401)
            .with_header(
                Header::from_bytes("WWW-Authenticate", "Bearer")
                    .expect("hardcoded header should be valid"),
            );
            let _ = request.respond(with_security_headers(response));
            return;
        }
    }

    // timeRange-only endpoints refuse a passed `days` outright (issue #12
    // Bug 1 family): a silent no-op reads as "this IS the days-filtered
    // result". One list here covers every handler that ignores days, so a
    // new endpoint can't quietly grow the old inconsistency.
    if DAYS_UNSUPPORTED.iter().any(|p| route_is(url.as_str(), p)) {
        let days_absent = parse_params(&url)
            .get("days")
            .map(|v| v.is_empty())
            .unwrap_or(true);
        if !days_absent {
            let resp = error_response(&reject_unsupported_days(&url).unwrap_err());
            let _ = request.respond(resp);
            return;
        }
    }

    let response: JsonResponse = match (method.as_str(), url.as_str()) {
        // The page itself, bare or with a query (?token=… in remote mode).
        ("GET", path) if path == "/" || path.starts_with("/?") => {
            let response = if client_accepts_zstd(request.headers()) {
                Response::from_data(HTML_ZSTD).with_header(
                    Header::from_bytes("Content-Encoding", "zstd")
                        .expect("hardcoded header should be valid"),
                )
            } else {
                Response::from_string(dashboard_html())
            }
            .with_header(
                Header::from_bytes("Content-Type", "text/html; charset=utf-8")
                    .expect("hardcoded header should be valid"),
            )
            // The page is rebuilt into the binary on every change; without
            // this the browser serves a stale page and the change "does not
            // land".
            .with_header(
                Header::from_bytes("Cache-Control", "no-cache")
                    .expect("hardcoded header should be valid"),
            )
            .with_header(
                Header::from_bytes("Vary", "Accept-Encoding")
                    .expect("hardcoded header should be valid"),
            );
            response
        }
        ("GET", path) if route_is(path, "/api/summary") => match handle_summary(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("POST", path) if route_is(path, "/api/sync") => {
            let mode = parse_params(path)
                .get("mode")
                .map(|s| s.as_str())
                .unwrap_or("incremental")
                .to_string();
            let _guard = write_lock.lock().unwrap_or_else(|e| e.into_inner());
            let result = if mode == "full" {
                store.sync_full()
            } else {
                store.sync()
            };
            let response = match result.and_then(|r| Ok(serde_json::to_string(&r)?)) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            };
            // R104: keep the search index warm automatically. The sync just
            // read every source, so the corpus may have moved on: flag the
            // index stale and let the background warm re-collect + rebuild.
            // Detached — the sync response is already gone; the build lock
            // collapses overlapping warms into one.
            {
                let ctx = Arc::clone(context);
                std::thread::spawn(move || {
                    ctx.mark_stale();
                    ctx.warm_if_needed();
                });
            }
            // R76:过午夜后的第一次成功 sync 是「昨日日报」推送铃。
            // detached:webhook 超时不许占着请求线程;store 是 Arc。
            {
                let store = Arc::clone(store);
                std::thread::spawn(move || maybe_daily_digest(&store));
            }
            // R82/R88:推送铃(detached,webhook 超时不占请求线程)——
            // 异常用量(`alert.anomaly`)与会话静默(`alert.session_idle`),
            // 都是 opt-in,fire_once 去重。
            {
                let store = Arc::clone(store);
                std::thread::spawn(move || {
                    let cfg = tokenbuddy::quota::load_config();
                    let Some(alert) = cfg.alert.as_ref() else {
                        return;
                    };
                    if alert.anomaly {
                        if let Ok(report) = store.query_anomalies() {
                            tokenbuddy::quota::maybe_anomaly_alert(&report, &cfg);
                        }
                    }
                    if alert.session_idle {
                        let start = tokenbuddy::cn_midnight(0);
                        if let Ok(ins) =
                            store.query_insights(None, None, Some(start), None, usize::MAX)
                        {
                            tokenbuddy::quota::maybe_session_idle_alert(&ins.sessions, &cfg);
                        }
                    }
                });
            }
            response
        }
        ("GET", path) if route_is(path, "/api/timeline") => match handle_timeline(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if route_is(path, "/api/metrics") => match handle_metrics(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if route_is(path, "/api/heatmap") => match handle_heatmap(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if route_is(path, "/api/insights") => match handle_insights(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if route_is(path, "/api/models") => match handle_models(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if route_is(path, "/api/digest") => match handle_digest(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if route_is(path, "/api/context/search") => {
            match handle_context_search(context, path) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/context/session") => {
            match handle_context_session(context, path) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/context/stats") => {
            match handle_context_stats(context) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/fleet/config") => match handle_fleet_config_get() {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("POST", path)
            if route_is(path, "/api/fleet/config")
                | route_is(path, "/api/fleet/config-test") =>
        {
            let is_test = route_is(path, "/api/fleet/config-test");
            match read_capped_body(&mut request, MAX_CONFIG_BODY_BYTES) {
                Ok(body) => {
                    let result = if is_test {
                        handle_fleet_config_test(&body)
                    } else {
                        handle_fleet_config_save(&body)
                    };
                    match result {
                        Ok(json) => json_response(json),
                        Err(e) => error_response(&e),
                    }
                }
                // A body over the cap is refused before it is read, so an
                // oversized request cannot turn into resident memory.
                Err(_) => Response::from_string(
                    serde_json::json!({ "error": "请求体超过 1 MiB 上限" }).to_string(),
                )
                .with_header(
                    Header::from_bytes("Content-Type", "application/json")
                        .expect("hardcoded header should be valid"),
                )
                .with_status_code(413),
            }
        }
        ("POST", path) if route_is(path, "/api/fleet/push") => {
            let _guard = write_lock.lock().unwrap_or_else(|e| e.into_inner());
            match handle_fleet_push(store) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("POST", path) if route_is(path, "/api/fleet/pull") => {
            let _guard = write_lock.lock().unwrap_or_else(|e| e.into_inner());
            match handle_fleet_pull() {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/active-time") => {
            let p = parse_params(path);
            match days_param(&p)
                .map(|days| tokenbuddy::cn_midnight(days.unwrap_or(7) - 1))
                .and_then(|start| {
                    store
                        .query_active_time(Some(start), None)
                        .and_then(|v| {
                            // issue #29:窗口锚点。
                            let mut json = serde_json::to_value(&v)?;
                            json["window"] = serde_json::json!({
                                "window_start": start,
                                "window_kind": "cn_calendar",
                            });
                            Ok(serde_json::to_string(&json)?)
                        })
                })
            {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/work-receipts") => {
            let p = parse_params(path);
            match days_param(&p).map(|days| tokenbuddy::cn_midnight(days.unwrap_or(7) - 1)) {
                Ok(since) => {
                    // 三源同形合并(ZCode/Claude/OpenCode),展示出口截 200。
                    let mut receipts = tokenbuddy::zcode::merged_receipts(since);
                    receipts.truncate(200);
                    // R92:会话 id 前缀下钻(与 MCP work_receipts 同参)。
                    let session = p
                        .get("session")
                        .map(|s| s.as_str())
                        .filter(|s| !s.is_empty());
                    if let Some(p) = session {
                        receipts.retain(|r| r.session_id.starts_with(p));
                    }
                    // R68:工具构成——原始工具名(各家方言)归一化为六类目。
                    let mut mix: std::collections::BTreeMap<String, u64> = Default::default();
                    for r in &receipts {
                        for (name, n) in &r.tools {
                            *mix.entry(tokenbuddy::tool_category(name).to_string())
                                .or_insert(0) += n;
                        }
                    }
                    // R81:跨会话返工热点——同一文件被 ≥2 个会话编辑。
                    let rework = tokenbuddy::zcode::rework_hotspots(&receipts, 10);
                    match serde_json::to_string(&serde_json::json!({
                        "receipts": receipts,
                        "tool_mix": mix,
                        "rework_top": rework,
                        "since": since,
                    }))
                    .map_err(anyhow::Error::from)
                    {
                        Ok(json) => json_response(json),
                        Err(e) => error_response(&e),
                    }
                }
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/forecast") => {
            match store
                .query_week_forecast()
                .and_then(|v| Ok(serde_json::to_string(&v)?))
            {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/context-health") => {
            match store
                .query_context_latest()
                .and_then(|v| Ok(serde_json::to_string(&v)?))
            {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/pivot") => {
            let p = parse_params(path);
            let range = param_i64(&p, "start")
                .and_then(|s| param_i64(&p, "end").map(|e| (s, e)));
            // issue #29:start > end 是把两个变量写反的笔误,返回
            // {"rows":[]} 与「这段时间没用量」无法区分——按客户端错误处理。
            if let Ok((Some(s), Some(e))) = range {
                if s > e {
                    // issue #29:start > end 是把两个变量写反的笔误,返回
                    // {"rows":[]} 与「这段时间没用量」无法区分——按客户端
                    // 错误处理,不产出空表。
                    let resp = error_response(&client_error(format!(
                        "参数 start({s}) 不能大于 end({e})——两个变量写反了?"
                    )));
                    let _ = request.respond(with_security_headers(resp));
                    return;
                }
            }
            match range {
                Ok((start, end)) => match store
                    .query_pivot(start, end)
                    .and_then(|pivot| {
                        let mut v = serde_json::to_value(&pivot)?;
                        v["window"] = serde_json::json!({
                            "window_start": start,
                            "window_end": end,
                            "window_kind": "explicit_epoch",
                        });
                        Ok(serde_json::to_string(&v)?)
                    })
                {
                    Ok(json) => json_response(json),
                    Err(e) => error_response(&e),
                },
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/anomalies") => match store
            .query_anomalies()
            .and_then(|report| Ok(serde_json::to_string(&report)?))
        {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if route_is(path, "/api/windows") => match store
            .query_windows()
            .and_then(|facts| Ok(serde_json::to_string(&facts)?))
        {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        // Quota snapshots: the read-only view never spawns a process — file
        // readers are live, command collectors report their stored state.
        // Only the explicit refresh endpoint runs configured commands.
        ("GET", path) if route_is(path, "/api/quota/config") => match handle_quota_config_get() {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if route_is(path, "/api/quota") => {
            match serde_json::to_string(&tokenbuddy::quota::collect_view(false))
                .map_err(anyhow::Error::from)
            {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("POST", path) if route_is(path, "/api/quota/config") => {
            match read_capped_body(&mut request, MAX_CONFIG_BODY_BYTES) {
                Ok(body) => match handle_quota_config_save(&body) {
                    Ok(json) => json_response(json),
                    Err(e) => error_response(&e),
                },
                Err(_) => error_response(&anyhow::anyhow!("请求体过大")),
            }
        }
        ("POST", path) if route_is(path, "/api/quota/alert-test") => match tokenbuddy::quota::send_test_alert(
            &tokenbuddy::quota::load_config(),
            tokenbuddy::now_ts(),
        ) {
            Ok(()) => json_response(serde_json::json!({"sent": true, "note": "测试消息已发出——去 webhook 那头确认收到"}).to_string()),
            Err(e) => json_response(serde_json::json!({"sent": false, "error": e}).to_string()),
        },
        ("POST", path) if route_is(path, "/api/quota/refresh") => {
            match serde_json::to_string(&tokenbuddy::quota::collect_view(true))
                .map_err(anyhow::Error::from)
            {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/brief") => match handle_brief(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if route_is(path, "/api/status") => match handle_status(store, context) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        // Watchdog heartbeat: touches neither the store nor the index, so a
        // wedged data layer still answers here while /api/status would not.
        ("GET", "/api/health") => json_response(
            serde_json::json!({
                "ok": true,
                "version": env!("CARGO_PKG_VERSION"),
                "ts": tokenbuddy::now_ts()
            })
            .to_string(),
        ),
        ("GET", "/api/docs") | ("GET", "/api/") => json_response(api_endpoints().to_string()),
        // issue #24:source 过滤器有了白名单，就得有机器可查的白名单本体。
        ("GET", "/api/sources") => json_response(
            serde_json::json!({ "sources": tokenbuddy::SOURCE_NAMES }).to_string(),
        ),
        ("GET", "/api/skill") => match handle_skill() {
            Ok(body) => json_response(body),
            Err(e) => error_response(&e),
        },
        ("GET", path) if route_is(path, "/api/fleet/hosts") => {
            let hosts = store.fleet_hosts();
            json_response(serde_json::json!({ "hosts": hosts }).to_string())
        }
        ("GET", path) if route_is(path, "/api/fleet/quota") => {
            let quotas = tokenbuddy::fleet::read_fleet_quotas(&tokenbuddy::fleet::fleet_dir());
            match serde_json::to_string(
                &serde_json::json!({ "hosts": quotas, "generated_at": tokenbuddy::now_ts() }),
            )
            .map_err(anyhow::Error::from)
            {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/fleet/summary") => {
            match handle_fleet_summary(store, path) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/fleet/metrics") => {
            match handle_fleet_metrics(store, path) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/fleet/models") => {
            match handle_fleet_models(store, path) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("POST", path) if route_is(path, "/api/context/click") => {
            // 缺 doc_id 是客户端错误，走 client_error 的 400 路径而不是 500
            // （issue #22 Bug 3——监控把 5xx 当 outage 告警）。
            match param_i64(&parse_params(path), "doc_id") {
                Ok(Some(id)) => {
                    let n = context.record_click(id);
                    json_response(serde_json::json!({ "doc_id": id, "clicks": n }).to_string())
                }
                Ok(None) => error_response(&client_error("click 需要 doc_id 参数（整数）")),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if route_is(path, "/api/context/quality") => {
            json_response(serde_json::to_string(&context.quality()).unwrap_or_default())
        }
        ("POST", path) if route_is(path, "/api/context/activate") => {
            // Opening the search view is explicit intent: start the build now
            // so it is ready by the time a query is typed. Never blocks.
            context.warm_if_needed();
            json_response(serde_json::json!({ "warming": true }).to_string())
        }
        ("POST", path) if route_is(path, "/api/context/rebuild") => {
            let ctx = Arc::clone(context);
            std::thread::spawn(move || {
                if let Err(e) = ctx.sync_and_build(true) {
                    eprintln!("[TokenBuddy] context rebuild failed: {e}");
                }
            });
            json_response(serde_json::json!({ "started": true }).to_string())
        }
        (_, path) => {
            // API consumers parse JSON everywhere else; a plain-text 404 was
            // one more silent parse failure for bots and watchdogs (issue #7).
            not_found_response(path)
        }
    };

    let _ = request.respond(with_security_headers(response));
}

/// `tokenbuddy push` — sync local collectors, then PutObject the fresh
/// data.parquet to the fleet bucket. Exits non-zero with a readable message
/// when fleet is not configured.
fn cmd_push() -> Result<()> {
    let cfg = fleet::require_config()?;
    let store = Store::open()?;
    let (sync, out) = fleet::push_now(&store, &cfg)?;
    println!(
        "本地同步：新增 {} 条记录（耗时 {:.1}s）",
        tokenbuddy::store::imported_total(&sync),
        sync.duration_ms as f64 / 1000.0
    );
    for e in &sync.errors {
        eprintln!("  ⚠ {} 采集失败：{}", e.source, e.message);
    }
    println!(
        "已推送到 {}/{}（host {}，{} bytes）",
        cfg.endpoint, cfg.bucket, out.host, out.bytes
    );
    Ok(())
}

/// `POST /api/fleet/push` — the dashboard button form of `cmd_push`.
fn handle_fleet_push(store: &Arc<Store>) -> Result<String> {
    let cfg = fleet::require_config()?;
    let (sync, out) = fleet::push_now(store, &cfg)?;
    Ok(serde_json::json!({ "sync": sync, "push": out }).to_string())
}

/// The secret is never sent back to the page: the settings form shows the
/// mask, and an unchanged (masked or empty) field means "keep what's on
/// disk". Two bullets so an actual key can never collide with the mask.
fn mask_secret(secret: &str) -> String {
    if secret.is_empty() {
        String::new()
    } else {
        let tail: String = secret
            .chars()
            .rev()
            .take(4)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("••{tail}••")
    }
}

/// Config form fields as sent by the settings page. Missing keys fall back
/// to the same defaults `parse_config` uses, so a partial form still saves
/// a complete file.
struct ConfigForm {
    enabled: bool,
    endpoint: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    region: String,
    path_style: bool,
    host_id: String,
    auto_push: bool,
    auto_pull: bool,
    push_interval_secs: u64,
    encrypt: bool,
}

impl ConfigForm {
    fn from_json(body: &str) -> Result<Self> {
        let v: serde_json::Value =
            serde_json::from_str(body).map_err(|e| anyhow::anyhow!("请求体不是合法 JSON：{e}"))?;
        let s = |k: &str| {
            v.get(k)
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .trim()
                .to_string()
        };
        let b = |k: &str, dflt: bool| v.get(k).and_then(|x| x.as_bool()).unwrap_or(dflt);
        Ok(ConfigForm {
            enabled: b("enabled", false),
            endpoint: s("endpoint"),
            bucket: s("bucket"),
            access_key: s("access_key"),
            secret_key: s("secret_key"),
            region: {
                let r = s("region");
                if r.is_empty() {
                    "us-east-1".to_string()
                } else {
                    r
                }
            },
            path_style: b("path_style", true),
            host_id: s("host_id"),
            auto_push: b("auto_push", true),
            auto_pull: b("auto_pull", true),
            encrypt: b("encrypt", false),
            push_interval_secs: v
                .get("push_interval_secs")
                .and_then(|x| x.as_u64())
                .unwrap_or(3600),
        })
    }

    /// Resolve into a full config. An empty secret means "unchanged" — keep
    /// whatever the current file holds (the page never round-trips the real
    /// key). An empty host_id falls back to the machine hostname, matching
    /// the hand-written default.
    fn into_config(self) -> Result<fleet::FleetConfig> {
        let existing_secret = fleet::load_config()
            .ok()
            .flatten()
            .map(|c| c.secret_key)
            .unwrap_or_default();
        let secret = if self.secret_key.is_empty() {
            existing_secret
        } else {
            self.secret_key
        };
        Ok(fleet::FleetConfig {
            enabled: self.enabled,
            endpoint: self.endpoint,
            bucket: self.bucket,
            access_key: self.access_key,
            secret_key: secret,
            region: self.region,
            path_style: self.path_style,
            host_id: if self.host_id.is_empty() {
                fleet::default_host_id()
            } else {
                self.host_id
            },
            auto_push: self.auto_push,
            auto_pull: self.auto_pull,
            push_interval_secs: self.push_interval_secs,
            encrypt: self.encrypt,
        })
    }
}

/// Validate by round-tripping through the same parser the loader uses —
/// one source of truth for what a legal config is.
fn validate_config(cfg: &fleet::FleetConfig) -> Result<fleet::FleetConfig> {
    fleet::parse_config(&fleet::render_config(cfg))
}

/// `GET /api/quota/config` — the collector list as saved, no masking needed
/// (quota.json holds no secrets; commands may embed keys the user inlined,
/// and the panel shows the user their own file).
fn handle_quota_config_get() -> Result<String> {
    let cfg = tokenbuddy::quota::load_config_strict()?;
    Ok(serde_json::json!({
        "path": tokenbuddy::quota::config_path().to_string_lossy(),
        "parsers": tokenbuddy::quota::PARSERS,
        // R102: the switchable collector roster, for the panel's toggles.
        "sources": tokenbuddy::SOURCE_NAMES,
        "config": cfg,
    })
    .to_string())
}

/// `POST /api/quota/config` — validate and atomically write quota.json.
/// Effect is immediate: the next read path / refresh picks the file up, no
/// restart, no state to invalidate.
fn handle_quota_config_save(body: &str) -> Result<String> {
    let cfg: tokenbuddy::quota::QuotaConfig =
        serde_json::from_str(body).map_err(|e| anyhow::anyhow!("JSON 无效:{e}"))?;
    tokenbuddy::quota::save_config(&cfg)?;
    Ok(serde_json::json!({
        "saved": true,
        "path": tokenbuddy::quota::config_path().to_string_lossy(),
        "collectors": cfg.collectors.len(),
        "disabled": cfg.disabled_sources.len(),
        "note": "已保存。停用源下次 sync 起不再读取;已入账历史保留。",
    })
    .to_string())
}

fn handle_fleet_config_get() -> Result<String> {
    let cfg = fleet::load_config()?;
    Ok(serde_json::json!({
        "path": fleet::config_path().to_string_lossy(),
        "config": cfg.map(|c| serde_json::json!({
            "enabled": c.enabled,
            "endpoint": c.endpoint,
            "bucket": c.bucket,
            "access_key": c.access_key,
            "secret_key": mask_secret(&c.secret_key),
            "region": c.region,
            "path_style": c.path_style,
            "host_id": c.host_id,
            "auto_push": c.auto_push,
            "auto_pull": c.auto_pull,
            "push_interval_secs": c.push_interval_secs,
            "encrypt": c.encrypt,
        })),
    })
    .to_string())
}

/// `POST /api/fleet/config` — validate and atomically write fleet.toml.
fn handle_fleet_config_save(body: &str) -> Result<String> {
    let cfg = validate_config(&ConfigForm::from_json(body)?.into_config()?)?;
    fleet::save_config(&cfg)?;
    Ok(serde_json::json!({
        "saved": true,
        "path": fleet::config_path().to_string_lossy(),
        "note": "已保存。手动 ⤓/⤒ 按钮立即生效；auto_push 后台线程需重启服务后生效。",
    })
    .to_string())
}

/// `POST /api/fleet/config-test` — try the form's values against the real
/// endpoint without saving. Same ListObjectsV2 call fleet-sync makes.
fn handle_fleet_config_test(body: &str) -> Result<String> {
    let cfg = validate_config(&ConfigForm::from_json(body)?.into_config()?)?;
    let n = fleet::test_connection(&cfg)?;
    Ok(serde_json::json!({
        "ok": true,
        "detail": format!(
            "连接成功：{} 已可达，bucket「{}」hosts/ 下现有 {} 个对象",
            cfg.endpoint, cfg.bucket, n
        ),
    })
    .to_string())
}

/// China-local wall clock for CLI output. The dashboard buckets days on the
/// same offset, so a timestamp printed here lines up with the labels on
/// screen instead of being eight hours behind them.
fn fmt_ts(ts: i64) -> String {
    let tz = chrono::FixedOffset::east_opt(8 * 3600).expect("UTC+8 is a valid offset");
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| {
            dt.with_timezone(&tz)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| "未知时间".to_string())
}

/// `tokenbuddy today` — one line for the shell prompt / statusline: today's
/// consumption and the trailing week. Reads the local parquet directly (no
/// server needed); costs a couple of column scans, well under 10 ms on a
/// real store. Same numbers as `GET /api/brief`.
fn cmd_today() -> Result<()> {
    let store = Store::open()?;
    let today = store.query_summary(None, None, Some(tokenbuddy::cn_midnight(0)), None)?;
    let week = store.query_summary(None, None, Some(tokenbuddy::cn_midnight(6)), None)?;
    let mut line = format!(
        "今日 {} tokens（入 {} · 出 {} · {} 请求） · 7日 {}",
        tokenbuddy::format_tokens(today.total_tokens),
        tokenbuddy::format_tokens(today.total_input_tokens),
        tokenbuddy::format_tokens(today.total_output_tokens),
        today.total_requests,
        tokenbuddy::format_tokens(week.total_tokens),
    );
    // 投入时长:今天与 AI 实际工作的小时数(R39)。
    if let Ok(active) = store.query_active_time(Some(tokenbuddy::cn_midnight(0)), None) {
        let secs: u64 = active.days.iter().map(|d| d.active_secs).sum();
        if secs > 0 {
            line.push_str(&format!(" · 投入 {:.1}h", secs as f64 / 3600.0));
        }
    }
    // 今日收据:改了几个文件、跑了几次测试(R56 三源)。零值不出,不凑。
    {
        let start = tokenbuddy::cn_midnight(0);
        let receipts = tokenbuddy::zcode::merged_receipts(start);
        let files: usize = receipts.iter().map(|r| r.files.len()).sum();
        let tests: u64 = receipts.iter().map(|r| r.test_count).sum();
        if files > 0 || tests > 0 {
            line.push_str(&format!(" · 改 {files} 文件/{tests} 测试"));
        }
    }
    // 上下文健康:最近一次水位最高的来源——超 75% 该收尾了。
    if let Ok(latest) = store.query_context_latest() {
        if let Some(highest) = latest.iter().max_by(|a, b| {
            a.ratio
                .partial_cmp(&b.ratio)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            if highest.ratio >= 0.75 {
                line.push_str(&format!(
                    " · ⚠ {} 上下文 {:.0}%",
                    highest.source,
                    highest.ratio * 100.0
                ));
            }
        }
    }
    // 套餐余量接进 statusline:每个 (source, plan) 最紧的窗口,cc-switch
    // 同款「5h:34%→3小时5分」形状。同样只读,不 spawn 任何进程。
    let quota_view = tokenbuddy::quota::collect_view(false);
    let now = tokenbuddy::now_ts();
    let binding = tokenbuddy::quota::binding_windows(&quota_view.snapshots);
    if !binding.is_empty() {
        let seg: Vec<String> = binding
            .iter()
            .map(|s| {
                format!(
                    "{} {} {}:{:.0}%→{}",
                    s.source,
                    s.plan,
                    s.window,
                    s.used_percent,
                    tokenbuddy::quota::format_countdown(s.resets_at, now)
                )
            })
            .collect();
        line.push_str(&format!(" · {}", seg.join(" · ")));
    }
    // 机群套餐:各主机(上次 fleet-sync/auto-pull 拉回的 quota.json)最紧
    // 窗口,used 降序最多 3 台——跨主机的余量在终端一眼可见(R93)。
    let fq = tokenbuddy::fleet::read_fleet_quotas(&tokenbuddy::fleet::fleet_dir());
    if let Some(seg) = fleet_today_segment(&fq, now) {
        line.push_str(&seg);
    }
    println!("{line}");
    Ok(())
}

/// 纯函数:机群各主机最紧窗口拼「 · 机群 a 62%→2h b 31%」段。
/// 无主机数据 / 无可用窗口 → None(零值不出,不凑)。
fn fleet_today_segment(fq: &[tokenbuddy::fleet::FleetHostQuota], now: i64) -> Option<String> {
    let mut hosts: Vec<(String, f64, Option<i64>)> = fq
        .iter()
        .filter_map(|h| {
            let bw = tokenbuddy::quota::binding_windows(&h.snapshots);
            bw.iter()
                .max_by(|a, b| a.used_percent.total_cmp(&b.used_percent))
                .map(|w| (h.host.clone(), w.used_percent, w.resets_at))
        })
        .collect();
    hosts.sort_by(|a, b| b.1.total_cmp(&a.1));
    hosts.truncate(3);
    if hosts.is_empty() {
        return None;
    }
    let seg: Vec<String> = hosts
        .iter()
        .map(|(h, used, resets)| {
            let cd = resets
                .map(|_| format!("→{}", tokenbuddy::quota::format_countdown(*resets, now)))
                .unwrap_or_default();
            format!("{h} {used:.0}%{cd}")
        })
        .collect();
    Some(format!(" · 机群 {}", seg.join(" ")))
}

/// `tokenbuddy statusline` — Claude Code statusline provider. Claude Code
/// pipes one JSON document per refresh on stdin (schema per the official
/// statusline docs: model.display_name, workspace.current_dir,
/// rate_limits.five_hour/seven_day{used_percentage,resets_at}); we print a
/// single enriched line: model, directory, today's TokenBuddy total, the
/// claude windows from stdin plus every quota snapshot TokenBuddy read
/// locally. Read-only paths only — a statusline refresh must never spawn a
/// collector command or block on the network.
fn cmd_statusline() -> Result<()> {
    let mut body = String::new();
    {
        use std::io::Read;
        std::io::stdin().read_to_string(&mut body)?;
    }
    let line = statusline_line(&body);
    println!("{line}");
    Ok(())
}

fn statusline_line(stdin_json: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(stdin_json.trim()).unwrap_or_default();
    let model = value
        .get("model")
        .and_then(|m| m.get("display_name"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let dir = value
        .get("workspace")
        .and_then(|w| w.get("current_dir"))
        .and_then(|v| v.as_str())
        .map(|d| {
            std::path::Path::new(d)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| d.to_string())
        })
        .unwrap_or_default();

    let mut parts: Vec<String> = Vec::new();
    if !model.is_empty() {
        parts.push(model.to_string());
    }
    if !dir.is_empty() {
        parts.push(dir);
    }

    // R99 — 当前会话上下文:stdin 的 context_window(官方 schema)。
    // used_percentage 由上游算好(入侧三项合计),早期会话可能为 null;
    // 回退用 current_usage 的入侧三项 ÷ 窗口大小,再没有就不出段——
    // 两条路都只报 Claude Code 自己说的,不猜。
    if let Some(cw) = value.get("context_window") {
        let mut pct = cw.get("used_percentage").and_then(|v| v.as_f64());
        if pct.is_none() {
            if let (Some(inp), Some(cr), Some(cw2), Some(size)) = (
                cw.pointer("/current_usage/input_tokens")
                    .and_then(|v| v.as_i64()),
                cw.pointer("/current_usage/cache_read_input_tokens")
                    .and_then(|v| v.as_i64()),
                cw.pointer("/current_usage/cache_creation_input_tokens")
                    .and_then(|v| v.as_i64()),
                cw.get("context_window_size").and_then(|v| v.as_i64()),
            ) {
                if size > 0 {
                    pct = Some((inp + cr + cw2) as f64 / size as f64 * 100.0);
                }
            }
        }
        if let Some(p) = pct {
            let warn = if p >= 75.0 { "⚠" } else { "" };
            parts.push(format!("{warn}ctx {p:.0}%"));
        }
    }

    // TokenBuddy 自家数字:今日 tokens + 上下文健康(>75% 该收尾了)。
    if let Ok(store) = Store::open() {
        if let Ok(today) = store.query_summary(None, None, Some(tokenbuddy::cn_midnight(0)), None) {
            if today.total_requests > 0 {
                parts.push(format!(
                    "今日 {}",
                    tokenbuddy::format_tokens(today.total_tokens)
                ));
            }
        }
        if let Ok(latest) = store.query_context_latest() {
            if let Some(highest) = latest.iter().max_by(|a, b| {
                a.ratio
                    .partial_cmp(&b.ratio)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }) {
                if highest.ratio >= 0.75 {
                    parts.push(format!(
                        "⚠{} ctx {:.0}%",
                        highest.source,
                        highest.ratio * 100.0
                    ));
                }
            }
        }
    }

    // 窗口配额:stdin 里的 claude 限流(优先,零成本)+ TokenBuddy 套餐快照。
    let now = tokenbuddy::now_ts();
    let mut snaps: Vec<tokenbuddy::quota::QuotaSnapshot> = Vec::new();
    if let Some(limits) = value.get("rate_limits") {
        for (key, label) in [("five_hour", "5h"), ("seven_day", "7d")] {
            if let Some(w) = limits.get(key) {
                if let Some(used) = w.get("used_percentage").and_then(|v| v.as_f64()) {
                    snaps.push(tokenbuddy::quota::QuotaSnapshot {
                        source: "claude".into(),
                        plan: String::new(),
                        window: label.into(),
                        used_percent: used,
                        resets_at: w.get("resets_at").and_then(|v| v.as_i64()),
                        collected_at: now,
                        origin: "file".into(),
                    });
                }
            }
        }
    }
    let view = tokenbuddy::quota::collect_view(false);
    for s in view.snapshots {
        snaps.push(s);
    }
    for s in tokenbuddy::quota::binding_windows(&snaps) {
        let plan = if s.plan.is_empty() {
            String::new()
        } else {
            format!("{} ", s.plan)
        };
        parts.push(format!(
            "{}{}:{:.0}%→{}",
            plan,
            s.window,
            s.used_percent,
            tokenbuddy::quota::format_countdown_short(s.resets_at, now)
        ));
    }

    if parts.is_empty() {
        return "TokenBuddy".to_string();
    }
    parts.join(" | ")
}

/// `tokenbuddy report [--days N]` — a markdown digest of one window on
/// stdout. Humans skim it; the tokenbuddy-analyze skill reads it instead of
/// raw logs, which is what keeps agent self-review affordable.
/// R86 — `tokenbuddy sessions`:终端里的最近会话清单。与仪表盘
/// insights 同源同口径(分型阶梯/请求数/token 总量),按最近活动排序。
fn cmd_sessions(days: i64, top: usize, json: bool) -> Result<()> {
    let store = Store::open()?;
    let start = if days <= 1 {
        tokenbuddy::cn_midnight(0)
    } else {
        tokenbuddy::cn_midnight(days - 1)
    };
    // 全量取回来只为总数;limit 给 usize::MAX,截断由这里的 last_ts 排序决定。
    let insights = store.query_insights(None, None, Some(start), None, usize::MAX)?;
    let mut rows = insights.sessions;
    let total = rows.len();
    rows.sort_by_key(|r| std::cmp::Reverse(r.last_ts));
    rows.truncate(top.max(1));
    if json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    println!(
        "最近 {days} 天 · {total} 会话(按最近活动,显示 {} 条)",
        rows.len()
    );
    let now = tokenbuddy::now_ts();
    for r in &rows {
        println!(
            "  {}  {}  {}  {}  {}…  {} 请求  {} tok",
            fmt_ago(r.last_ts, now),
            fmt_span(r.last_ts - r.first_ts),
            r.archetype,
            r.source,
            &r.session_id[..r.session_id.len().min(12)],
            r.requests,
            tokenbuddy::format_tokens(r.total_tokens),
        );
    }
    Ok(())
}

/// "刚刚/Nm前/Nh前/Nd前";0 = 从未活动,占位 —。
fn fmt_ago(ts: i64, now: i64) -> String {
    if ts <= 0 {
        return "—".into();
    }
    let d = (now - ts).max(0);
    if d < 60 {
        "刚刚".into()
    } else if d < 3600 {
        format!("{}m前", d / 60)
    } else if d < 86400 {
        format!("{}h前", d / 3600)
    } else {
        format!("{}d前", d / 86400)
    }
}

/// 会话跨度(墙钟):"<1m" / "45m" / "3h12m" / "2d4h"。
fn fmt_span(secs: i64) -> String {
    if secs < 60 {
        return "<1m".into();
    }
    let d = secs / 86400;
    let h = (secs % 86400) / 3600;
    let m = (secs % 3600) / 60;
    if d > 0 {
        format!("{d}d{h}h")
    } else if h > 0 {
        format!("{h}h{m:02}m")
    } else {
        format!("{m}m")
    }
}

fn cmd_report(days: i64, json: bool) -> Result<()> {
    let store = Store::open()?;
    let start = if days <= 1 {
        tokenbuddy::cn_midnight(0)
    } else {
        tokenbuddy::cn_midnight(days - 1)
    };
    let summary = store.query_summary(None, None, Some(start), None)?;
    let metrics = store.query_metrics(None, None, Some(start), None)?;
    let windows = store.query_windows()?;
    let anomalies = store.query_anomalies()?;
    let pivot = store.query_pivot(Some(start), None)?;
    let active = store.query_active_time(Some(start), None)?;
    let quota = tokenbuddy::quota::collect_view(false);
    let receipts = tokenbuddy::zcode::merged_receipts(start);
    let forecast = store.query_week_forecast()?;
    let input = tokenbuddy::report::ReportInput {
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
    if json {
        // --json:同一份聚合结构直接序列化给脚本(ccusage 式的全出口
        // --json);markdown 渲染与 JSON 出口共享同一份数字,永不分叉。
        println!("{}", serde_json::to_string_pretty(&input)?);
        return Ok(());
    }
    print!("{}", tokenbuddy::report::render(&input));
    Ok(())
}

fn default_export_dir() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(format!("tokenbuddy-export-{}", tokenbuddy::now_ts()))
}

/// `tokenbuddy export [--out DIR]` — copy the local ledger (+ context store
/// and sync bookkeeping) into a portable folder with a sha256 manifest.
fn cmd_export(out: &Path) -> Result<()> {
    let (dir, rows) = tokenbuddy::export::export_to(&tokenbuddy::data_dir(), out)?;
    println!(
        "已导出 {} 条记录到 {}\n迁移到新机器:复制该目录后运行\n  tokenbuddy import <目录> --force",
        rows,
        dir.display()
    );
    Ok(())
}

/// `tokenbuddy import DIR [--force]` — verify an export's manifest hashes,
/// then land its payload over the live store.
fn cmd_import(dir: &Path, force: bool) -> Result<()> {
    let (manifest_rows, rows) =
        tokenbuddy::export::import_from(dir, force, &tokenbuddy::data_dir())?;
    println!("已导入 {manifest_rows} 条记录(manifest 口径),落地后 {rows} 条可读。\n重启服务或重新打开仪表盘即可看到导入的数据。");
    Ok(())
}

/// `tokenbuddy doctor [--json]` — per-source diagnosis. Answers "why is my
/// tool not counted" without a dashboard: paths, readability, newest log
/// entry, expected SQLite tables, rows skipped by the blob bound.
fn cmd_doctor(json: bool) -> Result<()> {
    let report = tokenbuddy::doctor::diagnose()?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print!("{}", report.render());
    }
    Ok(())
}

/// R76 — daily digest push: the first sync after CN midnight sends
/// yesterday's report to the configured webhook (`alert.daily_digest`).
/// Once per day (key = the digest date); failures are silent noise — a
/// missed digest must not nag anyone with retries.
fn maybe_daily_digest(store: &Store) {
    let cfg = tokenbuddy::quota::load_config();
    let Some(alert) = cfg.alert else {
        return;
    };
    if !alert.daily_digest {
        return;
    }
    let today_key = format!(
        "digest|{}",
        chrono::DateTime::from_timestamp(tokenbuddy::now_ts(), 0)
            .map(|d| d.with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap()))
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_default()
    );
    // 今天已发过(或正在发)就回家:判定键是「今天」,内容是「昨天」。
    let state_text = std::fs::read_to_string(tokenbuddy::quota::alert_state_path())
        .ok()
        .and_then(|t| serde_json::from_str::<std::collections::BTreeMap<String, i64>>(&t).ok())
        .map(|m| m.contains_key(&today_key))
        .unwrap_or(false);
    if state_text {
        return;
    }
    let start = tokenbuddy::cn_midnight(1);
    let summary = match store.query_summary(None, None, Some(start), None) {
        Ok(s) => s,
        Err(_) => return,
    };
    let metrics = match store.query_metrics(None, None, Some(start), None) {
        Ok(m) => m,
        Err(_) => return,
    };
    let windows = match store.query_windows() {
        Ok(w) => w,
        Err(_) => return,
    };
    let anomalies = match store.query_anomalies() {
        Ok(a) => a,
        Err(_) => return,
    };
    let pivot = match store.query_pivot(Some(start), None) {
        Ok(p) => p,
        Err(_) => return,
    };
    let active = match store.query_active_time(Some(start), None) {
        Ok(a) => a,
        Err(_) => return,
    };
    let quota = tokenbuddy::quota::collect_view(false);
    let receipts = tokenbuddy::zcode::merged_receipts(start);
    let forecast = match store.query_week_forecast() {
        Ok(f) => f,
        Err(_) => return,
    };
    let input = tokenbuddy::report::ReportInput {
        days: 1,
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
    let text = tokenbuddy::report::render(&input);
    let now = tokenbuddy::now_ts();
    if tokenbuddy::quota::fire_once(&today_key, &alert, &text, now) {
        eprintln!("[TokenBuddy] daily digest sent (yesterday)");
    }
}

/// `tokenbuddy quota` — provider-reported plan capacity on the terminal.
/// Read-only unless `--refresh`, which is the only thing that may run the
/// user-configured collector commands. Resets render as a relative countdown
/// (absolute timestamps are timezone territory the CLI refuses to enter).
fn cmd_quota(refresh: bool, json: bool) -> Result<()> {
    let view = tokenbuddy::quota::collect_view(refresh);
    if json {
        println!("{}", serde_json::to_string_pretty(&view)?);
        return Ok(());
    }
    if view.snapshots.is_empty() && view.collectors.is_empty() {
        println!("没有套餐余量可显示。");
        println!(
            "  · Codex 用户:在 Codex CLI 会话产生限流数据后自动出现(零外呼,实时读本地 rollout)"
        );
        println!(
            "  · 其他来源:在 ~/.tokenbuddy/quota.json 配置命令采集器,例如 MiniMax Token Plan:"
        );
        println!("    {{\"collectors\":[{{\"name\":\"minimax\",\"command\":\"mmx quota show --output json\",\"parser\":\"minimax\"}}]}}");
        return Ok(());
    }
    let now = tokenbuddy::now_ts();
    for snap in &view.snapshots {
        let age = (now - snap.collected_at).max(0);
        // 新鲜度按数据的真实年龄说话:文件读取也可能读到一个老缓存,
        // "实时"只属于两分钟内的读数。
        let age_text = if age < 120 {
            "实时读本地日志".to_string()
        } else if age < 3600 {
            format!("{} 分钟前采集", age / 60)
        } else if age < 86400 {
            format!("{} 小时前采集", age / 3600)
        } else {
            format!("{} 天前采集", age / 86400)
        };
        let reset = match snap.resets_at {
            Some(at) if at <= now => "已过重置点".to_string(),
            Some(at) => {
                let left = at - now;
                let text = if left >= 86400 {
                    format!("{}天{}小时", left / 86400, (left % 86400) / 3600)
                } else if left >= 3600 {
                    format!("{}小时{}分", left / 3600, (left % 3600) / 60)
                } else {
                    format!("{}分{}秒", left / 60, left % 60)
                };
                format!("{text}后重置")
            }
            None => "重置时间未知".to_string(),
        };
        println!(
            "[{}] {} · {} 窗口:已用 {:.1}%  ({reset};{age_text})",
            snap.source, snap.plan, snap.window, snap.used_percent
        );
    }
    for c in &view.collectors {
        match (&c.last_error, c.last_ok_at) {
            (Some(err), _) => println!("[{}] ⚠ 最近一次采集失败:{err}", c.name),
            (None, Some(at)) => {
                println!("[{}] 最近采集成功({} 秒前)", c.name, (now - at).max(0))
            }
            (None, None) => println!(
                "[{}] 尚未采集过(用 --refresh 或 POST /api/quota/refresh)",
                c.name
            ),
        }
    }
    Ok(())
}

/// A one-glance consumption snapshot for statuslines, SSH banners and shell
/// prompts. Two windows (today, trailing week), no breakdown tables — the
/// whole point is one small JSON that scripts can read in milliseconds.
/// Same figures as `tokenbuddy today`.
fn handle_brief(store: &Store, path: &str) -> Result<String> {
    let params = parse_params(path);
    let host = std::env::var("TOKENBUDDY_HOST_ID").unwrap_or_else(|_| whoami_fallback());
    let windows = [
        ("today", tokenbuddy::cn_midnight(0)),
        ("week", tokenbuddy::cn_midnight(6)),
    ];
    let mut out = serde_json::Map::new();
    out.insert(
        "host".into(),
        serde_json::Value::String(
            params
                .get("host")
                .cloned()
                .filter(|h| !h.is_empty())
                .unwrap_or(host),
        ),
    );
    out.insert(
        "generated_at".into(),
        serde_json::json!(tokenbuddy::now_ts()),
    );
    for (name, start) in windows {
        let s = store.query_summary(None, None, Some(start), None)?;
        out.insert(
            name.into(),
            serde_json::json!({
                // issue #29:窗口锚点——「近 7 天」= cn_midnight(6) 起算的
                // 7 个中国本地自然日,与 summary?timeRange=7d 完全同口径。
                "window_start": start,
                "window_days": if name == "today" { 1 } else { 7 },
                "requests": s.total_requests,
                "input": s.total_input_tokens,
                "output": s.total_output_tokens,
                "cache_read": s.total_cache_read_tokens,
                "cache_creation": s.total_cache_creation_tokens,
                "total": s.total_tokens,
                "credits": s.total_credits,
            }),
        );
    }
    // 套餐余量:每个 (source, plan) 只带最紧的那个窗口——statusline 要的
    // 就是"哪个快用完"。只读路径,一个进程都不 spawn(statusline 对延迟
    // 和副作用都零容忍)。
    let quota_view = tokenbuddy::quota::collect_view(false);
    let binding = tokenbuddy::quota::binding_windows(&quota_view.snapshots);
    if !binding.is_empty() {
        out.insert("quota".into(), serde_json::json!(binding));
    }
    // 投入时长(今日):R39 的墙钟事实,statusline 一并展示。
    if let Ok(active) = store.query_active_time(Some(tokenbuddy::cn_midnight(0)), None) {
        let secs: u64 = active.days.iter().map(|d| d.active_secs).sum();
        if secs > 0 {
            out.insert("active_secs_today".into(), serde_json::json!(secs));
        }
    }
    Ok(serde_json::Value::Object(out).to_string())
}

fn whoami_fallback() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "local".into())
}

/// `GET /api/status` — the one call that lets the header be honest instead of
/// guessing: how many rows the store holds, when it was last refreshed, how
/// long that took, which collectors can see a log directory, and which ones
/// failed last time. The dashboard's first-run prompt and source-health
/// panel are both driven from here.
fn handle_status(store: &Store, context: &ContextHandle) -> Result<String> {
    // binary_mtime 是进程陈旧的权威证据（issue #17 / PR #21）：macOS 下
    // rename 换 inode，已在跑的进程仍持有旧镜像，version 区分不了它。
    let binary_mtime = std::env::current_exe()
        .ok()
        .and_then(|p| p.metadata().ok())
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs());
    let body = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "build_commit": env!("TOKENBUDDY_BUILD_COMMIT"),
        "build_time": env!("TOKENBUDDY_BUILD_TIME").parse::<u64>().unwrap_or(0),
        "binary_mtime": binary_mtime,
        "records": store.record_count().unwrap_or(0),
        "data_file": store.parquet_path().to_string_lossy(),
        "state": store.state(),
        "sources": store.source_status(),
        "index": context.index_stats(),
        "fleet_enabled": fleet::load_config().ok().flatten().is_some_and(|c| c.enabled),
    });
    Ok(body.to_string())
}

/// `tokenbuddy status` — the same facts without opening a browser, so the
/// answer to "is it running / has it synced" is available over SSH and in a
/// shell prompt.
fn cmd_status() -> Result<()> {
    let store = Store::open()?;
    let state = store.state();
    let records = store.record_count()?;
    println!("TokenBuddy {}", env!("CARGO_PKG_VERSION"));
    println!("  数据文件  {}", store.parquet_path().display());
    println!("  记录数    {records}");
    match state.last_sync_at {
        Some(at) => println!(
            "  上次同步  {}（{} · 新增 {} 条 · {:.1}s）",
            crate::fmt_ts(at),
            state.last_sync_mode.as_deref().unwrap_or("?"),
            state.last_sync_imported.unwrap_or(0),
            state.last_sync_duration_ms.unwrap_or(0) as f64 / 1000.0
        ),
        None => println!("  上次同步  从未同步过"),
    }
    let found: Vec<String> = store
        .source_status()
        .into_iter()
        .filter(|s| s.present)
        .map(|s| s.label)
        .collect();
    println!(
        "  检测到    {}",
        if found.is_empty() {
            "没有找到任何 agent 日志目录".to_string()
        } else {
            found.join(" / ")
        }
    );
    for e in &state.last_sync_errors {
        println!("  ⚠ {} 采集失败：{}", e.source, e.message);
    }
    // 套餐与收据:两条一行式支线,读不到就说没有,永不因它们失败。
    let quota_view = tokenbuddy::quota::collect_view(false);
    if quota_view.snapshots.is_empty() {
        println!("  套餐余量  未配置(Codex/Claude 文件源自动生效,其他见 quota.json)");
    } else {
        println!(
            "  套餐余量  {} 个窗口(quota --refresh 更新)",
            quota_view.snapshots.len()
        );
    }
    let receipts = tokenbuddy::zcode::work_receipts(tokenbuddy::cn_midnight(6))
        .into_iter()
        .chain(tokenbuddy::claude::work_receipts(tokenbuddy::cn_midnight(
            6,
        )))
        .chain(tokenbuddy::opencode::work_receipts(
            tokenbuddy::cn_midnight(6),
        ))
        .count();
    println!("  工作收据  近 7 天 {} 个会话(work-receipts)", receipts);
    Ok(())
}

/// `tokenbuddy fleet-sync` — pull every host's parquet from the bucket.
fn cmd_fleet_sync() -> Result<()> {
    let cfg = fleet::require_config()?;
    let out = fleet::pull_all(&cfg)?;
    if out.downloaded.is_empty() && out.skipped.is_empty() {
        println!("bucket 中还没有任何主机数据");
        return Ok(());
    }
    for host in &out.downloaded {
        println!("下载 {host}/data.parquet");
    }
    for host in &out.skipped {
        println!("跳过 {host}（未变化）");
    }
    Ok(())
}

/// `POST /api/fleet/pull` — the dashboard button form of `cmd_fleet_sync`.
fn handle_fleet_pull() -> Result<String> {
    let cfg = fleet::require_config()?;
    let out = fleet::pull_all(&cfg)?;
    Ok(serde_json::to_string(&out)?)
}

/// `GET /api/fleet/summary?timeRange=&source=&model=&host=` — fleet totals,
/// per-host rows and the host × source matrix in one call.
fn handle_fleet_summary(store: &Store, path: &str) -> Result<String> {
    let f = filters_from(path)?;
    let summary = store.query_fleet_summary(
        f.host.as_deref(),
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    with_window(serde_json::to_string(&summary)?, &f)
}

/// `GET /api/fleet/metrics?...` — latency/cache panel keyed by host.
fn handle_fleet_metrics(store: &Store, path: &str) -> Result<String> {
    let f = filters_from(path)?;
    let metrics = store.query_fleet_metrics(
        f.host.as_deref(),
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    with_window(serde_json::to_string(&metrics)?, &f)
}

/// `GET /api/fleet/models?...` — per-model comparison across hosts.
fn handle_fleet_models(store: &Store, path: &str) -> Result<String> {
    let f = filters_from(path)?;
    let models = store.query_fleet_models(
        f.host.as_deref(),
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    with_window(serde_json::to_string(&models)?, &f)
}

fn time_range_start(time_range: Option<&str>) -> Option<i64> {
    let days_ago = match time_range {
        Some("today") => 0,
        // 7d/30d/90d 是 Nd 的特例；任意 1–365 天的窗口都直接接受（issue #22
        // Bug 4——“近两周”不该被迫绕道 30d）。
        Some(tr) if tr.ends_with('d') => match tr[..tr.len() - 1].parse::<u32>() {
            // Nd = 含今天的 N 个中国本地自然日(1d = 今天),与 /api/brief 的
            // week = cn_midnight(6) 同口径——此前用 cn_midnight(n) 实际覆盖
            // 8 天,正是 #29 里 brief.week 比 summary?7d 少一截的根因。
            Ok(n) if (1..=365).contains(&n) => n as i64 - 1,
            _ => return None,
        },
        _ => return None,
    };
    Some(tokenbuddy::cn_midnight(days_ago))
}

/// The `timeRange` / `source` / `model` triple every report endpoint accepts.
#[derive(Debug)]
struct Filters {
    date_start: Option<i64>,
    date_end: Option<i64>,
    source: Option<String>,
    model: Option<String>,
    /// Fleet-only: narrow to one pulled host; "all"/absent means every host.
    /// Local endpoints ignore it.
    host: Option<String>,
}

/// No endpoint currently accepts an upper bound, but the field keeps the
/// call sites uniform and is what the digest's equal-length windows rely on.
/// An unrecognized non-empty `timeRange` is a client error, not a silent
/// "all time" — callers building watchdogs must be able to tell a typo from
/// a real window (issue #6/#12: 参数校验不一致).
fn filters_from(path: &str) -> Result<Filters> {
    let params = parse_params(path);
    // source 是唯一带白名单的可选过滤器（issue #24）：空串与缺参等价 =
    // 不过滤（query-string 序列化器默认把可选参数带上、值为空留空）；未知
    // 源名报 400 并列出合法 id，而不是静默返回全零账本——拼错的源名和
    // 「这段时间没用量」必须可区分，与 timeRange/mode/metric 的做法一致。
    // 匹配大小写不敏感（issue #30 Bug 5）：SOURCE_NAMES 全小写，"HERMES"
    // 归一后再过白名单，与 model 过滤的行为对齐。
    let source = match params.get("source").map(|s| s.as_str()) {
        Some("") | Some("all") | None => None,
        Some(s) => {
            let lowered = s.to_ascii_lowercase();
            if tokenbuddy::SOURCE_NAMES.contains(&lowered.as_str()) {
                Some(lowered)
            } else {
                return Err(client_error(format!(
                    "参数 source 必须是 {} 或 all，得到 “{}”",
                    tokenbuddy::SOURCE_NAMES.join("|"),
                    s
                )));
            }
        }
    };
    let model = match params.get("model").map(|s| s.as_str()) {
        Some("") | None => None,
        Some(s) => Some(s.to_string()),
    };
    let host = match params.get("host").map(|s| s.as_str()) {
        Some("") | Some("all") | None => None,
        Some(s) => Some(s.to_string()),
    };
    let date_start = match params.get("timeRange").map(|s| s.as_str()) {
        None | Some("") | Some("all") => Ok(None),
        tr => time_range_start(tr).map(Some).ok_or_else(|| {
            client_error(format!(
                "参数 timeRange 必须是 all|today|Nd（N 为 1–365 整数，如 7d|14d|30d），得到 “{}”",
                tr.unwrap_or_default()
            ))
        }),
    }?;
    Ok(Filters {
        date_start,
        date_end: None,
        source,
        model,
        host,
    })
}

fn handle_summary(store: &Store, path: &str) -> Result<String> {
    reject_unsupported_days(path)?;
    let f = filters_from(path)?;
    let summary = store.query_summary(
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    with_window(serde_json::to_string(&summary)?, &f)
}

/// issue #29:窗口锚点。所有带窗口的响应带 `window`(start/end/kind),
/// 「7d」到底是哪 7 天不再需要调用方自己猜;brief.week 与
/// summary?timeRange=7d 的口径对齐也因此可核对——两边都是 cn_midnight(6)
/// 起算的同一自然日窗口。
fn with_window(json: String, f: &Filters) -> Result<String> {
    let mut v: serde_json::Value = serde_json::from_str(&json)?;
    // #29 给对象响应补 window 锚点；timeline 的顶层数组做 IndexMut 会直接
    // panic（box 实测 /api/timeline 任意窗口 500）——数组原样返回，window
    // 只由对象响应携带。
    if v.is_object() {
        v["window"] = serde_json::json!({
            "window_start": f.date_start,
            "window_end": f.date_end.unwrap_or(tokenbuddy::now_ts()),
            "window_kind": if f.date_start.is_some() { "cn_calendar" } else { "all_time" },
        });
    }
    Ok(serde_json::to_string(&v)?)
}

/// `GET /api/context/search?q=...&source=&role=&project=&days=&limit=` —
/// full-text search over the conversation index. `days` snaps to
/// China-local midnights like every other time filter; `project` takes a
/// `project_label` value as listed by `/api/context/stats`.
fn handle_context_search(context: &ContextHandle, path: &str) -> Result<String> {
    let params = parse_params(path);
    let q = params.get("q").cloned().unwrap_or_default();
    // 与账本端点同一条白名单规则（issue #24）：搜索通道静默空结果更
    // 危险——调用方会把「没采」读成「没说过」。大小写不敏感（issue #30
    // Bug 5），归一成小写再过白名单。
    let source = match params
        .get("source")
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty() && *s != "all")
    {
        None => None,
        Some(s) => {
            let lowered = s.to_ascii_lowercase();
            if tokenbuddy::SOURCE_NAMES.contains(&lowered.as_str()) {
                Some(lowered)
            } else {
                return Err(client_error(format!(
                    "参数 source 必须是 {} 或 all，得到 “{}”",
                    tokenbuddy::SOURCE_NAMES.join("|"),
                    s
                )));
            }
        }
    };
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
    // R91 — 会话 id 前缀过滤(收据/会话表行点击跳转的通道)。
    let session = params
        .get("session")
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let days = days_param(&params)?;
    let limit = limit_param(&params, 30, 100)?;

    let exclude_session = params
        .get("exclude_session")
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty());
    let exclude_sessions: Vec<&str> = exclude_session.into_iter().collect();
    let filter = context::SearchFilter {
        source: source.as_deref(),
        role: role.as_deref(),
        project: project.as_deref(),
        session: session.as_deref(),
        since: days.map(tokenbuddy::cn_midnight),
        limit,
        exclude_sessions: &exclude_sessions,
    };
    let resp = context.search(&q, &filter)?;
    Ok(serde_json::to_string(&resp)?)
}

/// `GET /api/context/session?source=&session_id=&doc_id=&around=` — the
/// conversation around one search hit, so a match can be read in place.
/// Two equivalent entry paths, both honored (issue #22 Bug 2 — the error
/// message used to promise source+session_id while the handler demanded
/// doc_id): `doc_id` alone resolves its (source, session) pair, or pass
/// `source` + `session_id` directly without any doc_id.
fn handle_context_session(context: &ContextHandle, path: &str) -> Result<String> {
    let params = parse_params(path);
    // 与 search/账本同口径：source 大小写不敏感（issue #30 Bug 5）——索引里
    // 的源名全小写，"HERMES" 原样查 session 表会误报「会话不存在」。
    let source = params
        .get("source")
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();
    let session_id = params.get("session_id").cloned().unwrap_or_default();
    let doc_id = param_i64(&params, "doc_id")?;
    let around = param_usize(&params, "around")?.unwrap_or(10);
    let (source, session_id, anchor) = if !source.is_empty() && !session_id.is_empty() {
        (source, session_id, doc_id.unwrap_or(0))
    } else {
        let doc_id = doc_id.ok_or_else(|| {
            client_error("参数 doc_id 必填（整数），或同时提供 source 与 session_id")
        })?;
        let (source, session_id) = context.doc_session(doc_id)?.ok_or_else(|| {
            client_error(format!(
                "doc_id {doc_id} 不在索引中：请传索引里存在的 doc_id，或同时提供 source 与 session_id"
            ))
        })?;
        (source, session_id, doc_id)
    };
    let view = context
        .session_view(&source, &session_id, anchor, around)?
        .ok_or_else(|| {
            client_error(format!(
                "会话不存在：source={source} session_id={session_id}"
            ))
        })?;
    Ok(serde_json::to_string(&view)?)
}

/// The agent-facing skill ships inside the binary, so the dashboard can
/// always show the version that matches the running server (R103).
const SKILL_MD: &str = include_str!("../skills/tokenbuddy-analyze/SKILL.md");

/// `GET /api/skill` — the tokenbuddy-analyze SKILL.md as JSON: a dashboard
/// panel renders it and hands out a one-click copy so an agent operator can
/// drop it into `~/.<agent>/skills/` without leaving the console.
fn handle_skill() -> Result<String> {
    let description = SKILL_MD
        .lines()
        .find_map(|l| l.strip_prefix("description: "))
        .unwrap_or("")
        .trim();
    Ok(serde_json::json!({
        "name": "tokenbuddy-analyze",
        "description": description,
        // Where agents of this machine look for skills; the dashboard shows
        // them as copy targets. Server-side guess, client renders.
        "install_hints": ["~/.zcode/skills/tokenbuddy-analyze/SKILL.md",
                          "~/.claude/skills/tokenbuddy-analyze/SKILL.md",
                          "~/.opencode/skills/tokenbuddy-analyze/SKILL.md"],
        "markdown": SKILL_MD,
    })
    .to_string())
}

/// `GET /api/context/stats` — index build phase plus corpus figures, so the
/// dashboard can show "indexing" instead of an empty result list.
fn handle_context_stats(context: &ContextHandle) -> Result<String> {
    // issue #23:「查不到」必须能区分「没说过」与「没采」。coverage 把
    // 每个源标成 indexed / not_supported，搜索端静默空结果时调用方先看这里。
    let coverage: serde_json::Map<String, serde_json::Value> =
        serde_json::Map::from_iter(tokenbuddy::SOURCE_NAMES.iter().map(|s| {
            (
                s.to_string(),
                serde_json::json!(if tokenbuddy::context::DRAINED_SOURCES.contains(s) {
                    "indexed"
                } else {
                    "not_supported"
                }),
            )
        }));
    let body = serde_json::json!({
        "status": context.status(),
        "index": context.index_stats(),
        "coverage": coverage,
    });
    Ok(body.to_string())
}

fn handle_metrics(store: &Store, path: &str) -> Result<String> {
    let f = filters_from(path)?;
    let metrics = store.query_metrics(
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    with_window(serde_json::to_string(&metrics)?, &f)
}

/// `GET /api/insights?timeRange=&source=&model=&limit=` — the deep-analysis
/// panels (hour-of-day rhythm, daily cache efficiency, session leaderboard,
/// context fill trend) in one call.
fn handle_insights(store: &Store, path: &str) -> Result<String> {
    let f = filters_from(path)?;
    let limit = limit_param(&parse_params(path), 20, 100)?;
    let insights = store.query_insights(
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
        limit,
    )?;
    with_window(serde_json::to_string(&insights)?, &f)
}

fn handle_models(store: &Store, path: &str) -> Result<String> {
    let f = filters_from(path)?;
    let comparison = store.query_models(
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    with_window(serde_json::to_string(&comparison)?, &f)
}

fn handle_heatmap(store: &Store, path: &str) -> Result<String> {
    let params = parse_params(path);
    // 空串与缺参等价 = 默认值（issue #25 Bug 5）：URL 拼接器对可选参数的
    // 默认行为是「带上、值留空」，枚举参数把空串判死等于要求所有调用方
    // 做「空值即删键」的清洗。
    let mode = match params
        .get("mode")
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("model_x_source")
    {
        "model_x_source" | "model_x_day" => params
            .get("mode")
            .map(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("model_x_source"),
        other => {
            return Err(client_error(format!(
                "参数 mode 必须是 model_x_source|model_x_day，得到 “{other}”"
            )))
        }
    };
    let metric = match params
        .get("metric")
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("total_tokens")
    {
        "total_tokens" | "requests" | "avg_duration_ms" => params
            .get("metric")
            .map(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("total_tokens"),
        other => {
            return Err(client_error(format!(
                "参数 metric 必须是 total_tokens|requests|avg_duration_ms，得到 “{other}”"
            )))
        }
    };
    let f = filters_from(path)?;

    let heatmap = store.query_heatmap(
        mode,
        metric,
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    with_window(serde_json::to_string(&heatmap)?, &f)
}

fn handle_timeline(store: &Store, path: &str) -> Result<String> {
    reject_unsupported_days(path)?;
    let params = parse_params(path);
    let mode = match params
        .get("mode")
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("daily")
    {
        "hourly" => TimelineMode::Hourly,
        "daily" => TimelineMode::Daily,
        "weekly" => TimelineMode::Weekly,
        "monthly" => TimelineMode::Monthly,
        other => {
            return Err(client_error(format!(
                "参数 mode 必须是 daily|hourly|weekly|monthly，得到 “{other}”"
            )))
        }
    };
    // The model filter has to be forwarded: the dashboard sends it, and with
    // it dropped the timeline was the one panel that kept showing unfiltered
    // numbers while every other one narrowed.
    let f = filters_from(path)?;

    let timeline = store.query_timeline(
        mode,
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    with_window(serde_json::to_string(&timeline)?, &f)
}

/// Consolidated at-a-glance report for a recent window (default 7 days):
/// current vs previous window totals with change ratios, per-day buckets,
/// per-source split and top models by tokens — one call for the dashboard
/// digest panel instead of stitching several filtered queries client-side.
fn handle_digest(store: &Store, path: &str) -> Result<String> {
    let params = parse_params(path);
    let days: i64 = days_param(&params)?.unwrap_or(7);

    let (cur_start, now, prev_start) = tokenbuddy::digest_windows(days);

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
        // issue #29:两期窗口的锚点——本期从 cur_start 到此刻,上期等长回退。
        "window": {
            "window_start": cur_start,
            "window_end": now,
            "previous_start": prev_start,
            "window_kind": "cn_calendar",
        },
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
    // bin 测试进程里看不到 lib 的 cfg(test) 锁;statusline 要读真机账本
    // 与套餐快照,环境改动必须与同进程其他测试互斥。
    static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// statusline 渲染内部会 Store::open() + quota::collect_view——不隔离
    /// TOKENBUDDY_HOME 就会吃到宿主机真实数据:在跑着真 TokenBuddy 的
    /// CI runner(ci-runner)上,真实 codex 5h 窗口混进渲染行,
    /// `!line.contains("5h:")` 这类断言必炸。空目录 = 确定性渲染。
    fn statusline_isolated_home(name: &str) -> std::path::PathBuf {
        let dir = tokenbuddy::unique_test_dir(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        dir
    }

    #[test]
    fn statusline_renders_model_dir_today_and_windows() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = statusline_isolated_home("statusline");
        let stdin = r#"{
            "model": {"display_name": "Opus 4.8"},
            "workspace": {"current_dir": "/Users/example/code/project"},
            "rate_limits": {
                "five_hour": {"used_percentage": 23.5, "resets_at": 4102444800},
                "seven_day": {"used_percentage": 41.2, "resets_at": 4102444800}
            }
        }"#;
        let line = statusline_line(stdin);
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(line.contains("Opus 4.8"), "{line}");
        assert!(line.contains("local-token-compute"), "{line}");
        // 今日 tokens 依赖本机账本,测试环境不断言(并行测试可能指向空
        // TOKENBUDDY_HOME);stdin 派生的三段是确定性契约。
        // binding_windows 每源只带最紧窗口:claude 7d(41%)压过 5h(24%)。
        assert!(line.contains("7d:41%→"), "{line}");
        assert!(
            !line.contains("5h:"),
            "binding window only, not both: {line}"
        );
    }

    /// R99 ctx 段:used_percentage 直出;null 时回退 current_usage 入侧
    /// 三项 ÷ 窗口大小;两者皆无 → 不出段;≥75% 带 ⚠。
    #[test]
    fn statusline_context_segment_has_fallback_chain() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = statusline_isolated_home("statusline_ctx");
        let base = r#"{"model":{"display_name":"Opus"},"workspace":{"current_dir":"/x/y"}}"#;
        let direct = r#"{"context_window":{"used_percentage":43,"context_window_size":200000}}"#;
        let line = statusline_line(direct);
        assert!(line.contains("ctx 43%"), "{line}");

        let fallback = r#"{"context_window":{"used_percentage":null,"context_window_size":200000,
            "current_usage":{"input_tokens":60000,"cache_read_input_tokens":20000,"cache_creation_input_tokens":6000}}}"#;
        let line = statusline_line(fallback);
        assert!(line.contains("ctx 43%"), "{line}");

        let warn = r#"{"context_window":{"used_percentage":81}}"#;
        assert!(statusline_line(warn).contains("⚠ctx 81%"));

        let empty = statusline_line(base);
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(!empty.contains("ctx"), "{line}");
        assert!(!statusline_line("not json").contains("Opus"));
    }

    /// 毒化语料(statusline 版):LCG 变异的合法 JSON 喂 stdin 解析,
    /// 绝不 panic、绝不空输出。
    #[test]
    fn statusline_fuzz_corpus_never_panics() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = statusline_isolated_home("statusline_fuzz");
        let seed = r#"{"model":{"display_name":"Opus"},"workspace":{"current_dir":"/tmp/x"},"rate_limits":{"five_hour":{"used_percentage":23.5,"resets_at":1790640000},"seven_day":{"used_percentage":41.2,"resets_at":1791129600}}}"#;
        let mut lcg: u64 = 0x5EED_5757_5757;
        let mut next = move || {
            lcg = lcg
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            lcg
        };
        for _ in 0..300 {
            let mut bytes = seed.as_bytes().to_vec();
            match next() % 3 {
                0 => {
                    let cut = (next() as usize) % (bytes.len() + 1);
                    bytes.truncate(cut);
                }
                1 => {
                    let pos = (next() as usize) % bytes.len();
                    bytes[pos] = (next() % 256) as u8;
                }
                _ => {
                    let pos = (next() as usize) % (bytes.len() + 1);
                    let junk: Vec<u8> = (0..(next() % 8 + 1) as usize)
                        .map(|_| (next() % 256) as u8)
                        .collect();
                    bytes.splice(pos..pos, junk);
                }
            }
            let line = statusline_line(&String::from_utf8_lossy(&bytes));
            assert!(!line.is_empty(), "statusline went silent on poison");
        }
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn statusline_tolerates_empty_and_garbage_stdin() {
        // 空对象与毒化输入都绝不 panic;输出非空(statusline 不能哑掉)。
        // 不做等值/内容断言——真机上这两行含实时倒计时,逐字符比较会抖。
        for input in ["{}", "not json at all", ""] {
            let line = statusline_line(input);
            assert!(!line.is_empty(), "empty statusline for {input:?}");
        }
    }

    use super::{
        api_endpoints, client_accepts_zstd, cross_origin_check, days_param, filters_from,
        fleet_today_segment, fmt_ago, fmt_span, handle_skill, host_allowed, limit_param,
        mask_secret, not_found_message, origin_allowed, param_i64, parse_params, percent_decode,
        read_capped_body, reject_unsupported_days, request_has_token, resolve_bind_addr, route_is,
        statusline_line, tokens_equal, with_window, BadRequest, MAX_CONFIG_BODY_BYTES,
    };
    use tiny_http::Header;

    #[test]
    fn strict_params_reject_garbage_but_keep_absent_optional() {
        let p = parse_params("/x?days=abc&limit=7");
        assert!(param_i64(&p, "days").is_err(), "days=abc → 400, not ignore");
        assert_eq!(param_i64(&p, "limit").unwrap(), Some(7));
        assert_eq!(param_i64(&p, "missing").unwrap(), None);
        // Empty value reads as absent (the dashboard sends `x=` for unset).
        assert_eq!(param_i64(&parse_params("/x?days="), "days").unwrap(), None);
    }

    /// Issue #6/#12 recurring: an unknown `timeRange` value must be a 400,
    /// not a silent fall-through to "all time".
    #[test]
    fn unknown_time_range_is_a_client_error() {
        assert!(filters_from("/api/summary").unwrap().date_start.is_none());
        assert!(filters_from("/api/summary?timeRange=")
            .unwrap()
            .date_start
            .is_none());
        assert!(filters_from("/api/summary?timeRange=all")
            .unwrap()
            .date_start
            .is_none());
        assert!(filters_from("/api/summary?timeRange=7d")
            .unwrap()
            .date_start
            .is_some());
        // Nd 通用窗口（issue #22 Bug 4）：14d 合法；0/366 越界仍拒。
        assert!(filters_from("/api/summary?timeRange=14d")
            .unwrap()
            .date_start
            .is_some());
        assert!(filters_from("/api/summary?timeRange=365d")
            .unwrap()
            .date_start
            .is_some());
        for tr in ["abc", "0d", "366d", "7D", "d"] {
            let err = filters_from(&format!("/api/summary?timeRange={tr}"))
                .expect_err("unknown timeRange must be refused");
            assert!(
                err.chain()
                    .any(|c| c.downcast_ref::<BadRequest>().is_some()),
                "timeRange={tr} must map to 400: {err}"
            );
        }
    }

    /// Issue #30 Bug 4: route arms match the endpoint segment exactly — a
    /// sub-path or trailing slash under a fixed endpoint must fall through
    /// to the 404 arm, not slip into the handler as a 200.
    #[test]
    fn route_matching_is_exact_not_prefix() {
        assert!(route_is("/api/status", "/api/status"));
        assert!(route_is("/api/status?refresh=1", "/api/status"));
        assert!(!route_is("/api/status/extra", "/api/status"));
        assert!(!route_is("/api/status/anything/at/all", "/api/status"));
        assert!(!route_is("/api/status/", "/api/status"));
        assert!(!route_is("/api/statusX", "/api/status"));
        // The days-refusal pre-filter uses the same matcher, so a sub-path
        // 404s instead of answering the days 400 for an endpoint that
        // doesn't exist.
        assert!(!route_is("/api/summary/old?days=3", "/api/summary"));
    }

    /// Issue #30 Bug 6: the 404 hint must point at `/api/docs` — the
    /// machine-readable index — not `/api/`, whose JSON list is what a
    /// human mistyping a URL can't skim.
    #[test]
    fn not_found_hint_points_at_docs() {
        let msg = not_found_message("/api/foo");
        assert!(msg.contains("/api/docs"), "{msg}");
        assert!(msg.contains("/api/foo"), "{msg}");
    }

    /// #29 回归：timeline 的顶层数组过 with_window 不得 panic（box 实测
    /// /api/timeline 任意窗口全量 500）——数组原样返回，对象响应才带 window。
    #[test]
    fn with_window_leaves_arrays_alone() {
        let f = filters_from("/api/timeline?timeRange=7d").unwrap();
        let arr = with_window("[{\"label\":\"d1\"}]".to_string(), &f).unwrap();
        assert!(arr.starts_with('['), "{arr}");
        let obj = with_window("{\"records\":1}".to_string(), &f).unwrap();
        assert!(obj.contains("\"window\""), "{obj}");
    }

    /// Issue #30 Bug 5: the source whitelist matches case-insensitively and
    /// stores the canonical lowercase form, so `HERMES` filters like
    /// `hermes` instead of erroring or silently going empty.
    #[test]
    fn source_filter_is_case_insensitive() {
        assert_eq!(
            filters_from("/api/summary?source=HERMES")
                .unwrap()
                .source
                .as_deref(),
            Some("hermes")
        );
        assert_eq!(
            filters_from("/api/context/search?source=Hermes")
                .unwrap()
                .source
                .as_deref(),
            Some("hermes")
        );
        // Unknown names still 400 regardless of case — an unknown source
        // must stay distinguishable from "no usage in this window" (#24).
        let err =
            filters_from("/api/summary?source=NOPE").expect_err("unknown source must stay a 400");
        assert!(err
            .chain()
            .any(|c| c.downcast_ref::<BadRequest>().is_some()));
    }

    /// Issue #12 Bug 1: summary/timeline are timeRange-only; a passed `days`
    /// must be refused as a client error, not silently ignored.
    #[test]
    fn time_range_only_endpoints_reject_days() {
        assert!(reject_unsupported_days("/api/timeline?mode=daily").is_ok());
        assert!(reject_unsupported_days("/api/summary").is_ok());

        for path in [
            "/api/timeline?days=7",
            "/api/summary?days=abc",
            "/api/summary?days=",
        ] {
            let err = reject_unsupported_days(path)
                .expect_err("days must be refused on timeRange-only endpoints");
            assert!(
                err.chain()
                    .any(|c| c.downcast_ref::<BadRequest>().is_some()),
                "{path} must map to 400: {err}"
            );
            assert!(err.to_string().contains("timeRange"), "{path}: {err}");
        }
    }

    #[test]
    fn window_and_limit_ranges_fail_loudly() {
        // Issue #1 bug 1: days=abc used to be silently swallowed and the
        // handler answered a default window — a different question than the
        // one asked. The guard lives on the params layer the handlers share.
        let bad = parse_params("/x?days=abc");
        assert!(days_param(&bad).is_err());
        let zero = parse_params("/x?days=0");
        assert!(days_param(&zero).is_err(), "days=0 is a caller mistake");
        let big = parse_params("/x?days=366");
        assert!(days_param(&big).is_err());
        assert_eq!(days_param(&parse_params("/x")).unwrap(), None);
        assert_eq!(days_param(&parse_params("/x?days=365")).unwrap(), Some(365));

        assert!(limit_param(&parse_params("/x?limit=999"), 20, 100).is_err());
        assert!(limit_param(&parse_params("/x?limit=abc"), 20, 100).is_err());
        assert_eq!(limit_param(&parse_params("/x"), 20, 100).unwrap(), 20);
        assert_eq!(
            limit_param(&parse_params("/x?limit=1"), 20, 100).unwrap(),
            1
        );
    }

    /// R103: the embedded skill is valid frontmatter+markdown and the
    /// handler surfaces it with copy-ready fields.
    #[test]
    fn skill_endpoint_serves_embedded_markdown() {
        let body = handle_skill().unwrap();
        let v: serde_json::Value = serde_json::from_str(&body).expect("json");
        assert_eq!(v["name"], "tokenbuddy-analyze");
        let md = v["markdown"].as_str().unwrap();
        assert!(md.starts_with("---"), "frontmatter: {}", &md[..40]);
        assert!(md.contains("description: "), "frontmatter description");
        assert!(md.contains("## 什么时候用"), "body section");
        assert!(!v["description"].as_str().unwrap().is_empty());
        let hints = v["install_hints"].as_array().unwrap();
        assert!(hints.iter().any(|h| h.as_str().unwrap().contains(".zcode")));
    }

    #[test]
    fn api_docs_table_covers_the_essential_endpoints() {
        let docs = api_endpoints();
        let arr = docs.as_array().expect("docs is an array");
        assert!(arr.len() >= 25, "endpoint count: {}", arr.len());
        let paths: Vec<&str> = arr.iter().filter_map(|e| e["path"].as_str()).collect();
        for must in [
            "/api/health",
            "/api/docs",
            "/api/summary",
            "/api/context/search",
            "/api/status",
        ] {
            assert!(
                paths.iter().any(|p| p.split('?').next() == Some(must)),
                "{must} documented"
            );
        }
        for e in arr {
            assert!(
                e["method"].is_string() && e["desc"].is_string(),
                "shape {e}"
            );
        }
    }

    #[test]
    fn masks_the_secret_but_keeps_unset_empty() {
        assert_eq!(mask_secret(""), "");
        assert_eq!(mask_secret("ab"), "••ab••");
        assert_eq!(mask_secret("test2001-key"), "••-key••");
        assert!(!mask_secret("test2001-key").contains("test2001"));
    }

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
            let (cur_start, now, prev_start) = tokenbuddy::digest_windows(days);
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

    fn fake_request(body: &'static str) -> tiny_http::Request {
        tiny_http::TestRequest::new()
            .with_method(tiny_http::Method::Post)
            .with_path("/api/fleet/config")
            .with_body(body)
            .into()
    }

    /// R3: with a token required, both presentation channels work — the
    /// Authorization header for scripts and ?token= for a page navigation.
    #[test]
    fn token_check_accepts_header_and_query() {
        let with_auth = |v: &str| [hdr("Authorization", v)];
        assert!(!request_has_token(&[], "/", "s3cret"));
        assert!(request_has_token(
            &with_auth("Bearer s3cret"),
            "/",
            "s3cret"
        ));
        assert!(!request_has_token(
            &with_auth("Bearer wrong"),
            "/",
            "s3cret"
        ));
        assert!(!request_has_token(
            &with_auth("Basic s3cret"),
            "/",
            "s3cret"
        ));
        assert!(request_has_token(&[], "/?token=s3cret", "s3cret"));
        assert!(!request_has_token(&[], "/?token=wrong", "s3cret"));
        // percent-encoded query values decode before comparing
        assert!(request_has_token(&[], "/?token=s3%20cret", "s3 cret"));
        // the token name in the query must match exactly
        assert!(!request_has_token(&[], "/?not_token=s3cret", "s3cret"));
    }

    #[test]
    fn zstd_acceptance_is_parsed_from_accept_encoding() {
        let hdrs = |v: &str| [hdr("Accept-Encoding", v)];
        assert!(client_accepts_zstd(&hdrs("zstd")));
        assert!(client_accepts_zstd(&hdrs("gzip, deflate, br, zstd")));
        assert!(client_accepts_zstd(&hdrs("ZSTD")));
        assert!(client_accepts_zstd(&hdrs("br;q=1.0, zstd;q=0.9")));
        assert!(!client_accepts_zstd(&hdrs("gzip, deflate, br")));
        assert!(!client_accepts_zstd(&[]));
    }

    #[test]
    fn token_compare_is_exact() {
        assert!(tokens_equal("abc", "abc"));
        assert!(!tokens_equal("abc", "abd"));
        assert!(!tokens_equal("abc", "abcd"));
        assert!(tokens_equal("", ""));
    }

    /// R3: a non-loopback bind without TOKENBUDDY_TOKEN refuses to come up.
    #[test]
    fn remote_bind_requires_token() {
        let (ip, port) = resolve_bind_addr(None, 8080, false).expect("default is loopback");
        assert_eq!(ip, std::net::IpAddr::from([127, 0, 0, 1]));
        assert_eq!(port, 8080);

        let (ip, port) = resolve_bind_addr(Some("::1"), 8080, false).expect("::1 is loopback");
        assert_eq!(ip.to_string(), "::1");
        assert_eq!(port, 8080);

        let (ip, port) =
            resolve_bind_addr(Some("192.0.2.5:9000"), 8080, true).expect("token set");
        assert_eq!(ip.to_string(), "192.0.2.5");
        assert_eq!(port, 9000);

        assert!(resolve_bind_addr(Some("0.0.0.0"), 8080, false).is_err());
        assert!(resolve_bind_addr(Some("not-an-ip"), 8080, true).is_err());
    }

    /// F3 (security audit 2026-09-28): the config endpoints used to read the
    /// whole body into a String, so an oversized request turned into resident
    /// memory. Now anything over the cap is refused unread.
    #[test]
    fn config_body_over_cap_is_refused_without_reading() {
        let big: &'static str = String::from_utf8(vec![b'a'; MAX_CONFIG_BODY_BYTES + 1])
            .unwrap()
            .leak();
        assert!(read_capped_body(&mut fake_request(big), MAX_CONFIG_BODY_BYTES).is_err());

        let at_cap: &'static str = String::from_utf8(vec![b'a'; MAX_CONFIG_BODY_BYTES])
            .unwrap()
            .leak();
        let body = read_capped_body(&mut fake_request(at_cap), MAX_CONFIG_BODY_BYTES)
            .expect("a body at exactly the cap is accepted");
        assert_eq!(body.len(), MAX_CONFIG_BODY_BYTES);

        assert_eq!(
            read_capped_body(
                &mut fake_request("{\"enabled\":true}"),
                MAX_CONFIG_BODY_BYTES
            )
            .expect("a small valid body round-trips"),
            "{\"enabled\":true}"
        );
    }

    fn hdr(name: &str, value: &str) -> Header {
        Header::from_bytes(name, value).expect("test header should be valid")
    }

    /// F1/F2 (security audit 2026-09-28): loopback Hosts pass, a rebinding
    /// domain in Host is refused, and the wrong port is refused too.
    #[test]
    fn host_allowlist_blocks_rebinding() {
        let extra: Vec<String> = vec!["box.lan".into()];
        for host in [
            "127.0.0.1:8080",
            "localhost:8080",
            "[::1]:8080",
            "127.0.0.1",
            "LOCALHOST",
        ] {
            assert!(
                host_allowed(host, 8080, &extra),
                "loopback host {host} must pass"
            );
        }
        for host in [
            "evil.com:8080",
            "evil.com",
            "localhost:9999",
            "127.0.0.1:1",
            "",
            "box.lan.evil.com",
        ] {
            assert!(
                !host_allowed(host, 8080, &extra),
                "host {host} must be refused"
            );
        }
        assert!(
            host_allowed("box.lan:8080", 8080, &extra),
            "explicitly allowed host passes"
        );
        assert!(
            host_allowed("Box.LAN", 8080, &extra),
            "allowed host matches case-insensitively"
        );
    }

    /// Only http loopback origins count as same-origin for writes.
    #[test]
    fn origin_check_passes_same_origin_only() {
        let extra: Vec<String> = Vec::new();
        assert!(origin_allowed("http://127.0.0.1:8080", 8080, &extra));
        assert!(origin_allowed("http://localhost:8080", 8080, &extra));
        for bad in [
            "https://127.0.0.1:8080",
            "http://evil.com:8080",
            "null",
            "http://localhost:9999",
            "ftp://127.0.0.1:8080",
        ] {
            assert!(
                !origin_allowed(bad, 8080, &extra),
                "origin {bad} must be refused"
            );
        }
    }

    /// The full guard: Host on every method, Origin/Sec-Fetch-Site on POST,
    /// headerless non-browser clients (curl, statusline) untouched.
    #[test]
    fn cross_origin_check_rules() {
        let extra: Vec<String> = Vec::new();
        let loopback = [hdr("Host", "127.0.0.1:8080")];
        let curl = [];

        // GET/POST with no Origin at all: scripts and curl, untouched.
        assert!(cross_origin_check("GET", &curl, 8080, &extra).is_ok());
        assert!(cross_origin_check("POST", &curl, 8080, &extra).is_ok());
        assert!(cross_origin_check("POST", &loopback, 8080, &extra).is_ok());

        // Rebinding: any method with an attacker Host is refused.
        let rebinding = [hdr("Host", "evil.com:8080")];
        assert!(cross_origin_check("GET", &rebinding, 8080, &extra).is_err());
        assert!(cross_origin_check("POST", &rebinding, 8080, &extra).is_err());

        // Drive-by CSRF: browser cross-site POST carries a foreign Origin.
        let csrf = [
            hdr("Host", "127.0.0.1:8080"),
            hdr("Origin", "http://evil.com:80"),
        ];
        assert_eq!(
            cross_origin_check("POST", &csrf, 8080, &extra),
            Err("Origin 跨源（CSRF 防护）")
        );

        // A same-origin POST from the dashboard passes.
        let same = [
            hdr("Host", "127.0.0.1:8080"),
            hdr("Origin", "http://127.0.0.1:8080"),
        ];
        assert!(cross_origin_check("POST", &same, 8080, &extra).is_ok());

        // Sec-Fetch-Site defence in depth when Origin is absent.
        let fetch = [
            hdr("Host", "127.0.0.1:8080"),
            hdr("Sec-Fetch-Site", "cross-site"),
        ];
        assert!(cross_origin_check("POST", &fetch, 8080, &extra).is_err());
        let none_fetch = [hdr("Host", "127.0.0.1:8080"), hdr("Sec-Fetch-Site", "none")];
        assert!(cross_origin_check("POST", &none_fetch, 8080, &extra).is_ok());
    }
    #[test]
    fn fmt_ago_and_span_are_human() {
        let now = 1_800_000_000;
        assert_eq!(fmt_ago(now - 30, now), "刚刚");
        assert_eq!(fmt_ago(now - 5 * 60, now), "5m前");
        assert_eq!(fmt_ago(now - 3 * 3600, now), "3h前");
        assert_eq!(fmt_ago(now - 2 * 86400, now), "2d前");
        assert_eq!(fmt_ago(0, now), "—");
        assert_eq!(fmt_span(30), "<1m");
        assert_eq!(fmt_span(45 * 60), "45m");
        assert_eq!(fmt_span(3 * 3600 + 12 * 60), "3h12m");
        assert_eq!(fmt_span(2 * 86400 + 4 * 3600), "2d4h");
    }
    #[test]
    fn fleet_segment_picks_tightest_window_per_host() {
        use tokenbuddy::fleet::FleetHostQuota;
        use tokenbuddy::quota::QuotaSnapshot;
        let now = 1_800_000_000;
        let mk = |source: &str, used: f64, resets: Option<i64>| QuotaSnapshot {
            source: source.into(),
            plan: "general".into(),
            window: "interval".into(),
            used_percent: used,
            resets_at: resets,
            collected_at: now,
            origin: String::from("file"),
        };
        let fq = vec![
            FleetHostQuota {
                host: "mac-mini".into(),
                snapshots: vec![
                    mk("minimax", 31.0, Some(now + 3600)),
                    mk("codex", 62.0, Some(now + 7200)),
                ],
                generated_at: Some(now),
            },
            FleetHostQuota {
                host: "ci-runner".into(),
                snapshots: vec![mk("minimax", 20.0, None)],
                generated_at: Some(now),
            },
        ];
        let seg = fleet_today_segment(&fq, now).unwrap();
        assert!(seg.contains("机群"), "{seg}");
        assert!(seg.contains("mac-mini 62%"), "{seg}");
        assert!(seg.contains("ci-runner 20%"), "{seg}");
        // 最紧的排最前。
        assert!(seg.find("mac-mini").unwrap() < seg.find("ci-runner").unwrap());
        // 无数据 → None。
        assert!(fleet_today_segment(&[], now).is_none());
        // 截断:最多 3 台。
        let many: Vec<FleetHostQuota> = (0..5)
            .map(|i| FleetHostQuota {
                host: format!("h{i}"),
                snapshots: vec![mk("x", i as f64 * 10.0 + 5.0, None)],
                generated_at: Some(now),
            })
            .collect();
        let seg3 = fleet_today_segment(&many, now).unwrap();
        assert_eq!(seg3.matches('%').count(), 3, "{seg3}");
    }
}
