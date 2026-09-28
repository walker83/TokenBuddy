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
         \x20 tokenbuddy today            一行今日/7日用量(statusline 用)\n\
         \x20 tokenbuddy report [--days N]  markdown 日报/周报(stdout)\n\
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
            return cmd_report(days);
        }
        Some("mcp") => return tokenbuddy::mcp::run(),
        Some("doctor") => {
            let json = positional.iter().any(|a| a == "--json");
            return cmd_doctor(json);
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

    // The conversation index is NOT built at startup, on purpose. Statistics
    // come from `data.parquet` alone; the search index is a second, much
    // larger structure that a reader of simple totals never looks at. It is
    // built when the user opens the search view, and handed back after 15 idle
    // minutes — so the common case never pays for it at all.
    let context = Arc::new(ContextHandle::new(
        tokenbuddy::data_dir().join("context.parquet"),
    ));
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
        if cfg.enabled && cfg.auto_push {
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
        ("GET", path) if path.starts_with("/api/summary") => match handle_summary(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("POST", path) if path.starts_with("/api/sync") => {
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
            match result.and_then(|r| Ok(serde_json::to_string(&r)?)) {
                Ok(json) => {
                    // Deliberately does not touch the conversation corpus.
                    // Collecting it costs more memory than the entire token
                    // store, and the only consumer is the search index — so a
                    // sync done for the sake of the numbers must not pay for
                    // it. The next search collects and builds on demand.
                    json_response(json)
                }
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if path.starts_with("/api/timeline") => match handle_timeline(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if path.starts_with("/api/metrics") => match handle_metrics(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if path.starts_with("/api/heatmap") => match handle_heatmap(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if path.starts_with("/api/insights") => match handle_insights(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if path.starts_with("/api/models") => match handle_models(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if path.starts_with("/api/digest") => match handle_digest(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if path.starts_with("/api/context/search") => {
            match handle_context_search(context, path) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if path.starts_with("/api/context/session") => {
            match handle_context_session(context, path) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if path.starts_with("/api/context/stats") => {
            match handle_context_stats(context) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if path.starts_with("/api/fleet/config") => match handle_fleet_config_get() {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("POST", path)
            if path.starts_with("/api/fleet/config")
                | path.starts_with("/api/fleet/config-test") =>
        {
            let is_test = path.starts_with("/api/fleet/config-test");
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
        ("POST", path) if path.starts_with("/api/fleet/push") => {
            let _guard = write_lock.lock().unwrap_or_else(|e| e.into_inner());
            match handle_fleet_push(store) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("POST", path) if path.starts_with("/api/fleet/pull") => {
            let _guard = write_lock.lock().unwrap_or_else(|e| e.into_inner());
            match handle_fleet_pull() {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if path.starts_with("/api/pivot") => {
            let p = parse_params(path);
            let start = p.get("start").and_then(|v| v.parse::<i64>().ok());
            let end = p.get("end").and_then(|v| v.parse::<i64>().ok());
            match store
                .query_pivot(start, end)
                .and_then(|pivot| Ok(serde_json::to_string(&pivot)?))
            {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if path.starts_with("/api/anomalies") => match store
            .query_anomalies()
            .and_then(|report| Ok(serde_json::to_string(&report)?))
        {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if path.starts_with("/api/windows") => match store
            .query_windows()
            .and_then(|facts| Ok(serde_json::to_string(&facts)?))
        {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if path.starts_with("/api/brief") => match handle_brief(store, path) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if path.starts_with("/api/status") => match handle_status(store, context) {
            Ok(json) => json_response(json),
            Err(e) => error_response(&e),
        },
        ("GET", path) if path.starts_with("/api/fleet/hosts") => {
            let hosts = store.fleet_hosts();
            json_response(serde_json::json!({ "hosts": hosts }).to_string())
        }
        ("GET", path) if path.starts_with("/api/fleet/summary") => {
            match handle_fleet_summary(store, path) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if path.starts_with("/api/fleet/metrics") => {
            match handle_fleet_metrics(store, path) {
                Ok(json) => json_response(json),
                Err(e) => error_response(&e),
            }
        }
        ("GET", path) if path.starts_with("/api/fleet/models") => {
            match handle_fleet_models(store, path) {
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
        ("POST", path) if path.starts_with("/api/context/activate") => {
            // Opening the search view is explicit intent: start the build now
            // so it is ready by the time a query is typed. Never blocks.
            context.warm_if_needed();
            json_response(serde_json::json!({ "warming": true }).to_string())
        }
        ("POST", path) if path.starts_with("/api/context/rebuild") => {
            let ctx = Arc::clone(context);
            std::thread::spawn(move || {
                if let Err(e) = ctx.sync_and_build(true) {
                    eprintln!("[TokenBuddy] context rebuild failed: {e}");
                }
            });
            json_response(serde_json::json!({ "started": true }).to_string())
        }
        _ => Response::from_string("Not Found").with_status_code(404),
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

/// `GET /api/fleet/config` — current fleet.toml for the settings page.
/// `config: null` when the file does not exist yet (feature off).
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
    println!(
        "今日 {} tokens（入 {} · 出 {} · {} 请求） · 7日 {}",
        tokenbuddy::format_tokens(today.total_tokens),
        tokenbuddy::format_tokens(today.total_input_tokens),
        tokenbuddy::format_tokens(today.total_output_tokens),
        today.total_requests,
        tokenbuddy::format_tokens(week.total_tokens),
    );
    Ok(())
}

/// `tokenbuddy report [--days N]` — a markdown digest of one window on
/// stdout. Humans skim it; the tokenbuddy-analyze skill reads it instead of
/// raw logs, which is what keeps agent self-review affordable.
fn cmd_report(days: i64) -> Result<()> {
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
    print!(
        "{}",
        tokenbuddy::report::render(days, &summary, &metrics, &windows, &anomalies, &pivot)
    );
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
    let body = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
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
    let f = filters_from(path);
    let summary = store.query_fleet_summary(
        f.host.as_deref(),
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    Ok(serde_json::to_string(&summary)?)
}

/// `GET /api/fleet/metrics?...` — latency/cache panel keyed by host.
fn handle_fleet_metrics(store: &Store, path: &str) -> Result<String> {
    let f = filters_from(path);
    let metrics = store.query_fleet_metrics(
        f.host.as_deref(),
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    Ok(serde_json::to_string(&metrics)?)
}

/// `GET /api/fleet/models?...` — per-model comparison across hosts.
fn handle_fleet_models(store: &Store, path: &str) -> Result<String> {
    let f = filters_from(path);
    let models = store.query_fleet_models(
        f.host.as_deref(),
        f.source.as_deref(),
        f.model.as_deref(),
        f.date_start,
        f.date_end,
    )?;
    Ok(serde_json::to_string(&models)?)
}

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
    /// Fleet-only: narrow to one pulled host; "all"/absent means every host.
    /// Local endpoints ignore it.
    host: Option<String>,
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
    let host = match params.get("host").map(|s| s.as_str()) {
        Some("all") | None => None,
        Some(s) => Some(s.to_string()),
    };
    Filters {
        date_start: time_range_start(params.get("timeRange").map(|s| s.as_str())),
        date_end: None,
        source,
        model,
        host,
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
    use super::{
        client_accepts_zstd, cross_origin_check, digest_windows, host_allowed, mask_secret,
        origin_allowed, parse_params, percent_decode, read_capped_body, request_has_token,
        resolve_bind_addr, tokens_equal, MAX_CONFIG_BODY_BYTES,
    };
    use tiny_http::Header;

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
            resolve_bind_addr(Some("192.168.1.5:9000"), 8080, true).expect("token set");
        assert_eq!(ip.to_string(), "192.168.1.5");
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
}
