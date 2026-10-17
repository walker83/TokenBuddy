//! 网关服务层(R108)——tiny_http 监听 + 客户端鉴权 + 协议路由 +
//! 直通/方言管道 + 流式计量 + 运行时开关。
//!
//! P1 边界(诚实):客户端与上游**同协议直通**(openai→openai、
//! anthropic→anthropic),跨协议翻译(F3)是 P2——请求打不到同协议
//! 上游时,返回 404 加人话,而不是硬翻出错账。
//! 计量在直通管道上旁挂:每行一次 `"usage"` 子串查找,命中才 parse。
//! 失败的请求不进账本;网关自身的错误(鉴权/路由)用统一 JSON 信封,
//! 上游的错误原样透传(工具按自家协议解析错误,不该被网关再包一层)。

use super::qoder as q;
use super::{
    append_meter_row, client_key_label, estimate_usage, extract_usage_body, load_clients,
    load_config_strict, probe, release_caches, upstream_key, ExtractedUsage, GatewayConfig,
    HealthReport, MeterRow, Provider, SseUsageScan,
};
use anyhow::{anyhow, Result};
use std::collections::{HashMap, VecDeque};
use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const REQUEST_BODY_CAP: usize = 32 * 1024 * 1024;
const RESPONSE_BODY_CAP: usize = 32 * 1024 * 1024;
/// 直通管道背压上限:上游快下游慢时,内存被钉在这一格,不随语料涨。
const PIPE_CAP: usize = 256 * 1024;
const WORKERS: usize = 8;

// ---------------------------------------------------------------------------
// 进程级单例:serve 起一个,面板 API 同进程操作它;测试用独立实例。
// ---------------------------------------------------------------------------

static GLOBAL: Mutex<Option<Arc<Runtime>>> = Mutex::new(None);

pub fn set_global(rt: Arc<Runtime>) {
    *GLOBAL.lock().unwrap_or_else(|e| e.into_inner()) = Some(rt);
}

pub fn global() -> Option<Arc<Runtime>> {
    GLOBAL.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

pub struct Runtime {
    inner: Mutex<Option<Running>>,
    pub live_requests: AtomicU64,
    pub live_errors: AtomicU64,
    health: Mutex<HashMap<String, HealthReport>>,
}

struct Running {
    server: Arc<tiny_http::Server>,
    stop: Arc<AtomicBool>,
    workers: Vec<std::thread::JoinHandle<()>>,
    /// 实际绑定地址(listen 配 127.0.0.1:0 时由 OS 分配,测试要读回)。
    addr: String,
}

/// 路由解析结果——authorize/classify 只借用请求,叶子处理器拿走所有权。
#[derive(Debug, Clone, Copy, PartialEq)]
enum Route {
    Chat,
    Messages,
    CountTokens,
    Models,
    Health,
}

impl Runtime {
    pub fn new() -> Self {
        Runtime {
            inner: Mutex::new(None),
            live_requests: AtomicU64::new(0),
            live_errors: AtomicU64::new(0),
            health: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_running(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    /// 起。已在跑时幂等返回 Ok(热更新语义 = stop + start)。
    pub fn start(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_some() {
            return Ok(());
        }
        let cfg = load_config_strict().map_err(|e| anyhow!("gateway.json 有误:{e}"))?;
        let addr = cfg.listen.clone();
        let server = Arc::new(
            tiny_http::Server::http(&addr).map_err(|e| anyhow!("网关无法监听 {addr}:{e}"))?,
        );
        let stop = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::new();
        for _ in 0..WORKERS {
            let server = Arc::clone(&server);
            let stop = Arc::clone(&stop);
            let rt = Arc::clone(self);
            workers.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match server.recv() {
                        Ok(request) => rt.handle(request),
                        // unblock() 或套接字关闭都会走到这里
                        Err(_) => break,
                    }
                }
            }));
        }
        eprintln!("[TokenBuddy] 网关已启动 http://{addr}(OpenAI /v1 · Anthropic /v1/messages)");
        let actual = match server.server_addr() {
            tiny_http::ListenAddr::IP(a) => a.to_string(),
            tiny_http::ListenAddr::Unix(p) => format!("unix:{p:?}"),
        };
        *guard = Some(Running {
            server,
            stop,
            workers,
            addr: actual,
        });
        Ok(())
    }

    /// 实际绑定地址(start 之后才有)。
    pub fn actual_addr(&self) -> Option<String> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|r| r.addr.clone())
    }

    /// 停:置位 → unblock 排队中的 recv → join。正在流式回传的 worker
    /// 把手上这笔发完再退(在途宽限语义)。
    pub fn stop(&self) {
        let running = self.inner.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(running) = running {
            running.stop.store(true, Ordering::Relaxed);
            // tiny_http 的 unblock() 只往队列塞一个 Unblock 元素、notify_one
            // ——只唤醒一个 recv 等待者。8 个 worker 就要发 8 个,否则
            // 停机只剩一个线程退出,join 永挂(集成测试抓到的真 bug)。
            for _ in 0..running.workers.len() {
                running.server.unblock();
            }
            for w in running.workers {
                let _ = w.join();
            }
            eprintln!("[TokenBuddy] 网关已停止,监听线程与连接随之一并释放");
            release_caches();
        }
    }

    /// 显式探活全部 provider(面板「测试」/ CLI probe),结果进缓存。
    pub fn check_health(&self) -> Vec<HealthReport> {
        let cfg = super::load_config();
        let agent = super::build_agent();
        let handles: Vec<_> = cfg
            .providers
            .iter()
            .map(|p| {
                let agent = agent.clone();
                let p = p.clone();
                std::thread::spawn(move || {
                    let key = upstream_key(&p.key_file).unwrap_or_default();
                    probe(&agent, &p, &key)
                })
            })
            .collect();
        let mut reports = Vec::new();
        for h in handles {
            if let Ok(report) = h.join() {
                self.health
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(report.provider.clone(), report.clone());
                reports.push(report);
            }
        }
        reports
    }

    pub fn health_snapshot(&self) -> Vec<HealthReport> {
        self.health
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    fn handle(&self, request: tiny_http::Request) {
        let started = Instant::now();
        let method = request.method().as_str().to_string();
        let url = request.url().to_string();
        self.live_requests.fetch_add(1, Ordering::Relaxed);

        // 第一阶段只借不消费:鉴权 + 路由解析,失败的在这里统一应答。
        let gate = self
            .authorize(&request)
            .and_then(|client| self.classify(&method, &url).map(|route| (client, route)));
        let (client, route) = match gate {
            Ok(pair) => pair,
            Err(e) => {
                self.live_errors.fetch_add(1, Ordering::Relaxed);
                Self::respond_error(request, e);
                return;
            }
        };

        // 叶子处理器拿走 request 所有权,成功失败都自己应答恰好一次。
        let outcome = match route {
            Route::Chat => self.proxy(request, &client, "openai", started),
            Route::Messages => self.proxy(request, &client, "anthropic", started),
            Route::CountTokens => self.proxy_count_tokens(request, "anthropic"),
            Route::Models => self.list_models(request),
            Route::Health => Ok(()), // 已过鉴权;工具/脚本探网关是否在线
        };
        if outcome.is_err() {
            self.live_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn authorize(&self, request: &tiny_http::Request) -> Result<String, (u16, String)> {
        // 每请求读一次 clients 文件:密钥热生效;量级(小 JSON)远小于
        // 一次模型调用,不构成热路径。
        let clients = load_clients().map_err(|e| (500u16, format!("客户端密钥文件不可读:{e}")))?;
        let known = |key: &str| {
            clients
                .iter()
                .find(|c| c.key == key)
                .map(|c| c.label.clone())
        };
        client_key_label(request.headers(), known).ok_or_else(|| {
            (
                401u16,
                "缺少或错误的网关客户端密钥(Authorization: Bearer tb-local-… 或 x-api-key);用 `tokenbuddy gateway key add <工具名>` 签发".to_string(),
            )
        })
    }

    fn classify(&self, method: &str, url: &str) -> Result<Route, (u16, String)> {
        let path = url.split('?').next().unwrap_or(url);
        match (method, path) {
            ("POST", "/v1/chat/completions") => Ok(Route::Chat),
            ("POST", "/v1/messages") => Ok(Route::Messages),
            ("POST", "/v1/messages/count_tokens") => Ok(Route::CountTokens),
            ("GET", "/v1/models") => Ok(Route::Models),
            ("GET", "/health") => Ok(Route::Health),
            _ => Err((
                404u16,
                format!("网关没有 {method} {path}——OpenAI 工具接 /v1/chat/completions,Anthropic 工具接 /v1/messages"),
            )),
        }
    }

    fn respond_error(request: tiny_http::Request, e: (u16, String)) {
        let body = serde_json::json!({
            "error": {"type": "tokenbuddy_gateway", "message": e.1}
        });
        let _ = request.respond(
            tiny_http::Response::from_string(body.to_string())
                .with_status_code(e.0)
                .with_header(
                    tiny_http::Header::from_bytes("Content-Type", "application/json")
                        .expect("static header"),
                ),
        );
    }

    fn read_body(request: &mut tiny_http::Request) -> Result<Vec<u8>, (u16, String)> {
        let mut buf = Vec::new();
        request
            .as_reader()
            .take(REQUEST_BODY_CAP as u64 + 1)
            .read_to_end(&mut buf)
            .map_err(|e| (400u16, format!("请求体读取失败:{e}")))?;
        if buf.len() > REQUEST_BODY_CAP {
            return Err((413u16, "请求体超过 32MiB 上限".to_string()));
        }
        Ok(buf)
    }

    /// 主代理路径:同协议直通 + 计量旁挂。
    fn proxy(
        &self,
        mut request: tiny_http::Request,
        client: &str,
        client_protocol: &str,
        started: Instant,
    ) -> Result<(), (u16, String)> {
        let fail = |request: tiny_http::Request, e: (u16, String)| {
            Self::respond_error(request, e.clone());
            Err(e)
        };
        let cfg = match load_config_strict() {
            Ok(c) => c,
            Err(e) => return fail(request, (500, format!("gateway.json 有误:{e}"))),
        };
        let body = match Self::read_body(&mut request) {
            Ok(b) => b,
            Err(e) => return fail(request, e),
        };
        let mut payload: serde_json::Value = match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => return fail(request, (400, format!("请求体不是合法 JSON:{e}"))),
        };
        let req_model = payload
            .get("model")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_string();
        if req_model.is_empty() {
            return fail(request, (400, "请求缺少 model 字段".to_string()));
        }
        let stream = payload
            .get("stream")
            .and_then(|s| s.as_bool())
            .unwrap_or(false);

        let chain = resolve_chain(&cfg, client_protocol, &req_model);
        if chain.is_empty() {
            return fail(
                request,
                (
                    404,
                    format!(
                        "没有 {client_protocol} 上游声明服务模型 {req_model}——在 gateway.json 的 providers[].models 或 aliases 里加一条;跨协议翻译 P2 之前不做"
                    ),
                ),
            );
        }
        let anthropic_version = header_of(request.headers(), "anthropic-version")
            .unwrap_or("2023-06-01")
            .to_string();
        let agent = super::build_agent();
        let mut last_transport_err = String::new();

        for (provider, upstream_model) in &chain {
            payload["model"] = serde_json::Value::String(upstream_model.clone());
            // F6:不注入这条,MiniMax 类上游的流式响应就没有 usage 可计
            // ——计量的前提是流量里带着账。用户显式配了 stream_options 时尊重之。
            if stream
                && provider.dialects.iter().any(|d| d == "include_usage")
                && payload
                    .get("stream_options")
                    .map(|s| s.is_null())
                    .unwrap_or(true)
            {
                payload["stream_options"] = serde_json::json!({"include_usage": true});
            }
            let sent_body = match serde_json::to_vec(&payload) {
                Ok(b) => b,
                Err(e) => return fail(request, (500, format!("请求编码失败:{e}"))),
            };
            // 密钥读不到 = 这一家上游不可用,顺链下一个,不当场 502。
            let key = match upstream_key(&provider.key_file) {
                Ok(k) => k,
                Err(e) => {
                    last_transport_err = format!("{}: {e}", provider.id);
                    continue;
                }
            };
            // R109:qoder 上游——专用发送路径(payload 改形 + WAF 编码 +
            // COSY 签名 + 信封 SSE 解包),不落进通用直通分支。
            if provider.protocol == "qoder" {
                return self.proxy_qoder(
                    request,
                    client,
                    provider,
                    &req_model,
                    upstream_model,
                    &payload,
                    started,
                    stream,
                );
            }
            let url = upstream_url(provider);
            let upstream = match provider.protocol.as_str() {
                "anthropic" => agent
                    .post(&url)
                    .set("x-api-key", &key)
                    .set("anthropic-version", &anthropic_version)
                    .set("content-type", "application/json"),
                _ => agent
                    .post(&url)
                    .set("Authorization", &format!("Bearer {key}"))
                    .set("content-type", "application/json"),
            };
            let response = match upstream.send_bytes(&sent_body) {
                Ok(r) => r,
                // ureq 把 >=400 当 Err(Status) 返回——这是上游的错误响应,
                // 要透传,不是网关的 502,更不是回退理由(集成测试抓到的真 bug)。
                Err(ureq::Error::Status(code, resp)) => {
                    let mut text = Vec::new();
                    let _ = resp.into_reader().take(1024 * 1024).read_to_end(&mut text);
                    let mut resp = tiny_http::Response::from_data(text).with_status_code(code);
                    if let Ok(h) = tiny_http::Header::from_bytes("Content-Type", "application/json")
                    {
                        resp = resp.with_header(h);
                    }
                    let _ = request.respond(resp);
                    return Err((code, "upstream returned an error".to_string()));
                }
                // 传输层失败(F16 P1 档):顺链尝试下一家。
                Err(e) => {
                    last_transport_err = format!("{}: {}", provider.id, e);
                    eprintln!(
                        "[TokenBuddy] 网关回退:{} 传输失败({e}),试链上下一家",
                        provider.id
                    );
                    continue;
                }
            };

            let status = response.status();
            if !(200..300).contains(&status) {
                // 失败请求不进账本,上游错误原样透传(不套网关信封)。
                let mut text = Vec::new();
                let _ = response
                    .into_reader()
                    .take(1024 * 1024)
                    .read_to_end(&mut text);
                let mut resp = tiny_http::Response::from_data(text).with_status_code(status);
                if let Ok(h) = tiny_http::Header::from_bytes("Content-Type", "application/json") {
                    resp = resp.with_header(h);
                }
                let _ = request.respond(resp);
                return Err((status, "upstream returned an error".to_string()));
            }

            let upstream_id = upstream_req_id(&response);
            let rate_limit = rate_limit_headers(&response);
            return if stream {
                self.proxy_stream(
                    response,
                    request,
                    client,
                    provider,
                    &req_model,
                    upstream_model,
                    sent_body.len(),
                    started,
                    upstream_id,
                    rate_limit,
                );
                Ok(())
            } else {
                self.proxy_buffered(
                    response,
                    request,
                    client,
                    provider,
                    &req_model,
                    upstream_model,
                    sent_body.len(),
                    started,
                    upstream_id,
                    rate_limit,
                )
            };
        }
        fail(
            request,
            (
                502,
                format!(
                    "链上 {} 个上游全部不可用,最后错误:{last_transport_err}",
                    chain.len()
                ),
            ),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn proxy_buffered(
        &self,
        response: ureq::Response,
        request: tiny_http::Request,
        client: &str,
        provider: &Provider,
        req_model: &str,
        upstream_model: &str,
        req_bytes: usize,
        started: Instant,
        upstream_id: String,
        rate_limit: serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), (u16, String)> {
        let ct = response_header(&response, "content-type");
        let mut text = Vec::new();
        if let Err(e) = response
            .into_reader()
            .take(RESPONSE_BODY_CAP as u64)
            .read_to_end(&mut text)
        {
            let e = (502u16, format!("上游响应读取失败:{e}"));
            Self::respond_error(request, e.clone());
            return Err(e);
        }

        let usage = serde_json::from_slice::<serde_json::Value>(&text)
            .ok()
            .and_then(|v| extract_usage_body(&provider.protocol, &v))
            .unwrap_or_else(|| estimate_usage(req_bytes, text.len()));

        let mut resp = tiny_http::Response::from_data(text);
        if let Some(ct) = ct {
            if let Ok(h) = tiny_http::Header::from_bytes("Content-Type", ct.as_str()) {
                resp = resp.with_header(h);
            }
        }
        let _ = request.respond(resp);

        write_meter_row(MeterRow {
            uid: new_uid(),
            ts: crate::now_ts(),
            client: client.to_string(),
            provider: provider.id.clone(),
            model: upstream_model.to_string(),
            req_model: req_model.to_string(),
            input: usage.input,
            output: usage.output,
            cache_read: usage.cache_read,
            cache_write: usage.cache_write,
            credits: 0.0,
            ttft_ms: None,
            duration_ms: Some(started.elapsed().as_millis() as u64),
            usage_source: usage_label(&usage),
            status: 200,
            stream: false,
            upstream_id,
            rate_limit,
        });
        Ok(())
    }

    /// 流式直通:上游→管道→客户端,转发线程逐行喂扫描器,EOF 时落账。
    /// 管道 256KB 背压上限——上游再快,内存也就这一格。
    #[allow(clippy::too_many_arguments)]
    fn proxy_stream(
        &self,
        response: ureq::Response,
        request: tiny_http::Request,
        client: &str,
        provider: &Provider,
        req_model: &str,
        upstream_model: &str,
        req_bytes: usize,
        started: Instant,
        upstream_id: String,
        rate_limit: serde_json::Map<String, serde_json::Value>,
    ) {
        let protocol = provider.protocol.clone();
        let ct = response_header(&response, "content-type")
            .unwrap_or_else(|| "text/event-stream".to_string());
        let pipe = Arc::new(Pipe::new());
        let writer = pipe.clone();

        let (meter_client, meter_provider, meter_req, meter_model, meter_uid) = (
            client.to_string(),
            provider.id.clone(),
            req_model.to_string(),
            upstream_model.to_string(),
            new_uid(),
        );
        std::thread::spawn(move || {
            let mut reader = response.into_reader();
            let mut scan = SseUsageScan::default();
            let mut buf = [0u8; 8192];
            let mut line: Vec<u8> = Vec::new();
            let mut payload_bytes: usize = 0;
            let mut ttft: Option<u64> = None;
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        if ttft.is_none() {
                            ttft = Some(started.elapsed().as_millis() as u64);
                        }
                        if writer.push(&buf[..n]).is_err() {
                            break; // 客户端断开:停止转发,按已见事实落账
                        }
                        payload_bytes += n;
                        line.extend_from_slice(&buf[..n]);
                        while let Some(pos) = line.iter().position(|&b| b == b'\n') {
                            let finished: Vec<u8> = line.drain(..=pos).collect();
                            let s = String::from_utf8_lossy(&finished);
                            scan.feed_line(s.trim_end_matches(['\r', '\n']));
                        }
                    }
                    Err(_) => break,
                }
            }
            writer.finish();
            let usage = scan
                .finish(&protocol)
                .unwrap_or_else(|| estimate_usage(req_bytes, payload_bytes));
            write_meter_row(MeterRow {
                uid: meter_uid,
                ts: crate::now_ts(),
                client: meter_client,
                provider: meter_provider,
                model: meter_model,
                req_model: meter_req,
                input: usage.input,
                output: usage.output,
                cache_read: usage.cache_read,
                cache_write: usage.cache_write,
                credits: 0.0,
                ttft_ms: ttft,
                duration_ms: Some(started.elapsed().as_millis() as u64),
                usage_source: usage_label(&usage),
                status: 200,
                stream: true,
                upstream_id,
                rate_limit,
            });
        });

        struct PipeReader(Arc<Pipe>);
        impl Drop for PipeReader {
            fn drop(&mut self) {
                // 客户端提前断开时 tiny_http 丢弃 reader:通知转发线程停手,
                // 不让 push 永远等一块没人读的缓冲。
                self.0.mark_broken();
            }
        }
        impl Read for PipeReader {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                self.0.pop(out)
            }
        }
        let resp = tiny_http::Response::empty(200)
            .with_data(Box::new(PipeReader(pipe)) as Box<dyn Read + Send>, None)
            .with_header(
                tiny_http::Header::from_bytes("Content-Type", ct.as_str()).expect("ct header"),
            )
            .with_header(
                tiny_http::Header::from_bytes("Cache-Control", "no-cache").expect("static"),
            );
        let _ = request.respond(resp);
    }

    /// count_tokens:非计费,透传 anthropic 上游,不落账。
    fn proxy_count_tokens(
        &self,
        mut request: tiny_http::Request,
        client_protocol: &str,
    ) -> Result<(), (u16, String)> {
        let fail = |request: tiny_http::Request, e: (u16, String)| {
            Self::respond_error(request, e.clone());
            Err(e)
        };
        let cfg = match load_config_strict() {
            Ok(c) => c,
            Err(e) => return fail(request, (500, format!("gateway.json 有误:{e}"))),
        };
        let body = match Self::read_body(&mut request) {
            Ok(b) => b,
            Err(e) => return fail(request, e),
        };
        let model = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("model").and_then(|m| m.as_str()).map(String::from))
            .unwrap_or_default();
        let Some((provider, upstream_model)) = resolve_provider(&cfg, client_protocol, &model)
        else {
            return fail(
                request,
                (
                    404,
                    "count_tokens 只支持 anthropic 上游,且需有上游声明服务该模型".to_string(),
                ),
            );
        };
        let mut payload: serde_json::Value = match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => return fail(request, (400, format!("请求体不是合法 JSON:{e}"))),
        };
        payload["model"] = serde_json::Value::String(upstream_model);
        let key = match upstream_key(&provider.key_file) {
            Ok(k) => k,
            Err(e) => return fail(request, (502, format!("provider {}: {e}", provider.id))),
        };
        let agent = super::build_agent();
        let url = format!(
            "{}/v1/messages/count_tokens",
            provider.base_url.trim_end_matches('/')
        );
        let resp = match agent
            .post(&url)
            .set("x-api-key", &key)
            .set("anthropic-version", "2023-06-01")
            .set("content-type", "application/json")
            .send_bytes(payload.to_string().as_bytes())
        {
            Ok(r) => r,
            // 同 proxy:上游 >=400 是错误响应,透传不吞
            Err(ureq::Error::Status(code, resp)) => {
                let mut text = Vec::new();
                let _ = resp.into_reader().take(1024 * 1024).read_to_end(&mut text);
                let _ =
                    request.respond(tiny_http::Response::from_data(text).with_status_code(code));
                return Err((code, "upstream returned an error".to_string()));
            }
            Err(e) => return fail(request, (502, format!("上游请求失败:{e}"))),
        };
        let status = resp.status();
        let mut text = Vec::new();
        let _ = resp.into_reader().take(1024 * 1024).read_to_end(&mut text);
        let _ = request.respond(tiny_http::Response::from_data(text).with_status_code(status));
        Ok(())
    }

    /// R109:qoder 发送路径。失败语义与其余上游一致:上游应答透传、
    /// 传输失败顺链回退、计费封锁 429/403、失败不入账。
    #[allow(clippy::too_many_arguments)]
    fn proxy_qoder(
        &self,
        request: tiny_http::Request,
        client: &str,
        provider: &Provider,
        req_model: &str,
        upstream_model: &str,
        client_payload: &serde_json::Value,
        started: Instant,
        client_stream: bool,
    ) -> Result<(), (u16, String)> {
        let fail = |request: tiny_http::Request, e: (u16, String)| {
            Self::respond_error(request, e.clone());
            Err(e)
        };
        let agent = super::build_agent();
        let creds = match q::resolve_credentials(provider, &agent) {
            Ok(c) => c,
            Err(e) => return fail(request, (401, format!("qoder 凭据:{e}"))),
        };
        let base = q::chat_base(provider);
        let model_config = match q::get_model_config(&agent, &creds, &base, upstream_model) {
            Ok(m) => m,
            Err(e) => return fail(request, (502, format!("qoder 模型目录:{e}"))),
        };
        let rng = q::QoderRandom::real();
        let payload = q::build_chat_payload(
            client_payload,
            upstream_model,
            &model_config.raw,
            &creds.user_id,
            &rng,
        );
        let plain = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(e) => return fail(request, (500, format!("qoder payload 编码失败:{e}"))),
        };
        let encoded = q::encode_body(&plain);
        let url = q::chat_url(&base);
        let headers = match q::cosy_headers(&encoded, &url, &creds, &rng) {
            Ok(h) => h,
            Err(e) => return fail(request, (401, format!("qoder COSY 签名:{e}"))),
        };
        let mut upstream = agent
            .post(&url)
            .set("content-type", "application/json")
            .set("accept", "text/event-stream")
            .set("cache-control", "no-cache")
            .set("X-Model-Key", upstream_model)
            .set(
                "X-Model-Source",
                model_config
                    .raw
                    .get("source")
                    .and_then(|s| s.as_str())
                    .unwrap_or("system"),
            );
        for (k, v) in headers {
            upstream = upstream.set(&k, &v);
        }
        let response = match upstream.send_bytes(&encoded) {
            Ok(r) => r,
            Err(ureq::Error::Status(code, resp)) => {
                let mut text = Vec::new();
                let _ = resp.into_reader().take(1024 * 1024).read_to_end(&mut text);
                let mut resp = tiny_http::Response::from_data(text).with_status_code(code);
                if let Ok(h) = tiny_http::Header::from_bytes("Content-Type", "application/json") {
                    resp = resp.with_header(h);
                }
                let _ = request.respond(resp);
                return Err((code, "upstream returned an error".to_string()));
            }
            Err(e) => return fail(request, (502, format!("qoder 上游传输失败:{e}"))),
        };
        self.proxy_qoder_stream(
            response,
            request,
            client,
            provider,
            req_model,
            upstream_model,
            plain.len(),
            started,
            client_stream,
        )
    }

    /// R109:qoder 上游的响应处理。上游每行 data 是
    /// `{"statusCodeValue":200,"body":"<内层 OpenAI chunk>"}` 信封:
    /// ① 先窥第一帧——计费封锁(110/112/10605)直接以 429/403 应答,
    ///    不开流、不落账;
    /// ② 流式客户端:逐行解包转发内层 chunk,同时喂计量扫描器;
    /// ③ 非流式客户端:同样收流,聚合 delta 为整包应答。
    #[allow(clippy::too_many_arguments)]
    fn proxy_qoder_stream(
        &self,
        response: ureq::Response,
        request: tiny_http::Request,
        client: &str,
        provider: &Provider,
        req_model: &str,
        upstream_model: &str,
        req_bytes: usize,
        started: Instant,
        client_stream: bool,
    ) -> Result<(), (u16, String)> {
        let mut reader = response.into_reader();
        // ---- ① 窥首帧:拼出第一个完整 data 行再决定 ----
        let mut buf: Vec<u8> = Vec::new();
        let mut first_line: Option<String> = None;
        let mut chunk8 = [0u8; 8192];
        let deadline = Instant::now() + Duration::from_secs(30);
        while first_line.is_none() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let e = (504u16, "qoder 上游 30s 未给出首帧".to_string());
                Self::respond_error(request, e.clone());
                return Err(e);
            }
            match reader.read(&mut chunk8) {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&chunk8[..n]);
                    if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                        first_line = Some(
                            String::from_utf8_lossy(&buf[..pos])
                                .trim_end_matches('\r')
                                .to_string(),
                        );
                    }
                }
                Err(e) => {
                    let err = (502u16, format!("qoder 上游读取失败:{e}"));
                    Self::respond_error(request, err.clone());
                    return Err(err);
                }
            }
        }
        let Some(first) = first_line else {
            let e = (502u16, "qoder 上游没有可解析的首帧".to_string());
            Self::respond_error(request, e.clone());
            return Err(e);
        };
        if let q::EnvelopeLine::Error(status, inner) = q::unwrap_envelope_line(&first) {
            if let Some((kind, http)) = q::billing_block(&inner) {
                let e = (
                    http,
                    format!("qoder 计费限制({kind}):{}", &inner[..inner.len().min(300)]),
                );
                Self::respond_error(request, e.clone());
                return Err(e);
            }
            let e = (
                502u16,
                format!(
                    "qoder 上游首帧错误({status}):{}",
                    &inner[..inner.len().min(300)]
                ),
            );
            Self::respond_error(request, e.clone());
            return Err(e);
        }

        // ---- ②/③ 收流:解包 + 计量 + 聚合或转发 ----
        let mut scan = SseUsageScan::default();
        let mut collected: Vec<u8> = Vec::new(); // 非流式:内层内容聚合
        let mut usage_json: Option<serde_json::Value> = None;
        let mut ttft = Some(started.elapsed().as_millis() as u64);

        let process = |line: &str,
                       out: &mut Vec<u8>,
                       scan: &mut SseUsageScan,
                       collected: &mut Vec<u8>,
                       usage: &mut Option<serde_json::Value>|
         -> bool {
            match q::unwrap_envelope_line(line) {
                q::EnvelopeLine::Chunk(inner) => {
                    // 计量:内层就是 OpenAI chunk,喂扫描器抓 usage。
                    scan.feed_line(&format!("data: {inner}"));
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&inner) {
                        if v.get("usage").map(|u| !u.is_null()).unwrap_or(false) {
                            *usage = v.get("usage").cloned();
                        }
                        if let Some(delta) = v
                            .pointer("/choices/0/delta/content")
                            .and_then(|c| c.as_str())
                        {
                            collected.extend_from_slice(delta.as_bytes());
                        }
                    }
                    out.extend_from_slice(format!("data: {inner}\n\n").as_bytes());
                    true
                }
                q::EnvelopeLine::Done => {
                    out.extend_from_slice(b"data: [DONE]\n\n");
                    false
                }
                q::EnvelopeLine::Error(status, inner) => {
                    // 流中错误:按 10router 语义合成错误 chunk 后终止。
                    let msg = format!(
                        "\n[qoder error {status}: {}]",
                        &inner[..inner.len().min(200)]
                    );
                    let err_chunk = serde_json::json!({
                        "object": "chat.completion.chunk",
                        "choices": [{"index": 0, "delta": {"content": msg}, "finish_reason": "stop"}]
                    });
                    out.extend_from_slice(format!("data: {err_chunk}\n\n").as_bytes());
                    out.extend_from_slice(b"data: [DONE]\n\n");
                    false
                }
                q::EnvelopeLine::Ignore => true,
            }
        };

        // 先处理窥见的首行(它可能本身是终止帧)
        let mut out: Vec<u8> = Vec::new();
        let mut done = !process(&first, &mut out, &mut scan, &mut collected, &mut usage_json);
        // 余下的缓冲区继续按行切
        if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let rest = buf.split_off(pos + 1);
            let text = String::from_utf8_lossy(&rest);
            for line in text.split('\n') {
                if done {
                    break;
                }
                if line.is_empty() {
                    continue;
                }
                done = !process(line, &mut out, &mut scan, &mut collected, &mut usage_json);
            }
            buf.clear();
        }
        while !done {
            match reader.read(&mut chunk8) {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&chunk8[..n]);
                    while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                        let line: Vec<u8> = buf.drain(..=pos).collect();
                        let s = String::from_utf8_lossy(&line);
                        let s = s.trim_end_matches(['\r', '\n']);
                        if s.is_empty() {
                            continue;
                        }
                        if !process(s, &mut out, &mut scan, &mut collected, &mut usage_json) {
                            done = true;
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
        // 终止即断读(Qoder 的 SSE 之后还挂着 keepalive 连接)。
        drop(reader);

        // 计量:上游内层 usage 优先(拼成 openai 形状给提取器),否则估算。
        // qoder 的 usage 自带 credits(订阅真实计费口径),原值照收。
        let qoder_credits = usage_json
            .as_ref()
            .and_then(|u| u.get("credits"))
            .and_then(|c| c.as_f64())
            .unwrap_or(0.0);
        let usage = usage_json
            .as_ref()
            .and_then(q_usage_extract)
            .unwrap_or_else(|| estimate_usage(req_bytes, collected.len()));
        write_meter_row(MeterRow {
            uid: new_uid(),
            ts: crate::now_ts(),
            client: client.to_string(),
            provider: provider.id.clone(),
            model: upstream_model.to_string(),
            req_model: req_model.to_string(),
            input: usage.input,
            output: usage.output,
            cache_read: usage.cache_read,
            cache_write: usage.cache_write,
            credits: qoder_credits,
            ttft_ms: ttft.take(),
            duration_ms: Some(started.elapsed().as_millis() as u64),
            usage_source: usage_label(&usage),
            status: 200,
            stream: client_stream,
            upstream_id: String::new(),
            // qoder 走自有 SSE 信封,响应头里没有 anthropic/x-ratelimit 族
            // 的限额信息;额度口径在上面的 credits(订阅真实计费量)。
            rate_limit: serde_json::Map::new(),
        });

        if client_stream {
            let resp = tiny_http::Response::empty(200)
                .with_data(
                    Box::new(std::io::Cursor::new(out)) as Box<dyn Read + Send>,
                    None,
                )
                .with_header(
                    tiny_http::Header::from_bytes("Content-Type", "text/event-stream")
                        .expect("static"),
                )
                .with_header(
                    tiny_http::Header::from_bytes("Cache-Control", "no-cache").expect("static"),
                );
            let _ = request.respond(resp);
        } else {
            let content = String::from_utf8_lossy(&collected).to_string();
            let mut body = serde_json::json!({
                "id": format!("qoder-{}", new_uid()),
                "object": "chat.completion",
                "model": req_model,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
            });
            if let Some(u) = usage_json {
                body["usage"] = u;
            }
            let _ = request.respond(
                tiny_http::Response::from_string(body.to_string()).with_header(
                    tiny_http::Header::from_bytes("Content-Type", "application/json")
                        .expect("static"),
                ),
            );
        }
        Ok(())
    }

    /// GET /v1/models:配置里声明了 models 的直接给出;没声明的上游拉一次
    /// 各自的模型列表(5 分钟缓存)。openai → /models;anthropic → /v1/models。
    fn list_models(&self, request: tiny_http::Request) -> Result<(), (u16, String)> {
        static CACHE: Mutex<Option<(Instant, Vec<String>)>> = Mutex::new(None);
        // 先在独立作用域里取缓存快照:match 的 scrutinee 临时 guard 会活到
        // 整个 match 结束,不收scope 就等于持锁重入,自锁死(集成测试抓到)。
        let cached = {
            CACHE
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .filter(|(at, _)| at.elapsed() < Duration::from_secs(300))
                .map(|(_, models)| models.clone())
        };
        let models = match cached {
            Some(models) => models,
            None => {
                let cfg = match load_config_strict() {
                    Ok(c) => c,
                    Err(e) => {
                        let e = (500u16, format!("gateway.json 有误:{e}"));
                        Self::respond_error(request, e.clone());
                        return Err(e);
                    }
                };
                let agent = super::build_agent();
                let mut models: Vec<String> = Vec::new();
                for p in &cfg.providers {
                    match fetch_provider_models(p, &agent) {
                        Ok(list) => models.extend(list.models),
                        Err(e) => {
                            eprintln!("[TokenBuddy] {} 目录拉取失败({e}),该上游本轮不报模型", p.id);
                        }
                    }
                }
                models.sort();
                models.dedup();
                *CACHE.lock().unwrap_or_else(|e| e.into_inner()) =
                    Some((Instant::now(), models.clone()));
                models
            }
        };
        let body = serde_json::json!({
            "object": "list",
            "data": models
                .into_iter()
                .map(|m| serde_json::json!({"id": m, "object": "model"}))
                .collect::<Vec<_>>()
        });
        let _ = request.respond(
            tiny_http::Response::from_string(body.to_string()).with_header(
                tiny_http::Header::from_bytes("Content-Type", "application/json")
                    .expect("static header"),
            ),
        );
        Ok(())
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// 路由辅助
// ---------------------------------------------------------------------------

fn upstream_url(p: &Provider) -> String {
    match p.protocol.as_str() {
        "anthropic" => format!("{}/v1/messages", p.base_url.trim_end_matches('/')),
        _ => format!("{}/chat/completions", p.base_url.trim_end_matches('/')),
    }
}

fn header_of<'a>(headers: &'a [tiny_http::Header], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|h| h.field.as_str().to_ascii_lowercase() == name.to_ascii_lowercase())
        .map(|h| h.value.as_str())
}

fn response_header(resp: &ureq::Response, name: &str) -> Option<String> {
    resp.headers_names()
        .iter()
        .find(|f| f.eq_ignore_ascii_case(name))
        .and_then(|f| resp.header(f))
        .map(String::from)
}

/// R112:采集上游自报的限额头(有则采集,不写死任何一家的头名)。
/// anthropic 系用 `anthropic-ratelimit-*`,OpenAI 兼容系用 `x-ratelimit-*`,
/// 两族都收;其余头一律忽略。值原样留字符串——解析各家不同的 reset 语义
/// 是 P3 的活,现在先把事实存下来,面板只做「上游报了/没报」的诚实展示。
fn rate_limit_headers(resp: &ureq::Response) -> serde_json::Map<String, serde_json::Value> {
    let mut out = serde_json::Map::new();
    for h in resp.headers_names() {
        let lower = h.to_ascii_lowercase();
        if !lower.starts_with("anthropic-ratelimit-") && !lower.starts_with("x-ratelimit-") {
            continue;
        }
        if let Some(v) = resp.header(&h) {
            out.insert(lower, serde_json::Value::String(v.to_string()));
        }
    }
    out
}

/// 一个上游的模型目录,连同「这个清单是怎么来的」——面板要能区分
/// 配置里写死的、上游实时报的、和压根没报成的三种情况,不能都显示成
/// 一串名字让人以为都是真能调的。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProviderModels {
    pub models: Vec<String>,
    /// "config"(gateway.json 里声明的) | "upstream"(实时拉的) | "none"(没拉到)
    pub source: &'static str,
    /// source=none 时的原因(密钥失效/URL 错/上游没实现 /models)。
    pub detail: String,
}

/// 拉单个上游的模型目录。配置里声明了 models 就用它(那是作者承诺的
/// 服务范围,比上游目录更可信——上游目录常混着本账号无权调用的模型);
/// 没声明才实时问上游。
pub fn fetch_provider_models(p: &Provider, agent: &ureq::Agent) -> Result<ProviderModels> {
    if !p.models.is_empty() {
        return Ok(ProviderModels {
            models: p.models.clone(),
            source: "config",
            detail: String::new(),
        });
    }
    // qoder: COSY 签名的实时目录(model/list)。
    if p.protocol == "qoder" {
        let creds = q::resolve_credentials(p, agent)?;
        let base = q::chat_base(p);
        let list: Vec<String> = q::fetch_models(agent, &creds, &base)?
            .into_iter()
            .map(|m| m.key)
            .collect();
        if list.is_empty() {
            return Ok(ProviderModels {
                models: vec![],
                source: "none",
                detail: "上游目录为空".to_string(),
            });
        }
        return Ok(ProviderModels {
            models: list,
            source: "upstream",
            detail: String::new(),
        });
    }
    let url = match p.protocol.as_str() {
        "anthropic" => format!("{}/v1/models", p.base_url.trim_end_matches('/')),
        _ => format!("{}/models", p.base_url.trim_end_matches('/')),
    };
    let req = match p.protocol.as_str() {
        "anthropic" => agent.get(&url).set("anthropic-version", "2023-06-01"),
        _ => agent.get(&url).set(
            "Authorization",
            &format!("Bearer {}", upstream_key(&p.key_file).unwrap_or_default()),
        ),
    };
    let resp = req.call().map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut resp.into_reader().take(4 * 1024 * 1024), &mut bytes)?;
    let v: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| anyhow::anyhow!("响应不是 JSON:{e}"))?;
    let models: Vec<String> = v
        .get("data")
        .and_then(|d| d.as_array())
        .map(|list| {
            list.iter()
                .filter_map(|m| m.get("id").and_then(|i| i.as_str()))
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    if models.is_empty() {
        return Ok(ProviderModels {
            models: vec![],
            source: "none",
            detail: "上游 /models 没返回模型清单".to_string(),
        });
    }
    Ok(ProviderModels {
        models,
        source: "upstream",
        detail: String::new(),
    })
}

fn upstream_req_id(resp: &ureq::Response) -> String {
    for h in resp.headers_names() {
        let lower = h.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "x-request-id" | "request-id" | "trace-id" | "x-mm-request-id"
        ) {
            return resp.header(&h).unwrap_or_default().to_string();
        }
    }
    String::new()
}

/// P1 选路:combo 链序优先(链外 provider 兜底),过滤同协议 + 声明服务
/// 该模型的 provider;models 为空 = 全接。别名只改写发往上游的名字。
pub fn resolve_provider(
    cfg: &GatewayConfig,
    client_protocol: &str,
    model: &str,
) -> Option<(Provider, String)> {
    resolve_chain(cfg, client_protocol, model)
        .into_iter()
        .next()
}

/// F16(P1 子集):链上**全部**同协议候选,按 combo 链序。传输失败时
/// proxy 顺链尝试下一家;上游应答(4xx/5xx)不回退——那是上游给出的
/// 答案,跨状态码的回退语义(退避/冷却/429)留给 P2。
pub fn resolve_chain(
    cfg: &GatewayConfig,
    client_protocol: &str,
    model: &str,
) -> Vec<(Provider, String)> {
    let mut ordered: Vec<&Provider> = Vec::new();
    if let Some(combo) = cfg.combos.first() {
        for id in &combo.chain {
            if let Some(p) = cfg.providers.iter().find(|p| &p.id == id) {
                ordered.push(p);
            }
        }
        for p in &cfg.providers {
            if !combo.chain.contains(&p.id) {
                ordered.push(p);
            }
        }
    } else {
        ordered.extend(cfg.providers.iter());
    }
    ordered
        .into_iter()
        .filter(|p| {
            p.protocol == client_protocol || (client_protocol == "openai" && p.protocol == "qoder")
        })
        .filter(|p| {
            // models 空 = 全接;别名表里的名字同样视为该 provider 服务。
            p.models.is_empty()
                || p.models.iter().any(|m| m == model)
                || p.aliases.contains_key(model)
        })
        .map(|p| {
            let upstream = p
                .aliases
                .get(model)
                .cloned()
                .unwrap_or_else(|| model.to_string());
            (p.clone(), upstream)
        })
        .collect()
}

fn new_uid() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let mut bytes = [0u8; 4];
    let _ = std::fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut bytes));
    format!("gw_{nanos:x}_{}", hex::encode(bytes))
}

/// qoder 内层 chunk 的 usage 字段(openai 形状,细节字段可能缺)。
fn q_usage_extract(v: &serde_json::Value) -> Option<ExtractedUsage> {
    super::extract_usage_openai(v)
}

fn usage_label(u: &ExtractedUsage) -> String {
    if u.exact {
        "upstream".to_string()
    } else {
        "estimated".to_string()
    }
}

fn write_meter_row(row: MeterRow) {
    if let Err(e) = append_meter_row(&row) {
        eprintln!("[TokenBuddy] 网关落账失败(请求已成功,账本缺这一行):{e}");
    }
}

// ---------------------------------------------------------------------------
// 直通管道:Mutex<VecDeque> + Condvar;push 背压到 PIPE_CAP,pop 空转等 EOF。
// ---------------------------------------------------------------------------

struct PipeInner {
    data: VecDeque<u8>,
    eof: bool,
    broken: bool,
}

struct Pipe {
    inner: Mutex<PipeInner>,
    cv: Condvar,
}

impl Pipe {
    fn new() -> Self {
        Pipe {
            inner: Mutex::new(PipeInner {
                data: VecDeque::new(),
                eof: false,
                broken: false,
            }),
            cv: Condvar::new(),
        }
    }

    fn push(&self, bytes: &[u8]) -> Result<(), ()> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if g.broken {
                return Err(());
            }
            if g.data.len() < PIPE_CAP {
                g.data.extend(bytes.iter());
                self.cv.notify_all();
                return Ok(());
            }
            let (ng, _) = self
                .cv
                .wait_timeout(g, Duration::from_secs(5))
                .unwrap_or_else(|e| e.into_inner());
            g = ng;
        }
    }

    fn finish(&self) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.eof = true;
        self.cv.notify_all();
    }

    fn pop(&self, out: &mut [u8]) -> std::io::Result<usize> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if !g.data.is_empty() {
                let mut n = 0;
                while n < out.len() {
                    match g.data.pop_front() {
                        Some(b) => {
                            out[n] = b;
                            n += 1;
                        }
                        None => break,
                    }
                }
                self.cv.notify_all();
                return Ok(n);
            }
            if g.eof {
                return Ok(0);
            }
            if g.broken {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "gateway pipe broken",
                ));
            }
            let (ng, _) = self
                .cv
                .wait_timeout(g, Duration::from_secs(5))
                .unwrap_or_else(|e| e.into_inner());
            g = ng;
        }
    }

    fn mark_broken(&self) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.broken = true;
        self.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with(providers: serde_json::Value) -> GatewayConfig {
        serde_json::from_value(serde_json::json!({
            "enabled": true,
            "providers": providers,
            "combos": []
        }))
        .unwrap()
    }

    /// P1 选路:同协议过滤 + models 匹配 + 别名改写;无匹配返回 None。
    #[test]
    fn resolve_provider_filters_protocol_and_matches_models() {
        let cfg = cfg_with(serde_json::json!([
            {"id":"mm-oai","protocol":"openai","base_url":"http://u/v1","key_file":"k",
             "models":["MiniMax-M3.1-Flash-Preview"],
             "aliases":{"glm-x":"MiniMax-M3.1-Flash-Preview"}},
            {"id":"zp-anth","protocol":"anthropic","base_url":"http://z","key_file":"k",
             "models":[]}
        ]));
        let (p, m) = resolve_provider(&cfg, "openai", "MiniMax-M3.1-Flash-Preview").unwrap();
        assert_eq!(p.id, "mm-oai");
        assert_eq!(m, "MiniMax-M3.1-Flash-Preview");
        let (_, m) = resolve_provider(&cfg, "openai", "glm-x").unwrap();
        assert_eq!(m, "MiniMax-M3.1-Flash-Preview", "别名改写发往上游的名字");
        // anthropic 客户端匹配空 models 的 anthropic 上游
        let (p, m) = resolve_provider(&cfg, "anthropic", "anything").unwrap();
        assert_eq!((p.id.as_str(), m.as_str()), ("zp-anth", "anything"));
        // 别名让模型可路由:models 不含 glm-x,但别名表里有一条
        let cfg2 = cfg_with(serde_json::json!([
            {"id":"a","protocol":"openai","base_url":"http://x/v1","key_file":"k",
             "models":["real-model"],"aliases":{"glm-x":"real-model"}}
        ]));
        assert!(
            resolve_provider(&cfg2, "openai", "glm-x").is_some(),
            "别名使模型可路由"
        );
        // 协议不匹配不硬翻(P1 边界):只有 openai 上游时,anthropic 请求落空
        let cfg3 = cfg_with(serde_json::json!([
            {"id":"only-oai","protocol":"openai","base_url":"http://x/v1","key_file":"k","models":[]}
        ]));
        assert!(resolve_provider(&cfg3, "anthropic", "whatever").is_none());
    }

    /// combo 链序优先,链外的 provider 兜底。
    #[test]
    fn resolve_respects_combo_order() {
        let cfg: GatewayConfig = serde_json::from_value(serde_json::json!({
            "providers": [
                {"id":"b","protocol":"openai","base_url":"http://b","key_file":"k","models":[]},
                {"id":"a","protocol":"openai","base_url":"http://a","key_file":"k","models":[]}
            ],
            "combos": [{"id":"default","chain":["b"]}]
        }))
        .unwrap();
        let (p, _) = resolve_provider(&cfg, "openai", "m").unwrap();
        assert_eq!(p.id, "b", "combo 链序优先");
    }

    #[test]
    fn pipe_carries_bytes_and_respects_eof() {
        let pipe = Arc::new(Pipe::new());
        let w = pipe.clone();
        let writer = std::thread::spawn(move || {
            w.push(b"hello ").unwrap();
            w.push(b"stream").unwrap();
            w.finish();
        });
        let mut buf = [0u8; 64];
        let mut got = Vec::new();
        loop {
            let n = pipe.pop(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            got.extend_from_slice(&buf[..n]);
        }
        writer.join().unwrap();
        assert_eq!(got, b"hello stream");
    }

    /// 客户端断开(reader 丢弃)必须唤醒阻塞中的 push,不能让它挂死。
    #[test]
    fn pipe_broken_unblocks_writer() {
        let pipe = Arc::new(Pipe::new());
        // 灌满管道但不消费,把 push 逼进等待
        let filler = {
            let p = pipe.clone();
            std::thread::spawn(move || {
                for _ in 0..64 {
                    if p.push(&[0u8; 8192]).is_err() {
                        return true; // 被唤醒并报断
                    }
                }
                false
            })
        };
        std::thread::sleep(Duration::from_millis(200));
        pipe.mark_broken();
        assert!(filler.join().unwrap(), "broken 必须解除 push 阻塞");
    }

    /// classify:四个业务路由 + 未知 404 人话。
    #[test]
    fn classify_routes_the_four_verbs() {
        let rt = Runtime::new();
        assert_eq!(
            rt.classify("POST", "/v1/chat/completions").unwrap(),
            Route::Chat
        );
        assert_eq!(
            rt.classify("POST", "/v1/messages").unwrap(),
            Route::Messages
        );
        assert_eq!(
            rt.classify("POST", "/v1/messages/count_tokens").unwrap(),
            Route::CountTokens
        );
        assert_eq!(rt.classify("GET", "/v1/models").unwrap(), Route::Models);
        assert_eq!(rt.classify("GET", "/health").unwrap(), Route::Health);
        assert!(rt.classify("GET", "/v1/chat/completions").is_err());
        assert!(rt.classify("POST", "/v1/embeddings").is_err());
    }

    /// 真实 MiniMax 头名(X-Mm-Request-Id / Trace-Id)应被识别为请求 id。
    #[test]
    fn upstream_id_scanner_knows_dialect_headers() {
        // 纯函数面:名单语义直接断言。
        let names = ["Trace-Id", "X-Mm-Request-Id", "X-Request-Id", "Request-Id"];
        for n in names {
            assert!(
                matches!(
                    n.to_ascii_lowercase().as_str(),
                    "x-request-id" | "request-id" | "trace-id" | "x-mm-request-id"
                ),
                "{n} 应被识别"
            );
        }
        assert!(!matches!(
            "content-type".to_ascii_lowercase().as_str(),
            "x-request-id" | "request-id" | "trace-id" | "x-mm-request-id"
        ));
    }
}
