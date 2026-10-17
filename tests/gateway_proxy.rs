//! 网关端到端(R108)——进程内 mock 上游 → 真网关(tiny_http 监听)→
//! 真 HTTP 客户端(ureq),断言:直通字节、方言注入、计量落账、鉴权、
//! 路由诚实报错、停机排空。
//!
//! mock 上游是本文件自己起的 tiny_http 服务:openai 形状(SSE 三段,
//! usage 在末 chunk,2026-10-04 MiniMax 实测同款)与 anthropic 形状
//! (message_start×message_delta)各一,拒绝没有 Bearer 的上游调用。

#![cfg(feature = "gateway")] // --no-default-features 构建没有网关,整个文件退出编译
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokenbuddy::gateway::server::Runtime;
use tokenbuddy::gateway::{self, MeterRow};

const UPSTREAM_KEY: &str = "sk-mock-upstream";

/// openai 形状 mock:/v1/chat/completions。stream=true 回 SSE(内容两段 +
/// usage 末 chunk + [DONE]),stream=false 回整包 JSON;两类都校验上游
/// Bearer,并把「是否收到 include_usage 注入」记进原子布尔回给测试。
fn spawn_mock_openai(saw_include_usage: Arc<AtomicBool>) -> String {
    let server = Arc::new(tiny_http::Server::http("127.0.0.1:0").unwrap());
    let addr = match server.server_addr() {
        tiny_http::ListenAddr::IP(a) => a.to_string(),
        _ => panic!("ip listener"),
    };
    let handle = std::thread::spawn(move || {
        for mut request in server.incoming_requests() {
            let mut body = String::new();
            let _ = request.as_reader().read_to_string(&mut body);
            let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            let auth_ok = request.headers().iter().any(|h| {
                h.field.as_str().to_ascii_lowercase() == "authorization"
                    && h.value.as_str() == format!("Bearer {UPSTREAM_KEY}")
            });
            if !auth_ok {
                let _ = request.respond(
                    tiny_http::Response::from_string(r#"{"error":"bad key"}"#)
                        .with_status_code(401),
                );
                continue;
            }
            // R112:模型目录来源测试要的上游 /v1/models(OpenAI 形状)。
            if request.url().ends_with("/models") {
                let _ = request.respond(tiny_http::Response::from_string(
                    serde_json::json!({"object": "list", "data": [
                        {"id": "mock-model-1", "object": "model"},
                        {"id": "mock-model-2", "object": "model"}
                    ]})
                    .to_string(),
                ));
                continue;
            }
            let stream = v.get("stream").and_then(|s| s.as_bool()).unwrap_or(false);
            if v.pointer("/stream_options/include_usage") == Some(&serde_json::Value::Bool(true)) {
                saw_include_usage.store(true, Ordering::SeqCst);
            }
            if stream {
                let sse = concat!(
                    "data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"收到\"}}]}\n\n",
                    "data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{}}]}\n\n",
                    "data: {\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"total_tokens\":210,\"prompt_tokens\":208,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":197}}}\n\n",
                    "data: [DONE]\n\n",
                );
                let _ = request.respond(tiny_http::Response::from_string(sse).with_header(
                    tiny_http::Header::from_bytes("Content-Type", "text/event-stream").unwrap(),
                ));
            } else {
                let body = serde_json::json!({
                    "object": "chat.completion",
                    "choices": [{"message": {"role": "assistant", "content": "收到"}}],
                    "usage": {"prompt_tokens": 150, "completion_tokens": 5,
                               "prompt_tokens_details": {"cached_tokens": 100}}
                });
                let _ = request.respond(
                    tiny_http::Response::from_string(body.to_string())
                        .with_header(
                            tiny_http::Header::from_bytes("Content-Type", "application/json")
                                .unwrap(),
                        )
                        // R112: 上游自报限额头,网关要采集下来给面板画水位。
                        .with_header(
                            tiny_http::Header::from_bytes("x-ratelimit-remaining-requests", "42")
                                .unwrap(),
                        )
                        .with_header(
                            tiny_http::Header::from_bytes("x-ratelimit-limit-requests", "100")
                                .unwrap(),
                        ),
                );
            }
        }
    });
    let _ = handle; // 分离:incoming_requests 永不退出,join 会挂死;测试进程退出时统一回收
    addr
}

/// anthropic 形状 mock:/v1/messages。SSE:message_start(入侧 usage)+ 内容 +
/// message_delta(累计出侧)+ message_stop。
fn spawn_mock_anthropic() -> String {
    let server = Arc::new(tiny_http::Server::http("127.0.0.1:0").unwrap());
    let addr = match server.server_addr() {
        tiny_http::ListenAddr::IP(a) => a.to_string(),
        _ => panic!("ip listener"),
    };
    let handle = std::thread::spawn(move || {
        for mut request in server.incoming_requests() {
            let mut body = String::new();
            let _ = request.as_reader().read_to_string(&mut body);
            let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            let auth_ok = request.headers().iter().any(|h| {
                h.field.as_str().to_ascii_lowercase() == "x-api-key"
                    && h.value.as_str() == UPSTREAM_KEY
            });
            if !auth_ok {
                let _ = request.respond(
                    tiny_http::Response::from_string(
                        r#"{"type":"error","error":{"type":"authentication_error"}}"#,
                    )
                    .with_status_code(401),
                );
                continue;
            }
            if v.get("stream").and_then(|s| s.as_bool()).unwrap_or(false) {
                let sse = concat!(
                    "event: message_start\n",
                    "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":120,\"output_tokens\":1,\"cache_read_input_tokens\":30}}}\n\n",
                    "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"好\"}}\n\n",
                    "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":37}}\n\n",
                    "data: {\"type\":\"message_stop\"}\n\n",
                );
                let _ = request.respond(tiny_http::Response::from_string(sse).with_header(
                    tiny_http::Header::from_bytes("Content-Type", "text/event-stream").unwrap(),
                ));
            } else {
                let body = serde_json::json!({
                    "content": [{"type": "text", "text": "好"}],
                    "usage": {"input_tokens": 90, "output_tokens": 8,
                               "cache_read_input_tokens": 20, "cache_creation_input_tokens": 4}
                });
                let _ = request.respond(
                    tiny_http::Response::from_string(body.to_string()).with_header(
                        tiny_http::Header::from_bytes("Content-Type", "application/json").unwrap(),
                    ),
                );
            }
        }
    });
    let _ = handle; // 分离:incoming_requests 永不退出,join 会挂死;测试进程退出时统一回收
    addr
}

/// 每个测试独立的 TOKENBUDDY_HOME + 配置 + 密钥;返回 (runtime, 网关地址)。
fn spawn_gateway(
    providers: serde_json::Value,
    combos: serde_json::Value,
) -> (Arc<Runtime>, String) {
    std::fs::create_dir_all(
        gateway::usage_jsonl_path()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("keys"),
    )
    .unwrap();
    std::fs::write(tokenbuddy::data_dir().join("keys/k.key"), UPSTREAM_KEY).unwrap();
    let cfg = serde_json::json!({
        "enabled": true,
        "listen": "127.0.0.1:0",
        "providers": providers,
        "combos": combos,
    });
    std::fs::write(
        gateway::config_path(),
        serde_json::to_string_pretty(&cfg).unwrap(),
    )
    .unwrap();
    let rt = Arc::new(Runtime::new());
    rt.start().unwrap();
    let addr = rt.actual_addr().expect("started runtime has an addr");
    (rt, addr)
}

fn read_meter_rows() -> Vec<MeterRow> {
    let path = gateway::usage_jsonl_path();
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        // 与 meter 线程并发时按行容错(采集器 parse_row 同口径)
        .filter_map(|l| serde_json::from_str::<MeterRow>(l).ok())
        .collect()
}

/// openai 流式端到端:直通含 SSE 内容与 [DONE];include_usage 注入到达
/// 上游;末 chunk usage 计入账本(cache 明细进 cache_read 列)。
#[test]
fn openai_stream_end_to_end() {
    let _guard = tokenbuddy::TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let dir = tokenbuddy::unique_test_dir("gw-e2e-oai");
    std::env::set_var("TOKENBUDDY_HOME", &dir);
    let saw = Arc::new(AtomicBool::new(false));
    let up_addr = spawn_mock_openai(Arc::clone(&saw));
    let key = gateway::add_client("claude").unwrap().key;
    let (rt, gw_addr) = spawn_gateway(
        serde_json::json!([{
            "id": "mock-oai", "protocol": "openai",
            "base_url": format!("http://{up_addr}/v1"),
            "key_file": "keys/k.key",
            "models": ["MiniMax-M3.1-Flash-Preview"],
            "dialects": ["include_usage", "cached_tokens_in_details"]
        }]),
        serde_json::json!([]),
    );

    let resp = ureq::post(&format!("http://{gw_addr}/v1/chat/completions"))
        .set("Authorization", &format!("Bearer {key}"))
        .set("content-type", "application/json")
        .send_bytes(
            serde_json::json!({
                "model": "MiniMax-M3.1-Flash-Preview",
                "stream": true,
                "messages": [{"role": "user", "content": "只回复:收到"}]
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp.header("content-type").unwrap_or_default().to_string();
    let mut text = String::new();
    resp.into_reader().read_to_string(&mut text).unwrap();
    assert!(text.contains("收到"), "直通字节: {text}");
    assert!(text.contains("[DONE]"));
    assert!(
        content_type.starts_with("text/event-stream"),
        "{content_type}"
    );
    assert!(saw.load(Ordering::SeqCst), "include_usage 必须被注入到上游");

    // 转发线程落账是异步的:轮询至多 2s。
    let mut rows = Vec::new();
    for _ in 0..40 {
        rows = read_meter_rows();
        if !rows.is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert_eq!(rows.len(), 1, "{rows:?}");
    let row = &rows[0];
    assert_eq!(row.client, "claude");
    assert_eq!(row.provider, "mock-oai");
    assert_eq!(row.model, "MiniMax-M3.1-Flash-Preview");
    assert_eq!((row.input, row.output, row.cache_read), (208, 2, 197));
    assert_eq!(row.usage_source, "upstream");
    assert!(row.stream);
    assert!(row.ttft_ms.is_some(), "流式要有 TTFT");
    assert!(
        row.duration_ms.is_some(),
        "时长要有(本地 mock 可能 0ms,是合法事实)"
    );
    assert!(row.uid.starts_with("gw_"));

    // 账本采集器读同一批行(第 18 源闭环)。
    gateway::release_collector_cache();
    let records = gateway::collect_records().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].source.as_str(), "gateway");
    assert_eq!(records[0].session_id.as_deref(), Some("claude"));
    assert_eq!(records[0].record_id.as_deref(), Some(row.uid.as_str()));

    rt.stop();
    std::env::remove_var("TOKENBUDDY_HOME");
    let _ = std::fs::remove_dir_all(&dir);
}

/// openai 非流式:整包透传 + usage 直报。
#[test]
fn openai_non_stream_end_to_end() {
    let _guard = tokenbuddy::TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let dir = tokenbuddy::unique_test_dir("gw-e2e-oai-ns");
    std::env::set_var("TOKENBUDDY_HOME", &dir);
    let up_addr = spawn_mock_openai(Arc::new(AtomicBool::new(false)));
    let key = gateway::add_client("zcode").unwrap().key;
    let (rt, gw_addr) = spawn_gateway(
        serde_json::json!([{
            "id": "mock-oai", "protocol": "openai",
            "base_url": format!("http://{up_addr}/v1"),
            "key_file": "keys/k.key", "models": []
        }]),
        serde_json::json!([]),
    );
    let resp = ureq::post(&format!("http://{gw_addr}/v1/chat/completions"))
        .set("Authorization", &format!("Bearer {key}"))
        .set("content-type", "application/json")
        .send_bytes(
            serde_json::json!({
                "model": "any-model", "stream": false,
                "messages": [{"role": "user", "content": "hi"}]
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = serde_json::from_reader(resp.into_reader()).unwrap();
    assert_eq!(body["choices"][0]["message"]["content"], "收到");
    for _ in 0..40 {
        if !read_meter_rows().is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let rows = read_meter_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0].input, rows[0].output, rows[0].cache_read),
        (150, 5, 100)
    );
    assert!(!rows[0].stream);
    rt.stop();
    std::env::remove_var("TOKENBUDDY_HOME");
    let _ = std::fs::remove_dir_all(&dir);
}

/// anthropic 流式:message_start×message_delta 合成入/出侧,缓存两列齐。
#[test]
fn anthropic_stream_end_to_end() {
    let _guard = tokenbuddy::TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let dir = tokenbuddy::unique_test_dir("gw-e2e-anth");
    std::env::set_var("TOKENBUDDY_HOME", &dir);
    let up_addr = spawn_mock_anthropic();
    let key = gateway::add_client("claude").unwrap().key;
    let (rt, gw_addr) = spawn_gateway(
        serde_json::json!([{
            "id": "mock-anth", "protocol": "anthropic",
            "base_url": format!("http://{up_addr}"),
            "key_file": "keys/k.key", "models": []
        }]),
        serde_json::json!([]),
    );
    let resp = ureq::post(&format!("http://{gw_addr}/v1/messages"))
        .set("x-api-key", &key)
        .set("anthropic-version", "2023-06-01")
        .set("content-type", "application/json")
        .send_bytes(
            serde_json::json!({
                "model": "glm-5", "stream": true, "max_tokens": 32,
                "messages": [{"role": "user", "content": "只回复:好"}]
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap();
    assert_eq!(resp.status(), 200);
    let mut text = String::new();
    resp.into_reader().read_to_string(&mut text).unwrap();
    assert!(
        text.contains("message_start") && text.contains("message_stop"),
        "{text}"
    );
    for _ in 0..40 {
        if !read_meter_rows().is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let rows = read_meter_rows();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(
        (
            rows[0].input,
            rows[0].output,
            rows[0].cache_read,
            rows[0].cache_write
        ),
        (120, 37, 30, 0)
    );
    assert_eq!(rows[0].usage_source, "upstream");
    rt.stop();
    std::env::remove_var("TOKENBUDDY_HOME");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 鉴权:无密钥/坏密钥 401;坏模型 404 人话;停机后连接拒绝。
#[test]
fn auth_routing_and_shutdown() {
    let _guard = tokenbuddy::TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let dir = tokenbuddy::unique_test_dir("gw-e2e-auth");
    std::env::set_var("TOKENBUDDY_HOME", &dir);
    let up_addr = spawn_mock_openai(Arc::new(AtomicBool::new(false)));
    let key = gateway::add_client("tool").unwrap().key;
    let (rt, gw_addr) = spawn_gateway(
        serde_json::json!([{
            "id": "mock-oai", "protocol": "openai",
            "base_url": format!("http://{up_addr}/v1"),
            "key_file": "keys/k.key",
            "models": ["known-model"]
        }]),
        serde_json::json!([]),
    );

    // 无密钥 401
    let resp = ureq::post(&format!("http://{gw_addr}/v1/chat/completions"))
        .set("content-type", "application/json")
        .send_bytes(
            serde_json::json!({"model": "known-model", "messages": []})
                .to_string()
                .as_bytes(),
        );
    match resp {
        Err(ureq::Error::Status(401, r)) => {
            let body = r.into_string().unwrap();
            assert!(body.contains("tokenbuddy_gateway"), "{body}");
        }
        other => panic!("无密钥应 401,得到 {other:?}"),
    }
    // 坏密钥 401
    let resp = ureq::post(&format!("http://{gw_addr}/v1/chat/completions"))
        .set("Authorization", "Bearer tb-local-wrong")
        .set("content-type", "application/json")
        .send_bytes(
            serde_json::json!({"model": "known-model", "messages": []})
                .to_string()
                .as_bytes(),
        );
    assert!(matches!(resp, Err(ureq::Error::Status(401, _))));
    // 未知模型 404 + 人话(不带流式,错误信封即可)
    let resp = ureq::post(&format!("http://{gw_addr}/v1/chat/completions"))
        .set("Authorization", &format!("Bearer {key}"))
        .set("content-type", "application/json")
        .send_bytes(
            serde_json::json!({"model": "ghost-model", "messages": []})
                .to_string()
                .as_bytes(),
        );
    match resp {
        Err(ureq::Error::Status(404, r)) => {
            let body = r.into_string().unwrap();
            assert!(body.contains("ghost-model"), "{body}");
        }
        other => panic!("未知模型应 404,得到 {other:?}"),
    }
    // 失败请求不进账本
    assert!(read_meter_rows().is_empty());
    // /v1/models 聚合
    let resp = ureq::get(&format!("http://{gw_addr}/v1/models"))
        .set("Authorization", &format!("Bearer {key}"))
        .call()
        .unwrap();
    let models: serde_json::Value = serde_json::from_reader(resp.into_reader()).unwrap();
    assert_eq!(models["data"][0]["id"], "known-model");

    // 停机:监听线程退出,新连接拒绝
    rt.stop();
    assert!(!rt.is_running());
    let refused = std::net::TcpStream::connect(&gw_addr).is_err();
    assert!(refused, "stop 后 {gw_addr} 应拒绝连接");

    std::env::remove_var("TOKENBUDDY_HOME");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 上游 401 原样透传(不套网关信封),失败不落账。
#[test]
fn upstream_auth_error_passes_through() {
    let _guard = tokenbuddy::TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let dir = tokenbuddy::unique_test_dir("gw-e2e-uperr");
    std::env::set_var("TOKENBUDDY_HOME", &dir);
    // key 文件内容故意错掉:mock 上游将回 401
    std::fs::create_dir_all(tokenbuddy::data_dir().join("keys")).unwrap();
    std::fs::write(tokenbuddy::data_dir().join("keys/k.key"), "WRONG").unwrap();
    let up_addr = spawn_mock_anthropic();
    let key = gateway::add_client("claude").unwrap().key;
    let cfg = serde_json::json!({
        "enabled": true, "listen": "127.0.0.1:0",
        "providers": [{"id":"a","protocol":"anthropic","base_url":format!("http://{up_addr}"),"key_file":"keys/k.key","models":[]}]
    });
    std::fs::write(gateway::config_path(), cfg.to_string()).unwrap();
    let rt = Arc::new(Runtime::new());
    rt.start().unwrap();
    let gw_addr = rt.actual_addr().unwrap();

    let resp = ureq::post(&format!("http://{gw_addr}/v1/messages"))
        .set("x-api-key", &key)
        .set("content-type", "application/json")
        .send_bytes(
            serde_json::json!({"model": "glm-5", "max_tokens": 8, "messages": []})
                .to_string()
                .as_bytes(),
        );
    match resp {
        Err(ureq::Error::Status(401, r)) => {
            let body = r.into_string().unwrap();
            assert!(body.contains("authentication_error"), "上游错误原样:{body}");
            assert!(
                !body.contains("tokenbuddy_gateway"),
                "不得套网关信封:{body}"
            );
        }
        Err(ureq::Error::Status(code, r)) => {
            let body = r.into_string().unwrap();
            panic!("应 401,得到 {code}: {body}");
        }
        other => panic!("应 401,得到 {other:?}"),
    }
    assert!(read_meter_rows().is_empty(), "失败请求不落账");
    rt.stop();
    std::env::remove_var("TOKENBUDDY_HOME");
    let _ = std::fs::remove_dir_all(&dir);
}

/// F16(P1 档)combo 回退:链首指向死端口(传输失败),链尾正常——
/// 请求应落到链尾且入账记的是链尾 provider。
#[test]
fn combo_falls_over_on_transport_error() {
    let _guard = tokenbuddy::TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let dir = tokenbuddy::unique_test_dir("gw-e2e-combo");
    std::env::set_var("TOKENBUDDY_HOME", &dir);
    let up_addr = spawn_mock_openai(Arc::new(AtomicBool::new(false)));
    let key = gateway::add_client("tool").unwrap().key;
    // 先占一个端口再释放,拿一个"几乎必死"的端口;更稳的做法是直接用
    // 未监听的高位端口。
    let dead = "127.0.0.1:9"; // discard 端口,连接立拒
    let cfg = serde_json::json!({
        "enabled": true, "listen": "127.0.0.1:0",
        "providers": [
            {"id":"dead-first","protocol":"openai","base_url":format!("http://{dead}/v1"),"key_file":"keys/k.key","models":[]},
            {"id":"live-second","protocol":"openai","base_url":format!("http://{up_addr}/v1"),"key_file":"keys/k.key","models":[]}
        ],
        "combos": [{"id":"default","chain":["dead-first","live-second"]}]
    });
    std::fs::create_dir_all(tokenbuddy::data_dir().join("keys")).unwrap();
    std::fs::write(tokenbuddy::data_dir().join("keys/k.key"), UPSTREAM_KEY).unwrap();
    std::fs::write(gateway::config_path(), cfg.to_string()).unwrap();
    let rt = Arc::new(Runtime::new());
    rt.start().unwrap();
    let gw_addr = rt.actual_addr().unwrap();

    let resp = ureq::post(&format!("http://{gw_addr}/v1/chat/completions"))
        .set("Authorization", &format!("Bearer {key}"))
        .set("content-type", "application/json")
        .send_bytes(
            serde_json::json!({"model":"any","stream":false,"messages":[]})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    assert_eq!(resp.status(), 200, "链尾应接住");
    for _ in 0..40 {
        if !read_meter_rows().is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let rows = read_meter_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].provider, "live-second", "账记在接住请求的那家");
    rt.stop();
    std::env::remove_var("TOKENBUDDY_HOME");
    let _ = std::fs::remove_dir_all(&dir);
}

/// R109:qoder 反向代理协议级端到端。mock 上游做真校验:
/// ① Authorization=COSY.payload.sig,重算 md5 复核签名;
/// ② Cosy-Bodyhash == md5(body);
/// ③ WAF 解码回原文,payload 形状(system 提升/model_config/model key)正确;
/// ④ 回信封 SSE(含末帧 usage),断言客户端收到纯 OpenAI SSE + 入账 upstream。
#[test]
fn qoder_mock_end_to_end() {
    let _guard = tokenbuddy::TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let dir = tokenbuddy::unique_test_dir("gw-e2e-qoder");
    std::env::set_var("TOKENBUDDY_HOME", &dir);
    let server = Arc::new(tiny_http::Server::http("127.0.0.1:0").unwrap());
    let up_addr = match server.server_addr() {
        tiny_http::ListenAddr::IP(a) => a.to_string(),
        _ => panic!("ip listener"),
    };
    let verifier = std::thread::spawn(move || {
        use tokenbuddy::gateway::qoder::decode_body;
        for mut request in server.incoming_requests() {
            let is_model_list = request.url().starts_with("/algo/api/v2/model/list");
            let mut body = Vec::new();
            let _ = request.as_reader().read_to_end(&mut body);
            let hdr = |name: &str| {
                request
                    .headers()
                    .iter()
                    .find(|h| h.field.as_str().to_ascii_lowercase() == name.to_ascii_lowercase())
                    .map(|h| h.value.as_str().to_string())
                    .unwrap_or_default()
            };
            // ① 签名复核
            let auth = hdr("Authorization");
            assert!(
                auth.starts_with("Bearer COSY."),
                "Authorization 形状: {auth}"
            );
            let parts: Vec<&str> = auth["Bearer ".len()..].split('.').collect();
            assert_eq!(parts.len(), 3, "COSY.{{payload}}.{{sig}}: {auth}");
            assert_eq!(parts[0], "COSY");
            let mut sig_input = Vec::new();
            sig_input.extend_from_slice(parts[1].as_bytes());
            sig_input.push(b'\n');
            sig_input.extend_from_slice(hdr("Cosy-Key").as_bytes());
            sig_input.push(b'\n');
            sig_input.extend_from_slice(hdr("Cosy-Date").as_bytes());
            sig_input.push(b'\n');
            sig_input.extend_from_slice(&body);
            sig_input.push(b'\n');
            sig_input.extend_from_slice(hdr("Cosy-Sigpath").as_bytes());
            let expect = tokenbuddy::gateway::qoder::md5_hex(&sig_input);
            assert_eq!(parts[2], expect, "COSY 签名必须可复核");
            assert_eq!(
                hdr("Cosy-Bodyhash"),
                tokenbuddy::gateway::qoder::md5_hex(&body)
            );
            assert_eq!(hdr("Cosy-Bodylength"), body.len().to_string());
            if is_model_list {
                let _ = request.respond(
                    tiny_http::Response::from_string(
                        r#"{"chat":[{"key":"qfmodel","display_name":"Q","is_reasoning":true,"max_output_tokens":8192,"enable":true}]}"#,
                    )
                    .with_header(
                        tiny_http::Header::from_bytes("Content-Type", "application/json").unwrap(),
                    ),
                );
                continue;
            }
            // ③ WAF 解码 + payload 形状
            let plain = decode_body(&body).expect("WAF 体应可解码");
            let v: serde_json::Value = serde_json::from_slice(&plain).unwrap();
            assert_eq!(v["system"], "你是助手", "system 必须提升");
            assert_eq!(v["model_config"]["key"], "qfmodel");
            assert_eq!(v["chat_context"]["extra"]["modelConfig"]["key"], "qfmodel");
            assert_eq!(v["parameters"]["max_tokens"], 64);
            assert_eq!(v["session_type"], "qodercli");
            // ④ 信封 SSE:两帧内容 + usage 末帧 + [DONE]
            let inner1 = r#"{"id":"q1","object":"chat.completion.chunk","model":"qfmodel","choices":[{"index":0,"delta":{"content":"收到"}}]}"#;
            let inner2 = r#"{"id":"q1","object":"chat.completion.chunk","model":"qfmodel","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":77,"completion_tokens":9,"prompt_tokens_details":{"cached_tokens":31}}}"#;
            let sse = format!(
                "data: {}\n\ndata: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                serde_json::json!({"statusCodeValue": 200, "body": inner1}),
                serde_json::json!({"statusCodeValue": 200, "body": inner2}),
                serde_json::json!({"statusCodeValue": 200, "body": "[DONE]"}),
            );
            let _ = request.respond(tiny_http::Response::from_string(sse).with_header(
                tiny_http::Header::from_bytes("Content-Type", "text/event-stream").unwrap(),
            ));
        }
    });

    let key = gateway::add_client("zcode").unwrap().key;
    std::fs::create_dir_all(tokenbuddy::data_dir().join("keys")).unwrap();
    std::fs::write(tokenbuddy::data_dir().join("keys/q.key"), "jt-mock-token").unwrap();
    // 模型目录走同一 mock:/algo/api/v2/model/list(COSY GET)。
    let cfg = serde_json::json!({
        "enabled": true, "listen": "127.0.0.1:0",
        "providers": [{
            "id": "qoder-sub",
            "protocol": "qoder",
            "base_url": format!("http://{up_addr}"),
            "key_file": "keys/q.key",
            "user_id": "U-mock-42",
            "deployment": "cn",
            "models": ["qfmodel"]
        }],
        "combos": []
    });
    std::fs::write(gateway::config_path(), cfg.to_string()).unwrap();
    let rt = Arc::new(Runtime::new());
    rt.start().unwrap();
    let gw_addr = rt.actual_addr().unwrap();

    let resp = ureq::post(&format!("http://{gw_addr}/v1/chat/completions"))
        .set("Authorization", &format!("Bearer {key}"))
        .set("content-type", "application/json")
        .send_bytes(
            serde_json::json!({
                "model": "qfmodel", "stream": true, "max_tokens": 64,
                "messages": [
                    {"role": "system", "content": "你是助手"},
                    {"role": "user", "content": "只回复:收到"}
                ]
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap_or_else(|e| match e {
            ureq::Error::Status(code, r) => {
                let body = r.into_string().unwrap_or_default();
                panic!("HTTP {code}: {body}");
            }
            other => panic!("{other:?}"),
        });
    assert_eq!(resp.status(), 200);
    let mut text = String::new();
    resp.into_reader().read_to_string(&mut text).unwrap();
    assert!(text.contains("收到"), "内层 chunk 解包直通: {text}");
    assert!(text.contains("[DONE]"));
    assert!(
        !text.contains("statusCodeValue"),
        "信封不得漏给客户端: {text}"
    );
    assert!(
        text.contains("\"prompt_tokens\":77"),
        "usage 帧直通: {text}"
    );

    for _ in 0..40 {
        if !read_meter_rows().is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let rows = read_meter_rows();
    assert_eq!(rows.len(), 1, "{rows:?}");
    let row = &rows[0];
    assert_eq!(row.provider, "qoder-sub");
    assert_eq!(row.model, "qfmodel");
    assert_eq!(
        (row.input, row.output, row.cache_read),
        (77, 9, 31),
        "qoder 内层 usage 直报入账"
    );
    assert_eq!(row.usage_source, "upstream");
    rt.stop();
    std::env::remove_var("TOKENBUDDY_HOME");
    let _ = std::fs::remove_dir_all(&dir);
    // mock 线程(incoming_requests 永不退出)与 openai mock 同款:分离,不 join。
    drop(verifier);
}

/// R112:面板新要的四件事——限额头采集、用量分桶、模型目录来源标注。
/// 走真网关 + 真 HTTP:mock 上游回 x-ratelimit-*,断言落进 meter 行,
/// usage_stats 能分出今日/窗口/分维,且配置声明与实时拉取两种来源分开标。
#[test]
fn panel_facts_end_to_end_rate_limit_usage_and_catalog() {
    let _guard = tokenbuddy::TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let dir = tokenbuddy::unique_test_dir("gw-panel");
    std::env::set_var("TOKENBUDDY_HOME", &dir);
    let saw = Arc::new(AtomicBool::new(false));
    let up_addr = spawn_mock_openai(Arc::clone(&saw));
    let key = gateway::add_client("claude").unwrap().key;
    // 两个上游:一个声明 models(来源=配置),一个不声明(来源=实时拉取)。
    let (rt, gw_addr) = spawn_gateway(
        serde_json::json!([{
            "id": "mock-declared", "protocol": "openai",
            "base_url": format!("http://{up_addr}/v1"),
            "key_file": "keys/k.key",
            "models": ["MiniMax-M3.1-Flash-Preview"],
            "dialects": ["include_usage", "cached_tokens_in_details"]
        }, {
            "id": "mock-live", "protocol": "openai",
            "base_url": format!("http://{up_addr}/v1"),
            "key_file": "keys/k.key"
        }]),
        serde_json::json!([]),
    );

    // 非流式请求:响应头带 x-ratelimit-remaining-requests: 42
    let resp = ureq::post(&format!("http://{gw_addr}/v1/chat/completions"))
        .set("Authorization", &format!("Bearer {key}"))
        .set("content-type", "application/json")
        .send_bytes(
            serde_json::json!({"model": "MiniMax-M3.1-Flash-Preview", "messages": [
                {"role": "user", "content": "hi"}]})
            .to_string()
            .as_bytes(),
        )
        .expect("gateway answers");
    assert_eq!(resp.status(), 200);

    for _ in 0..40 {
        if !read_meter_rows().is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let rows = read_meter_rows();
    assert_eq!(rows.len(), 1, "{rows:?}");
    // 限额头原样落盘:两家头族收,值不加工
    assert_eq!(
        rows[0].rate_limit.get("x-ratelimit-remaining-requests"),
        Some(&serde_json::Value::String("42".into())),
        "上游自报限额头必须被采集: {:?}",
        rows[0].rate_limit
    );
    assert_eq!(
        rows[0].rate_limit.get("x-ratelimit-limit-requests"),
        Some(&serde_json::Value::String("100".into()))
    );

    // usage_stats:刚才那笔进今日、进窗口、进分维
    let u = gateway::usage_stats(tokenbuddy::now_ts());
    assert_eq!(u.total.requests, 1);
    assert_eq!(u.today.requests, 1, "刚发的请求今天就该可见,不等 sync");
    let w = u.window.as_ref().expect("窗口内有流量");
    assert_eq!(w.bucket.requests, 1);
    assert_eq!(w.bucket.total_tokens, 150 + 5 + 100);
    assert_eq!(u.by_provider[0].0, "mock-declared");
    assert_eq!(u.by_client[0].0, "claude");
    assert_eq!(u.rate_limit_from, "mock-declared");
    assert_eq!(u.today.estimated_requests, 0, "上游报了 usage,不该是估算");

    rt.stop();
    std::env::remove_var("TOKENBUDDY_HOME");
    let _ = std::fs::remove_dir_all(&dir);
}

/// R112:模型目录的来源标注——声明过的按配置列,没声明的实时问上游;
/// 上游目录为空时报 none + 原因,不静默变成空列表。
#[test]
fn model_catalog_marks_config_vs_upstream_provenance() {
    let _guard = tokenbuddy::TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let dir = tokenbuddy::unique_test_dir("gw-catalog");
    std::env::set_var("TOKENBUDDY_HOME", &dir);
    let up_addr = spawn_mock_openai(Arc::new(AtomicBool::new(false)));
    // 上游要 Bearer 才肯给目录:密钥文件得先落盘(fetch_provider_models 自己读)。
    std::fs::create_dir_all(tokenbuddy::data_dir().join("keys")).unwrap();
    std::fs::write(tokenbuddy::data_dir().join("keys/k.key"), UPSTREAM_KEY).unwrap();

    let declared = gateway::Provider {
        id: "declared".into(),
        protocol: "openai".into(),
        base_url: format!("http://{up_addr}/v1"),
        key_file: "keys/k.key".into(),
        models: vec!["M3".into(), "M3-mini".into()],
        aliases: Default::default(),
        user_id: String::new(),
        deployment: String::new(),
        dialects: vec![],
    };
    let live = gateway::Provider {
        models: vec![],
        ..declared.clone()
    };
    let agent = gateway::build_agent();

    let a = gateway::server::fetch_provider_models(&declared, &agent).expect("config list");
    assert_eq!(a.source, "config");
    assert_eq!(a.models, vec!["M3".to_string(), "M3-mini".to_string()]);

    // 没声明 models:实时问上游 /v1/models
    let b = gateway::server::fetch_provider_models(&live, &agent).expect("live list");
    assert_eq!(b.source, "upstream", "没声明就该实时拉:{b:?}");
    assert!(
        b.models.iter().any(|m| m == "mock-model-1"),
        "{:?}",
        b.models
    );

    // 上游不可达:报 none + 原因,不是空列表冒充"没有模型"
    let dead = gateway::Provider {
        id: "dead".into(),
        base_url: "http://127.0.0.1:1/v1".into(),
        models: vec![],
        ..declared.clone()
    };
    let c = gateway::server::fetch_provider_models(&dead, &agent);
    assert!(c.is_err(), "拉不到就该 Err,由调用方说明原因");

    std::env::remove_var("TOKENBUDDY_HOME");
    let _ = std::fs::remove_dir_all(&dir);
}

/// qoder 目录 mock:只答 `/algo/api/v2/model/list`,并把收到的 path 记进
/// `seen`(用来证明探活打的是签名目录,不是 `{base}/models`)。
/// mode: 0=两个模型 1=401 2=空目录。
fn spawn_mock_qoder_catalog(
    mode: Arc<std::sync::atomic::AtomicUsize>,
    seen: Arc<std::sync::Mutex<Vec<String>>>,
) -> String {
    let server = Arc::new(tiny_http::Server::http("127.0.0.1:0").unwrap());
    let addr = match server.server_addr() {
        tiny_http::ListenAddr::IP(a) => a.to_string(),
        _ => panic!("ip listener"),
    };
    std::thread::spawn(move || {
        let json = |body: &str| {
            tiny_http::Response::from_string(body.to_string()).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap(),
            )
        };
        for request in server.incoming_requests() {
            seen.lock().unwrap().push(request.url().to_string());
            let resp = match mode.load(Ordering::SeqCst) {
                0 => json(r#"{"chat":[{"key":"qfmodel"},{"key":"qmodel"}]}"#),
                1 => tiny_http::Response::from_string("nope").with_status_code(401),
                _ => json(r#"{"chat":[]}"#),
            };
            let _ = request.respond(resp);
        }
    });
    addr
}

/// 回归:qoder provider 的 `base_url` 按设计可以留空(端点由 deployment 取
/// 官方值),探活曾照 openai 拼 `{base}/models`,空 base_url 就成了相对 URL
/// `/models`——面板上显示成「不可达 Bad URL: RelativeUrlWithoutBase」,
/// 而真相是这条 provider 配置完全正常。现在 qoder 走 COSY 签名的
/// model/list,URL 恒为绝对地址。
#[test]
fn qoder_health_probe_uses_signed_model_list_not_relative_models_url() {
    let _guard = tokenbuddy::TEST_ENV_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let dir = tokenbuddy::unique_test_dir("gw-probe-qoder");
    std::env::set_var("TOKENBUDDY_HOME", &dir);

    let mode = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let addr = spawn_mock_qoder_catalog(mode.clone(), seen.clone());

    // 裸令牌(jt-)要配 user_id,PAT 才自动解析。
    std::fs::create_dir_all(tokenbuddy::data_dir().join("keys")).unwrap();
    std::fs::write(
        tokenbuddy::data_dir().join("keys/qoder-probe.key"),
        "jt-mock-token\n",
    )
    .unwrap();

    let p = gateway::Provider {
        id: "qoder-sub".into(),
        protocol: "qoder".into(),
        base_url: format!("http://{addr}"),
        key_file: "keys/qoder-probe.key".into(),
        models: vec![],
        aliases: Default::default(),
        user_id: "U-mock-1".into(),
        deployment: "cn".into(),
        dialects: vec![],
    };
    let agent = gateway::build_agent();

    // ① 200 + 有模型 → ok,且请求落在签名目录上
    let r = gateway::probe(&agent, &p, "jt-mock-token");
    assert_eq!(r.state, gateway::HealthState::Ok, "{}", r.detail);
    assert!(!r.detail.contains("Bad URL"), "{}", r.detail);
    let paths = seen.lock().unwrap().clone();
    assert!(
        paths
            .iter()
            .all(|u| u.starts_with("/algo/api/v2/model/list")),
        "探活必须打 COSY model/list,不是 {{base}}/models:{paths:?}"
    );
    assert!(!paths.iter().any(|u| u.ends_with("/models")), "{paths:?}");

    // ② 401 → auth_failed,不是「不可达」——别让人去查防火墙
    mode.store(1, Ordering::SeqCst);
    let r = gateway::probe(&agent, &p, "jt-mock-token");
    assert_eq!(r.state, gateway::HealthState::AuthFailed, "{}", r.detail);

    // ③ 200 但目录空 → reachable(端点在,这账号手里没料)
    mode.store(2, Ordering::SeqCst);
    let r = gateway::probe(&agent, &p, "jt-mock-token");
    assert_eq!(r.state, gateway::HealthState::Reachable, "{}", r.detail);

    // ④ 根因锚点:base_url 留空时探活 URL 仍必须是绝对地址(cn/global 各一)。
    //    这里只断言 URL 构造,不对官方域名发任何请求(测试不出网)。
    let url_for = |deployment: &str| {
        let p = gateway::Provider {
            base_url: String::new(),
            deployment: deployment.to_string(),
            ..p.clone()
        };
        tokenbuddy::gateway::qoder::model_list_url(&tokenbuddy::gateway::qoder::chat_base(&p))
    };
    assert!(
        url_for("cn").starts_with("https://gateway.qoder.com.cn/algo/api/v2/model/list"),
        "{}",
        url_for("cn")
    );
    assert!(
        url_for("global").starts_with("https://api3.qoder.sh/algo/api/v2/model/list"),
        "{}",
        url_for("global")
    );

    gateway::release_caches();
    std::env::remove_var("TOKENBUDDY_HOME");
    let _ = std::fs::remove_dir_all(&dir);
}
