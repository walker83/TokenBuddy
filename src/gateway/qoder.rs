//! Qoder 反向代理(R109)——把 Qoder 订阅当成网关的一个上游。
//!
//! 协议移植自 10router(CLIProxyAPIPlus 脉络),四块:
//! ① COSY 混合签名:用户信息 AES-128-CBC(密钥=UUID 前 16 字符,IV=密钥)
//!    加密,RSA-PKCS1v15 包 AES 密钥,MD5 签名
//!    `payload64\n cosyKey\n ts\n body\n sigPath`,17 个 Cosy-*/X-* 头;
//! ② WAF 编码:base64 → 三段重排 [尾][中][头] → 自定义字母表替换,
//!    URL 带 &Encode=1;
//! ③ 请求改形:OpenAI messages → Qoder chat_context 形状(system 提升、
//!    content 拍平、model_config 从 /model/list 实时拉——配错会被上游
//!    静默降级,所以缺配置是硬错误);
//! ④ SSE 信封解包:上游每行 `{"statusCodeValue":200,"body":"<内层 OpenAI
//!    chunk>"}`,解包直通;计费封锁码(110/112/10605)映射 403/429。
//!
//! 凭据:官方 PAT(pt-…,qoder.cn/account/integrations 签发)经
//! openapi…/jobToken/exchange 换短命 jt- 令牌 + userinfo 取 userId;
//! 也可直接填 jt-/dt- 令牌(provider.user_id 必填)。

use crate::gateway::Provider;
use anyhow::{anyhow, bail, Result};
use base64::Engine as _;
use md5::Digest as Md5Digest;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

// ---- 端点(CN 与国际) ----
pub const CN_CHAT_BASE: &str = "https://gateway.qoder.com.cn";
pub const CN_OPENAPI_BASE: &str = "https://openapi.qoder.com.cn";
pub const GLOBAL_CHAT_BASE: &str = "https://api3.qoder.sh";
pub const GLOBAL_OPENAPI_BASE: &str = "https://openapi.qoder.sh";
pub const CHAT_SIG_PATH: &str = "/api/v2/service/pro/sse/agent_chat_generation";

/// 推理/目录端点基址:provider.base_url 自定义(测试/mock/自建中继),
/// 留空则按 deployment 取官方(CN=gateway.qoder.com.cn)。
pub fn chat_base(p: &Provider) -> String {
    let base = p.base_url.trim().trim_end_matches('/');
    if !base.is_empty() {
        return base.to_string();
    }
    if p.deployment == "global" {
        GLOBAL_CHAT_BASE.to_string()
    } else {
        CN_CHAT_BASE.to_string()
    }
}

pub fn chat_url(base: &str) -> String {
    format!("{base}/algo{CHAT_SIG_PATH}?FetchKeys=llm_model_result&AgentId=agent_common&Encode=1")
}
pub fn model_list_url(base: &str) -> String {
    format!("{base}/algo/api/v2/model/list")
}
fn job_exchange_url(cn: bool) -> String {
    format!(
        "{}/api/v1/jobToken/exchange",
        if cn {
            CN_OPENAPI_BASE
        } else {
            GLOBAL_OPENAPI_BASE
        }
    )
}
fn userinfo_url(cn: bool) -> String {
    format!(
        "{}/api/v1/userinfo",
        if cn {
            CN_OPENAPI_BASE
        } else {
            GLOBAL_OPENAPI_BASE
        }
    )
}

// ---- COSY 常量(与上游校验严格匹配,勿改) ----
const IDE_VERSION: &str = "1.0.0";
const CLIENT_TYPE: &str = "5";
const DATA_POLICY: &str = "disagree";
const LOGIN_VERSION: &str = "v2";
const MACHINE_OS: &str = "x86_64_windows";
const MACHINE_TYPE: &str = "5";
const RSA_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDA8iMH5c02LilrsERw9t6Pv5Nc
4k6Pz1EaDicBMpdpxKduSZu5OANqUq8er4GM95omAGIOPOh+Nx0spthYA2BqGz+l
6HRkPJ7S236FZz73In/KVuLnwI8JJ2CbuJap8kvheCCZpmAWpb/cPx/3Vr/J6I17
XcW+ML9FoCI6AOvOzwIDAQAB
-----END PUBLIC KEY-----";

// ---- WAF 编码字母表 ----
const STD_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const CUSTOM_ALPHABET: &[u8] = b"_doRTgHZBKcGVjlvpC,@aFSx#DPuNJme&i*MzLOEn)sUrthbf%Y^w.(kIQyXqWA!";

fn substitution_table() -> [u8; 128] {
    let mut t = [0u8; 128];
    for (i, slot) in t.iter_mut().enumerate() {
        *slot = i as u8;
    }
    for (i, &s) in STD_ALPHABET.iter().enumerate() {
        t[s as usize] = CUSTOM_ALPHABET[i];
    }
    t[b'=' as usize] = b'$';
    t
}

/// WAF 编码:base64 → 三段重排 [尾][中][头] → 字母表替换。
/// 输出全 ASCII。
pub fn encode_body(plain: &[u8]) -> Vec<u8> {
    let table = substitution_table();
    let std = B64.encode(plain);
    let bytes = std.as_bytes();
    let n = bytes.len();
    let a = n / 3;
    let mut out = Vec::with_capacity(n);
    // [tail][mid][head]
    out.extend_from_slice(&bytes[n - a..]);
    out.extend_from_slice(&bytes[a..n - a]);
    out.extend_from_slice(&bytes[..a]);
    for b in out.iter_mut() {
        if (*b as usize) < 128 {
            *b = table[*b as usize];
        }
    }
    out
}

/// WAF 解码(诊断/测试用):替换还原 → 重排逆转 → base64 解码。
pub fn decode_body(encoded: &[u8]) -> Result<Vec<u8>> {
    // 逆表:恒等打底,65 个特定映射最后写——自定义字母表里有一半字符
    // 本身也是普通字节(_、,、@…),先写恒等再写映射会把它们抹回自身。
    let mut inv = [0u8; 256];
    for (i, slot) in inv.iter_mut().enumerate() {
        *slot = i as u8;
    }
    for (k, &c) in CUSTOM_ALPHABET.iter().enumerate() {
        inv[c as usize] = STD_ALPHABET[k];
    }
    inv[b'$' as usize] = b'=';
    let std_bytes: Vec<u8> = encoded.iter().map(|&b| inv[b as usize]).collect();
    let n = std_bytes.len();
    let a = n / 3;
    // rearranged[i] = orig[src];tail 段(0..a)来自 orig[n-a..n],
    // mid 段(a..n-a)来自 orig[a..n-a],head 段(n-a..n)来自 orig[0..a]。
    let mut orig = vec![0u8; n];
    for (i, &c) in std_bytes.iter().enumerate() {
        let src = if i < a {
            n - a + i
        } else if i < n - a {
            i
        } else {
            i - (n - a)
        };
        orig[src] = c;
    }
    let s = String::from_utf8(orig).map_err(|_| anyhow!("decoded body not ascii"))?;
    Ok(B64.decode(s)?)
}

/// 随机源:测试注入固定值,生产走 /dev/urandom。
pub struct QoderRandom {
    pub uuid: Box<dyn Fn() -> String + Send + Sync>,
    pub now: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl QoderRandom {
    pub fn real() -> Self {
        QoderRandom {
            uuid: Box::new(random_uuid),
            now: Box::new(|| crate::now_ts() as u64),
        }
    }
    /// UUIDv4 形状:8-4-4-4-12 hex。测试注入固定值。
    pub fn fixed(uuid_value: &str, ts: u64) -> Self {
        let u = uuid_value.to_string();
        QoderRandom {
            uuid: Box::new(move || u.clone()),
            now: Box::new(move || ts),
        }
    }
}

pub fn random_uuid() -> String {
    let mut b = [0u8; 16];
    let _ = std::fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut b));
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h = hex::encode(b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// 机器 id:每安装一份持久一个(与 IDE 的 machine_id 独立)。
pub fn machine_id() -> String {
    let path = crate::data_dir().join("gateway").join("qoder-machine-id");
    if let Ok(s) = std::fs::read_to_string(&path) {
        let s = s.trim();
        if !s.is_empty() {
            return s.to_string();
        }
    }
    let id = random_uuid();
    let _ = std::fs::create_dir_all(path.parent().expect("parent"));
    let _ = std::fs::write(&path, &id);
    id
}

/// 解析后的 COSY 凭据。
#[derive(Debug, Clone)]
pub struct Creds {
    pub user_id: String,
    pub token: String,
}

/// PAT(pt-…)→ jt- 换取 + userId;结果按 PAT 缓存(短命令牌提前 60s 刷新)。
/// jt-/dt- 直接使用(要求 provider.user_id 已填)。
pub fn resolve_credentials(p: &Provider, agent: &ureq::Agent) -> Result<Creds> {
    let raw = crate::gateway::upstream_key(&p.key_file)?;
    let cn = p.deployment != "global";
    if raw.starts_with("pt-") {
        return lookup_pat(&raw, cn, agent);
    }
    let user_id = p.user_id.trim().to_string();
    if user_id.is_empty() {
        bail!(
            "qoder provider 用裸令牌({}…)时必须在 gateway.json 里填 user_id;推荐改用 PAT(pt-…,qoder.{}/account/integrations 签发)自动解析",
            &raw[..3],
            if cn { "cn" } else { "com" }
        );
    }
    Ok(Creds {
        user_id,
        token: raw,
    })
}

static PAT_CACHE: Mutex<Option<HashMap<String, (Creds, Instant)>>> = Mutex::new(None);

fn lookup_pat(pat: &str, cn: bool, agent: &ureq::Agent) -> Result<Creds> {
    let mut cache = PAT_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let map = cache.get_or_insert_with(HashMap::new);
    if let Some((creds, at)) = map.get(pat) {
        if at.elapsed() < Duration::from_secs(50 * 60) {
            return Ok(creds.clone());
        }
    }
    let resp = agent
        .post(&job_exchange_url(cn))
        .set("content-type", "application/json")
        .set("accept", "application/json")
        .set("user-agent", "qodercli/1.0.0")
        .set("Cosy-Version", IDE_VERSION)
        .set("Cosy-ClientType", CLIENT_TYPE)
        .send_bytes(format!("{{\"personal_token\":\"{pat}\"}}").as_bytes())?;
    let status = resp.status();
    let body = resp.into_string().unwrap_or_default();
    anyhow::ensure!(
        status == 200,
        "PAT 换令牌失败({status}):{}",
        &body[..body.len().min(200)]
    );
    let v: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| anyhow!("jobToken/exchange 响应非 JSON:{e}"))?;
    let token = v
        .get("token")
        .and_then(|t| t.as_str())
        .ok_or_else(|| anyhow!("jobToken/exchange 没返回 token"))?
        .to_string();
    // userId:userinfo 用 jt 令牌直查(非 COSY)。
    let user_id = agent
        .get(&userinfo_url(cn))
        .set("Authorization", &format!("Bearer {token}"))
        .set("accept", "application/json")
        .set("user-agent", "qodercli/1.0.0")
        .call()
        .ok()
        .and_then(|r| r.into_string().ok())
        .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
        .and_then(|v| {
            ["id", "userId", "user_id"]
                .iter()
                .find_map(|k| v.get(k).and_then(|x| x.as_str()).map(String::from))
        })
        .unwrap_or_default();
    anyhow::ensure!(
        !user_id.is_empty(),
        "userinfo 没解析出 userId(令牌可能无效)"
    );
    let creds = Creds { user_id, token };
    map.insert(pat.to_string(), (creds.clone(), Instant::now()));
    Ok(creds)
}

// ---------------------------------------------------------------------------
// COSY 签名
// ---------------------------------------------------------------------------

/// MD5 十六进制(签名复核/诊断公开给测试与上游校验用)。
pub fn md5_hex(data: &[u8]) -> String {
    format!("{:x}", md5::Md5::digest(data))
}

fn aes_cbc_b64(plaintext: &str, key: &str) -> Result<String> {
    use aes::cipher::{block_padding::Pkcs7, BlockEncryptMut, KeyIvInit};
    type Enc = cbc::Encryptor<aes::Aes128>;
    let key_bytes = key.as_bytes();
    anyhow::ensure!(
        key_bytes.len() == 16,
        "aes key 需 16 字节,得到 {}",
        key_bytes.len()
    );
    // IV = 密钥字节(qodercli 同款;密钥每请求全新,IV 随之唯一)。
    let mut buf = vec![0u8; plaintext.len() + 16];
    let ct = Enc::new(key_bytes.into(), key_bytes.into())
        .encrypt_padded_b2b_mut::<Pkcs7>(plaintext.as_bytes(), &mut buf)
        .map_err(|_| anyhow!("AES 加密失败(缓冲不足)"))?;
    Ok(B64.encode(ct))
}

fn rsa_b64(data: &str) -> Result<String> {
    use rsa::pkcs1v15::Pkcs1v15Encrypt;
    use rsa::pkcs8::DecodePublicKey;
    let mut rng = rand_fallback();
    let key = rsa::RsaPublicKey::from_public_key_pem(RSA_PUBLIC_KEY)
        .map_err(|e| anyhow!("RSA 公钥解析失败:{e}"))?;
    let ct = key
        .encrypt(&mut rng, Pkcs1v15Encrypt, data.as_bytes())
        .map_err(|e| anyhow!("RSA 加密失败:{e}"))?;
    Ok(B64.encode(ct))
}

/// rsa 0.9 的 encrypt 需要 `impl CryptoRngCore`(RngCore + CryptoRng 两个
/// trait);用 /dev/urandom 直接喂,不引入完整 rand 依赖栈。
struct UrandomFill;
impl rand_core::CryptoRng for UrandomFill {}
impl rand_core::RngCore for UrandomFill {
    fn next_u32(&mut self) -> u32 {
        let mut b = [0u8; 4];
        self.fill_bytes(&mut b);
        u32::from_le_bytes(b)
    }
    fn next_u64(&mut self) -> u64 {
        let mut b = [0u8; 8];
        self.fill_bytes(&mut b);
        u64::from_le_bytes(b)
    }
    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand_core::Error> {
        self.fill_bytes(dest);
        Ok(())
    }
    fn fill_bytes(&mut self, dest: &mut [u8]) {
        if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
            if std::io::Read::read_exact(&mut f, dest).is_ok() {
                return;
            }
        }
        // 回退:时钟抖动(仅极端环境;加密强度降级但可用)。
        let mut seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E3779B97F4A7C15)
            ^ (std::process::id() as u64) << 32;
        for d in dest.iter_mut() {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            *d = (seed & 0xff) as u8;
        }
    }
}
fn rand_fallback() -> UrandomFill {
    UrandomFill
}

/// sigPath:去掉 /algo 前缀的路径(查询串不计入)。
pub fn compute_sig_path(url: &str) -> String {
    let after_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let path_only = after_scheme.split('?').next().unwrap_or(after_scheme);
    let path = match path_only.find('/') {
        Some(pos) => &path_only[pos..],
        None => "",
    };
    match path.strip_prefix("/algo") {
        Some(stripped) => stripped.to_string(),
        None => path.to_string(),
    }
}

/// COSY 头全集。body = 实际发送的(已编码)字节。
pub fn cosy_headers(
    body: &[u8],
    url: &str,
    creds: &Creds,
    rng: &QoderRandom,
) -> Result<Vec<(String, String)>> {
    anyhow::ensure!(!creds.user_id.is_empty(), "cosy: user id 为空");
    anyhow::ensure!(!creds.token.is_empty(), "cosy: auth token 为空");

    let aes_key: String = (rng.uuid)().chars().take(16).collect();
    let user_info = serde_json::json!({
        "uid": creds.user_id,
        "security_oauth_token": creds.token,
        "name": "",
        "aid": "",
        "email": "",
    })
    .to_string();
    // serde_json::json! 保持插入序(与 JS JSON.stringify 字段序一致)。
    let info = aes_cbc_b64(&user_info, &aes_key)?;
    let cosy_key = rsa_b64(&aes_key)?;

    let timestamp = ((rng.now)()).to_string();
    let request_id = (rng.uuid)();
    let payload_json = serde_json::json!({
        "version": "v1",
        "requestId": request_id,
        "info": info,
        "cosyVersion": IDE_VERSION,
        "ideVersion": "",
    })
    .to_string();
    let payload_b64 = B64.encode(payload_json.as_bytes());

    let sig_path = compute_sig_path(url);
    let mut sig_input = Vec::new();
    sig_input.extend_from_slice(payload_b64.as_bytes());
    sig_input.push(b'\n');
    sig_input.extend_from_slice(cosy_key.as_bytes());
    sig_input.push(b'\n');
    sig_input.extend_from_slice(timestamp.as_bytes());
    sig_input.push(b'\n');
    sig_input.extend_from_slice(body);
    sig_input.push(b'\n');
    sig_input.extend_from_slice(sig_path.as_bytes());
    let sig = format!("{:x}", md5::Md5::digest(&sig_input));

    let machine = machine_id();
    let body_hash = format!("{:x}", md5::Md5::digest(body));

    let h = |k: &str, v: String| (k.to_string(), v);
    Ok(vec![
        h("Authorization", format!("Bearer COSY.{payload_b64}.{sig}")),
        h("Cosy-Key", cosy_key),
        h("Cosy-User", creds.user_id.clone()),
        h("Cosy-Date", timestamp),
        h("Cosy-Version", IDE_VERSION.to_string()),
        h("Cosy-Machineid", machine.clone()),
        h("Cosy-Machinetoken", machine),
        h("Cosy-Machinetype", MACHINE_TYPE.to_string()),
        h("Cosy-Machineos", MACHINE_OS.to_string()),
        h("Cosy-Clienttype", CLIENT_TYPE.to_string()),
        h("Cosy-Clientip", "127.0.0.1".to_string()),
        h("Cosy-Bodyhash", body_hash),
        h("Cosy-Bodylength", body.len().to_string()),
        h("Cosy-Sigpath", sig_path),
        h("Cosy-Data-Policy", DATA_POLICY.to_string()),
        h("Cosy-Organization-Id", String::new()),
        h("Cosy-Organization-Tags", String::new()),
        h("Login-Version", LOGIN_VERSION.to_string()),
        h("X-Request-Id", (rng.uuid)()),
    ])
}

// ---------------------------------------------------------------------------
// 模型目录(COSY 签名的 GET;响应 {chat:[{key,…}]})
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ModelConfig {
    pub key: String,
    pub raw: serde_json::Value,
}

type ModelCacheMap = HashMap<String, (Vec<ModelConfig>, Instant)>;
static MODEL_CACHE: Mutex<Option<ModelCacheMap>> = Mutex::new(None);

/// 签名 GET model/list 并解析出 chat 目录。**允许返回空 vec**——「200 但
/// 目录空」和「连不上」是两回事(端点在、这账号手里没料),调用方(探活)
/// 要能分开报,不能都塞进一个 Err。
pub fn fetch_models_raw(
    agent: &ureq::Agent,
    creds: &Creds,
    base: &str,
) -> Result<Vec<ModelConfig>> {
    let url = model_list_url(base);
    let rng = QoderRandom::real();
    let headers = cosy_headers(&[], &url, creds, &rng)?;
    let mut req = agent
        .get(&url)
        .set("accept", "application/json")
        .set("accept-encoding", "identity");
    for (k, v) in headers {
        req = req.set(&k, &v);
    }
    let resp = req.call()?;
    let status = resp.status();
    anyhow::ensure!(status == 200, "model/list 返回 {status}");
    let v: serde_json::Value = serde_json::from_reader(resp.into_reader())?;
    let chat = v
        .get("chat")
        .and_then(|c| c.as_array())
        .ok_or_else(|| anyhow!("model/list 响应缺 chat 数组"))?;
    let mut out = Vec::new();
    for entry in chat {
        if let Some(key) = entry.get("key").and_then(|k| k.as_str()) {
            out.push(ModelConfig {
                key: key.to_string(),
                raw: entry.clone(),
            });
        }
    }
    Ok(out)
}

pub fn fetch_models(agent: &ureq::Agent, creds: &Creds, base: &str) -> Result<Vec<ModelConfig>> {
    let out = fetch_models_raw(agent, creds, base)?;
    anyhow::ensure!(!out.is_empty(), "model/list 为空");
    Ok(out)
}

/// 取(并缓存 10 分钟)某模型的 model_config;缺配置 = 硬错误
/// (配错会被上游静默降级到别的模型,宁可拒绝)。
pub fn get_model_config(
    agent: &ureq::Agent,
    creds: &Creds,
    base: &str,
    key: &str,
) -> Result<ModelConfig> {
    {
        let cache = MODEL_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(map) = cache.as_ref() {
            if let Some((list, at)) = map.get(&creds.user_id) {
                if at.elapsed() < Duration::from_secs(600) {
                    if let Some(m) = list.iter().find(|m| m.key == key) {
                        return Ok(m.clone());
                    }
                    bail!(
                        "qoder 模型目录里没有 {key}(可用:{})",
                        list.iter()
                            .map(|m| m.key.as_str())
                            .collect::<Vec<_>>()
                            .join(",")
                    );
                }
            }
        }
    }
    let list = fetch_models(agent, creds, base)?;
    let found = match list.iter().find(|m| m.key == key) {
        Some(m) => m.clone(),
        None => bail!(
            "qoder 模型目录里没有 {key}(可用:{})",
            list.iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ),
    };
    MODEL_CACHE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_or_insert_with(HashMap::new)
        .insert(creds.user_id.clone(), (list, Instant::now()));
    Ok(found)
}

// ---------------------------------------------------------------------------
// 请求改形:OpenAI → Qoder payload
// ---------------------------------------------------------------------------

fn extract_text(content: &serde_json::Value) -> String {
    match content {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(parts) => parts
            .iter()
            .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// system 提升出 messages(Qoder 拒绝 messages 里的 system);content 拍平
/// (文本合并为字符串;含 image_url 块时保持数组)。
fn normalize_messages(messages: &[serde_json::Value]) -> (Vec<serde_json::Value>, String) {
    let mut system_parts: Vec<String> = Vec::new();
    let mut out = Vec::new();
    for m in messages {
        let role = m.get("role").and_then(|r| r.as_str()).unwrap_or("user");
        if role == "system" {
            let t = extract_text(m.get("content").unwrap_or(&serde_json::Value::Null));
            if !t.is_empty() {
                system_parts.push(t);
            }
            continue;
        }
        let mut cloned = m.clone();
        let content = m.get("content").cloned().unwrap_or(serde_json::Value::Null);
        let has_image = content.as_array().is_some_and(|parts| {
            parts.iter().any(|p| {
                p.get("type").and_then(|t| t.as_str()) == Some("image_url")
                    || p.get("type").and_then(|t| t.as_str()) == Some("image")
            })
        });
        cloned["content"] = if has_image {
            // image 块保留为 image_url 形状,文本块保序
            content
        } else {
            serde_json::Value::String(extract_text(&content))
        };
        out.push(cloned);
    }
    (out, system_parts.join("\n\n"))
}

fn last_user_text(messages: &[serde_json::Value]) -> String {
    for m in messages.iter().rev() {
        if m.get("role").and_then(|r| r.as_str()) == Some("user") {
            return extract_text(m.get("content").unwrap_or(&serde_json::Value::Null));
        }
    }
    String::new()
}

fn short_hash(parts: &[&str]) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    for p in parts {
        h.update("\0");
        h.update(p.as_bytes());
    }
    let d = h.finalize();
    hex::encode(&d[..8])
}

/// OpenAI chat 请求 → Qoder payload(执行器同款形状)。
pub fn build_chat_payload(
    body: &serde_json::Value,
    qoder_key: &str,
    model_config: &serde_json::Value,
    user_id: &str,
    rng: &QoderRandom,
) -> serde_json::Value {
    let messages = body
        .get("messages")
        .and_then(|m| m.as_array())
        .cloned()
        .unwrap_or_default();
    let (messages, system_text) = normalize_messages(&messages);
    let last_user = last_user_text(&messages);
    let is_reasoning = model_config
        .get("is_reasoning")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let max_out = model_config
        .get("max_output_tokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let mut max_tokens: u64 = if max_out > 0 { max_out } else { 32_768 };
    for k in ["max_tokens", "max_completion_tokens"] {
        if let Some(v) = body.get(k).and_then(|v| v.as_u64()) {
            if v > 0 && v < max_tokens {
                max_tokens = v;
            }
        }
    }
    let session_id = short_hash(&["qoder-session", user_id, qoder_key]);
    let record_id = short_hash(&[
        "qoder-record",
        qoder_key,
        &serde_json::to_string(&messages).unwrap_or_default(),
        &max_tokens.to_string(),
    ]);
    let request_id = (rng.uuid)();
    serde_json::json!({
        "request_id": request_id,
        "request_set_id": record_id,
        "chat_record_id": record_id,
        "session_id": session_id,
        "stream": true,
        "chat_task": "FREE_INPUT",
        "is_reply": true,
        "is_retry": false,
        "source": 1,
        "version": "3",
        "session_type": "qodercli",
        "agent_id": "agent_common",
        "task_id": "common",
        "code_language": "",
        "chat_prompt": "",
        "image_urls": null,
        "aliyun_user_type": "",
        "system": system_text,
        "messages": messages,
        "tools": body.get("tools").cloned().unwrap_or_else(|| serde_json::json!([])),
        "parameters": { "max_tokens": max_tokens },
        "chat_context": {
            "chatPrompt": "",
            "imageUrls": null,
            "extra": {
                "context": [],
                "modelConfig": { "key": qoder_key, "is_reasoning": is_reasoning },
                "originalContent": last_user,
            },
            "features": [],
            "text": last_user,
        },
        "model_config": model_config,
        "business": {
            "product": "cli",
            "version": "1.0.0",
            "type": "agent",
            "stage": "start",
            "id": (rng.uuid)(),
            "name": last_user.chars().take(30).collect::<String>(),
            "begin_at": (rng.now)(),
        },
    })
}

// ---------------------------------------------------------------------------
// SSE 信封解包
// ---------------------------------------------------------------------------

/// 一行上游 data 的解包结果。
#[derive(Debug, Clone, PartialEq)]
pub enum EnvelopeLine {
    /// 内层是完整 OpenAI chunk(原样字符串)。
    Chunk(String),
    /// 终止。
    Done,
    /// 上游错误(statusCodeValue != 200)。
    Error(u16, String),
    /// 无法解析(忽略该行)。
    Ignore,
}

pub fn unwrap_envelope_line(line: &str) -> EnvelopeLine {
    let t = line.trim_start();
    let Some(data) = t.strip_prefix("data:") else {
        return EnvelopeLine::Ignore;
    };
    let data = data.trim_start();
    if data == "[DONE]" {
        return EnvelopeLine::Done;
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(data) else {
        return EnvelopeLine::Ignore;
    };
    let status = v
        .get("statusCodeValue")
        .and_then(|s| s.as_u64())
        .unwrap_or(200) as u16;
    let inner = v.get("body").and_then(|b| b.as_str()).unwrap_or("");
    if status != 200 {
        return EnvelopeLine::Error(status, inner.to_string());
    }
    if inner == "[DONE]" {
        return EnvelopeLine::Done;
    }
    if inner.is_empty() {
        return EnvelopeLine::Ignore;
    }
    // 内层不得带裸换行(SSE 单事件一行)。
    EnvelopeLine::Chunk(inner.replace(['\r', '\n'], ""))
}

/// 计费封锁码(110=日次数超限,112=额度耗尽,10605=排队限流)。
pub fn billing_block(inner: &str) -> Option<(&'static str, u16)> {
    if inner.contains("\"code\":\"110\"") || inner.contains("\"code\":\"112\"") {
        return Some(("quota_exhausted", 403));
    }
    if inner.contains("\"code\":\"10605\"") {
        return Some(("queue_throttle", 429));
    }
    if inner.to_ascii_lowercase().contains("pricingurl") {
        return Some(("pricing_url", 403));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // openssl 交叉验证向量(2026-10-04 本机生成):
    // key=16 字符 "aaaaaaaa-bbbb-cc",IV=key,PKCS7,明文=user info json。
    #[test]
    fn aes_cbc_matches_openssl_vector() {
        let info =
            r#"{"uid":"U123","security_oauth_token":"dt-test","name":"","aid":"","email":""}"#;
        let got = aes_c64(info, "aaaaaaaa-bbbb-cc");
        assert_eq!(
            got,
            "Enj9D0rGE4OCBcMKBu71/4lVgWNh4n6lrjlh6xNuKzyxnuhu88GIvDhgB8Yx58WArusb1O0JsFJL6RS9SPxJsfu2WzvBm23lCYk9T09K0tw="
        );
    }
    fn aes_c64(plaintext: &str, key: &str) -> String {
        aes_cbc_b64(plaintext, key).unwrap()
    }

    // python 独立实现对照的 WAF 编码向量。
    #[test]
    fn encode_body_matches_python_reference() {
        assert_eq!(
            String::from_utf8(encode_body(b"hello")).unwrap(),
            "q$FruHPH"
        );
        assert_eq!(
            String::from_utf8(encode_body(br#"{"a":"bcd"}"#)).unwrap(),
            "zBEw$Mn*#OjmYKiB"
        );
        // 长文本:三段重排正确性(往返)
        let plain =
            r#"{"messages":[{"role":"user","content":"你好,世界"}],"model":"qfmodel"}"#.repeat(4);
        let enc = encode_body(plain.as_bytes());
        assert_ne!(enc, plain.as_bytes());
    }

    #[test]
    fn encode_decode_roundtrip() {
        let plain: Vec<u8> = r#"{"uid":"U1","prompt":"中文+emoji😀"}"#.repeat(10).into_bytes();
        let enc = encode_body(&plain);
        let dec = decode_body(&enc).unwrap();
        assert_eq!(dec, plain);
    }

    // md5 签名公式向量(2026-10-04 md5 命令生成)。
    #[test]
    fn signature_formula_matches_md5_vector() {
        let digest = format!(
            "{:x}",
            md5::Md5::digest(
                b"COSYPAYLOAD64\nCOSYKEY64\n1700000000\nTESTBODY\n/api/v2/service/pro/sse/agent_chat_generation"
            )
        );
        assert_eq!(digest, "65b7dfc7c1a5e441a362c8314973dc96");
    }

    /// 结构性签名验证:固定 uuid/ts 下,头集合的 sig 可由自身部件重算复核,
    /// Authorization 形状/Bodyhash/Sigpath 全对——这是 mock 上游将要做的同款校验。
    #[test]
    fn cosy_headers_structurally_verifiable() {
        let creds = Creds {
            user_id: "U123".into(),
            token: "dt-test".into(),
        };
        let rng = QoderRandom {
            uuid: Box::new(|| "11111111-2222-3333-4444-555555555555".to_string()),
            now: Box::new(|| 1_700_000_000),
        };
        let url = chat_url(CN_CHAT_BASE);
        let headers = cosy_headers(b"TESTBODY", &url, &creds, &rng).unwrap();
        let get = |k: &str| {
            headers
                .iter()
                .find(|(hk, _)| hk == k)
                .map(|(_, v)| v.clone())
                .unwrap()
        };
        let auth = get("Authorization");
        assert!(auth.starts_with("Bearer "), "{auth}");
        let (tag, rest) = auth["Bearer ".len()..].split_once('.').unwrap();
        let (payload64, sig) = rest.split_once('.').unwrap();
        assert_eq!(tag, "COSY");
        // payload 可解码,info 是 AES 密文,cosyVersion 对
        let payload = B64.decode(payload64).unwrap();
        let pv: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(pv["version"], "v1");
        assert_eq!(pv["requestId"], "11111111-2222-3333-4444-555555555555");
        assert_eq!(pv["cosyVersion"], "1.0.0");
        assert!(pv["info"].as_str().unwrap().len() > 40);
        // sig = md5(payload64 \n cosyKey \n ts \n body \n sigPath)
        let cosy_key = get("Cosy-Key");
        let ts = get("Cosy-Date");
        let sig_path = get("Cosy-Sigpath");
        let mut input = Vec::new();
        input.extend_from_slice(payload64.as_bytes());
        input.push(b'\n');
        input.extend_from_slice(cosy_key.as_bytes());
        input.push(b'\n');
        input.extend_from_slice(ts.as_bytes());
        input.push(b'\n');
        input.extend_from_slice(b"TESTBODY");
        input.push(b'\n');
        input.extend_from_slice(sig_path.as_bytes());
        assert_eq!(sig, format!("{:x}", md5::Md5::digest(&input)));
        // 其余头
        assert_eq!(
            get("Cosy-Bodyhash"),
            format!("{:x}", md5::Md5::digest(b"TESTBODY"))
        );
        assert_eq!(get("Cosy-Bodylength"), "8");
        assert_eq!(get("Cosy-User"), "U123");
        assert_eq!(
            get("Cosy-Sigpath"),
            "/api/v2/service/pro/sse/agent_chat_generation"
        );
        assert_eq!(headers.len(), 19);
    }

    #[test]
    fn sig_path_strips_algo() {
        assert_eq!(
            compute_sig_path("https://gateway.qoder.com.cn/algo/api/v2/model/list"),
            "/api/v2/model/list"
        );
        assert_eq!(
            compute_sig_path("https://gateway.qoder.com.cn/algo/api/v2/service/pro/sse/agent_chat_generation?FetchKeys=1&Encode=1"),
            "/api/v2/service/pro/sse/agent_chat_generation"
        );
    }

    #[test]
    fn envelope_unwrap_and_billing() {
        let inner =
            r#"{"id":"x","object":"chat.completion.chunk","choices":[{"delta":{"content":"hi"}}]}"#;
        let line = format!(
            "data: {}",
            serde_json::json!({"statusCodeValue":200,"body":inner})
        );
        assert_eq!(
            unwrap_envelope_line(&line),
            EnvelopeLine::Chunk(inner.to_string())
        );
        assert_eq!(
            unwrap_envelope_line("data: {\"statusCodeValue\":200,\"body\":\"[DONE]\"}"),
            EnvelopeLine::Done
        );
        assert_eq!(unwrap_envelope_line("data: [DONE]"), EnvelopeLine::Done);
        assert_eq!(unwrap_envelope_line("event: ping"), EnvelopeLine::Ignore);
        assert_eq!(unwrap_envelope_line("data: not json"), EnvelopeLine::Ignore);
        assert_eq!(
            unwrap_envelope_line(
                r#"data: {"statusCodeValue":403,"body":"{\"code\":\"112\",\"message\":\"quota\"}"}"#
            ),
            EnvelopeLine::Error(403, r#"{"code":"112","message":"quota"}"#.into())
        );
        // 计费码识别(嵌套转义形态)
        let nested = "{\"code\":\"10605\",\"message\":\"{\\\"isQueued\\\":true}\"}";
        assert_eq!(billing_block(nested), Some(("queue_throttle", 429)));
        assert_eq!(
            billing_block("plain pricingUrl here"),
            Some(("pricing_url", 403))
        );
        assert_eq!(
            billing_block(r#"{"code":"110"}"#),
            Some(("quota_exhausted", 403))
        );
        assert_eq!(billing_block("normal"), None);
    }

    #[test]
    fn payload_shape_mirrors_executor() {
        let body = serde_json::json!({
            "model": "qfmodel",
            "stream": true,
            "max_tokens": 128,
            "messages": [
                {"role": "system", "content": "你是助手"},
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
                {"role": "user", "content": "再说一次"}
            ]
        });
        let cfg = serde_json::json!({"key":"qfmodel","is_reasoning":true,"max_output_tokens":8192});
        let rng = QoderRandom::fixed("99999999-8888-7777-6666-555555555555", 1_700_000_012);
        let p = build_chat_payload(&body, "qfmodel", &cfg, "U9", &rng);
        // system 提升
        assert_eq!(p["system"], "你是助手");
        assert_eq!(p["messages"].as_array().unwrap().len(), 3, "system 移出");
        assert_eq!(p["messages"][0]["content"], "hi");
        assert_eq!(p["model_config"]["key"], "qfmodel");
        assert_eq!(p["chat_context"]["extra"]["modelConfig"]["key"], "qfmodel");
        assert_eq!(p["chat_context"]["text"], "再说一次");
        assert_eq!(
            p["parameters"]["max_tokens"], 128,
            "客户端小于模型上限时用客户端值"
        );
        assert_eq!(p["business"]["product"], "cli");
        assert_eq!(p["session_type"], "qodercli");
        assert_eq!(p["business"]["begin_at"], 1_700_000_012);
        // max_tokens 上限生效
        let body2 =
            serde_json::json!({"model":"qfmodel","messages":[{"role":"user","content":"x"}]});
        let p2 = build_chat_payload(&body2, "qfmodel", &cfg, "U9", &rng);
        assert_eq!(p2["parameters"]["max_tokens"], 8192);
    }

    /// 签名毒化:非 UTF-8 / 空体 / 超长 URL,不 panic 只报错或容错。
    #[test]
    fn cosy_never_panics_on_poison() {
        let creds = Creds {
            user_id: String::new(),
            token: String::new(),
        };
        let rng = QoderRandom::real();
        assert!(cosy_headers(b"", "https://x/algo/p", &creds, &rng).is_err());
        let ok = Creds {
            user_id: "u".into(),
            token: "t".into(),
        };
        let weird: Vec<u8> = (0..=255u8).collect();
        let h = cosy_headers(
            &weird,
            "https://gateway.qoder.com.cn/algo/a?b=%zz",
            &ok,
            &rng,
        );
        assert!(h.is_ok(), "任意字节体也要能签:{h:?}");
    }
}
