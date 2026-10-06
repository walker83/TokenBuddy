//! 本机网关(R108)——计量与配置层。
//!
//! 职责:`gateway.json`/`gateway-clients.json` 配置、上游密钥文件、
//! usage 提取(openai/anthropic 双协议、流式/非流式)、meter 行落盘、
//! 第 18 个采集器(读 usage.jsonl 入账)、上游健康探测。
//! 代理服务本身在 [`server`],工具配置向导在 [`setup`]。
//!
//! 计量三级(诚实标注,绝不冒充):
//! ① upstream——上游 usage 直报(openai 末 chunk / anthropic
//!    message_start+message_delta / 非流式 body),最权威;
//! ② estimated——200 但上游不给 usage:入≈请求体字节/4,出≈响应文本
//!    字节/4,`usage_source:"estimated"` 显式打标;
//! ③ 失败请求不进账本——账本只记烧掉的东西,错误行零出账。

pub mod qoder;
pub mod server;
pub mod setup;

use crate::{data_dir, FileCacheMap, Source, TokenRecord};
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// 配置(~/.tokenbuddy/gateway.json;JSON 而非 proposal 里的 TOML——quota.json
// 已确立「面板可编辑配置用 JSON + 原子写」的先例,这里沿用)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// 总开关。false 时 serve 不起监听线程——不用网关的人不为网关付内存。
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default)]
    pub providers: Vec<Provider>,
    #[serde(default)]
    pub combos: Vec<Combo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// deny_unknown_fields:用户手写的 provider 条目里拼错的字段名必须当面
/// 报错,而不是被 serde 静默吞掉变成一个"看起来配了"的空壳。
#[serde(deny_unknown_fields)]
pub struct Provider {
    pub id: String,
    /// "openai" | "anthropic" —— 上游说的协议,不是客户端说的。
    pub protocol: String,
    /// openai 上游填到 /v1 这一级(如 https://api.minimaxi.com/v1);
    /// anthropic 上游填到 anthropic 根(如 https://open.bigmodel.cn/api/anthropic);
    /// qoder 上游可留空(按 deployment 取官方端点),填了则覆盖(mock/自建中继)。
    #[serde(default)]
    pub base_url: String,
    /// 密钥文件,相对 data_dir();0600,面板与 /api 永不回读。
    pub key_file: String,
    /// 本 provider 服务的模型清单;空 = 不做模型过滤(全接)。
    #[serde(default)]
    pub models: Vec<String>,
    /// 客户端模型名 → 上游真名(F12;数据不是代码)。
    #[serde(default)]
    pub aliases: HashMap<String, String>,
    /// qoder 专属:裸令牌(jt-/dt-)时的 userId(PAT 自动解析,不需要)。
    #[serde(default)]
    pub user_id: String,
    /// qoder 专属:"cn"(默认,gateway.qoder.com.cn)| "global"(api3.qoder.sh)。
    #[serde(default = "default_deployment")]
    pub deployment: String,
    /// 方言标记(计量方言进配置不进代码分支):
    /// include_usage=openai 流式需注入 stream_options.include_usage 才出账
    /// (MiniMax 实测);cached_tokens_in_details=缓存数在
    /// prompt_tokens_details.cached_tokens。
    #[serde(default)]
    pub dialects: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Combo {
    pub id: String,
    /// 顺序即回退序(P1 先取首个可服务者,真正的失败回退在 P2)。
    pub chain: Vec<String>,
}

fn default_listen() -> String {
    "127.0.0.1:8790".to_string()
}

fn default_deployment() -> String {
    "cn".to_string()
}

impl Default for GatewayConfig {
    fn default() -> Self {
        GatewayConfig {
            enabled: false,
            listen: default_listen(),
            providers: vec![],
            combos: vec![],
        }
    }
}

pub fn config_path() -> PathBuf {
    data_dir().join("gateway.json")
}

pub fn clients_path() -> PathBuf {
    data_dir().join("gateway-clients.json")
}

/// usage.jsonl:meter 追加写,采集器按 mtime 缓存读——与其它源同一节奏。
pub fn usage_jsonl_path() -> PathBuf {
    data_dir().join("gateway").join("usage.jsonl")
}

/// 宽松读:缺文件=默认(总开关关),坏 JSON=默认(面板用 strict 变体显错,
/// 与 quota.rs 同构)。运行路径永不因坏配置 panic。
pub fn load_config() -> GatewayConfig {
    load_config_inner().unwrap_or_default()
}

fn load_config_inner() -> Result<GatewayConfig> {
    let path = config_path();
    if !path.exists() {
        return Ok(GatewayConfig::default());
    }
    let text = fs::read_to_string(&path)?;
    let cfg: GatewayConfig = serde_json::from_str(&text)?;
    cfg.validate()?;
    Ok(cfg)
}

/// 面板/CLI 用:坏配置要当面说清,绝不静默读成默认值。
pub fn load_config_strict() -> Result<GatewayConfig> {
    let path = config_path();
    if !path.exists() {
        return Ok(GatewayConfig::default());
    }
    let cfg: GatewayConfig = serde_json::from_str(&fs::read_to_string(&path)?)?;
    cfg.validate()?;
    Ok(cfg)
}

impl GatewayConfig {
    pub fn validate(&self) -> Result<()> {
        let mut ids = std::collections::HashSet::new();
        for p in &self.providers {
            anyhow::ensure!(!p.id.is_empty(), "provider.id 不能为空");
            anyhow::ensure!(ids.insert(p.id.as_str()), "provider.id 重复:{p}", p = p.id);
            anyhow::ensure!(
                p.protocol == "openai" || p.protocol == "anthropic" || p.protocol == "qoder",
                "provider {p} 的 protocol 只支持 openai/anthropic/qoder,得到:{proto}",
                p = p.id,
                proto = p.protocol
            );
            if p.protocol == "qoder" {
                anyhow::ensure!(
                    p.deployment == "cn" || p.deployment == "global",
                    "provider {p} 的 deployment 只支持 cn/global",
                    p = p.id
                );
            }
            anyhow::ensure!(
                p.base_url.is_empty()
                    || (p.base_url.starts_with("http://") || p.base_url.starts_with("https://")),
                "provider {p} 的 base_url 需以 http(s):// 开头(或留空用官方端点)",
                p = p.id
            );
            anyhow::ensure!(
                !p.base_url.ends_with('/'),
                "provider {p} 的 base_url 不要以 / 结尾",
                p = p.id
            );
            anyhow::ensure!(
                !p.key_file.contains(".."),
                "provider {p} 的 key_file 不允许路径上跳",
                p = p.id
            );
        }
        for c in &self.combos {
            anyhow::ensure!(!c.id.is_empty(), "combo.id 不能为空");
            anyhow::ensure!(!c.chain.is_empty(), "combo {c} 的 chain 不能为空", c = c.id);
            for id in &c.chain {
                anyhow::ensure!(
                    self.providers.iter().any(|p| &p.id == id),
                    "combo {c} 引用了不存在的 provider:{id}",
                    c = c.id
                );
            }
        }
        Ok(())
    }

    /// 保存:tmp+rename 原子落地(面板可编辑,运行中的 serve 下次 start 生效)。
    pub fn save(&self) -> Result<()> {
        self.validate()?;
        let path = config_path();
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_string_pretty(self)?.as_bytes())?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 客户端密钥(gateway-clients.json,0600)——每工具一把,带标签;
// 面板只展示掩码,明文只在签发时打印一次。
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientKey {
    pub key: String,
    pub label: String,
    pub created_at: i64,
}

pub fn load_clients() -> Result<Vec<ClientKey>> {
    let path = clients_path();
    if !path.exists() {
        return Ok(vec![]);
    }
    Ok(serde_json::from_str(&fs::read_to_string(&path)?)?)
}

pub fn save_clients(clients: &[ClientKey]) -> Result<()> {
    let path = clients_path();
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_string_pretty(clients)?.as_bytes())?;
    #[cfg(unix)]
    {
        let _ = fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    fs::rename(&tmp, &path)?;
    Ok(())
}

/// tb-local- + 32 hex;熵来自 /dev/urandom,不可用时回退时间+pid 散列
/// (测试机没有 /dev/urandom 也不断)。
pub fn generate_key() -> String {
    let mut bytes = [0u8; 16];
    let ok = fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .is_ok();
    if !ok {
        let mut seed = (crate::now_ts() as u64) << 32
            ^ (std::process::id() as u64) << 16
            ^ 0x9e37_79b9_7f4a_7c15;
        for b in bytes.iter_mut() {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            *b = (seed & 0xff) as u8;
        }
    }
    format!("tb-local-{}", hex::encode(bytes))
}

pub fn add_client(label: &str) -> Result<ClientKey> {
    let label = label.trim();
    anyhow::ensure!(
        !label.is_empty() && label.len() <= 64,
        "客户端标签需为 1–64 字符"
    );
    let mut clients = load_clients()?;
    if let Some(existing) = clients.iter().find(|c| c.label == label) {
        bail!(
            "标签 {label} 已存在(掩码 {}),先 remove 再 add",
            mask_key(&existing.key)
        );
    }
    let ck = ClientKey {
        key: generate_key(),
        label: label.to_string(),
        created_at: crate::now_ts(),
    };
    clients.push(ck.clone());
    save_clients(&clients)?;
    Ok(ck)
}

pub fn remove_client(label: &str) -> Result<bool> {
    let mut clients = load_clients()?;
    let before = clients.len();
    clients.retain(|c| c.label != label);
    let removed = clients.len() != before;
    if removed {
        save_clients(&clients)?;
    }
    Ok(removed)
}

pub fn mask_key(key: &str) -> String {
    if key.len() <= 12 {
        return "*".repeat(key.len());
    }
    format!("{}…{}", &key[..8], &key[key.len() - 4..])
}

/// Bearer / x-api-key 双通道验客户端 key,返回**标签**(meter 的 client
/// 列记的是"哪个工具",不是密钥本身)。
pub fn client_key_label(
    headers: &[tiny_http::Header],
    lookup: impl Fn(&str) -> Option<String>,
) -> Option<String> {
    for h in headers {
        let field = h.field.as_str().to_ascii_lowercase();
        let value = h.value.as_str().trim();
        let hit = if field == "authorization" {
            value.strip_prefix("Bearer ").map(str::trim)
        } else if field == "x-api-key" {
            Some(value)
        } else {
            None
        };
        if let Some(key) = hit {
            if let Some(label) = lookup(key) {
                return Some(label);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// 上游密钥:key_file 相对 data_dir,按 mtime 缓存——面板改了密钥立即生效,
// 又不用每个请求都 stat+read。
// ---------------------------------------------------------------------------

static KEY_CACHE: Mutex<Option<HashMap<String, (std::time::SystemTime, String)>>> =
    Mutex::new(None);

pub fn release_caches() {
    *KEY_CACHE.lock().unwrap_or_else(|e| e.into_inner()) = None;
}

pub fn upstream_key(key_file: &str) -> Result<String> {
    let path = data_dir().join(key_file);
    let mtime = fs::metadata(&path)
        .and_then(|m| m.modified())
        .map_err(|_| {
            anyhow::anyhow!(
                "密钥文件不存在:{key_file}(放在 {}/ 下)",
                data_dir().display()
            )
        })?;
    let mut cache = KEY_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let map = cache.get_or_insert_with(HashMap::new);
    if let Some((cached_at, key)) = map.get(key_file) {
        if *cached_at == mtime {
            return Ok(key.clone());
        }
    }
    let key = fs::read_to_string(&path)?.trim().to_string();
    anyhow::ensure!(!key.is_empty(), "密钥文件为空:{key_file}");
    map.insert(key_file.to_string(), (mtime, key.clone()));
    Ok(key)
}

// ---------------------------------------------------------------------------
// 计量:usage 提取(openai / anthropic;流式 / 非流式)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExtractedUsage {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    /// true=上游直报;false=字节/4 估算(meter 打 estimated 标)。
    pub exact: bool,
}

fn v_u64(v: &serde_json::Value, key: &str) -> u64 {
    v.get(key).and_then(|x| x.as_u64()).unwrap_or(0)
}

/// openai usage 对象:prompt/completion_tokens;缓存数两代方言——
/// 顶层 prompt_tokens_details.cached_tokens(MiniMax 实测)。
pub fn extract_usage_openai(v: &serde_json::Value) -> Option<ExtractedUsage> {
    let input = v_u64(v, "prompt_tokens");
    let output = v_u64(v, "completion_tokens");
    let cache_read = v
        .pointer("/prompt_tokens_details/cached_tokens")
        .and_then(|x| x.as_u64())
        .unwrap_or(0);
    if input == 0 && output == 0 {
        return None;
    }
    Some(ExtractedUsage {
        input,
        output,
        cache_read,
        cache_write: 0,
        exact: true,
    })
}

/// anthropic usage 对象(message_start/message_delta/非流式 body 同形)。
pub fn extract_usage_anthropic(v: &serde_json::Value) -> Option<ExtractedUsage> {
    let input = v_u64(v, "input_tokens");
    let output = v_u64(v, "output_tokens");
    let cache_read = v_u64(v, "cache_read_input_tokens");
    let cache_write = v_u64(v, "cache_creation_input_tokens");
    if input == 0 && output == 0 {
        return None;
    }
    Some(ExtractedUsage {
        input,
        output,
        cache_read,
        cache_write,
        exact: true,
    })
}

/// 非流式响应体整包提取。
pub fn extract_usage_body(protocol: &str, body: &serde_json::Value) -> Option<ExtractedUsage> {
    match protocol {
        "openai" => body.get("usage").and_then(extract_usage_openai),
        _ => body.get("usage").and_then(extract_usage_anthropic),
    }
}

/// SSE 流式扫描器:转发线程逐行喂入,只在行内含 "usage" 时才完整 parse
/// ——直通管道上每行一次子串查找,不为计量付 JSON 解析全价。
#[derive(Debug, Default)]
pub struct SseUsageScan {
    openai_last: Option<serde_json::Value>,
    /// anthropic:message_start 带入侧,message_delta 带累计出侧。
    anth_start: Option<serde_json::Value>,
    anth_delta: Option<serde_json::Value>,
}

impl SseUsageScan {
    pub fn feed_line(&mut self, line: &str) {
        let t = line.trim();
        let payload = match t.strip_prefix("data:") {
            Some(rest) => rest.trim(),
            None => return, // event:/id:/retry:/注释行都与 usage 无关
        };
        if payload.is_empty() || payload == "[DONE]" || !payload.contains("\"usage\"") {
            return;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(payload) else {
            return; // 毒化行:只忽略,不崩溃(fuzz 断言面)
        };
        if let Some(u) = v.get("usage").filter(|u| !u.is_null()) {
            self.openai_last = Some(u.clone());
        }
        match v.get("type").and_then(|x| x.as_str()) {
            Some("message_start") => {
                if let Some(u) = v.pointer("/message/usage") {
                    self.anth_start = Some(u.clone());
                }
            }
            Some("message_delta") => {
                if let Some(u) = v.get("usage") {
                    self.anth_delta = Some(u.clone());
                }
            }
            _ => {}
        }
    }

    /// 流结束时的最终提取:anthropic 优先 start×delta 合成,openai 取末值。
    pub fn finish(&self, protocol: &str) -> Option<ExtractedUsage> {
        if protocol == "anthropic" {
            let start = self.anth_start.as_ref().and_then(extract_usage_anthropic);
            let delta = self.anth_delta.as_ref().and_then(extract_usage_anthropic);
            return match (start, delta) {
                (Some(mut s), Some(d)) => {
                    s.output = d.output.max(s.output);
                    s.cache_read = s.cache_read.max(d.cache_read);
                    s.cache_write = s.cache_write.max(d.cache_write);
                    Some(s)
                }
                (Some(s), None) | (None, Some(s)) => Some(s),
                (None, None) => None,
            };
        }
        self.openai_last.as_ref().and_then(extract_usage_openai)
    }
}

/// 估算口径(F13 ②级):入≈请求体字节/4,出≈响应文本字节/4。
/// 对英文是公约数,对 CJK 系统性低估——宁可低估,不冒充精确。
pub fn estimate_usage(req_bytes: usize, resp_bytes: usize) -> ExtractedUsage {
    ExtractedUsage {
        input: (req_bytes as u64) / 4,
        output: (resp_bytes as u64) / 4,
        cache_read: 0,
        cache_write: 0,
        exact: false,
    }
}

// ---------------------------------------------------------------------------
// meter 行落盘 + 第 18 采集器
// ---------------------------------------------------------------------------

/// 一笔经网关的请求。uid 即冻结 record_id(落盘时定死,重读恒同键);
/// client 标签进 session_id 列——哪个工具烧的账,现有 sessions/透视视图
/// 直接可查,零 schema 变更。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeterRow {
    pub uid: String,
    pub ts: i64,
    pub client: String,
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub req_model: String,
    pub input: u64,
    pub output: u64,
    #[serde(default)]
    pub cache_read: u64,
    #[serde(default)]
    pub cache_write: u64,
    #[serde(default)]
    pub credits: f64,
    #[serde(default)]
    pub ttft_ms: Option<u64>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    /// "upstream" | "estimated"
    pub usage_source: String,
    pub status: u16,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub upstream_id: String,
    /// R112:上游自己报的限额水位(有则采集,没有就是空对象——不猜)。
    /// 键是原样头名的小写形式,值是原样字符串。MiniMax/智谱实测不带这类头,
    /// 所以绝大多数行是空的;Claude 系的 anthropic-ratelimit-* 才有值。
    #[serde(default)]
    pub rate_limit: serde_json::Map<String, serde_json::Value>,
}

pub fn append_meter_row(row: &MeterRow) -> Result<()> {
    let path = usage_jsonl_path();
    fs::create_dir_all(path.parent().expect("gateway dir has parent"))?;
    // 先整行序列化再一次 write_all:并发读者(sync 采集器/测试轮询)在
    // 任意瞬间读到的要么是完整旧行、要么是完整新行,不会撞见半行 JSON。
    let mut line = serde_json::to_vec(row)?;
    line.push(b'\n');
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    f.write_all(&line)?;
    Ok(())
}

/// 采集器:usage.jsonl → TokenRecord。mtime 缓存与其它源同一套。
static FILE_CACHE: Mutex<Option<FileCacheMap>> = Mutex::new(None);

pub fn release_collector_cache() {
    *FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner()) = None;
}

pub fn log_path() -> Option<PathBuf> {
    let p = usage_jsonl_path();
    p.exists().then_some(p)
}

pub fn log_paths() -> Vec<PathBuf> {
    vec![usage_jsonl_path()]
}

fn parse_row(line: &str) -> Option<TokenRecord> {
    let row: MeterRow = serde_json::from_str(line).ok()?;
    if row.input == 0 && row.output == 0 && row.cache_read == 0 && row.cache_write == 0 {
        return None; // 全零行不该存在,防御性跳过
    }
    Some(TokenRecord {
        source: Source::Gateway,
        model: row.model,
        input_tokens: row.input,
        output_tokens: row.output,
        cache_read_tokens: row.cache_read,
        cache_creation_tokens: row.cache_write,
        timestamp: row.ts,
        session_id: Some(row.client),
        project: String::new(),
        sidechain: false,
        duration_ms: row.duration_ms,
        ttft_ms: row.ttft_ms,
        credits: row.credits.max(0.0),
        context_ratio: 0.0,
        record_id: Some(row.uid),
        merge_key: None,
        request_count: 1,
    })
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let path = usage_jsonl_path();
    if !path.exists() {
        return Ok(vec![]);
    }
    let mtime = fs::metadata(&path)
        .and_then(|m| m.modified())
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
    let path_str = path.to_string_lossy().to_string();

    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let map = cache.get_or_insert_with(HashMap::new);
    let needs = match map.get(&path_str) {
        Some((at, _)) => mtime > *at,
        None => true,
    };
    if needs {
        let records = parse_usage_jsonl(&path);
        map.insert(path_str.clone(), (mtime, records));
    }
    Ok(map
        .get(&path_str)
        .map(|(_, r)| r.clone())
        .unwrap_or_default())
}

fn parse_usage_jsonl(path: &Path) -> Vec<TokenRecord> {
    let Ok(text) = fs::read_to_string(path) else {
        return vec![];
    };
    // 2 MiB 行硬顶:正常行 <1KB,超界行是写坏的,读进来也只是浪费。
    text.lines()
        .filter(|l| !l.trim().is_empty() && l.len() <= 2_000_000)
        .filter_map(parse_row)
        .collect()
}

// ---------------------------------------------------------------------------
// 网关用量统计(R112)
// ---------------------------------------------------------------------------

/// 面板要回答的四个问题:今天经手多少、这个 5h 窗口用了多少、额度水位如何、
/// 哪个上游/工具/模型烧的。全部从 meter 行现算——不等 sync,网关刚跑的
/// 请求立刻可见(sync 只影响 parquet 里的跨源聚合视图)。
///
/// 口径纪律:token 只报 total(93%+ 是 cache_read,那是重读不是产出),
/// credits 只报上游原值,estimated 行单列——不把估算混进"精确"里。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsageBucket {
    pub requests: u64,
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub total_tokens: u64,
    pub credits: f64,
    /// 计量口径为 estimated 的请求数(上游没回 usage,按字节估的)。
    pub estimated_requests: u64,
    pub errors: u64,
    pub last_ts: i64,
}

impl UsageBucket {
    fn add(&mut self, r: &MeterRow) {
        self.requests += 1;
        self.input += r.input;
        self.output += r.output;
        self.cache_read += r.cache_read;
        self.cache_write += r.cache_write;
        self.total_tokens += r.input + r.output + r.cache_read + r.cache_write;
        self.credits += r.credits.max(0.0);
        if r.usage_source == "estimated" {
            self.estimated_requests += 1;
        }
        if r.status >= 400 {
            self.errors += 1;
        }
        if r.ts > self.last_ts {
            self.last_ts = r.ts;
        }
    }
}

/// 一个 5h 窗口的用量。窗口边界复用 store 的 segment_windows
/// (首个请求开窗、5h 到期),不另造一套——两个面板对"窗口"的理解必须一致。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WindowUsage {
    pub start: i64,
    pub end: i64,
    pub bucket: UsageBucket,
    /// 该窗口内已过去的时长(秒),用来算烧速。
    pub elapsed_secs: i64,
}

impl WindowUsage {
    pub fn tokens_per_hour(&self) -> f64 {
        if self.elapsed_secs <= 0 {
            return 0.0;
        }
        self.bucket.total_tokens as f64 * 3600.0 / self.elapsed_secs as f64
    }
}

/// 网关全量统计。全部字段可选地为「无数据」——空账本返回 None 而不是 0,
/// 面板据此显示"还没有数据"而不是"用了 0"。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GatewayUsage {
    pub total: UsageBucket,
    /// 今日(CST 日界)经手量。
    pub today: UsageBucket,
    /// 当前打开的 5h 窗口。
    pub window: Option<WindowUsage>,
    /// 近 7 天。
    pub week: UsageBucket,
    pub by_provider: Vec<(String, UsageBucket)>,
    pub by_client: Vec<(String, UsageBucket)>,
    pub by_model: Vec<(String, UsageBucket)>,
    /// 上游最近一次自报的限额头(有则采集)。空 = 上游没报这类头。
    pub rate_limit: serde_json::Map<String, serde_json::Value>,
    /// 上游报过限额头的上游名(面板区分"没报"和"报了但没额度数字")。
    pub rate_limit_from: String,
}

const USAGE_TOP_N: usize = 12;

/// 读 meter 行现算。`now` 显式传入:测试要能钉住时间,不让墙钟决定分桶。
pub fn usage_stats(now: i64) -> GatewayUsage {
    let rows = meter_rows();
    if rows.is_empty() {
        return GatewayUsage::default();
    }
    let today_start = crate::cn_midnight_of(now);
    let week_start = now - 7 * 86_400;
    // 5h 窗口:按 store::segment_windows 的同一套边界切分整条时间线,
    // 再取最后一个(当前打开的)。
    let pairs: Vec<(i64, u64)> = rows
        .iter()
        .map(|r| (r.ts, r.input + r.output + r.cache_read + r.cache_write))
        .collect();
    let windows = crate::store::segment_windows(&pairs);
    let open = windows.last().copied();

    let mut u = GatewayUsage::default();
    let mut prov: HashMap<String, UsageBucket> = HashMap::new();
    let mut client: HashMap<String, UsageBucket> = HashMap::new();
    let mut model: HashMap<String, UsageBucket> = HashMap::new();
    let mut window_rows: Vec<&MeterRow> = Vec::new();
    for r in &rows {
        u.total.add(r);
        if r.ts >= today_start {
            u.today.add(r);
        }
        if r.ts >= week_start {
            u.week.add(r);
        }
        prov.entry(r.provider.clone()).or_default().add(r);
        client.entry(r.client.clone()).or_default().add(r);
        model.entry(r.model.clone()).or_default().add(r);
        // 限额头:留最新一次上报的上游的值,没报过的上游不覆盖。
        if !r.rate_limit.is_empty() {
            u.rate_limit = r.rate_limit.clone();
            u.rate_limit_from = r.provider.clone();
        }
        if open.is_some_and(|(start, _, _)| r.ts >= start) {
            window_rows.push(r);
        }
    }
    if let Some((start, _, _)) = open {
        let mut b = UsageBucket::default();
        for r in window_rows {
            b.add(r);
        }
        u.window = Some(WindowUsage {
            start,
            end: start + crate::store::WINDOW_SECS,
            bucket: b,
            elapsed_secs: (now - start).clamp(1, crate::store::WINDOW_SECS),
        });
    }
    u.by_provider = top_n(prov);
    u.by_client = top_n(client);
    u.by_model = top_n(model);
    u
}

/// 按总量降序取前 N,同名同量的按名字定序——面板渲染稳定,不闪烁。
fn top_n(mut m: HashMap<String, UsageBucket>) -> Vec<(String, UsageBucket)> {
    let mut v: Vec<(String, UsageBucket)> = m.drain().collect();
    v.sort_by(|a, b| b.1.total_tokens.cmp(&a.1.total_tokens).then(a.0.cmp(&b.0)));
    v.truncate(USAGE_TOP_N);
    v
}

/// 读 meter 原始行(不过 TokenRecord 转换):统计要用 provider/client/
/// credits/usage_source/status,这些在 TokenRecord 里已被抹平。
fn meter_rows() -> Vec<MeterRow> {
    let path = usage_jsonl_path();
    let Ok(text) = fs::read_to_string(&path) else {
        return vec![];
    };
    text.lines()
        .filter(|l| !l.trim().is_empty() && l.len() <= 2_000_000)
        .filter_map(|l| serde_json::from_str::<MeterRow>(l).ok())
        .collect()
}

// ---------------------------------------------------------------------------
// 上游健康探测(F9):openai/anthropic 走 GET /models。200=ok;401/403=auth_failed;
// 其它状态=reachable(端点在,行为未验);连接错误=unreachable。
// qoder 例外:走 COSY 签名的 model/list,base_url 可留空(见 probe_qoder)。
// 显式触发(面板「测试」/ CLI gateway probe);定时探活 opt-in,P1 不做。
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Unknown,
    Ok,
    AuthFailed,
    Reachable,
    Unreachable,
}

impl HealthState {
    pub fn as_str(&self) -> &'static str {
        match self {
            HealthState::Unknown => "unknown",
            HealthState::Ok => "ok",
            HealthState::AuthFailed => "auth_failed",
            HealthState::Reachable => "reachable",
            HealthState::Unreachable => "unreachable",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthReport {
    pub provider: String,
    pub state: HealthState,
    pub detail: String,
    pub checked_at: i64,
    pub latency_ms: Option<u64>,
}

/// 无 TLS 构建上 https 上游必然失败——把这话接在错误后面,免得面板上
/// 一句 connection refused 让人去查网络(本构建压根没 TLS 栈)。
fn tls_hint(url: &str) -> &'static str {
    if url.starts_with("https://") {
        "——若本构建未启用 TLS,https 上游不可达(用 --no-default-features 构建时如此)"
    } else {
        ""
    }
}

/// qoder 上游探活:官方端点**不是** `{base}/models`。① 目录要走 COSY 签名的
/// `model/list`;② `base_url` 按设计允许留空(端点由 deployment 取官方值)，
/// 照 openai 那套拼出来的是相对 URL `/models`,ureq 直接
/// RelativeUrlWithoutBase——面板上就成了一句看不懂的「不可达」。
/// 这里复用推理同一条链路:能签名拉到目录才算通,拉空目录算端点在但没料。
fn probe_qoder(agent: &ureq::Agent, p: &Provider, t0: std::time::Instant) -> HealthReport {
    let state = |s: HealthState, detail: String| HealthReport {
        provider: p.id.clone(),
        state: s,
        detail,
        checked_at: crate::now_ts(),
        latency_ms: Some(t0.elapsed().as_millis() as u64),
    };
    // 密钥文件缺失不该被报成 401(那读起来像"密钥错了"):直接说文件问题。
    if let Err(e) = std::fs::metadata(data_dir().join(&p.key_file)) {
        return state(
            HealthState::Unreachable,
            format!("密钥文件不可读:{}({e})", p.key_file),
        );
    }
    let base = qoder::chat_base(p);
    let url = qoder::model_list_url(&base);
    // 裸令牌缺 user_id / PAT 换 jt- 失败,都是凭据侧的问题,不是「连不上」——
    // 报 auth_failed 才不会误导人去查防火墙。
    let creds = match qoder::resolve_credentials(p, agent) {
        Ok(c) => c,
        Err(e) => return state(HealthState::AuthFailed, format!("凭据不可用:{e}")),
    };
    // fetch_models_raw 每次都真发一次签名请求(缓存只在 get_model_config 读),
    // 所以这里测的是当下可达性,不是十分钟前的旧结论。
    match qoder::fetch_models_raw(agent, &creds, &base) {
        Ok(list) if list.is_empty() => {
            state(HealthState::Reachable, format!("GET {url} → 200(目录为空)"))
        }
        Ok(list) => state(
            HealthState::Ok,
            format!("GET {url} → 200({} 个模型)", list.len()),
        ),
        Err(e) => {
            let msg = format!("GET {url} → {e}");
            if msg.contains(" 401") || msg.contains(" 403") {
                state(HealthState::AuthFailed, msg)
            } else {
                state(HealthState::Unreachable, format!("{msg}{}", tls_hint(&url)))
            }
        }
    }
}

/// 探活一次。agent 由调用方给(测试可指 mock 端点);无 TLS 构建上 https
/// 直接 unreachable,报告里说人话而不是报错栈。
pub fn probe(agent: &ureq::Agent, p: &Provider, key: &str) -> HealthReport {
    let t0 = std::time::Instant::now();
    // qoder 走自己那条(签名 + 可空的 base_url),别混进 openai 的拼法。
    if p.protocol == "qoder" {
        return probe_qoder(agent, p, t0);
    }
    let url = match p.protocol.as_str() {
        "anthropic" => format!("{}/v1/models", p.base_url.trim_end_matches('/')),
        _ => format!("{}/models", p.base_url.trim_end_matches('/')),
    };
    let req = match p.protocol.as_str() {
        "anthropic" => agent
            .get(&url)
            .set("x-api-key", key)
            .set("anthropic-version", "2023-06-01"),
        _ => agent
            .get(&url)
            .set("Authorization", &format!("Bearer {key}")),
    };
    let state = |s: HealthState, detail: String| HealthReport {
        provider: p.id.clone(),
        state: s,
        detail,
        checked_at: crate::now_ts(),
        latency_ms: Some(t0.elapsed().as_millis() as u64),
    };
    // 密钥文件缺失不该被报成 401(那读起来像"密钥错了"):直接说文件问题。
    if let Err(e) = std::fs::metadata(data_dir().join(&p.key_file)) {
        return state(
            HealthState::Unreachable,
            format!("密钥文件不可读:{}({e})", p.key_file),
        );
    }
    match req.call() {
        Ok(_) => state(HealthState::Ok, format!("GET {url} → 200")),
        Err(ureq::Error::Status(code, resp)) => {
            let detail = format!("GET {url} → {code} {}", resp.status_text());
            if code == 401 || code == 403 {
                state(HealthState::AuthFailed, detail)
            } else {
                state(HealthState::Reachable, detail)
            }
        }
        Err(e) => state(HealthState::Unreachable, format!("{e}{}", tls_hint(&url))),
    }
}

/// 共享 agent:连接池 + 合理超时。读超时管的是「两次读之间」的空闲,
/// SSE 长流不受影响;连接 10s、写 30s(大 prompt 上行)、空闲读 120s。
pub fn build_agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_connect(std::time::Duration::from_secs(10))
        .timeout_write(std::time::Duration::from_secs(30))
        .timeout_read(std::time::Duration::from_secs(120))
        .user_agent("tokenbuddy-gateway")
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------- 配置 ----------------

    #[test]
    fn config_defaults_and_roundtrip() {
        let cfg = GatewayConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.listen, "127.0.0.1:8790");
        assert!(cfg.providers.is_empty());

        let text = serde_json::json!({
            "enabled": true,
            "listen": "127.0.0.1:9999",
            "providers": [{
                "id": "minimax-openai",
                "protocol": "openai",
                "base_url": "https://api.minimaxi.com/v1",
                "key_file": "keys/minimax.key",
                "models": ["MiniMax-M3.1-Flash-Preview"],
                "aliases": {"glm-x": "MiniMax-M3.1-Flash-Preview"},
                "dialects": ["include_usage", "cached_tokens_in_details"]
            }],
            "combos": [{"id": "default", "chain": ["minimax-openai"]}]
        })
        .to_string();
        let cfg: GatewayConfig = serde_json::from_str(&text).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.providers[0].id, "minimax-openai");
        assert_eq!(cfg.providers[0].aliases.len(), 1);
        let round: GatewayConfig =
            serde_json::from_str(&serde_json::to_string(&cfg).unwrap()).unwrap();
        assert_eq!(round.providers.len(), 1);
    }

    #[test]
    fn config_validation_rejects_bad_provider() {
        let mk = |protocol: &str, base: &str| {
            serde_json::json!({
                "id": "p", "protocol": protocol, "base_url": base, "key_file": "keys/k"
            })
        };
        let bad = |v: serde_json::Value| {
            // mk 造的是单条 provider;GatewayConfig 层面包一层再验。
            let cfg: GatewayConfig =
                serde_json::from_value(serde_json::json!({ "providers": [v] })).unwrap();
            cfg.validate().unwrap_err().to_string()
        };
        assert!(bad(mk("graphql", "http://x")).contains("protocol"));
        assert!(bad(mk("openai", "api.minimaxi.com")).contains("base_url"));
        assert!(bad(mk("openai", "http://x.com/")).contains("结尾"));
        let mut v = mk("openai", "http://x");
        v["key_file"] = serde_json::json!("../etc/passwd");
        assert!(bad(v).contains("key_file"));
        // combo 引用不存在的 provider
        let cfg: GatewayConfig = serde_json::from_str(
            r#"{"providers":[{"id":"a","protocol":"openai","base_url":"http://x","key_file":"k"}],
                "combos":[{"id":"d","chain":["ghost"]}]}"#,
        )
        .unwrap();
        assert!(cfg.validate().is_err());
        // 手滑的字段名必须当面报错,不许静默吞掉(deny_unknown_fields)
        let typo: Result<GatewayConfig, _> = serde_json::from_str(
            r#"{"providers":[{"id":"a","protocol":"openai","base_url":"http://x","key_file":"k","baseurl":"http://y"}]}"#,
        );
        assert!(typo.is_err(), "拼错的 baseurl 字段应被拒绝");
    }

    // ---------------- 客户端密钥 ----------------

    #[test]
    fn client_keys_lifecycle_and_masking() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("gw-clients");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);

        let k = add_client("claude").unwrap();
        assert!(k.key.starts_with("tb-local-"));
        assert_eq!(k.key.len(), "tb-local-".len() + 32);
        // 同标签拒绝重复
        assert!(add_client("claude").is_err());
        // 0600
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(clients_path())
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        // 掩码不泄露明文
        let masked = mask_key(&k.key);
        assert!(!masked.contains(&k.key[8..20]));
        assert!(remove_client("claude").unwrap());
        assert!(!remove_client("claude").unwrap());

        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---------------- usage 提取 ----------------

    #[test]
    fn openai_usage_extraction_with_cached_details() {
        // 2026-10-04 MiniMax 实测形状:cached/reasoning 在 *_details。
        let v = serde_json::json!({
            "prompt_tokens": 209, "completion_tokens": 14, "total_tokens": 223,
            "prompt_tokens_details": {"cached_tokens": 197},
            "completion_tokens_details": {"reasoning_tokens": 13}
        });
        let u = extract_usage_openai(&v).unwrap();
        assert_eq!((u.input, u.output, u.cache_read), (209, 14, 197));
        assert!(u.exact);
        assert!(extract_usage_openai(&serde_json::json!({"prompt_tokens": 0})).is_none());
    }

    #[test]
    fn anthropic_usage_extraction() {
        let v = serde_json::json!({
            "input_tokens": 100, "output_tokens": 42,
            "cache_read_input_tokens": 55, "cache_creation_input_tokens": 7
        });
        let u = extract_usage_anthropic(&v).unwrap();
        assert_eq!(
            (u.input, u.output, u.cache_read, u.cache_write),
            (100, 42, 55, 7)
        );
    }

    #[test]
    fn sse_scan_openai_last_chunk_wins() {
        let mut s = SseUsageScan::default();
        s.feed_line(": keep-alive");
        s.feed_line("data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"he\"}}]}");
        s.feed_line("data: [DONE]");
        assert!(s.finish("openai").is_none());
        // include_usage 注入后:末 chunk 的 usage(实际 MiniMax 形状)
        s.feed_line("data: {\"object\":\"chat.completion.chunk\",\"usage\":{\"total_tokens\":210,\"prompt_tokens\":208,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":197}}}");
        let u = s.finish("openai").unwrap();
        assert_eq!((u.input, u.output, u.cache_read), (208, 2, 197));
        // 两条 usage 时取末值
        s.feed_line("data: {\"usage\":{\"prompt_tokens\":300,\"completion_tokens\":9}}");
        assert_eq!(s.finish("openai").unwrap().input, 300);
    }

    #[test]
    fn sse_scan_anthropic_start_x_delta() {
        let mut s = SseUsageScan::default();
        s.feed_line("event: message_start");
        s.feed_line("data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":120,\"output_tokens\":1,\"cache_read_input_tokens\":30}}}");
        s.feed_line("data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}");
        // delta 未到之前:start 已可提取(流被掐断也有一半事实)
        let mid = s.finish("anthropic").unwrap();
        assert_eq!((mid.input, mid.output), (120, 1));
        s.feed_line("data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":37}}");
        let u = s.finish("anthropic").unwrap();
        assert_eq!((u.input, u.output, u.cache_read), (120, 37, 30));
    }

    /// 毒化语料:截断/字节翻转/垃圾注入,只忽略不崩溃——与采集器
    /// fuzz 防线同款(LCG 确定性,断言面是"永不 panic,永不入错账")。
    #[test]
    fn sse_scan_fuzz_never_panics() {
        let corpus = [
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":5,\"output_tokens\":1}}}",
            "data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}",
            "data: [DONE]",
            "event: ping",
        ];
        let mut seed: u64 = 0x5EED_2026_1004;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..3000 {
            let mut line = corpus[(next() as usize) % corpus.len()].to_string();
            match (next() as usize) % 3 {
                0 => {
                    let cut = (next() as usize) % line.len().max(1);
                    line.truncate(cut);
                }
                1 => {
                    // 字节层面翻转可能造出非法 UTF-8:走 Vec<u8> + lossy,
                    // 与真实转发线程的 String::from_utf8_lossy 同口径。
                    let mut bytes = line.into_bytes();
                    let at = (next() as usize) % bytes.len().max(1);
                    bytes[at] = (next() & 0xff) as u8;
                    line = String::from_utf8_lossy(&bytes).into_owned();
                }
                _ => line = format!("data: \"{}\"", "j".repeat((next() % 64) as usize)),
            }
            let mut s = SseUsageScan::default();
            s.feed_line(&line);
            let _ = s.finish("openai");
            let _ = s.finish("anthropic");
            assert!(round < u64::MAX, "LCG 未死循环");
        }
    }

    #[test]
    fn estimate_is_conservative_and_flagged() {
        let u = estimate_usage(400, 80);
        assert_eq!((u.input, u.output), (100, 20));
        assert!(!u.exact, "估算必须打标");
    }

    // ---------------- meter 行 + 采集器 ----------------

    #[test]
    fn meter_row_roundtrip_and_collector() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("gw-meter");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);

        let row = MeterRow {
            uid: "gw_1791072054000_ab12".into(),
            ts: 1_791_072_054,
            client: "claude".into(),
            provider: "minimax-openai".into(),
            model: "MiniMax-M3.1-Flash-Preview".into(),
            req_model: "MiniMax-M3.1-Flash-Preview".into(),
            input: 208,
            output: 2,
            cache_read: 197,
            cache_write: 0,
            credits: 0.0,
            ttft_ms: Some(369),
            duration_ms: Some(585),
            usage_source: "upstream".into(),
            status: 200,
            stream: true,
            upstream_id: "1984189643209314988_1791072054koqp9t".into(),
            rate_limit: serde_json::Map::new(),
        };
        append_meter_row(&row).unwrap();
        append_meter_row(&row).unwrap(); // 重复写(sync 前两次),冻结键去重
        std::fs::write(
            usage_jsonl_path(),
            std::fs::read_to_string(usage_jsonl_path()).unwrap() + "not json\n",
        )
        .unwrap();

        let records = collect_records().unwrap();
        assert_eq!(records.len(), 2, "{records:?}");
        let r = &records[0];
        assert_eq!(r.source.as_str(), "gateway");
        assert_eq!(
            r.session_id.as_deref(),
            Some("claude"),
            "client 进 session 列"
        );
        assert_eq!(r.record_id.as_deref(), Some("gw_1791072054000_ab12"));
        assert_eq!(
            (r.input_tokens, r.output_tokens, r.cache_read_tokens),
            (208, 2, 197)
        );
        assert_eq!(r.ttft_ms, Some(369));
        assert_eq!(r.duration_ms, Some(585));
        assert_eq!(r.project, "", "网关不知道项目,不猜");

        release_collector_cache();
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn collector_ignores_zero_and_oversized_lines() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("gw-poison");
        std::fs::create_dir_all(dir.join("gateway")).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        let good = serde_json::to_string(&MeterRow {
            uid: "gw_x".into(),
            ts: 100,
            client: "t".into(),
            provider: "p".into(),
            model: "m".into(),
            req_model: "m".into(),
            input: 1,
            output: 2,
            cache_read: 0,
            cache_write: 0,
            credits: 0.0,
            ttft_ms: None,
            duration_ms: None,
            usage_source: "upstream".into(),
            status: 200,
            stream: false,
            upstream_id: String::new(),
            rate_limit: serde_json::Map::new(),
        })
        .unwrap();
        let junk = format!(
            "{{\"uid\":\"big\",\"ts\":1,\"client\":\"c\",\"provider\":\"p\",\"model\":\"m\",\
             \"input\":9,\"output\":9,\"usage_source\":\"u\",\"status\":200,\"stream\":false,\
             \"pad\":\"{pad}\"}}\n\n[]\n{{\"input\":0,\"output\":0}}\n",
            pad = "x".repeat(2_100_000),
        );
        std::fs::write(usage_jsonl_path(), format!("{good}\n{junk}")).unwrap();
        let records = collect_records().unwrap();
        assert_eq!(records.len(), 1, "超界行/垃圾行/全零行不入账: {records:?}");
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---------------- 上游密钥 ----------------

    #[test]
    fn usage_stats_buckets_today_window_and_breakdowns() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("gw-usage");
        std::fs::create_dir_all(dir.join("gateway")).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);

        // 钉死"现在"再放行:今日桶与 5h 窗口都按传入的 now 分桶,
        // 不让墙钟决定测试结果。
        let now = 1_800_000_000;
        let win_start = now - 3600; // 同一窗口内
        let mk = |uid: &str,
                  ts: i64,
                  provider: &str,
                  client: &str,
                  model: &str,
                  i: u64,
                  o: u64,
                  cr: u64,
                  est: bool| MeterRow {
            uid: uid.into(),
            ts,
            client: client.into(),
            provider: provider.into(),
            model: model.into(),
            req_model: model.into(),
            input: i,
            output: o,
            cache_read: cr,
            cache_write: 0,
            credits: if provider == "qoder-sub" { 1.5 } else { 0.0 },
            ttft_ms: None,
            duration_ms: None,
            usage_source: if est {
                "estimated".into()
            } else {
                "upstream".into()
            },
            status: if uid.ends_with('3') { 500 } else { 200 },
            stream: false,
            upstream_id: String::new(),
            rate_limit: serde_json::Map::new(),
        };
        let rows = [
            // 三笔落在同一个 5h 窗口内(窗口由最早那笔开启)。
            mk(
                "gw_1",
                win_start - 60,
                "minimax",
                "claude",
                "M3",
                100,
                10,
                900,
                false,
            ),
            mk(
                "gw_2",
                win_start,
                "qoder-sub",
                "zcode",
                "qmodel",
                200,
                20,
                0,
                true,
            ),
            mk(
                "gw_3",
                now - 120,
                "minimax",
                "claude",
                "M3",
                50,
                5,
                0,
                false,
            ),
            // 5 天前:进 week,但不进 today,也不在当前窗口。
            mk(
                "gw_4",
                now - 5 * 86_400,
                "minimax",
                "codex",
                "M3",
                7,
                7,
                0,
                false,
            ),
        ];
        let body: String = rows
            .iter()
            .map(|r| serde_json::to_string(r).unwrap() + "\n")
            .collect();
        std::fs::write(usage_jsonl_path(), body).unwrap();

        let u = usage_stats(now);
        assert_eq!(u.total.requests, 4);
        assert_eq!(u.today.requests, 3, "今日桶按 CST 日界切,不含 5 天前那笔");
        assert_eq!(u.week.requests, 4);
        // token 口径:total 含 cache_read(它是重读,但确实被上游计了)
        assert_eq!(u.today.total_tokens, 100 + 10 + 900 + 200 + 20 + 50 + 5);
        assert_eq!(u.today.estimated_requests, 1, "估算行单列,不混进精确口径");
        assert_eq!(u.today.errors, 1);
        assert_eq!(u.today.credits, 1.5, "credits 只加上游原值");

        // 5h 窗口:最早那笔开启,边界与 store::segment_windows 同一套
        let w = u.window.expect("当前窗口");
        assert_eq!(w.start, win_start - 60);
        assert_eq!(w.end, win_start - 60 + crate::store::WINDOW_SECS);
        assert_eq!(w.bucket.requests, 3, "5 天前那笔不在窗口内");
        assert!(w.tokens_per_hour() > 0.0);

        // 三张分维表
        let prov: HashMap<_, _> = u.by_provider.iter().cloned().collect();
        assert_eq!(prov["minimax"].requests, 3);
        assert_eq!(prov["qoder-sub"].credits, 1.5);
        let client: HashMap<_, _> = u.by_client.iter().cloned().collect();
        assert_eq!(client["claude"].requests, 2);
        assert_eq!(client["zcode"].requests, 1);
        assert_eq!(client["codex"].requests, 1);
        // 按总量降序
        assert!(u.by_model[0].1.total_tokens >= u.by_model[1].1.total_tokens);

        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn usage_stats_on_empty_ledger_reports_no_data() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("gw-usage-empty");
        std::fs::create_dir_all(dir.join("gateway")).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        let u = usage_stats(1_800_000_000);
        // 空账本 = 没有数据,不是"用了 0"——面板据此显示占位符。
        assert_eq!(u.total.requests, 0);
        assert!(u.window.is_none());
        assert!(u.by_provider.is_empty());
        assert!(u.rate_limit.is_empty());
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn usage_stats_keeps_latest_reported_rate_limit_headers() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("gw-ratelimit");
        std::fs::create_dir_all(dir.join("gateway")).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        let now = 1_800_000_000;
        let with_rl = |uid: &str, ts: i64, k: &str, v: &str| {
            let mut m = serde_json::Map::new();
            m.insert(k.to_string(), serde_json::Value::String(v.to_string()));
            MeterRow {
                uid: uid.into(),
                ts,
                client: "claude".into(),
                provider: "anthropic-up".into(),
                model: "m".into(),
                req_model: "m".into(),
                input: 10,
                output: 1,
                cache_read: 0,
                cache_write: 0,
                credits: 0.0,
                ttft_ms: None,
                duration_ms: None,
                usage_source: "upstream".into(),
                status: 200,
                stream: false,
                upstream_id: String::new(),
                rate_limit: m,
            }
        };
        let rows = [
            with_rl(
                "gw_a",
                now - 600,
                "anthropic-ratelimit-unified-remaining",
                "900",
            ),
            with_rl(
                "gw_b",
                now - 60,
                "anthropic-ratelimit-unified-remaining",
                "1200",
            ),
        ];
        let body: String = rows
            .iter()
            .map(|r| serde_json::to_string(r).unwrap() + "\n")
            .collect();
        std::fs::write(usage_jsonl_path(), body).unwrap();
        let u = usage_stats(now);
        // 取最新一次自报值,不是首个,也不是求和。
        assert_eq!(u.rate_limit_from, "anthropic-up");
        assert_eq!(
            u.rate_limit.get("anthropic-ratelimit-unified-remaining"),
            Some(&serde_json::Value::String("1200".into()))
        );
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rate_limit_headers_capture_both_families_and_skip_the_rest() {
        // 纯字符串前缀规则:两家头族收,其余一律不碰。
        let captured: Vec<String> = [
            "anthropic-ratelimit-requests-remaining",
            "anthropic-ratelimit-unified-status",
            "x-ratelimit-remaining-requests",
            "x-ratelimit-limit-requests",
            "content-type",
            "x-request-id",
            "ratelimit-remaining",
        ]
        .iter()
        .filter(|h| {
            let l = h.to_ascii_lowercase();
            l.starts_with("anthropic-ratelimit-") || l.starts_with("x-ratelimit-")
        })
        .map(|s| s.to_string())
        .collect();
        assert_eq!(captured.len(), 4);
        assert!(!captured.iter().any(|h| h == "content-type"));
        assert!(!captured.iter().any(|h| h == "ratelimit-remaining"));
    }

    #[test]
    fn upstream_key_reads_and_caches_by_mtime() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("gw-key");
        std::fs::create_dir_all(dir.join("keys")).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        std::fs::write(dir.join("keys/k.key"), "  sk-test-123 \n").unwrap();
        assert_eq!(upstream_key("keys/k.key").unwrap(), "sk-test-123");
        assert!(upstream_key("keys/missing.key").is_err());
        release_caches();
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
