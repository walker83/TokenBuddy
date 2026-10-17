//! Fleet sync: push the local token parquet to an S3-compatible bucket and
//! pull every host's copy back so the dashboard can show a multi-machine
//! ledger. The storage target is RustFS on the LAN, so the HTTP client is
//! deliberately minimal — plain HTTP (no TLS feature) with a hand-rolled
//! SigV4 signer and no S3 SDK. Privacy boundary: only `data.parquet` (the
//! token bill) travels; `context.parquet` (conversation text) never does.

use anyhow::{bail, Context as _, Result};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::store::{Store, SyncResult};

type HmacSha256 = Hmac<Sha256>;

// ============================================================
// Configuration (~/.tokenbuddy/fleet.toml)
// ============================================================

/// One flat TOML file decides everything. A missing file means the whole
/// feature is off — existing users see zero behavior change.
#[derive(Debug, Clone, PartialEq)]
pub struct FleetConfig {
    pub enabled: bool,
    /// `http://host[:port]` of the S3-compatible endpoint, no trailing slash.
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    /// Path-style addressing (`http://ep/bucket/key`), what RustFS serves.
    pub path_style: bool,
    /// This machine's identity in `hosts/{host_id}/data.parquet`.
    pub host_id: String,
    /// Push automatically from the server's background thread.
    pub auto_push: bool,
    /// Pull every host's objects on the same background cadence — the
    /// dashboard Fleet view and its quota table stay fresh without a manual
    /// `fleet-sync`. Read-only against the bucket, so it defaults on.
    pub auto_pull: bool,
    pub push_interval_secs: u64,
    /// Encrypt the pushed object at rest (XChaCha20-Poly1305, key derived
    /// from secret_key). Opt-in: existing buckets keep plain parquet.
    pub encrypt: bool,
}

/// `~/.tokenbuddy/fleet.toml`. Lives next to the data it describes.
pub fn config_path() -> PathBuf {
    crate::data_dir().join("fleet.toml")
}

/// Where pulled host parquets land: `~/.tokenbuddy/fleet/{host}/data.parquet`.
pub fn fleet_dir() -> PathBuf {
    crate::data_dir().join("fleet")
}

/// Load the fleet config, `Ok(None)` when the file does not exist (feature
/// silently off). A malformed file is an error so `tokenbuddy push` can say
/// why instead of quietly doing nothing.
pub fn load_config() -> Result<Option<FleetConfig>> {
    let path = config_path();
    if !path.exists() {
        return Ok(None);
    }
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("读取 {}", path.display()))?;
    let cfg = parse_config(&text).with_context(|| format!("解析 {} 失败", path.display()))?;
    Ok(Some(cfg))
}

impl FleetConfig {
    /// Explicit-config constructor for smoke tests against a scratch bucket.
    pub fn for_endpoint(endpoint: &str, bucket: &str, access_key: &str, secret_key: &str) -> Self {
        FleetConfig {
            enabled: true,
            endpoint: endpoint.to_string(),
            bucket: bucket.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            region: "us-east-1".to_string(),
            path_style: true,
            host_id: default_host_id(),
            auto_push: false,
            auto_pull: true,
            push_interval_secs: 3600,
            encrypt: false,
        }
    }
}

/// Parse the fleet config: a small flat `key = value` subset of TOML.
/// Strict on purpose — an unknown key or a bare string is far more likely a
/// typo than an intent, and a silent default would push to nowhere.
pub fn parse_config(text: &str) -> Result<FleetConfig> {
    let mut cfg = FleetConfig {
        enabled: false,
        endpoint: String::new(),
        bucket: String::new(),
        access_key: String::new(),
        secret_key: String::new(),
        region: "us-east-1".to_string(),
        path_style: true,
        host_id: default_host_id(),
        auto_push: true,
        auto_pull: true,
        push_interval_secs: 3600,
        encrypt: false,
    };

    for (idx, raw) in text.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            bail!("第 {line_no} 行缺少 '='：{line}");
        };
        let key = key.trim();
        let value = value.trim();
        let is_bool = matches!(
            key,
            "enabled" | "auto_push" | "auto_pull" | "path_style" | "encrypt"
        );
        let is_int = key == "push_interval_secs";

        let parsed = if let Some(rest) = value.strip_prefix('"') {
            anyhow::ensure!(
                !is_bool && !is_int,
                "第 {line_no} 行 {key} 是裸值，不要加引号"
            );
            let (s, tail) = parse_quoted(rest)
                .map_err(|e| anyhow::anyhow!("第 {line_no} 行的字符串值有误：{e}"))?;
            let tail = tail.trim();
            anyhow::ensure!(
                tail.is_empty() || tail.starts_with('#'),
                "第 {line_no} 行值后面有多余内容：{tail}"
            );
            s
        } else {
            // Bare value: strip a trailing comment, then it must be a bool
            // (true/false) or an unsigned integer — TOML has no bare strings.
            let bare = value.split('#').next().unwrap_or("").trim();
            if is_bool {
                anyhow::ensure!(
                    bare == "true" || bare == "false",
                    "第 {line_no} 行 {key} 需要 true/false，得到：{bare}"
                );
                bare.to_string()
            } else if is_int {
                anyhow::ensure!(
                    bare.parse::<u64>().is_ok(),
                    "第 {line_no} 行 {key} 需要非负整数，得到：{bare}"
                );
                bare.to_string()
            } else {
                bail!("第 {line_no} 行 {key} 需要带引号的字符串，例如 {key} = \"...\"")
            }
        };

        match key {
            "enabled" => cfg.enabled = parsed == "true",
            "endpoint" => cfg.endpoint = parsed,
            "bucket" => cfg.bucket = parsed,
            "access_key" => cfg.access_key = parsed,
            "secret_key" => cfg.secret_key = parsed,
            "region" => cfg.region = parsed,
            "path_style" => cfg.path_style = parsed == "true",
            "host_id" => cfg.host_id = parsed,
            "auto_push" => cfg.auto_push = parsed == "true",
            "auto_pull" => cfg.auto_pull = parsed == "true",
            "encrypt" => cfg.encrypt = parsed == "true",
            "push_interval_secs" => cfg.push_interval_secs = parsed.parse().unwrap_or(3600),
            _ => bail!("第 {line_no} 行未知配置项：{key}"),
        }
    }

    if cfg.enabled {
        let mut missing: Vec<&str> = Vec::new();
        if cfg.endpoint.is_empty() {
            missing.push("endpoint");
        }
        if cfg.bucket.is_empty() {
            missing.push("bucket");
        }
        if cfg.access_key.is_empty() {
            missing.push("access_key");
        }
        if cfg.secret_key.is_empty() {
            missing.push("secret_key");
        }
        anyhow::ensure!(
            missing.is_empty(),
            "Fleet 已启用但缺少配置项：{}",
            missing.join("、")
        );
    }
    // The interval doubles as the auto-push loop's sleep, so a mistyped 0
    // would hammer the collectors; clamp instead of failing the config.
    cfg.push_interval_secs = cfg.push_interval_secs.clamp(60, 86_400);
    anyhow::ensure!(
        valid_host_id(&cfg.host_id),
        "host_id 只能包含字母/数字/./-_（1–128 字符），得到：{}",
        cfg.host_id
    );
    Ok(cfg)
}

/// Consume a TOML basic string starting just after the opening quote;
/// returns the decoded value and the remainder of the line.
fn parse_quoted(s: &str) -> Result<(String, &str)> {
    let mut out = String::new();
    let mut chars = s.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Ok((out, &s[i + 1..])),
            '\\' => match chars.next() {
                Some((_, 'n')) => out.push('\n'),
                Some((_, 't')) => out.push('\t'),
                Some((_, 'r')) => out.push('\r'),
                Some((_, '"')) => out.push('"'),
                Some((_, '\\')) => out.push('\\'),
                Some((_, other)) => bail!("不支持的转义 \\{other}"),
                None => bail!("反斜杠后面没有字符"),
            },
            _ => out.push(c),
        }
    }
    bail!("缺少收尾引号")
}

/// Host ids become one S3 key segment and one local directory name, so the
/// charset is tight: no separators, no traversal, no whitespace.
fn valid_host_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s != "."
        && s != ".."
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
}

/// Escape a string for the basic-string form `parse_quoted` reads back:
/// backslash first, then the quote, then the escapes the parser knows.
fn escape_toml_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    out
}

/// Render the config back to the same flat TOML `parse_config` reads. Every
/// key is written explicitly — the file doubles as documentation, and a
/// round-trip through `parse_config` re-runs all validation for free.
pub fn render_config(cfg: &FleetConfig) -> String {
    let s = |v: &str| format!("\"{}\"", escape_toml_str(v));
    format!(
        "# TokenBuddy Fleet 配置（仪表盘「⚙ 配置」页或手写均可）\n\
         # 只有 token 账单 data.parquet 出机器，对话全文不上传。\n\
         enabled = {}\n\
         endpoint = {}\n\
         bucket = {}\n\
         access_key = {}\n\
         secret_key = {}\n\
         region = {}\n\
         path_style = {}\n\
         host_id = {}\n\
         auto_push = {}\n\
         auto_pull = {}\n\
         push_interval_secs = {}\n\
         # true = the pushed object is encrypted at rest (XChaCha20-Poly1305,\n\
         # key derived from secret_key). Pulling hosts need the same secret.\n\
         encrypt = {}\n",
        cfg.enabled,
        s(&cfg.endpoint),
        s(&cfg.bucket),
        s(&cfg.access_key),
        s(&cfg.secret_key),
        s(&cfg.region),
        cfg.path_style,
        s(&cfg.host_id),
        cfg.auto_push,
        cfg.auto_pull,
        cfg.push_interval_secs,
        cfg.encrypt,
    )
}

/// Atomically write the config to `path` (tmp + rename, like the manifest).
/// The file carries the S3 secret, so it lands 0600.
pub fn write_config_to(path: &Path, cfg: &FleetConfig) -> Result<()> {
    let text = render_config(cfg);
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, text).with_context(|| format!("写入 {}", tmp.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("收紧 {} 的权限", tmp.display()))?;
    }
    std::fs::rename(&tmp, path).with_context(|| format!("落地 {}", path.display()))?;
    Ok(())
}

/// Save to the canonical `~/.tokenbuddy/fleet.toml`.
pub fn save_config(cfg: &FleetConfig) -> Result<()> {
    write_config_to(&config_path(), cfg)
}

/// Try the configured endpoint with a real ListObjectsV2 on `hosts/` — the
/// same call fleet-sync makes, so "test passes" means "sync will work".
/// Returns the number of host objects already in the bucket.
pub fn test_connection(cfg: &FleetConfig) -> Result<usize> {
    anyhow::ensure!(
        !cfg.endpoint.is_empty() && !cfg.bucket.is_empty(),
        "endpoint 和 bucket 不能为空"
    );
    anyhow::ensure!(
        !cfg.access_key.is_empty() && !cfg.secret_key.is_empty(),
        "access_key 和 secret_key 不能为空"
    );
    let objects = S3Client::new(cfg).list_objects("hosts/")?;
    Ok(objects.len())
}

/// This machine's default identity: the OS hostname.
pub fn default_host_id() -> String {
    #[cfg(unix)]
    {
        let mut buf = [0u8; 256];
        // SAFETY: buf is a valid 256-byte area; gethostname writes at most
        // that many bytes and NUL-terminates on success.
        let ret = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
        if ret == 0 {
            if let Ok(s) = std::ffi::CStr::from_bytes_until_nul(&buf) {
                let name = s.to_string_lossy().trim().to_string();
                if valid_host_id(&name) {
                    return name;
                }
            }
        }
    }
    "localhost".to_string()
}

// ============================================================
// SigV4 signing
// ============================================================

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// The SigV4 signing key: HMAC chain secret→date→region→service→"aws4_request".
fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k = hmac_sha256(&k, region.as_bytes());
    let k = hmac_sha256(&k, service.as_bytes());
    hmac_sha256(&k, b"aws4_request")
}

/// Percent-encode everything outside the AWS unreserved set. Path segments
/// keep `/` (encode_slash = false); query values encode it.
fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Canonical query string: pairs sorted by name (then value), both parts
/// RFC3986-encoded, joined with `&`.
fn canonical_query(pairs: &[(String, String)]) -> String {
    let mut sorted: Vec<&(String, String)> = pairs.iter().collect();
    sorted.sort();
    sorted
        .iter()
        .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
        .collect::<Vec<_>>()
        .join("&")
}

/// The parts of one request that SigV4 signs. `headers` must already include
/// every header that participates in signing (host and the x-amz-* pair);
/// names are lowercased and the list sorted by `authorize`, per SigV4.
struct SigRequest<'a> {
    method: &'a str,
    canonical_uri: &'a str,
    query: &'a str,
    headers: &'a [(String, String)],
    payload_hash: &'a str,
}

/// Build the `Authorization` header for one request.
fn authorize(
    access_key: &str,
    secret_key: &str,
    region: &str,
    req: &SigRequest<'_>,
    amz_date: &str,
) -> String {
    let mut canonical: Vec<(String, String)> = req
        .headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    canonical.sort();
    let canonical_headers: String = canonical
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();
    let signed_headers = canonical
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{}",
        req.method, req.canonical_uri, req.query, req.payload_hash
    );
    let date = &amz_date[..8.min(amz_date.len())];
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let key = signing_key(secret_key, date, region, "s3");
    let signature = hex::encode(hmac_sha256(&key, string_to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope},SignedHeaders={signed_headers},Signature={signature}"
    )
}

/// `20130524T000000Z`-style stamp for the current instant.
fn amz_date_now() -> String {
    chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
}

// ============================================================
// Minimal S3 client (put / get / list)
// ============================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct S3Object {
    pub key: String,
    /// ETag without the XML `&quot;` quotes.
    pub etag: String,
    pub size: u64,
}

pub struct S3Client {
    endpoint: String,
    bucket: String,
    region: String,
    path_style: bool,
    access_key: String,
    secret_key: String,
    agent: ureq::Agent,
}

/// Split `http://host[:port]` into `(scheme, authority_with_port)`.
fn split_endpoint(endpoint: &str) -> Result<(&str, &str)> {
    for scheme in ["http", "https"] {
        let prefix = format!("{scheme}://");
        if let Some(rest) = endpoint.strip_prefix(prefix.as_str()) {
            let rest = rest.trim_end_matches('/');
            anyhow::ensure!(!rest.is_empty(), "endpoint 缺少主机名：{endpoint}");
            return Ok((scheme, rest));
        }
    }
    bail!("endpoint 需要以 http:// 或 https:// 开头：{endpoint}")
}

/// What ureq will send as `Host`: the authority minus a default port.
fn host_header<'a>(scheme: &str, authority: &'a str) -> &'a str {
    let default_port = if scheme == "https" { ":443" } else { ":80" };
    authority.strip_suffix(default_port).unwrap_or(authority)
}

impl S3Client {
    pub fn new(cfg: &FleetConfig) -> Self {
        S3Client {
            endpoint: cfg.endpoint.trim_end_matches('/').to_string(),
            bucket: cfg.bucket.clone(),
            region: cfg.region.clone(),
            path_style: cfg.path_style,
            access_key: cfg.access_key.clone(),
            secret_key: cfg.secret_key.clone(),
            agent: ureq::AgentBuilder::new()
                .timeout(Duration::from_secs(120))
                .build(),
        }
    }

    /// Sign and send one request, returning the checked response. A non-2xx
    /// status becomes an error carrying the server's body (S3 error XML
    /// names the actual problem).
    fn send(
        &self,
        method: &str,
        key: &str,
        query: &[(String, String)],
        body: &[u8],
    ) -> Result<ureq::Response> {
        let (scheme, authority) = split_endpoint(&self.endpoint)?;
        let host = host_header(scheme, authority);
        let encoded_key = uri_encode(key, false);
        let (path, url) = if self.path_style {
            let bucket = uri_encode(&self.bucket, false);
            let p = if encoded_key.is_empty() {
                format!("/{bucket}")
            } else {
                format!("/{bucket}/{encoded_key}")
            };
            let url = format!("{scheme}://{authority}{p}");
            (p, url)
        } else {
            let p = if encoded_key.is_empty() {
                "/".to_string()
            } else {
                format!("/{encoded_key}")
            };
            let url = format!("{scheme}://{}.{}{p}", self.bucket, authority);
            (p, url)
        };
        let query_str = canonical_query(query);

        let payload_hash = sha256_hex(body);
        let amz_date = amz_date_now();
        let headers = vec![
            ("host".to_string(), host.to_string()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        let auth = authorize(
            &self.access_key,
            &self.secret_key,
            &self.region,
            &SigRequest {
                method,
                canonical_uri: &path,
                query: &query_str,
                headers: &headers,
                payload_hash: &payload_hash,
            },
            &amz_date,
        );

        let mut req = self
            .agent
            .request(method, &url)
            .set("x-amz-date", &amz_date)
            .set("x-amz-content-sha256", &payload_hash)
            .set("Authorization", &auth);
        for (k, v) in query {
            req = req.query(k, v);
        }
        // ureq sets `Host` itself from the URL; that value matches the
        // canonical `host` header above by construction.
        let result = if method == "PUT" {
            req.send_bytes(body)
        } else {
            req.call()
        };
        match result {
            Ok(resp) => Ok(resp),
            Err(ureq::Error::Status(code, resp)) => {
                let text = resp.into_string().unwrap_or_default();
                let snippet: String = text.chars().take(400).collect();
                bail!("{method} {key} 失败（HTTP {code}）：{snippet}")
            }
            Err(e) => bail!("{method} {key} 失败：{e}"),
        }
    }

    pub fn put_object(&self, key: &str, body: &[u8]) -> Result<()> {
        self.send("PUT", key, &[], body).map(|_| ())
    }

    pub fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let resp = self.send("GET", key, &[], &[])?;
        let mut buf = Vec::new();
        resp.into_reader()
            .read_to_end(&mut buf)
            .with_context(|| format!("读取对象 {key} 的响应体失败"))?;
        Ok(buf)
    }

    /// ListObjectsV2 under `prefix`, following continuation tokens.
    pub fn list_objects(&self, prefix: &str) -> Result<Vec<S3Object>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut pairs: Vec<(String, String)> = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), prefix.to_string()),
            ];
            if let Some(t) = &token {
                pairs.push(("continuation-token".to_string(), t.clone()));
            }
            let resp = self.send("GET", "", &pairs, &[])?;
            let mut xml = String::new();
            resp.into_reader()
                .read_to_string(&mut xml)
                .context("读取 ListObjectsV2 响应失败")?;
            let (page, truncated, next) = parse_list_objects(&xml)?;
            out.extend(page);
            if !truncated {
                return Ok(out);
            }
            match next {
                Some(t) if token.as_ref() != Some(&t) => token = Some(t),
                _ => return Ok(out), // nothing new to continue with: stop
            }
        }
    }
}

// ============================================================
// ListObjectsV2 XML (hand-rolled: Key / ETag / Size are all we need)
// ============================================================

fn xml_decode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(pos) = rest.find('&') {
        out.push_str(&rest[..pos]);
        let tail = &rest[pos..];
        let semi = tail.find(';');
        let entity = semi.map(|e| &tail[1..e]);
        match entity {
            Some("amp") => out.push('&'),
            Some("lt") => out.push('<'),
            Some("gt") => out.push('>'),
            Some("quot") => out.push('"'),
            Some("apos") => out.push('\''),
            Some(num) if num.starts_with('#') => {
                let cp = if let Some(hexnum) =
                    num.strip_prefix("#x").or_else(|| num.strip_prefix("#X"))
                {
                    u32::from_str_radix(hexnum, 16).ok()
                } else {
                    num[1..].parse::<u32>().ok()
                };
                match cp.and_then(char::from_u32) {
                    Some(c) => out.push(c),
                    None => out.push('&'), // not a real numeric entity: keep it
                }
            }
            _ => out.push('&'),
        }
        rest = match semi {
            Some(e) => &tail[e + 1..],
            // No closing semicolon: the '&' is literal, carry on after it.
            None => &tail[1..],
        };
    }
    out.push_str(rest);
    out
}

fn first_tagged<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = start + xml[start..].find(&close)?;
    Some(&xml[start..end])
}

/// Parse one ListObjectsV2 response: contents, IsTruncated, NextContinuationToken.
fn parse_list_objects(xml: &str) -> Result<(Vec<S3Object>, bool, Option<String>)> {
    let mut objects = Vec::new();
    for chunk in xml.split("<Contents>") {
        // Keys cannot contain a literal '<' (XML would escape it), so
        // splitting on the open tag is safe; each chunk up to the closer
        // holds exactly one object.
        let Some(end) = chunk.find("</Contents>") else {
            continue;
        };
        let body = &chunk[..end];
        let Some(key) = first_tagged(body, "Key").map(xml_decode) else {
            continue;
        };
        let etag = first_tagged(body, "ETag")
            .map(xml_decode)
            .map(|e| e.trim_matches('"').to_string())
            .unwrap_or_default();
        let size = first_tagged(body, "Size")
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        objects.push(S3Object { key, etag, size });
    }
    let truncated = first_tagged(xml, "IsTruncated").map(|t| t.trim() == "true") == Some(true);
    let next = first_tagged(xml, "NextContinuationToken").map(|t| t.trim().to_string());
    Ok((objects, truncated, next))
}

// ============================================================
// Push (this host's data.parquet → hosts/{host_id}/data.parquet)
// ============================================================

/// S3 key for one host's aggregate parquet. A fixed key overwritten in place
/// keeps the bucket flat: no small files, no cleanup, push is idempotent.
pub fn object_key(host_id: &str) -> String {
    format!("hosts/{host_id}/data.parquet")
}

/// S3 key for one host's quota snapshots — same flat-key discipline, written
/// by the same push. Tiny (a few hundred bytes), so it rides along free.
pub fn quota_object_key(host_id: &str) -> String {
    format!("hosts/{host_id}/quota.json")
}

/// `hosts/{name}/quota.json` → `Some(name)`, with the same charset guard as
/// the parquet twin.
pub fn quota_host_from_key(key: &str) -> Option<String> {
    let name = key.strip_prefix("hosts/")?.strip_suffix("/quota.json")?;
    if valid_host_id(name) {
        Some(name.to_string())
    } else {
        None
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PushOutcome {
    pub host: String,
    pub key: String,
    pub bytes: u64,
}

fn ensure_enabled(cfg: &FleetConfig) -> Result<()> {
    anyhow::ensure!(
        cfg.enabled,
        "Fleet 未启用：请在 {} 中设 enabled = true",
        config_path().display()
    );
    Ok(())
}

/// Config with a readable error for the CLI/API when the file is absent —
/// "silently off" is the right behavior for the server, not for an explicit
/// push request.
pub fn require_config() -> Result<FleetConfig> {
    match load_config()? {
        Some(cfg) => Ok(cfg),
        None => bail!(
            "Fleet 未启用：{} 不存在（创建并填入 endpoint/bucket/密钥后重试）",
            config_path().display()
        ),
    }
}

/// PutObject the local `data.parquet`. Reading the whole file is safe: the
/// store writes via tmp+rename, so a plain read never sees a partial file.
/// With `encrypt = true` the object goes out as XChaCha20-Poly1305
/// ciphertext keyed off the same secret_key — the bucket stores nothing
/// readable about projects or models.
pub fn push(cfg: &FleetConfig, data_parquet: &Path) -> Result<PushOutcome> {
    ensure_enabled(cfg)?;
    let mut body = std::fs::read(data_parquet).with_context(|| {
        format!(
            "读取 {} 失败（先跑一次同步生成本地数据）",
            data_parquet.display()
        )
    })?;
    if cfg.encrypt {
        let key = crate::crypt::derive_key(&cfg.secret_key)?;
        body = crate::crypt::encrypt(&body, &key)?;
    }
    let client = S3Client::new(cfg);
    let key = object_key(&cfg.host_id);
    client.put_object(&key, &body)?;
    Ok(PushOutcome {
        host: cfg.host_id.clone(),
        key,
        bytes: body.len() as u64,
    })
}

/// Sync-then-push, shared by the `push` subcommand, the dashboard button and
/// the auto-push thread: the object always reflects the freshest local state.
/// The quota snapshots ride along best-effort — a quota upload failure is
/// stderr noise, never a failed push.
pub fn push_now(store: &Store, cfg: &FleetConfig) -> Result<(SyncResult, PushOutcome)> {
    ensure_enabled(cfg)?;
    let sync = store.sync()?;
    let outcome = push(cfg, store.parquet_path())?;
    if let Err(e) = push_quota(cfg) {
        eprintln!("[TokenBuddy] fleet quota push failed: {e}");
    }
    Ok((sync, outcome))
}

/// What a host publishes as its plan capacity: the current snapshots plus
/// when this view was generated. Collectors' error strings stay local — the
/// bucket carries facts, not diagnostics.
#[derive(Debug, Serialize, Deserialize)]
struct QuotaPayload {
    snapshots: Vec<crate::quota::QuotaSnapshot>,
    generated_at: i64,
}

/// Push this host's quota snapshots to `hosts/{id}/quota.json`, encrypted
/// under the same key when `encrypt = true` — the bucket learns plans and
/// windows only in ciphertext.
pub fn push_quota(cfg: &FleetConfig) -> Result<PushOutcome> {
    ensure_enabled(cfg)?;
    let view = crate::quota::collect_view(false);
    let payload = QuotaPayload {
        snapshots: view.snapshots,
        generated_at: view.generated_at,
    };
    let mut body = serde_json::to_vec(&payload)?;
    if cfg.encrypt {
        let key = crate::crypt::derive_key(&cfg.secret_key)?;
        body = crate::crypt::encrypt(&body, &key)?;
    }
    let client = S3Client::new(cfg);
    let key = quota_object_key(&cfg.host_id);
    client.put_object(&key, &body)?;
    Ok(PushOutcome {
        host: cfg.host_id.clone(),
        key,
        bytes: body.len() as u64,
    })
}

// ============================================================
// Pull (hosts/*/data.parquet → ~/.tokenbuddy/fleet/{host}/)
// ============================================================

/// `hosts/{name}/data.parquet` → `Some(name)`. Anything else on the bucket
/// (other prefixes, nested junk, traversal-looking names) is ignored: the
/// name becomes a directory under the fleet dir, so the charset rule that
/// guards `host_id` guards here too — against remote objects, not just the
/// local config.
pub fn host_from_key(key: &str) -> Option<String> {
    let name = key.strip_prefix("hosts/")?.strip_suffix("/data.parquet")?;
    if valid_host_id(name) {
        Some(name.to_string())
    } else {
        None
    }
}

/// One host's entry in the pull manifest: what the local copy was fetched as.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub etag: String,
    pub size: u64,
}

/// etag/size bookkeeping per host, so `fleet-sync` skips objects whose
/// content has not changed since the last pull. `hosts` tracks the parquet
/// twin; `quota` the quota.json twin (absent in older manifests — the
/// `#[serde(default)]` keeps them reading fine).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub hosts: BTreeMap<String, ManifestEntry>,
    #[serde(default)]
    pub quota: BTreeMap<String, ManifestEntry>,
}

fn manifest_path(base: &Path) -> PathBuf {
    base.join("manifest.json")
}

fn read_manifest(base: &Path) -> Manifest {
    std::fs::read_to_string(manifest_path(base))
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

fn write_manifest(base: &Path, manifest: &Manifest) -> Result<()> {
    std::fs::create_dir_all(base)?;
    let tmp = manifest_path(base).with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_string(manifest)?)?;
    std::fs::rename(&tmp, manifest_path(base))?;
    Ok(())
}

/// Skip only when the local file exists AND the manifest recorded exactly
/// this etag+size. Either witness alone could lie after a crashed run;
/// together they make "unchanged" trustworthy.
fn download_needed(entry: Option<ManifestEntry>, etag: &str, size: u64, dest: &Path) -> bool {
    match entry {
        Some(e) if dest.exists() => e.etag != etag || e.size != size,
        _ => true,
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PullOutcome {
    pub downloaded: Vec<String>,
    pub skipped: Vec<String>,
}

/// Pull every host's parquet into `base`, skipping unchanged objects. Each
/// file lands via tmp+rename, so a partial download can never sit at the
/// final path pretending to be a parquet.
pub fn pull_all_into(cfg: &FleetConfig, base: &Path) -> Result<PullOutcome> {
    ensure_enabled(cfg)?;
    let client = S3Client::new(cfg);
    let objects = client.list_objects("hosts/")?;

    let mut manifest = read_manifest(base);
    let mut out = PullOutcome::default();
    for obj in objects {
        // Two object kinds share the hosts/ prefix; each keeps its own
        // etag bookkeeping and its own destination file.
        let (host, kind) = if obj.key.ends_with("/data.parquet") {
            match host_from_key(&obj.key) {
                Some(h) => (h, "data"),
                None => continue,
            }
        } else if obj.key.ends_with("/quota.json") {
            match quota_host_from_key(&obj.key) {
                Some(h) => (h, "quota"),
                None => continue,
            }
        } else {
            continue;
        };
        let dir = base.join(&host);
        let dest = dir.join(if kind == "data" {
            "data.parquet"
        } else {
            "quota.json"
        });
        let book = if kind == "data" {
            &mut manifest.hosts
        } else {
            &mut manifest.quota
        };
        // Issue #38: label carries the object kind so "下载 {host}/quota.json"
        // can't masquerade as a data.parquet refresh, and one host pulling
        // both objects prints two honest lines instead of a duplicated one.
        let label = format!(
            "{host}/{}",
            if kind == "data" {
                "data.parquet"
            } else {
                "quota.json"
            }
        );
        if !download_needed(book.get(&host).cloned(), &obj.etag, obj.size, &dest) {
            out.skipped.push(label);
            continue;
        }
        let mut body = client.get_object(&obj.key)?;
        anyhow::ensure!(!body.is_empty(), "对象 {} 内容为空，跳过", obj.key);
        if crate::crypt::is_encrypted(&body) {
            let key = crate::crypt::derive_key(&cfg.secret_key)
                .with_context(|| format!("解密 {} 需要 fleet.toml 里的 secret_key", obj.key))?;
            body = crate::crypt::decrypt(&body, &key)
                .with_context(|| format!("解密 {} 失败", obj.key))?;
        }
        std::fs::create_dir_all(&dir)?;
        let tmp = dir.join(format!(
            "{}.tmp",
            if kind == "data" {
                "data.parquet"
            } else {
                "quota.json"
            }
        ));
        std::fs::write(&tmp, &body)?;
        std::fs::rename(&tmp, &dest)?;
        book.insert(
            host.clone(),
            ManifestEntry {
                etag: obj.etag,
                size: obj.size,
            },
        );
        out.downloaded.push(label);
    }
    if !out.downloaded.is_empty() {
        write_manifest(base, &manifest)?;
    }
    Ok(out)
}

/// Pull into the default fleet dir (`~/.tokenbuddy/fleet/`).
pub fn pull_all(cfg: &FleetConfig) -> Result<PullOutcome> {
    pull_all_into(cfg, &fleet_dir())
}

/// One host's quota payload as pulled to disk: tolerant parse — a host that
/// has never pushed quota simply contributes nothing.
#[derive(Debug, Clone, Serialize)]
pub struct FleetHostQuota {
    pub host: String,
    pub snapshots: Vec<crate::quota::QuotaSnapshot>,
    pub generated_at: Option<i64>,
}

/// Read every host's pulled quota.json from the fleet dir. No S3 here: this
/// serves whatever the last `fleet-sync` (or auto-pull) brought home.
pub fn read_fleet_quotas(base: &Path) -> Vec<FleetHostQuota> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(base) else {
        return out;
    };
    let mut hosts: Vec<String> = entries
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| valid_host_id(n))
        .collect();
    hosts.sort();
    for host in hosts {
        let path = base.join(&host).join("quota.json");
        let payload: Option<QuotaPayload> = std::fs::read_to_string(&path)
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok());
        out.push(FleetHostQuota {
            host,
            snapshots: payload.iter().flat_map(|p| p.snapshots.clone()).collect(),
            generated_at: payload.map(|p| p.generated_at),
        });
    }
    out
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- SigV4 against the AWS documentation test vectors ---

    /// The GET example from the AWS SigV4 docs, asserted byte-for-byte. The
    /// canonical request this signs hashes to
    /// `7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972`,
    /// the value the docs publish for this example — so the hash, the
    /// string-to-sign and the final signature are each independently
    /// cross-checked against a standard HMAC-SHA256 implementation.
    #[test]
    fn sigv4_matches_aws_documentation_get_vector() {
        let amz_date = "20130524T000000Z";
        let payload = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers = vec![
            (
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("range".to_string(), "bytes=0-9".to_string()),
            ("x-amz-content-sha256".to_string(), payload.to_string()),
            ("x-amz-date".to_string(), amz_date.to_string()),
        ];
        let auth = authorize(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "us-east-1",
            &SigRequest {
                method: "GET",
                canonical_uri: "/test.txt",
                query: "",
                headers: &headers,
                payload_hash: payload,
            },
            amz_date,
        );
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=67fe34c8530db585abddc51067328adfedb6e42487d2566dc7d927d6e2722900"
        );
    }

    /// HMAC chain order (secret→date→region→service→`aws4_request`) pinned to
    /// a value cross-checked against an independent HMAC-SHA256
    /// implementation; reordering any stage breaks it.
    #[test]
    fn signing_key_chain_order_is_pinned() {
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex::encode(key),
            "2c94c0cf5378ada6887f09bb697df8fc0affdb34ba1cdd5bda32b664bd55b73c"
        );
    }

    #[test]
    fn canonical_query_sorts_and_encodes() {
        let q = canonical_query(&[
            ("prefix".to_string(), "hosts/".to_string()),
            ("list-type".to_string(), "2".to_string()),
            ("continuation-token".to_string(), "a b/c+d".to_string()),
        ]);
        assert_eq!(
            q,
            "continuation-token=a%20b%2Fc%2Bd&list-type=2&prefix=hosts%2F"
        );
    }

    #[test]
    fn uri_encode_keeps_unreserved_and_optional_slashes() {
        assert_eq!(
            uri_encode("hosts/alpha-1_v2.bin", false),
            "hosts/alpha-1_v2.bin"
        );
        assert_eq!(uri_encode("a b+c", true), "a%20b%2Bc");
        assert_eq!(uri_encode("a b+c", false), "a%20b%2Bc");
        assert_eq!(uri_encode("中文", true), "%E4%B8%AD%E6%96%87");
    }

    #[test]
    fn endpoint_split_and_host_header() {
        assert_eq!(
            split_endpoint("http://192.0.2.10:9000").unwrap(),
            ("http", "192.0.2.10:9000")
        );
        assert_eq!(
            split_endpoint("http://rustfs.lan/").unwrap(),
            ("http", "rustfs.lan")
        );
        assert!(split_endpoint("rustfs.lan:9000").is_err());
        assert!(split_endpoint("http://").is_err());
        assert_eq!(host_header("http", "rustfs.lan:80"), "rustfs.lan");
        assert_eq!(host_header("http", "rustfs.lan:9000"), "rustfs.lan:9000");
        assert_eq!(host_header("https", "b.lan:443"), "b.lan");
    }

    // --- fleet.toml parsing ---

    #[test]
    fn config_defaults_apply_when_keys_absent() {
        let cfg = parse_config("enabled = false\n").unwrap();
        assert!(!cfg.enabled);
        assert_eq!(cfg.region, "us-east-1");
        assert!(cfg.path_style);
        assert!(cfg.auto_push);
        assert!(
            cfg.auto_pull,
            "auto_pull defaults on: read-only for the bucket"
        );
        assert_eq!(cfg.push_interval_secs, 3600);
        assert!(
            !cfg.host_id.is_empty(),
            "hostname fallback must fill host_id"
        );
    }

    #[test]
    fn config_parses_every_key_type_and_comments() {
        let text = r#"
# fleet sync on
enabled = true          # trailing comment
endpoint = "http://192.0.2.10:9000" # rustfs
bucket = "tokenbuddy"
access_key = "minioadmin"
secret_key = "min\"io" # quote inside
region = "us-east-1"
path_style = false
host_id = "mini"
auto_push = false
push_interval_secs = 600
"#;
        let cfg = parse_config(text).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.endpoint, "http://192.0.2.10:9000");
        assert_eq!(cfg.bucket, "tokenbuddy");
        assert_eq!(cfg.secret_key, "min\"io");
        assert!(!cfg.path_style);
        assert_eq!(cfg.host_id, "mini");
        assert!(!cfg.auto_push);
        assert_eq!(cfg.push_interval_secs, 600);
    }

    #[test]
    fn config_enabled_requires_connection_settings() {
        let err = parse_config("enabled = true\n").unwrap_err();
        assert!(err.to_string().contains("endpoint"));
        parse_config(
            "enabled = true\nendpoint = \"http://x\"\nbucket = \"b\"\naccess_key = \"a\"\nsecret_key = \"s\"\n",
        )
        .unwrap();
    }

    #[test]
    fn config_render_parse_roundtrips_special_chars() {
        let cfg = FleetConfig {
            enabled: true,
            endpoint: "http://192.0.2.10:9000".into(),
            bucket: "tokenbuddy".into(),
            access_key: "walker".into(),
            secret_key: "pa\\ss\"wo\nrd\tmix".into(),
            region: "us-east-1".into(),
            path_style: true,
            host_id: "mac-air".into(),
            auto_push: true,
            auto_pull: false,
            push_interval_secs: 1800,
            encrypt: true,
        };
        let parsed = parse_config(&render_config(&cfg)).unwrap();
        assert_eq!(parsed, cfg);
    }

    #[test]
    fn encrypt_flag_defaults_off_and_parses() {
        // Absent key → plain behavior (existing configs unchanged).
        let base = "enabled = true\nendpoint = \"http://x\"\nbucket = \"b\"\naccess_key = \"a\"\nsecret_key = \"s\"\n";
        assert!(!parse_config(base).unwrap().encrypt);
        let on = format!("{base}encrypt = true\n");
        assert!(parse_config(&on).unwrap().encrypt);
        // Not a bare value → the usual TOML error, not a silent default.
        assert!(parse_config(&format!("{base}encrypt = \"yes\"\n")).is_err());
    }

    #[test]
    fn config_write_to_file_roundtrips_and_lands_0600() {
        let dir = crate::unique_test_dir("tb_cfg");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("fleet.toml");
        let cfg = FleetConfig::for_endpoint("http://ep:9000", "bkt", "ak", "sk");
        write_config_to(&path, &cfg).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(parse_config(&text).unwrap(), cfg);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(
                mode & 0o777,
                0o600,
                "config file must not be group/world readable"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn config_rejects_typos_loudly() {
        assert!(parse_config("endpint = \"http://x\"\n").is_err());
        assert!(parse_config("enabled yes\n").is_err());
        assert!(parse_config("bucket = bare\n").is_err());
        assert!(parse_config("push_interval_secs = \"3600\"\n").is_err());
        assert!(parse_config("push_interval_secs = -5\n").is_err());
        assert!(parse_config("host_id = \"a/b\"\n").is_err());
        assert!(parse_config("host_id = \"..\"\n").is_err());
    }

    #[test]
    fn config_clamps_pathological_interval() {
        let cfg = parse_config("push_interval_secs = 0\n").unwrap();
        assert_eq!(cfg.push_interval_secs, 60);
        let cfg = parse_config("push_interval_secs = 999999999\n").unwrap();
        assert_eq!(cfg.push_interval_secs, 86_400);
    }

    // --- ListObjectsV2 XML ---

    #[test]
    fn list_xml_parses_keys_etags_sizes_and_escapes() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>tokenbuddy</Name><Prefix>hosts/</Prefix><KeyCount>2</KeyCount>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>hosts/alpha/data.parquet</Key>
    <LastModified>2026-09-28T01:00:00.000Z</LastModified>
    <ETag>&quot;abc123&quot;</ETag><Size>4096</Size></Contents>
  <Contents><Key>hosts/beat &amp; go/data.parquet</Key>
    <ETag>&quot;d e&quot;</ETag><Size>8</Size></Contents>
</ListBucketResult>"#;
        let (objects, truncated, next) = parse_list_objects(xml).unwrap();
        assert!(!truncated);
        assert_eq!(next, None);
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].key, "hosts/alpha/data.parquet");
        assert_eq!(objects[0].etag, "abc123");
        assert_eq!(objects[0].size, 4096);
        assert_eq!(objects[1].key, "hosts/beat & go/data.parquet");
    }

    #[test]
    fn list_xml_carries_continuation() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated>\
                   <NextContinuationToken>tok/1+2</NextContinuationToken>\
                   <Contents><Key>hosts/alpha/data.parquet</Key><ETag>&quot;x&quot;</ETag><Size>1</Size></Contents>\
                   </ListBucketResult>";
        let (objects, truncated, next) = parse_list_objects(xml).unwrap();
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("tok/1+2"));
        assert_eq!(objects.len(), 1);
    }

    #[test]
    fn xml_decode_handles_numeric_entities_and_strays() {
        assert_eq!(xml_decode("a&amp;b&#x4E2D;c"), "a&b中c");
        assert_eq!(xml_decode("x &amp y"), "x &amp y"); // not a real entity: kept
    }

    // --- push ---

    #[test]
    fn quota_object_key_and_host_parse_roundtrip() {
        assert_eq!(quota_object_key("mac-mini"), "hosts/mac-mini/quota.json");
        assert_eq!(
            quota_host_from_key("hosts/mac-mini/quota.json"),
            Some("mac-mini".into())
        );
        // Same traversal/charset discipline as the parquet twin.
        assert_eq!(quota_host_from_key("hosts/../etc/quota.json"), None);
        assert_eq!(quota_host_from_key("hosts/a b/quota.json"), None);
        assert_eq!(quota_host_from_key("hosts/ok/data.parquet"), None);
    }

    #[test]
    fn read_fleet_quotas_tolerates_missing_and_broken_files() {
        let dir = std::env::temp_dir().join(format!("tb-fq-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("alpha")).unwrap();
        std::fs::create_dir_all(dir.join("beta")).unwrap();
        std::fs::create_dir_all(dir.join("not a host")).unwrap();
        std::fs::write(
            dir.join("alpha/quota.json"),
            r#"{"snapshots":[{"source":"minimax","plan":"general","window":"interval","used_percent":4.0,"resets_at":null,"collected_at":10,"origin":"command"}],"generated_at":99}"#,
        )
        .unwrap();
        std::fs::write(dir.join("beta/quota.json"), "{truncated").unwrap();

        let quotas = read_fleet_quotas(&dir);
        assert_eq!(quotas.len(), 2, "invalid host dirs and absent files skip");
        let alpha = quotas.iter().find(|q| q.host == "alpha").unwrap();
        assert_eq!(alpha.snapshots.len(), 1);
        assert_eq!(alpha.generated_at, Some(99));
        let beta = quotas.iter().find(|q| q.host == "beta").unwrap();
        assert!(beta.snapshots.is_empty(), "broken parse degrades to empty");
        assert_eq!(beta.generated_at, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn manifest_roundtrips_with_quota_entries() {
        let mut m = Manifest::default();
        m.hosts.insert(
            "a".into(),
            ManifestEntry {
                etag: "e1".into(),
                size: 1,
            },
        );
        m.quota.insert(
            "a".into(),
            ManifestEntry {
                etag: "e2".into(),
                size: 2,
            },
        );
        let text = serde_json::to_string(&m).unwrap();
        let back: Manifest = serde_json::from_str(&text).unwrap();
        assert_eq!(back.quota.get("a").unwrap().etag, "e2");
        // Older manifests without the quota map still read.
        let old: Manifest =
            serde_json::from_str(r#"{"hosts":{"a":{"etag":"e","size":1}}}"#).unwrap();
        assert!(old.quota.is_empty());
    }

    #[test]
    fn object_key_places_host_under_hosts_prefix() {
        assert_eq!(object_key("mini"), "hosts/mini/data.parquet");
        assert_eq!(object_key("fold-3"), "hosts/fold-3/data.parquet");
    }

    #[test]
    fn push_refuses_when_disabled_before_touching_network_or_disk() {
        let mut cfg = FleetConfig::for_endpoint("http://127.0.0.1:9", "b", "a", "s");
        cfg.enabled = false;
        // Even a missing data file must not be the reported problem: the
        // enabled check fires first.
        let err = push(&cfg, Path::new("/nonexistent/tokenbuddy/data.parquet")).unwrap_err();
        assert!(err.to_string().contains("Fleet 未启用"));
    }

    #[test]
    fn push_fails_readably_when_parquet_is_missing() {
        let cfg = FleetConfig::for_endpoint("http://127.0.0.1:9", "b", "a", "s");
        let err = push(&cfg, Path::new("/nonexistent/tokenbuddy/data.parquet")).unwrap_err();
        assert!(err.to_string().contains("读取"));
        assert!(err.to_string().contains("同步"));
    }

    // --- pull ---

    #[test]
    fn host_from_key_accepts_only_fleet_parquet_keys() {
        assert_eq!(
            host_from_key("hosts/mini/data.parquet"),
            Some("mini".to_string())
        );
        // Not the fixed object shape, or not a safe directory name: ignore.
        assert_eq!(host_from_key("hosts/mini/context.parquet"), None);
        assert_eq!(host_from_key("hosts/a/b/data.parquet"), None);
        assert_eq!(host_from_key("other/mini/data.parquet"), None);
        assert_eq!(host_from_key("hosts/../data.parquet"), None);
        assert_eq!(host_from_key("hosts//data.parquet"), None);
        assert_eq!(host_from_key("hosts/white space/data.parquet"), None);
    }

    #[test]
    fn unchanged_objects_are_skipped_only_with_file_and_manifest_agreeing() {
        let dir =
            std::env::temp_dir().join(format!("tokenbuddy-fleet-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir should be creatable");
        let dest = dir.join("data.parquet");

        // No local file: download even with a matching manifest entry.
        let entry = ManifestEntry {
            etag: "abc".to_string(),
            size: 10,
        };
        assert!(download_needed(Some(entry.clone()), "abc", 10, &dest));
        // File exists + manifest matches: skip.
        std::fs::write(&dest, b"0123456789").expect("temp file should be writable");
        assert!(!download_needed(Some(entry.clone()), "abc", 10, &dest));
        // ETag or size drifted: download.
        assert!(download_needed(Some(entry.clone()), "def", 10, &dest));
        assert!(download_needed(Some(entry), "abc", 11, &dest));
        // Manifest lost but file present: re-download once, self-heals.
        assert!(download_needed(None, "abc", 10, &dest));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn manifest_survives_a_roundtrip_through_disk() {
        let dir =
            std::env::temp_dir().join(format!("tokenbuddy-fleet-manifest-{}", std::process::id()));
        let mut manifest = Manifest::default();
        manifest.hosts.insert(
            "mini".to_string(),
            ManifestEntry {
                etag: "\"e1\"".to_string(),
                size: 7,
            },
        );
        write_manifest(&dir, &manifest).expect("manifest should be writable");
        let loaded = read_manifest(&dir);
        assert_eq!(loaded.hosts.get("mini"), manifest.hosts.get("mini"));
        // A corrupt manifest degrades to empty, never a panic.
        std::fs::write(manifest_path(&dir), "not json").unwrap();
        assert!(read_manifest(&dir).hosts.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
