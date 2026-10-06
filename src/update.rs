//! 自升级(issue #56):检查更新 + 一键升级,静默升级可配置。
//!
//! 定位纪律:TokenBuddy 零外呼,所以**默认一切关闭**——`update.json` 不存在
//! 就是一个网络包都不发的老版本行为;用户在面板/CLI 里显式打开检查后才按
//! 间隔访问 release 源。检查本身也不进二进制:GitHub 要 HTTPS,而本构建
//! 刻意不带 TLS(fleet 同款取舍),curl/git 都是系统里现成的。
//!
//! 两种部署方式(issue 原文「按当前部署方式」):
//! * `github` — install.sh 装的二进制:下载 release tarball + sha256 校验,
//!   原子换二进制(launchd/systemd/runsv 重启,裸进程 kill+nohup 兜底),
//!   健康检查不过自动回滚 `.bak`。
//! * `git` — 源码目录里跑的(target/release):`git pull --ff-only` +
//!   `cargo build --release`。这条**永不参与静默升级**,开发机的 working
//!   tree 不许后台线程自作主张,只能显式点一次装一次。

use anyhow::{anyhow, Context as _};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};

/// 公开发布仓(install.sh 同款)。检查 URL 与下载 URL 都由它拼出。
pub const RELEASE_REPO: &str = "walker83/TokenBuddy";

const CURL: &str = "curl";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfig {
    /// 周期性检查更新。**默认 false**:不发这个配置文件,工具保持零外呼。
    #[serde(default)]
    pub enabled: bool,
    /// 检查到新版本后自动安装重启(仅 github 模式;git 模式一律只提醒)。
    #[serde(default)]
    pub auto_install: bool,
    /// 检查间隔(小时)。1–168,默认 24。
    #[serde(default = "default_interval")]
    pub interval_hours: u32,
    /// `auto`|`github`|`git`。auto 按二进制位置判断:target/ 下=git,否则 github。
    #[serde(default)]
    pub mode: String,
}

fn default_interval() -> u32 {
    24
}

impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            auto_install: false,
            interval_hours: default_interval(),
            mode: "auto".to_string(),
        }
    }
}

/// 一次检查的结果,落 `update-state.json` 供 GET /api/update 零网络读取。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CheckResult {
    pub checked_at: i64,
    pub mode: String,
    pub current_version: String,
    /// github 模式=最新 tag;git 模式=origin/main 短提交。检查失败为空。
    pub latest_version: String,
    pub update_available: bool,
    #[serde(default)]
    pub error: String,
}

pub fn config_path() -> PathBuf {
    crate::data_dir().join("update.json")
}

pub fn state_path() -> PathBuf {
    crate::data_dir().join("update-state.json")
}

pub fn load_config() -> UpdateConfig {
    match std::fs::read(config_path()) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
        Err(_) => UpdateConfig::default(),
    }
}

fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let tmp = path.with_extension("tmp");
    {
        let mut f = open_private(&tmp)?;
        f.write_all(bytes)?;
        f.flush()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(unix)]
fn open_private(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    // 0600:配置虽无密钥,「本地个人工具」的文件权限从紧没错。
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_private(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
}

pub fn save_config(cfg: &UpdateConfig) -> anyhow::Result<()> {
    if cfg.interval_hours < 1 || cfg.interval_hours > 168 {
        return Err(anyhow!(
            "interval_hours 超出范围(1–168):{}",
            cfg.interval_hours
        ));
    }
    match cfg.mode.as_str() {
        "auto" | "github" | "git" => {}
        other => return Err(anyhow!("未知 mode:{other}(可用:auto|github|git)")),
    }
    let bytes = serde_json::to_vec_pretty(cfg).context("序列化 update.json 失败")?;
    write_private(&config_path(), &bytes).context("写 update.json 失败")
}

/// serve 进程把监听端口告诉升级模块:runner 的裸进程兜底要按端口找旧进程。
static SERVE_PORT: AtomicU16 = AtomicU16::new(0);
static INSTALL_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

pub fn set_serve_port(port: u16) {
    SERVE_PORT.store(port, Ordering::SeqCst);
}

fn serve_port() -> u16 {
    let inlined = SERVE_PORT.load(Ordering::SeqCst);
    if inlined > 0 {
        return inlined;
    }
    // CLI 触发时 serve 自己没登记过端口——读 server.json(serve 启动时落盘)。
    let path = crate::data_dir().join("server.json");
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| {
            serde_json::from_str::<serde_json::Value>(&s)
                .ok()
                .and_then(|v| v.get("port").and_then(|p| p.as_u64()))
                .map(|p| p as u16)
        })
        .unwrap_or(0)
}

/// 当前版本(CARGO_PKG_VERSION,如 "0.10.0")。
pub fn current_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// 部署方式探测。二进制在 cargo target 目录里=源码安装(git);
/// install.sh 装到 ~/.local/bin 之类=github release。
pub fn detect_mode() -> String {
    let exe = std::env::current_exe().unwrap_or_default();
    let s = exe.to_string_lossy();
    if s.contains("/target/release/") || s.contains("/target/debug/") {
        "git".to_string()
    } else {
        "github".to_string()
    }
}

fn effective_mode(cfg: &UpdateConfig) -> String {
    match cfg.mode.as_str() {
        "github" | "git" => cfg.mode.clone(),
        _ => detect_mode(),
    }
}

/// `(major, minor, patch)`;解析失败返回 None。tag 允许带 v 前缀。
fn semver_of(version: &str) -> Option<(u64, u64, u64)> {
    let v = version.trim().trim_start_matches('v');
    let mut it = v.split('.');
    let maj = it.next()?.parse().ok()?;
    let min = it.next()?.parse().ok()?;
    let pat = it.next().unwrap_or("0");
    // 带后缀的(0.11.0-rc1)按数字前缀取,别整个解析失败。
    let pat_num: String = pat.chars().take_while(|c| c.is_ascii_digit()).collect();
    let pat: u64 = if pat_num.is_empty() {
        0
    } else {
        pat_num.parse().ok()?
    };
    Some((maj, min, pat))
}

/// latest 是否比 current 新。两边都无法解析成 semver 时退化为「不相等即新」。
pub fn is_newer(latest: &str, current: &str) -> bool {
    match (semver_of(latest), semver_of(current)) {
        (Some(a), Some(b)) => a > b,
        _ => latest != current,
    }
}

/// 从 GitHub releases/latest 的响应体里抠 tag_name(纯函数,测试友好)。
pub fn parse_latest_tag(body: &[u8]) -> anyhow::Result<String> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| anyhow!("release 响应不是合法 JSON:{e}"))?;
    let tag = value
        .get("tag_name")
        .and_then(|t| t.as_str())
        .ok_or_else(|| anyhow!("release 响应缺少 tag_name(限流?)"))?;
    Ok(tag.to_string())
}

/// 检查更新(github:curl releases/latest;git:fetch + rev-parse)。
pub fn check() -> CheckResult {
    let cfg = load_config();
    let mode = effective_mode(&cfg);
    let mut result = CheckResult {
        checked_at: now_secs(),
        mode: mode.clone(),
        current_version: current_version(),
        ..Default::default()
    };
    let outcome = if mode == "git" {
        check_git()
    } else {
        check_github()
    };
    match outcome {
        Ok(latest) => {
            result.latest_version = latest.clone();
            result.update_available = is_newer(&latest, &current_version());
        }
        Err(e) => result.error = format!("{e:#}"),
    }
    result
}

fn check_github() -> anyhow::Result<String> {
    let url = format!("https://api.github.com/repos/{RELEASE_REPO}/releases/latest");
    let out = std::process::Command::new(CURL)
        .args(["-fsSL", "--max-time", "15", &url])
        .output()
        .map_err(|e| anyhow!("启动 curl 失败:{e}"))?;
    if !out.status.success() {
        return Err(anyhow!(
            "curl 查询最新 release 失败({}):{}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    parse_latest_tag(&out.stdout)
}

fn check_git() -> anyhow::Result<String> {
    let repo = repo_root()?;
    run(&repo, "git", &["fetch", "origin"])?;
    run(&repo, "git", &["rev-parse", "--short=8", "origin/HEAD"])
        .or_else(|_| run(&repo, "git", &["rev-parse", "--short=8", "origin/main"]))
}

/// git 模式的仓库根:二进制在 `<repo>/target/<profile>/tokenbuddy`,上三级。
fn repo_root() -> anyhow::Result<PathBuf> {
    let exe = std::env::current_exe().context("定位当前二进制失败")?;
    let dir = exe
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .ok_or_else(|| anyhow!("二进制路径不符合 target/<profile>/ 形状:{}", exe.display()))?;
    Ok(dir.to_path_buf())
}

fn run(dir: &Path, program: &str, args: &[&str]) -> anyhow::Result<String> {
    let out = std::process::Command::new(program)
        .current_dir(dir)
        .args(args)
        .output()
        .with_context(|| format!("运行 {program} {} 失败", args.join(" ")))?;
    if !out.status.success() {
        return Err(anyhow!(
            "{program} {} 失败:{}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}

/// GET /api/update 的响应体:配置 + 最近一次检查(纯本地读,零网络)。
pub fn status_json() -> String {
    let cfg = load_config();
    let last: Option<CheckResult> = std::fs::read(state_path())
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok());
    serde_json::json!({
        "current_version": current_version(),
        "mode": effective_mode(&cfg),
        "config": cfg,
        "last_check": last,
        "install_in_progress": INSTALL_IN_PROGRESS.load(Ordering::SeqCst),
    })
    .to_string()
}

/// POST /api/update/check:现在就查一次并落盘。
pub fn check_and_record() -> anyhow::Result<CheckResult> {
    let result = check();
    let bytes = serde_json::to_vec(&result).context("序列化检查结果失败")?;
    write_private(&state_path(), &bytes).context("写 update-state.json 失败")?;
    Ok(result)
}

/// POST /api/update/install(CLI 与面板共用):生成 runner 脚本 detached
/// 起跑。serve 场景由 HTTP 层在响应后调 `schedule_self_exit` 让旧进程退位。
/// 返回目标版本(仪表盘轮询 /api/status 直到 version 变化)。
pub fn start_install() -> anyhow::Result<String> {
    if INSTALL_IN_PROGRESS.swap(true, Ordering::SeqCst) {
        return Err(anyhow!("升级已在进行中"));
    }
    let cfg = load_config();
    let mode = effective_mode(&cfg);
    // 以最近一次检查为准;没有、失败或超过 24h 就现查一次。
    let fresh = std::fs::read(state_path())
        .ok()
        .and_then(|b| serde_json::from_slice::<CheckResult>(&b).ok())
        .filter(|c| c.checked_at > now_secs() - 24 * 3600 && c.error.is_empty());
    let latest = match fresh {
        Some(c) if !c.latest_version.is_empty() => c.latest_version,
        _ => {
            let r = check_and_record()?;
            if r.error.is_empty() {
                r.latest_version
            } else {
                INSTALL_IN_PROGRESS.store(false, Ordering::SeqCst);
                return Err(anyhow!("检查更新失败:{}", r.error));
            }
        }
    };
    if !is_newer(&latest, &current_version()) {
        INSTALL_IN_PROGRESS.store(false, Ordering::SeqCst);
        return Err(anyhow!(
            "已是最新版本({}),没有可安装的更新",
            current_version()
        ));
    }
    let script = render_runner(&mode, &latest, &current_version())?;
    spawn_runner(&script)?;
    Ok(latest)
}

fn spawn_runner(script: &str) -> anyhow::Result<()> {
    let runner = crate::data_dir().join("update-runner.sh");
    std::fs::write(&runner, script).context("写 update-runner.sh 失败")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o700)).ok();
    }
    let log = crate::data_dir().join("update.log");
    std::process::Command::new("sh")
        .arg("-c")
        .arg(format!(
            "nohup sh {} >> {} 2>&1 &",
            shell_quote(&runner.to_string_lossy()),
            shell_quote(&log.to_string_lossy())
        ))
        .spawn()
        .context("启动升级 runner 失败")?;
    Ok(())
}

/// serve 回完响应后调:给升级 runner 留 1.2s 落地,然后体面退出,
/// KeepAlive(launchd/systemd/runsv)会用新二进制把服务拉回来。
pub fn schedule_self_exit() {
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_millis(1200));
        eprintln!("[TokenBuddy] 升级重启:进程退出,交由服务管理器/runner 换新二进制拉起");
        std::process::exit(0);
    });
}

fn compile_target() -> &'static str {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        "aarch64-apple-darwin"
    }
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    {
        "x86_64-apple-darwin"
    }
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        "aarch64-unknown-linux-gnu"
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        "x86_64-unknown-linux-musl"
    }
    #[cfg(not(any(
        all(target_os = "macos", target_arch = "aarch64"),
        all(target_os = "macos", target_arch = "x86_64"),
        all(target_os = "linux", target_arch = "aarch64"),
        all(target_os = "linux", target_arch = "x86_64")
    )))]
    {
        "unknown"
    }
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// 版本号进 shell 前的白名单:release 资产名由 tag 拼出,不能让一个恶意
/// tag 名把命令注入进 runner。
fn safe_version(version: &str) -> bool {
    !version.is_empty()
        && version.len() <= 40
        && version
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '+'))
}

/// 生成升级 runner。纯字符串函数,单测直接断言关键步骤都在。
///
/// 重启顺序:launchd → systemd --user → runsv(termux)→ 裸进程兜底。
/// 健康检查:PORT>0 打 /api/status 比对 HEALTH_KEY;=0(没有可发现的服务)
/// 退化为 `--version` 能跑且(github 模式)版本号正确。失败回滚 `.bak`。
fn render_runner(mode: &str, latest: &str, current: &str) -> anyhow::Result<String> {
    if !safe_version(latest) {
        return Err(anyhow!("版本号形迹可疑,拒绝进 shell:{latest:?}"));
    }
    let bin = std::env::current_exe().context("定位当前二进制失败")?;
    let bin = shell_quote(&bin.to_string_lossy());
    let port = serve_port();
    let pid = std::process::id();

    let fetch_and_stage = if mode == "git" {
        format!(
            r#"REPO={repo}
echo "git 模式:拉取并重新构建…"
git -C "$REPO" pull --ff-only || {{ echo "git pull 失败(本地未提交改动或冲突?),放弃"; exit 1; }}
WANT=$(git -C "$REPO" rev-parse --short=8 HEAD)
cargo build --release --quiet || {{ echo "cargo build 失败,放弃"; exit 1; }}
STAGE="$REPO/target/release/tokenbuddy"
HEALTH_KEY=build_commit"#,
            repo = shell_quote(&repo_root()?.to_string_lossy())
        )
    } else {
        let ver_num = latest.trim_start_matches('v');
        format!(
            r#"echo "下载 {latest} …"
TMP=$(mktemp -d)
TGZ=tokenbuddy-{ver_num}-{target}.tar.gz
{curl} -fsSL --max-time 300 -o "$TMP/$TGZ" "https://github.com/{repo}/releases/download/{latest}/$TGZ" || {{ echo "下载失败"; rm -rf "$TMP"; exit 1; }}
if {curl} -fsSL --max-time 60 -o "$TMP/$TGZ.sha256" "https://github.com/{repo}/releases/download/{latest}/$TGZ.sha256" 2>/dev/null; then
  ( cd "$TMP" && {{ sha256sum -c --status "$TGZ.sha256" 2>/dev/null || shasum -a 256 -c "$TGZ.sha256" >/dev/null 2>&1; }} ) || {{ echo "sha256 校验失败,拒绝安装"; rm -rf "$TMP"; exit 1; }}
else
  echo "⚠ 发布未附 sha256,跳过校验"
fi
tar -xzf "$TMP/$TGZ" -C "$TMP" || {{ echo "解压失败"; rm -rf "$TMP"; exit 1; }}
STAGE=$(find "$TMP" -type f -name tokenbuddy | head -1)
[ -n "$STAGE" ] || {{ echo "压缩包里没有 tokenbuddy 二进制"; rm -rf "$TMP"; exit 1; }}
HEALTH_KEY=version
WANT={ver_num}
trap 'rm -rf "$TMP"' EXIT"#,
            curl = CURL,
            repo = RELEASE_REPO,
            latest = latest,
            ver_num = ver_num,
            target = compile_target(),
        )
    };

    Ok(format!(
        r#"#!/bin/sh
# TokenBuddy 自升级 runner(update.rs 生成,勿手编)——{current} -> {latest} (mode={mode})
BIN={bin}
OLD_PID={pid}
PORT={port}

restart_service() {{
  if launchctl print "gui/$(id -u)/com.tokenbuddy.server" >/dev/null 2>&1; then
    echo "经 launchd 重启"; launchctl kickstart -k "gui/$(id -u)/com.tokenbuddy.server"; return 0
  fi
  if systemctl --user is-active --quiet tokenbuddy.service 2>/dev/null; then
    echo "经 systemd --user 重启"; systemctl --user restart tokenbuddy.service; return 0
  fi
  if [ -x /data/data/com.termux/files/usr/bin/sv ] && [ -d /data/data/com.termux/files/usr/var/service/tokenbuddy ]; then
    echo "经 runsv 重启"
    /data/data/com.termux/files/usr/bin/sv down /data/data/com.termux/files/usr/var/service/tokenbuddy
    sleep 1
    install -m 0755 "$STAGE" "$BIN"
    /data/data/com.termux/files/usr/bin/sv up /data/data/com.termux/files/usr/var/service/tokenbuddy
    return 0
  fi
  if [ "$PORT" -le 0 ]; then
    echo "无服务管理器且无可发现的服务:安装完成,下次启动生效"
    return 0
  fi
  echo "无服务管理器:kill 旧进程后裸起"
  kill "$OLD_PID" 2>/dev/null
  lsof -ti tcp:"$PORT" 2>/dev/null | xargs kill 2>/dev/null
  sleep 1
  TOKENBUDDY_NO_OPEN=1 nohup "$BIN" serve --port "$PORT" >>"$HOME/.tokenbuddy/update.log" 2>&1 &
}}

{fetch_and_stage}

cp -p "$BIN" "$BIN.bak" 2>/dev/null || echo "⚠ 旧二进制备份失败(继续,但没有回滚兜底)"
# install 先 unlink 再放新文件,旧进程持着旧 inode 不挡道
install -m 0755 "$STAGE" "$BIN" || {{ echo "install 失败,放弃"; exit 1; }}
sleep 1
restart_service

ok=0
if [ "$PORT" -gt 0 ]; then
  # sed 程序整体单引号,$HEALTH_KEY 在引号外拼接——多层转义的地雷不碰
  for i in $(seq 1 60); do
    V=$(curl -fsS --max-time 2 "http://127.0.0.1:$PORT/api/status" 2>/dev/null | sed -n -E 's/.*"'$HEALTH_KEY'":"([^"]*)".*/\1/p' | head -1)
    if [ "$V" = "$WANT" ]; then ok=1; break; fi
    sleep 0.5
  done
else
  if [ "$HEALTH_KEY" = version ]; then
    "$BIN" --version 2>/dev/null | grep -q "$WANT" && ok=1
  else
    "$BIN" --version >/dev/null 2>&1 && ok=1
  fi
fi
if [ "$ok" = 1 ]; then
  echo "升级成功: $WANT"
  exit 0
fi
echo "健康检查未通过(期望 $HEALTH_KEY=$WANT)——回滚旧二进制"
install -m 0755 "$BIN.bak" "$BIN" 2>/dev/null
restart_service
echo "回滚完成"
"#,
        bin = bin,
        pid = pid,
        port = port,
        current = current,
        latest = latest,
        mode = mode,
        fetch_and_stage = fetch_and_stage,
    ))
}

/// serve 启动时的后台检查线程。未启用时 5 分钟醒一次看配置变没变;
/// 启用后按 interval_hours 节奏检查,auto_install(github 模式限定)才装。
pub fn background_loop() {
    loop {
        std::thread::sleep(std::time::Duration::from_secs(300));
        let cfg = load_config();
        if !cfg.enabled {
            continue;
        }
        let interval_secs = (cfg.interval_hours as u64).max(1) * 3600;
        let last: Option<CheckResult> = std::fs::read(state_path())
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok());
        if let Some(last) = &last {
            if last.checked_at > now_secs() - interval_secs as i64 {
                continue;
            }
        }
        let result = check();
        let auto = cfg.auto_install && result.update_available && result.mode != "git";
        if let Ok(bytes) = serde_json::to_vec(&result) {
            write_private(&state_path(), &bytes).ok();
        }
        if result.error.is_empty() && result.update_available {
            eprintln!(
                "[TokenBuddy] 发现新版本 {}(当前 {}){}",
                result.latest_version,
                result.current_version,
                if auto { ",自动升级开始" } else { "" }
            );
        }
        if auto {
            // 静默升级:runner 起来后本进程自退,服务管理器拉起新的。
            // 失败只留日志——后台线程不许把服务带崩。
            if INSTALL_IN_PROGRESS.swap(true, Ordering::SeqCst) {
                continue;
            }
            match render_runner(
                &result.mode,
                &result.latest_version,
                &result.current_version,
            ) {
                Ok(script) => {
                    if spawn_runner(&script).is_ok() {
                        schedule_self_exit();
                        return; // 本进程即将退出,线程收摊
                    }
                    INSTALL_IN_PROGRESS.store(false, Ordering::SeqCst);
                }
                Err(_) => INSTALL_IN_PROGRESS.store(false, Ordering::SeqCst),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semver_compare() {
        assert!(is_newer("0.11.0", "0.10.0"));
        assert!(is_newer("v0.11.0", "0.10.0"));
        assert!(is_newer("0.10.1", "0.10.0"));
        assert!(!is_newer("0.10.0", "0.10.0"));
        assert!(!is_newer("0.9.9", "0.10.0"));
        // 无法解析的形态退化为不等即新
        assert!(is_newer("20261006", "0.10.0"));
        assert!(!is_newer("same", "same"));
    }

    #[test]
    fn parse_tag_from_release_json() {
        let tag = parse_latest_tag(br#"{"tag_name":"v0.11.0","name":"x"}"#).unwrap();
        assert_eq!(tag, "v0.11.0");
        assert!(parse_latest_tag(br#"{"message":"API rate limit exceeded"}"#).is_err());
        assert!(parse_latest_tag(b"not json").is_err());
    }

    #[test]
    fn version_allowlist_blocks_injection() {
        assert!(safe_version("v0.11.0"));
        assert!(safe_version("0.11.0-rc1"));
        assert!(!safe_version("; rm -rf $HOME"));
        assert!(!safe_version("$(echo pwn)"));
        assert!(!safe_version(""));
    }

    #[test]
    fn config_roundtrip_and_validation() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!(
            "tb-update-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        // 缺省=全关(零外呼)
        let cfg = load_config();
        assert!(!cfg.enabled);
        assert!(!cfg.auto_install);
        assert_eq!(cfg.interval_hours, 24);
        // 合法保存
        save_config(&UpdateConfig {
            enabled: true,
            auto_install: true,
            interval_hours: 12,
            mode: "github".into(),
        })
        .unwrap();
        let cfg = load_config();
        assert!(cfg.enabled && cfg.auto_install && cfg.interval_hours == 12);
        // 非法值拒绝
        let bad = UpdateConfig {
            interval_hours: 0,
            ..cfg.clone()
        };
        assert!(save_config(&bad).is_err());
        let bad = UpdateConfig {
            mode: "pwn".into(),
            ..cfg
        };
        assert!(save_config(&bad).is_err());
        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn runner_script_contains_safety_steps() {
        let script = render_runner("github", "v0.11.0", "0.10.0").unwrap();
        assert!(script.contains("sha256"), "校验步骤必须在");
        assert!(script.contains("install -m 0755"));
        assert!(script.contains(".bak"), "回滚备份必须在");
        assert!(script.contains("launchctl kickstart"));
        assert!(script.contains("systemctl --user restart"));
        assert!(script.contains("sv down"), "runsv 分支必须在");
        assert!(script.contains("回滚"), "健康检查失败要回滚");
        assert!(
            script.contains("https://github.com/walker83/TokenBuddy/releases/download/v0.11.0/"),
            "下载 URL 必须指向固定 release 资产"
        );
        // git 模式:必须拉代码重新构建,且没有下载步骤
        let git = render_runner("git", "v0.11.0", "0.10.0").unwrap();
        assert!(git.contains("pull --ff-only"));
        assert!(git.contains("cargo build --release"));
        assert!(!git.contains("releases/download"));
    }

    #[test]
    fn runner_is_valid_shell() {
        // 模板是 format! 拼出来的,花括号失衡这类病只有真解析才抓得到
        for mode in ["github", "git"] {
            let script = render_runner(mode, "v0.11.0", "0.10.0").unwrap();
            let dir = std::env::temp_dir().join(format!(
                "tb-runner-shntest-{}-{}-{}",
                mode,
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let p = dir.join("runner.sh");
            std::fs::write(&p, &script).unwrap();
            let out = std::process::Command::new("sh")
                .arg("-n")
                .arg(&p)
                .output()
                .unwrap();
            let _ = std::fs::remove_dir_all(&dir);
            assert!(
                out.status.success(),
                "runner({mode}) 不是合法 sh:{}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }
}
