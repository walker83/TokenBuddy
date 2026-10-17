//! 工具一键接入(F11)——把 base_url + 客户端密钥写进工具自己的配置。
//!
//! 纪律:这是 TokenBuddy 第一个「写工具配置」的功能,必须显式、可逆——
//! 先备份(`<file>.tb-backup-<ts>`)→ 幂等更新(有 TokenBuddy 条目就原地改,
//! 没有就追加)→ 人话报告改了什么、旧值是什么、怎么还原。
//! opencode 的 JSONC 手改风险高,只出手册不落笔。
//!
//! 支持面(实测配置格式在案):claude(settings.json env)、
//! zcode(v2/provider_config.json,provider rule + order)、
//! codex(config.toml 追加 model_provider,env key 让用户自己 export)。

use super::{add_client, load_clients, load_config_strict};
use anyhow::{anyhow, bail, Result};
use std::path::PathBuf;

/// TokenBuddy 在各工具配置里的标记,幂等更新与 restore 都认它。
pub const MARKER: &str = "TokenBuddy Gateway";

fn home() -> PathBuf {
    dirs::home_dir().unwrap_or_else(|| PathBuf::from("."))
}

pub fn supported_tools() -> &'static [&'static str] {
    &["claude", "zcode", "codex", "opencode"]
}

fn backup(path: &std::path::Path) -> Result<PathBuf> {
    if !path.exists() {
        return Ok(path.to_path_buf()); // 首次创建,无需备份
    }
    let ts = crate::now_ts();
    // 文件名后缀追加(不是 with_extension:那会把 settings.json 的
    // "json" 换掉,restore 就找不回来了)。
    let name = format!(
        "{}.tb-backup-{ts}",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
    );
    let bak = path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(name);
    std::fs::copy(path, &bak)?;
    Ok(bak)
}

/// 还原到最近一次备份。
pub fn restore(tool: &str) -> Result<String> {
    let target = match tool {
        "claude" => home().join(".claude/settings.json"),
        "zcode" => home().join(".zcode/v2/provider_config.json"),
        "codex" => home().join(".codex/config.toml"),
        other => bail!("restore 不支持 {other};支持:claude/zcode/codex"),
    };
    let dir = target
        .parent()
        .ok_or_else(|| anyhow!("配置文件没有父目录"))?;
    let mut backups: Vec<PathBuf> = std::fs::read_dir(dir)?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| {
                    n.starts_with(&format!(
                        "{}.tb-backup-",
                        target.file_name().unwrap_or_default().to_string_lossy()
                    ))
                })
                .unwrap_or(false)
        })
        .collect();
    backups.sort_by_key(|p| std::fs::metadata(p).and_then(|m| m.modified()).ok());
    let Some(latest) = backups.pop() else {
        bail!("没有找到 {tool} 的备份文件,无从还原");
    };
    std::fs::copy(&latest, &target)?;
    Ok(format!(
        "已把 {} 还原为备份 {}",
        target.display(),
        latest.display()
    ))
}

/// 接入入口:解析网关地址与客户端密钥(没有就签发一把),再按工具写入。
pub fn run(tool: &str) -> Result<String> {
    let cfg = load_config_strict()?;
    if !cfg.enabled {
        bail!("网关未启用:先 `tokenbuddy gateway on`(或面板开关)再 setup");
    }
    let host = cfg
        .listen
        .rsplit_once(':')
        .map(|(h, _)| h.to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let host = if host == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        host
    };
    let port = cfg
        .listen
        .rsplit_once(':')
        .map(|(_, p)| p.to_string())
        .unwrap_or_else(|| "8790".to_string());
    let base = format!("http://{host}:{port}");

    let key = match load_clients()?.into_iter().find(|c| c.label == tool) {
        Some(c) => c.key,
        None => add_client(tool)?.key,
    };

    match tool {
        "claude" => setup_claude(&base, &key),
        "zcode" => setup_zcode(&cfg, &base, &key),
        "codex" => setup_codex(&base, &key),
        "opencode" => Ok(format!(
            "opencode 用 JSONC 手改更稳,把这段加进 {}/opencode.json 的 provider 节点:\n\
             \"tokenbuddy\": {{\n  \"npm\": \"@ai-sdk/openai-compatible\",\n  \
             \"options\": {{ \"baseURL\": \"{base}/v1\", \"apiKey\": \"{key}\" }}\n}}",
            home().join(".config").display()
        )),
        other => bail!(
            "还不支持 {other} 的自动接入;支持:{}。其它工具手动指向 \
             {base}(OpenAI /v1/chat/completions · Anthropic /v1/messages),密钥 `gateway key add` 签发",
            supported_tools().join("/")
        ),
    }
}

fn setup_claude(base: &str, key: &str) -> Result<String> {
    let path = home().join(".claude/settings.json");
    let bak = backup(&path)?;
    let mut root: serde_json::Value = if path.exists() {
        serde_json::from_str(&std::fs::read_to_string(&path)?)
            .map_err(|e| anyhow!("{} 不是合法 JSON,拒绝手改:{e}", path.display()))?
    } else {
        serde_json::json!({})
    };
    let old_base = root
        .pointer("/env/ANTHROPIC_BASE_URL")
        .and_then(|v| v.as_str())
        .unwrap_or("(未设置)")
        .to_string();
    let env = root
        .as_object_mut()
        .expect("settings.json root is object")
        .entry("env")
        .or_insert_with(|| serde_json::json!({}));
    env["ANTHROPIC_BASE_URL"] = serde_json::Value::String(base.to_string());
    env["ANTHROPIC_AUTH_TOKEN"] = serde_json::Value::String(key.to_string());
    std::fs::write(&path, serde_json::to_string_pretty(&root)?)?;
    Ok(format!(
        "claude 已接入:{}\n  ANTHROPIC_BASE_URL {old_base} → {base}\n  ANTHROPIC_AUTH_TOKEN → {key}\n\
         备份:{}(还原:`tokenbuddy gateway restore claude`)\n\
         注意:原 ANTHROPIC_MODEL 等模型名映射保留;重启 claude 生效。",
        path.display(),
        bak.display()
    ))
}

fn setup_zcode(cfg: &super::GatewayConfig, base: &str, key: &str) -> Result<String> {
    let path = home().join(".zcode/v2/provider_config.json");
    let bak = backup(&path)?;
    let text = if path.exists() {
        std::fs::read_to_string(&path)?
    } else {
        r#"{"schemaVersion":1,"config":{"providerOrder":[],"providerConfigRules":{"providerRules":[]}}}"#.to_string()
    };
    let mut root: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| anyhow!("{} 不是合法 JSON,拒绝手改:{e}", path.display()))?;
    let models: Vec<String> = cfg
        .providers
        .iter()
        .flat_map(|p| p.models.iter().cloned())
        .take(8)
        .collect();
    let rule = serde_json::json!({
        "providerId": "tb-gateway-openai",
        "providerName": MARKER,
        "config": {
            "group": "standard-personal",
            "access": {"type": "api-key", "apiKey": key},
            "api": {"type": "openai-chat-completions", "baseUrl": format!("{base}/v1")},
            "personalModelIds": models,
            "modelOrder": []
        }
    });
    let obj = root.as_object_mut().ok_or_else(|| anyhow!("结构不符"))?;
    let config = obj
        .get_mut("config")
        .and_then(|c| c.as_object_mut())
        .ok_or_else(|| anyhow!("provider_config 缺 config 节"))?;
    let rules = config
        .get_mut("providerConfigRules")
        .and_then(|r| r.get_mut("providerRules"))
        .and_then(|r| r.as_array_mut())
        .ok_or_else(|| anyhow!("provider_config 缺 providerRules 数组"))?;
    let replaced = rules
        .iter_mut()
        .any(|r| r.get("providerName").and_then(|n| n.as_str()) == Some(MARKER));
    if replaced {
        for r in rules.iter_mut() {
            if r.get("providerName").and_then(|n| n.as_str()) == Some(MARKER) {
                *r = rule.clone();
            }
        }
    } else {
        rules.push(rule);
        if let Some(order) = config
            .get_mut("providerOrder")
            .and_then(|o| o.as_array_mut())
        {
            if !order
                .iter()
                .any(|v| v.as_str() == Some("tb-gateway-openai"))
            {
                order.push(serde_json::Value::String("tb-gateway-openai".into()));
            }
        }
    }
    std::fs::write(&path, serde_json::to_string_pretty(&root)?)?;
    Ok(format!(
        "zcode 已接入:{}\n  provider 「{MARKER}」 → {base}/v1(openai 兼容)\n\
         备份:{}(还原:`tokenbuddy gateway restore zcode`)\n\
         在 ZCode 模型选择里挑「{MARKER}」下的模型即可经网关入账。",
        path.display(),
        bak.display()
    ))
}

fn setup_codex(base: &str, key: &str) -> Result<String> {
    let path = home().join(".codex/config.toml");
    if path.exists() {
        let text = std::fs::read_to_string(&path)?;
        if text.contains("model_providers.tokenbuddy") {
            bail!(
                "config.toml 已有 [model_providers.tokenbuddy],为免弄坏 TOML 请手改或先删旧段再重跑"
            );
        }
        let bak = backup(&path)?;
        let block = format!(
            "\n[model_providers.tokenbuddy]\nname = \"{MARKER}\"\nbase_url = \"{base}/v1\"\n\
             env_key = \"TOKENBUDDY_GATEWAY_KEY\"\nwire_api = \"chat\"\n"
        );
        let mut out = text.clone();
        if !out.ends_with('\n') {
            out.push('\n');
        }
        out.push_str(&block);
        std::fs::write(&path, out)?;
        Ok(format!(
            "codex 已接入:{}\n  [model_providers.tokenbuddy] → {base}/v1\n\
             备份:{}(还原:`tokenbuddy gateway restore codex`)\n\
             还差一步:export TOKENBUDDY_GATEWAY_KEY={key}\n\
             (写进 ~/.zshrc 后 `codex -m <模型> --config model_provider=tokenbuddy` 或在 config.toml 设 model_provider)",
            path.display(),
            bak.display()
        ))
    } else {
        let bak = backup(&path)?;
        std::fs::create_dir_all(path.parent().expect("codex dir"))?;
        std::fs::write(
            &path,
            format!(
                "[model_providers.tokenbuddy]\nname = \"{MARKER}\"\nbase_url = \"{base}/v1\"\n\
                 env_key = \"TOKENBUDDY_GATEWAY_KEY\"\nwire_api = \"chat\"\n"
            ),
        )?;
        Ok(format!(
            "codex 已接入(新建 {})\nexport TOKENBUDDY_GATEWAY_KEY={key}\n备份:{}",
            path.display(),
            bak.display()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TEST_ENV_LOCK;

    /// 每个用例独立的假 HOME;不碰 gateway.json(enabled 由 run 前置校验,
    /// 这里直接测三个 setup_* 写入函数的幂等与备份)。
    fn temp_home(tag: &str) -> PathBuf {
        let dir = crate::unique_test_dir(&format!("gwsetup-{tag}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("HOME", &dir);
        dir
    }

    #[test]
    fn claude_setup_is_idempotent_and_backs_up() {
        let _g = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = temp_home("claude");
        std::fs::create_dir_all(dir.join(".claude")).unwrap();
        std::fs::write(
            dir.join(".claude/settings.json"),
            r#"{"model":"opus","env":{"ANTHROPIC_BASE_URL":"https://old.example.com","OTHER":"keep"}}"#,
        )
        .unwrap();

        let msg = setup_claude("http://127.0.0.1:8790", "tb-local-k1").unwrap();
        assert!(msg.contains("https://old.example.com"), "旧值要报告:{msg}");
        let root: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join(".claude/settings.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(root["env"]["ANTHROPIC_BASE_URL"], "http://127.0.0.1:8790");
        assert_eq!(root["env"]["ANTHROPIC_AUTH_TOKEN"], "tb-local-k1");
        assert_eq!(root["env"]["OTHER"], "keep", "无关 env 保留");
        assert_eq!(root["model"], "opus", "无关顶层键保留");
        // 备份存在且可还原
        let msg2 = setup_claude("http://127.0.0.1:8791", "tb-local-k2").unwrap();
        assert!(msg2.contains("tb-backup-"));
        assert!(restore("claude").unwrap().contains("还原"));
        let restored: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join(".claude/settings.json")).unwrap(),
        )
        .unwrap();
        // 最近一次备份 = 第二次 setup 写 8791 之前的快照 = 8790 时的旧值。
        assert_eq!(
            restored["env"]["ANTHROPIC_BASE_URL"],
            "http://127.0.0.1:8790"
        );
        std::env::remove_var("HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn zcode_setup_appends_then_updates_in_place() {
        let _g = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = temp_home("zcode");
        std::fs::create_dir_all(dir.join(".zcode/v2")).unwrap();
        let cfg: crate::gateway::GatewayConfig = serde_json::from_value(serde_json::json!({
            "enabled": true,
            "providers": [{"id":"mm","protocol":"openai","base_url":"http://x/v1",
                           "key_file":"k","models":["MiniMax-M3.1-Flash-Preview"]}]
        }))
        .unwrap();

        setup_zcode(&cfg, "http://127.0.0.1:8790", "tb-local-z").unwrap();
        setup_zcode(&cfg, "http://127.0.0.1:8790", "tb-local-z2").unwrap();
        let root: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join(".zcode/v2/provider_config.json")).unwrap(),
        )
        .unwrap();
        let rules = root["config"]["providerConfigRules"]["providerRules"]
            .as_array()
            .unwrap();
        let tb_rules = rules.iter().filter(|r| r["providerName"] == MARKER).count();
        assert_eq!(tb_rules, 1, "重复 setup 原地更新,不堆积:{rules:?}");
        assert_eq!(
            rules[0]["config"]["api"]["baseUrl"],
            "http://127.0.0.1:8790/v1"
        );
        assert_eq!(
            rules[0]["config"]["personalModelIds"][0],
            "MiniMax-M3.1-Flash-Preview"
        );
        let order = root["config"]["providerOrder"].as_array().unwrap();
        assert!(order
            .iter()
            .any(|v| v.as_str() == Some("tb-gateway-openai")));
        std::env::remove_var("HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn codex_setup_appends_and_refuses_second_write() {
        let _g = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = temp_home("codex");
        std::fs::create_dir_all(dir.join(".codex")).unwrap();
        std::fs::write(dir.join(".codex/config.toml"), "model = \"gpt-5\"\n").unwrap();

        let msg = setup_codex("http://127.0.0.1:8790", "tb-local-c").unwrap();
        assert!(msg.contains("TOKENBUDDY_GATEWAY_KEY"));
        let text = std::fs::read_to_string(dir.join(".codex/config.toml")).unwrap();
        assert!(text.contains("[model_providers.tokenbuddy]"));
        assert!(text.contains("model = \"gpt-5\""), "原有 TOML 保留");
        // 第二次写拒绝(防 TOML 文本手术弄坏文件)
        assert!(setup_codex("http://127.0.0.1:8790", "k").is_err());
        std::env::remove_var("HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
