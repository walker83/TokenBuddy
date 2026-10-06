//! Collector for Mirasim (issue #18) — local ndjson usage ledger, zero
//! network. One file per month: `~/.mirasim/insights/usage-YYYY-MM.ndjson`,
//! one JSON call record per line.
//!
//! ⚠️ 口径警告(与 zcode 正好相反,重构时别"统一化"掉):
//! mirasim 的 `input` 是**净新增输入**,真实输入 = `input + cacheRead +
//! cacheWrite`,入库前必须**加**;zcode 的 `model_usage.input_tokens` 已经
//! **包含** cache_read/cache_creation,入库前必须**减**(见 zcode.rs 文件头)。
//! 两边是互补的坑,一边做加法一边做减法,注释互相指向,防止后人把其中
//! 一边改错——照抄 zcode 的减法会让 mirasim 少算缓存,照抄加法会让
//! zcode 多算。
//!
//! `reasoning` 视作 output 的子集(Hermes/OpenAI 口径),不重复计入。
//! `ts` 缺失或不可解析的行**整行跳过**——绝不用 mtime 兜底:账本行必须有
//! 自己的时间,兜底时间戳会让每次同步都长出新行(缺稳定标识的行宁可不发)。
//! `status` 非 2xx 的行照样入账:账本记的是"上游计了什么",失败的调用
//! 可能仍有真实 token 消耗;异常排查看 mirasim 自己的日志。

use crate::{Source, TokenRecord};
use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};

/// 环境变量覆盖(测试与多实例),缺省 `~/.mirasim/insights`。
fn insights_dir() -> PathBuf {
    if let Ok(custom) = std::env::var("MIRASIM_DATA_DIR") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".mirasim/insights")
}

/// Candidate log locations, for `tokenbuddy doctor`.
pub fn log_paths() -> Vec<PathBuf> {
    vec![insights_dir()]
}

/// 逐行解析一个 ndjson 文件。坏行跳过,不拖垮整月。
fn parse_ndjson(path: &Path) -> Result<Vec<TokenRecord>> {
    let data = fs::read_to_string(path)?;
    let mut records = Vec::new();
    for line in data.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let call_id = v.get("id").and_then(|x| x.as_str()).unwrap_or("");
        // ts 缺则整行跳过(文件头注释:宁可不发,不用兜底时间戳)。
        let ts = v
            .get("ts")
            .and_then(|x| x.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.timestamp())
            .filter(|t| *t > 0);
        let Some(ts) = ts else { continue };
        let num = |k: &str| {
            v.get(k)
                .and_then(|x| x.as_f64())
                .map(|f| f.max(0.0) as u64)
                .unwrap_or(0)
        };
        let net_input = num("input");
        let cache_read = num("cacheRead");
        let cache_write = num("cacheWrite");
        let output = num("output");
        // 真实输入 = 净新增 + 缓存读 + 缓存写(文件头口径警告)。全零行
        // 不入账——没有可计费的东西。
        let input = net_input + cache_read + cache_write;
        if input == 0 && output == 0 {
            continue;
        }
        let model = v
            .get("model")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("unknown");
        // 项目归因只用确定性字段:repo 优先,workspace 兜底;都没有就留空,
        // 让它进 "—" 桶(#40 口径:诚实留空,不猜)。
        let project = ["repo", "workspace"]
            .iter()
            .find_map(|k| {
                v.get(k)
                    .and_then(|x| x.as_str())
                    .map(str::to_string)
                    .filter(|s| !s.is_empty())
            })
            .unwrap_or_default();
        let duration_ms = v
            .get("durationMs")
            .and_then(|x| x.as_f64())
            .map(|f| f.max(0.0) as u64);
        records.push(TokenRecord {
            source: Source::Mirasim,
            model: model.to_string(),
            input_tokens: input,
            output_tokens: output,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            timestamp: ts,
            session_id: None,
            project,
            duration_ms,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            // 稳定 id:协议自带的调用 id,重读恒同键,天然幂等。
            record_id: if call_id.is_empty() {
                // 无 id 的行用 内容键 兜底:ts 到秒 + 净输入,重读同值同键;
                // 回填改动 input 会换键,但那正是 #20 说的回填双计面,
                // doctor 的未定型行指标盯得住。
                None
            } else {
                Some(format!("mirasim_{call_id}"))
            },
            sidechain: false,
            merge_key: None,
            request_count: 1,
        });
    }
    Ok(records)
}

/// Parse every month file, newest last. ndjson files are append-only, so the
/// store-side record-id dedupe makes re-reads free.
pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let dir = insights_dir();
    let Ok(entries) = fs::read_dir(&dir) else {
        return Ok(vec![]);
    };
    let mut files: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.extension().and_then(|e| e.to_str()) == Some("ndjson")
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("usage-"))
                    .unwrap_or(false)
        })
        .collect();
    files.sort();
    let mut all = Vec::new();
    for f in &files {
        all.extend(parse_ndjson(f).unwrap_or_default());
    }
    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TEST_ENV_LOCK;

    const TS: &str = "2026-10-05T02:00:00Z";

    fn write_dir(name: &str, lines: &[serde_json::Value]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tb-mirasim-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let text = lines
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(dir.join("usage-2026-10.ndjson"), text).unwrap();
        std::env::set_var("MIRASIM_DATA_DIR", &dir);
        dir
    }

    /// 口径核心:真实输入 = input + cacheRead + cacheWrite(加法,与 zcode
    /// 的减法相反);reasoning 是 output 子集不重复计;dedupe 锁 mirasim_<id>。
    #[test]
    fn real_input_adds_cache_and_id_is_stable_key() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = write_dir(
            "calc",
            &[serde_json::json!({
                "id": "call_01", "ts": TS, "provider": "glm", "model": "glm-5",
                "input": 100, "cacheRead": 2000, "cacheWrite": 300, "output": 50,
                "reasoning": 20, "status": 200, "durationMs": 1234.0,
                "repo": "/home/w/code/foo"
            })],
        );
        let records = collect_records().unwrap();
        assert_eq!(records.len(), 1, "{records:?}");
        let r = &records[0];
        assert_eq!(r.source.as_str(), "mirasim");
        assert_eq!(
            r.input_tokens, 2_400,
            "100 净新增 + 2000 缓存读 + 300 缓存写"
        );
        assert_eq!(r.output_tokens, 50, "reasoning 已在 output 里,不重复计");
        assert_eq!(r.record_id.as_deref(), Some("mirasim_call_01"));
        assert_eq!(r.timestamp, 1_791_165_600); // 2026-10-05T02:00:00Z
        assert_eq!(r.project, "/home/w/code/foo", "repo 是确定性归因字段");
        assert_eq!(r.duration_ms, Some(1234));
        assert_eq!(r.cache_read_tokens, 0, "缓存已并入 input,不再单列重复计");
        std::env::remove_var("MIRASIM_DATA_DIR");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// ts 缺失/不可解析整行跳过(不落 mtime 兜底);坏 JSON 行不拖垮整月;
    /// 全零行不入账;非 2xx 照常入账。
    #[test]
    fn hostile_lines_degrade_honestly() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = write_dir(
            "hostile",
            &[
                serde_json::json!({"id": "a", "model": "m", "input": 10}), // 无 ts
                serde_json::json!({"id": "b", "ts": "not-a-time", "input": 10}),
                serde_json::json!({"id": "c", "ts": TS, "model": "m", "input": 0, "output": 0}),
                serde_json::json!({"id": "d", "ts": TS, "model": "m", "input": 5, "status": 500}),
                serde_json::json!({"ts": TS, "model": "m", "input": 7}), // 无 id
            ],
        );
        let broken = dir.join("usage-2026-09.ndjson");
        std::fs::write(
            &broken,
            format!("{{not json}}\n\n{{\"id\":\"x\",\"ts\":\"{TS}\",\"input\":3}}"),
        )
        .unwrap();
        let records = collect_records().unwrap();
        let ids: Vec<Option<&str>> = records.iter().map(|r| r.record_id.as_deref()).collect();
        assert_eq!(
            ids,
            vec![Some("mirasim_x"), Some("mirasim_d"), None],
            "只入账 d(非 2xx 照计)、x(隔壁月份文件的有效行)与无 id 的内容键行;坏行/全零行/无 ts 行全跳过"
        );
        let no_id = records.iter().find(|r| r.record_id.is_none()).unwrap();
        assert_eq!(no_id.input_tokens, 7);
        std::env::remove_var("MIRASIM_DATA_DIR");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = broken;
    }
}
