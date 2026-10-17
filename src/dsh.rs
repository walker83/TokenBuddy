//! Collector for DeepSeek Harness (issue #62) — the local-first AI harness
//! whose session logs are **zstd-compressed JSONL**, the first compressed
//! source in the ledger. One file per session:
//! `~/.dsh/sessions/<munged-cwd>/session-<uuid>/session.v4.jsonl.zstd`.
//!
//! ⚠️ 口径警告(与 zcode 相反,与 mirasim 同款,重构时别"统一化"掉):
//! dsh 的 `data.usage.inputTokens` 是**净新增输入,不含 cache**——真机实测
//! 每条 `assistant/message` 行都满足 `totalTokens == inputTokens +
//! outputTokens + cacheReadTokens + cacheWriteTokens`(如 47191 + 832 +
//! 1339 + 0 == 49362),即缓存读/写单独列在 cacheRead/cacheWrite 里,不在
//! input 里。因此真实输入 = `inputTokens + cacheReadTokens +
//! cacheWriteTokens`,入库前必须**加**(见 mirasim.rs 文件头,那边同款);
//! zcode 的 `model_usage.input_tokens` 则已**包含**缓存,入库前要**减**
//! (见 zcode.rs 文件头)。照抄 zcode 的减法会让 dsh 少算全部缓存读。
//! 缓存并入 input 后 `cache_read_tokens`/`cache_creation_tokens` 置 0,
//! 不再单列重复计(mirasim 同款)。
//!
//! 每行是一条事件(`type` + 毫秒 `time` + `seq`),计费只在
//! `type == "assistant/message"` 的行:一行 = 一次 API 调用(step)的
//! **逐行增量**,不是快照累计——`inputTokens` 逐行上下波动,不单调。
//! 模型在 `data.message.source.{provider, model}`(一个会话中途可切模型,
//! 逐行取,不取会话级 `model/selection`);消息身份在 `data.message.id`
//! (uuid,会话内唯一,issue 勘察里写的 `data.id` 实际不存在)。
//! `reasoning` 是 content 里的块,usage 未单列 reasoning tokens,无可拆,
//! 对齐其它源:output 就是 output,不重复计。
//!
//! 项目归因照 qwen 先例:首行 `{"type":"session"}` 的 `cwd`。
//! `step/start`(`data.{turn, step}`)与 assistant/message 的行时间差给出
//! duration;`data.stream[0].time`(首 chunk)与 step/start 差给出 ttft,
//! 缺任一侧或为负(时钟漂移)则留空——不猜。
//!
//! 解压用 `zstd` crate:它已是 runtime 依赖(main.rs 解内嵌面板资产、
//! context.rs 压检索 arena、parquet 的 zstd feature 都在用,背后同一份
//! zstd-sys),不再引 `ruzstd` 平添第二份实现。损坏的帧解码失败整个文件
//! 跳过(行级坏行单跳),不拖垮其它会话。

use crate::{Source, TokenRecord};
use anyhow::Result;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// 环境变量覆盖(测试与多实例),缺省 `~/.dsh`;会话日志在其下 `sessions/`。
fn sessions_root() -> PathBuf {
    if let Ok(custom) = std::env::var("DSH_HOME") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed).join("sessions");
        }
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".dsh/sessions")
}

/// Candidate log locations, for `tokenbuddy doctor`.
pub fn log_paths() -> Vec<PathBuf> {
    vec![sessions_root()]
}

/// Where this collector reads from, when that place exists on this machine.
pub fn log_path() -> Option<PathBuf> {
    log_paths().into_iter().find(|p| p.exists())
}

/// 只认 `session.v*.jsonl.zstd` 会话日志——目录里还有 session.lock 等
/// 邻居,文件名前缀 `session.` + 后缀 `.jsonl.zstd` 双重卡死,散落的
/// 其它 zstd 文件不是会话。
fn is_session_log(p: &Path) -> bool {
    p.file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.starts_with("session.") && n.ends_with(".jsonl.zstd"))
        .unwrap_or(false)
}

/// 递归找全部会话文件。dsh 自己把 cwd munge 成目录名、再套一层
/// `session-<uuid>`,深度不承诺固定,按根递归(qwen drain 同款栈遍历)。
fn session_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![sessions_root()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if is_session_log(&p) {
                files.push(p);
            }
        }
    }
    files.sort();
    files
}

/// 毫秒 epoch(行级 `time`)→ 秒;缺失或 ≤0 返回 None——账本行必须有
/// 自己的时间,不用 mtime 兜底(mirasim 同款:兜底时间戳会让每次同步
/// 都长出新行)。
fn line_time_ms(v: &serde_json::Value) -> Option<i64> {
    v.get("time").and_then(|x| x.as_i64()).filter(|t| *t > 0)
}

/// `data.{turn, step}` 复合键,step/start 与 assistant/message 靠它配对。
fn step_key(data: &serde_json::Value) -> Option<(i64, i64)> {
    let turn = data.get("turn").and_then(|x| x.as_i64())?;
    let step = data.get("step").and_then(|x| x.as_i64())?;
    Some((turn, step))
}

/// usage 里的无符号计数:缺失按 0,负数钳 0,浮点/天文数字经 f64 饱和
/// 转换——中毒语料不许炸出 panic。
fn usage_num(usage: &serde_json::Value, key: &str) -> u64 {
    usage
        .get(key)
        .and_then(|x| x.as_f64())
        .map(|f| f.max(0.0) as u64)
        .unwrap_or(0)
}

/// 逐行解析一个会话文件。坏行单跳;解码失败(截断帧、坏 magic)整文件
/// Err,由调用方按文件跳过。
fn parse_session_file(path: &Path) -> Result<Vec<TokenRecord>> {
    let compressed = fs::read(path)?;
    let raw = zstd::decode_all(&compressed[..])?;
    let text = String::from_utf8_lossy(&raw);

    let mut records = Vec::new();
    // 首行 session 元数据:cwd 归项目,id 归会话;缺席(截断文件)时退化
    // 为 "unknown" 会话、空项目——诚实留空,不猜(qwen/#40 口径)。
    let mut session_id = String::from("unknown");
    let mut project = String::new();
    // step/start 的毫秒时间,按 (turn, step) 配对给后续 assistant/message。
    let mut step_starts: HashMap<(i64, i64), i64> = HashMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        match v.get("type").and_then(|t| t.as_str()).unwrap_or("") {
            "session" => {
                if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                    session_id = id.to_string();
                }
                if let Some(cwd) = v.get("cwd").and_then(|x| x.as_str()) {
                    project = cwd.to_string();
                }
            }
            "step/start" => {
                let data = v.get("data");
                if let (Some(key), Some(t)) = (data.and_then(step_key), line_time_ms(&v)) {
                    step_starts.insert(key, t);
                }
            }
            "assistant/message" => {
                let Some(time_ms) = line_time_ms(&v) else {
                    continue;
                };
                let Some(data) = v.get("data") else {
                    continue;
                };
                let Some(usage) = data.get("usage") else {
                    continue;
                };
                let net_input = usage_num(usage, "inputTokens");
                let cache_read = usage_num(usage, "cacheReadTokens");
                let cache_write = usage_num(usage, "cacheWriteTokens");
                let output = usage_num(usage, "outputTokens");
                // 真实输入 = 净新增 + 缓存读 + 缓存写(文件头口径警告)。
                // 全零行不入账——没有可计费的东西。
                let input = net_input
                    .saturating_add(cache_read)
                    .saturating_add(cache_write);
                if input == 0 && output == 0 {
                    continue;
                }
                let model = data
                    .get("message")
                    .and_then(|m| m.get("source"))
                    .and_then(|s| s.get("model"))
                    .and_then(|x| x.as_str())
                    .filter(|s| !s.is_empty())
                    .unwrap_or("unknown");
                let msg_id = data
                    .get("message")
                    .and_then(|m| m.get("id"))
                    .and_then(|x| x.as_str())
                    .filter(|s| !s.is_empty());
                // duration/ttft:与配对 step/start 的差;缺配对或负差留空。
                let started = step_key(data).and_then(|k| step_starts.get(&k).copied());
                let duration_ms = started.map(|s| (time_ms - s).max(0) as u64);
                let ttft_ms = started.and_then(|s| {
                    data.get("stream")
                        .and_then(|s| s.as_array())
                        .and_then(|a| a.first())
                        .and_then(|c| c.get("time"))
                        .and_then(|x| x.as_i64())
                        .map(|first| first - s)
                        .filter(|dt| *dt >= 0)
                        .map(|dt| dt as u64)
                });
                records.push(TokenRecord {
                    source: Source::Dsh,
                    model: model.to_string(),
                    input_tokens: input,
                    output_tokens: output,
                    cache_read_tokens: 0,
                    cache_creation_tokens: 0,
                    timestamp: time_ms / 1000,
                    session_id: Some(session_id.clone()),
                    project: project.clone(),
                    duration_ms,
                    ttft_ms,
                    credits: 0.0,
                    context_ratio: 0.0,
                    // 稳定 id:会话 uuid + 消息 uuid,重读恒同键,天然幂等。
                    // 无消息 id 的行退 None,由 store 的冻结兜底键接管
                    // ({前缀}_{timestamp}_{input},见 docs/dedupe-audit)。
                    record_id: msg_id.map(|m| format!("{session_id}_{m}")),
                    sidechain: false,
                    merge_key: None,
                    request_count: 1,
                });
            }
            _ => {}
        }
    }
    Ok(records)
}

/// Parse every session file, newest last. Session logs are append-only, so
/// the store-side record-id dedupe makes re-reads free.
pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let mut all = Vec::new();
    for f in &session_files() {
        all.extend(parse_session_file(f).unwrap_or_default());
    }
    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TEST_ENV_LOCK;

    /// 把 JSONL 文本压成 zstd 帧写进 `<root>/<munged>/session-<id>/`。
    fn write_session(root: &Path, munged: &str, sess: &str, lines: &[serde_json::Value]) {
        let text = lines
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        let frame = zstd::bulk::compress(text.as_bytes(), 0).unwrap();
        let dir = root.join(munged).join(format!("session-{sess}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("session.v4.jsonl.zstd"), frame).unwrap();
    }

    fn setup_home(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tb-dsh-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // 采集器扫的是 $DSH_HOME/sessions,返回值即会话根。
        let sessions = dir.join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        std::env::set_var("DSH_HOME", &dir);
        sessions
    }

    /// 口径核心:inputTokens 是净新增,真实输入 = input + cacheRead +
    /// cacheWrite(加法,zcode 的反面);缓存并入后不单列;模型逐行取
    /// `data.message.source.model`;cwd 归项目;duration/ttft 由 step/start
    /// 配对;去重键 = 会话 id + 消息 id,重读同键。
    #[test]
    fn net_new_input_adds_cache_and_keys_are_stable() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let root = setup_home("calc");

        // 真机同款 shape:session 首行 + step/start + 两条 assistant/message
        // (同一会话两步,第二步换了模型)。
        let t0 = 1_790_730_976_000i64; // step/start
        let t1 = 1_790_730_978_110i64; // 第一步消息完成
        let t2 = 1_790_730_982_365i64; // 第二步消息完成
        write_session(
            &root,
            "--Users-alice-proj--",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            &[
                serde_json::json!({
                    "type": "session", "version": 4,
                    "id": "session-abcd", "createdAt": t0,
                    "cwd": "/Users/alice/proj", "delegationDepth": 0,
                }),
                serde_json::json!({
                    "type": "step/start", "seq": 6, "time": t0,
                    "data": {"turn": 1, "step": 1},
                }),
                serde_json::json!({
                    "type": "assistant/message", "seq": 18, "time": t1,
                    "data": {
                        "turn": 1, "step": 1,
                        "message": {
                            "role": "assistant",
                            "id": "msg-1",
                            "source": {"kind": "model", "provider": "deepseek-account",
                                       "model": "deepseek-flash"},
                        },
                        "usage": {"inputTokens": 100, "outputTokens": 50,
                                  "cacheReadTokens": 2000, "cacheWriteTokens": 300,
                                  "totalTokens": 2450},
                        "stream": [{"type": "chunk", "time": t0 + 1500}],
                    },
                }),
                serde_json::json!({
                    "type": "step/start", "seq": 24, "time": t1 + 10,
                    "data": {"turn": 1, "step": 2},
                }),
                serde_json::json!({
                    "type": "assistant/message", "seq": 34, "time": t2,
                    "data": {
                        "turn": 1, "step": 2,
                        "message": {
                            "role": "assistant",
                            "id": "msg-2",
                            "source": {"kind": "model", "provider": "minimax-cn",
                                       "model": "MiniMax-M3.1-Flash-Preview"},
                        },
                        // 真机算例:47191 + 832 + 1339 + 0 == 49362
                        "usage": {"inputTokens": 47191, "outputTokens": 832,
                                  "cacheReadTokens": 1339, "cacheWriteTokens": 0,
                                  "totalTokens": 49362},
                    },
                }),
            ],
        );

        let records = collect_records().unwrap();
        assert_eq!(records.len(), 2, "{records:?}");
        let first = records
            .iter()
            .find(|r| r.record_id.as_deref() == Some("session-abcd_msg-1"))
            .unwrap();
        assert_eq!(first.source.as_str(), "dsh");
        assert_eq!(
            first.input_tokens, 2_400,
            "100 净新增 + 2000 缓存读 + 300 缓存写"
        );
        assert_eq!(first.output_tokens, 50);
        assert_eq!(
            first.cache_read_tokens, 0,
            "缓存已并入 input,不再单列重复计"
        );
        assert_eq!(first.model, "deepseek-flash");
        assert_eq!(first.project, "/Users/alice/proj", "session cwd 归项目");
        assert_eq!(first.session_id.as_deref(), Some("session-abcd"));
        assert_eq!(first.timestamp, t1 / 1000, "毫秒 time 换算秒");
        assert_eq!(first.duration_ms, Some((t1 - t0) as u64));
        assert_eq!(first.ttft_ms, Some(1500));
        assert_eq!(first.request_count, 1);

        let second = records
            .iter()
            .find(|r| r.record_id.as_deref() == Some("session-abcd_msg-2"))
            .unwrap();
        assert_eq!(
            second.input_tokens, 48_530,
            "真机算例:47191 净新增 + 1339 缓存读 + 0 缓存写(totalTokens 49362 还含 output 832)"
        );
        assert_eq!(second.model, "MiniMax-M3.1-Flash-Preview", "逐行取模型");
        assert_eq!(second.ttft_ms, None, "无 stream 首块,ttft 诚实留空(不猜)");

        // 重读恒同键 + 同消息 id 不同会话不同键(去重键会话域隔离)。
        let again = collect_records().unwrap();
        assert_eq!(
            first.record_id,
            again
                .iter()
                .find(|r| r.output_tokens == 50)
                .unwrap()
                .record_id
        );
        write_session(
            &root,
            "--Users-alice-other--",
            "ffff-0000",
            &[
                serde_json::json!({
                    "type": "session", "version": 4, "id": "session-zzz",
                    "createdAt": t0, "cwd": "/Users/alice/other",
                }),
                serde_json::json!({
                    "type": "assistant/message", "seq": 1, "time": t1,
                    "data": {"turn": 1, "step": 1,
                             "message": {"id": "msg-1",
                                         "source": {"model": "deepseek-flash"}},
                             "usage": {"inputTokens": 5, "outputTokens": 1,
                                       "cacheReadTokens": 0, "cacheWriteTokens": 0}},
                }),
            ],
        );
        let records = collect_records().unwrap();
        let dup = records
            .iter()
            .filter(|r| {
                r.record_id
                    .as_deref()
                    .map(|id| id.ends_with("_msg-1"))
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        assert_eq!(dup.len(), 2, "同消息 id 落在两个会话 = 两条独立账");
        assert_eq!(
            dup.iter()
                .map(|r| r.record_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("session-zzz_msg-1"), Some("session-abcd_msg-1")],
            "去重键会话域隔离:消息 uuid 相同、会话不同 → 键不同(文件序 other < proj)"
        );

        std::env::remove_var("DSH_HOME");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// 敌意行/敌意文件诚实降级:无 time 整行跳过(不落 mtime 兜底)、
    /// 无 usage 行跳过、全零行不入账、坏 JSON 行单跳、负数计数钳 0 照常
    /// 入账、截断 zstd 帧与坏 magic 文件整体跳过且不拖垮隔壁会话、
    /// session 首行缺席退 "unknown" 会话与空项目。
    #[test]
    fn hostile_files_and_lines_degrade_honestly() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let root = setup_home("hostile");

        // 文件 A:session 首行 + 混入各路坏行 + 好行(负数钳 0 后可计)。
        write_session(
            &root,
            "--proj-a--",
            "sess-a",
            &[
                serde_json::json!({
                    "type": "session", "version": 4, "id": "sess-a",
                    "createdAt": 1_790_729_000_000i64, "cwd": "/Users/alice/a",
                }),
                serde_json::json!({
                    "type": "assistant/message", "seq": 1, "time": 1_790_730_000_000i64,
                    "data": {"turn": 1, "step": 1,
                             "message": {"id": "no-usage", "source": {"model": "m"}}},
                }), // 无 usage
                serde_json::json!({
                    "type": "assistant/message", "seq": 2,
                    "data": {"turn": 1, "step": 1, "usage": {"inputTokens": 10},
                             "message": {"id": "no-time", "source": {"model": "m"}}},
                }), // 无 time
                serde_json::json!({
                    "type": "assistant/message", "seq": 3, "time": 1_790_730_001_000i64,
                    "data": {"turn": 1, "step": 1,
                             "usage": {"inputTokens": 0, "outputTokens": 0,
                                       "cacheReadTokens": 0, "cacheWriteTokens": 0},
                             "message": {"id": "all-zero", "source": {"model": "m"}}},
                }), // 全零
                serde_json::json!({
                    "type": "assistant/message", "seq": 4, "time": 1_790_730_002_000i64,
                    "data": {"turn": 1, "step": 1,
                             "usage": {"inputTokens": -5, "outputTokens": 7,
                                       "cacheReadTokens": -100, "cacheWriteTokens": 0},
                             "message": {"id": "neg", "source": {"model": "m"}}},
                }), // 负数钳 0
                serde_json::json!({
                    "type": "assistant/message", "seq": 5, "time": 1_790_730_003_000i64,
                    "data": {"turn": 1, "step": 1,
                             "usage": {"inputTokens": 9},
                             "message": {"role": "assistant"}},
                }), // 无 id、无 model
            ],
        );
        // 再补一条裸坏行:直接改写压缩内容做不到,单开一个只含坏 JSON 的
        // 合法帧文件。
        {
            let dir = root.join("--proj-a--").join("session-sess-b");
            std::fs::create_dir_all(&dir).unwrap();
            let frame = zstd::bulk::compress(b"{not json}\n\n[]\n\"str\"\n", 0).unwrap();
            std::fs::write(dir.join("session.v4.jsonl.zstd"), frame).unwrap();
        }

        // 文件 C:截断的 zstd 帧(真帧拦腰砍半)→ 解码失败整体跳过。
        {
            let dir = root.join("--proj-a--").join("session-sess-c");
            std::fs::create_dir_all(&dir).unwrap();
            let frame = zstd::bulk::compress(b"{\"type\":\"session\"}", 0).unwrap();
            let cut = &frame[..frame.len() / 2];
            std::fs::write(dir.join("session.v4.jsonl.zstd"), cut).unwrap();
        }
        // 文件 D:坏 magic(裸垃圾字节)→ 整体跳过。
        {
            let dir = root.join("--proj-a--").join("session-sess-d");
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("session.v4.jsonl.zstd"), [0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        }
        // 文件 E:session 首行缺席 → "unknown" 会话、空项目,照样入账。
        write_session(
            &root,
            "--proj-e--",
            "sess-e",
            &[serde_json::json!({
                "type": "assistant/message", "seq": 0, "time": 1_790_730_004_000i64,
                "data": {"turn": 1, "step": 1,
                         "usage": {"inputTokens": 3, "outputTokens": 4},
                         "message": {"id": "orphan", "source": {"model": "m"}}},
            })],
        );
        // 邻居文件:session.lock 与非会话命名的 zstd 都不进发现。
        std::fs::write(
            root.join("--proj-a--")
                .join("session-sess-a")
                .join("session.lock"),
            b"",
        )
        .unwrap();
        std::fs::write(root.join("--proj-a--").join("notes.jsonl.zstd"), b"junk").unwrap();

        let records = collect_records().unwrap();
        let ids: Vec<Option<&str>> = records.iter().map(|r| r.record_id.as_deref()).collect();
        assert_eq!(
            ids,
            vec![
                Some("sess-a_neg"),
                None,
                Some("unknown_orphan")
            ],
            "只入账 neg(负数钳 0)、无 id 内容键行(unknown_ + store 兜底)、orphan(无 session 首行);坏行/无 usage/全零行/截断帧/坏 magic 全跳过"
        );
        let neg = &records[0];
        assert_eq!(neg.input_tokens, 0, "负计数钳 0,不炸不欠账");
        assert_eq!(neg.output_tokens, 7);
        let no_id = &records[1];
        assert_eq!(no_id.input_tokens, 9);
        assert_eq!(no_id.record_id, None, "无消息 id 退 None,store 兜底键接管");
        let orphan = &records[2];
        assert_eq!(orphan.session_id.as_deref(), Some("unknown"));
        assert_eq!(orphan.project, "", "无 cwd 诚实留空");

        std::env::remove_var("DSH_HOME");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// LCG 变异中毒轮(tests/collector_fuzz.rs 先例):确定性伪随机 +
    /// 各路畸形行压成合法 zstd 帧、乱字节也压帧、再丢一个裸乱字节文件,
    /// 采集器必须不 panic、只回 sane 子集。整个测试握 TEST_ENV_LOCK,
    /// 环境变量进程级状态不跨测试互踩。
    #[test]
    fn lcg_poisoned_corpus_never_panics() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let root = setup_home("poison");

        struct Lcg(u64);
        impl Lcg {
            fn next_byte(&mut self) -> u8 {
                self.0 = self
                    .0
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (self.0 >> 33) as u8
            }
            fn garbage(&mut self, len: usize) -> Vec<u8> {
                (0..len).map(|_| self.next_byte()).collect()
            }
        }
        let mut rng = Lcg(0x5EED_2026_1006);
        let poison: Vec<Vec<u8>> = vec![
            br#"{"type":"assistant/message","time":123,"data":{"usag"#.to_vec(),
            b"[1,2,3]".to_vec(),
            b"null".to_vec(),
            br#"{"type":123,"data":{"usage":{"inputTokens":"lots","outputTokens":true},
                "message":{"id":null,"source":{"model":42}}}}"#
                .to_vec(),
            br#"{"type":"assistant/message","time":456,"data":{"usage":{"inputTokens":18446744073709551615,
                "outputTokens":-9223372036854775808},"message":{"id":"big","source":{"model":"m"}}}}"#
                .to_vec(),
            b"\x00\x01\x02\xff\xfe".to_vec(),
            rng.garbage(200),
            rng.garbage(400),
        ];
        let mut payload: Vec<u8> = b"\n\n".to_vec();
        for case in &poison {
            payload.extend_from_slice(case);
            payload.push(b'\n');
        }
        let dir = root.join("--poisoned--").join("session-p1");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("session.v4.jsonl.zstd"),
            zstd::bulk::compress(&payload, 0).unwrap(),
        )
        .unwrap();
        // 乱字节也压成帧(解码多半失败,不许 panic)+ 裸乱字节冒充会话文件。
        let dir2 = root.join("--poisoned--").join("session-p2");
        std::fs::create_dir_all(&dir2).unwrap();
        std::fs::write(
            dir2.join("session.v4.jsonl.zstd"),
            zstd::bulk::compress(&rng.garbage(512), 0).unwrap(),
        )
        .unwrap();
        let dir3 = root.join("--poisoned--").join("session-p3");
        std::fs::create_dir_all(&dir3).unwrap();
        std::fs::write(dir3.join("session.v4.jsonl.zstd"), rng.garbage(64)).unwrap();

        let records = collect_records();
        assert!(records.is_ok(), "中毒语料上采集器不许 panic");

        std::env::remove_var("DSH_HOME");
        let _ = std::fs::remove_dir_all(&root);
    }
}
