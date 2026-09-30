//! Collector for the Cline task-log family: Cline, Roo Code, Kilo Code.
//!
//! Roo Code and Kilo Code are forks of Cline, so all three write the same
//! VS Code globalStorage layout — `globalStorage/<ext-id>/tasks/<taskId>/
//! ui_messages.json` — and every API request lands as a `say:"api_req_started"`
//! entry whose `text` is a JSON blob with `tokensIn` / `tokensOut` /
//! `cacheReads` / `cacheWrites`. The format is confirmed against the
//! open-source tokscale parsers (junhoyeo/tokscale `roocode.rs`), which read
//! exactly these fields for all three products.
//!
//! The `cost` field in the payload is deliberately ignored: this tool does
//! not do money. `modelInfo.modelId` (Cline 4.x+, per message) is the model
//! identity; for older tasks we fall back to the first `<model>` tag inside
//! `environment_details` in the sibling `api_conversation_history.json`,
//! scanned with a hard byte cap so a huge conversation never reaches the heap.

use crate::{file_mtime, FileCacheMap, Source, TokenRecord};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

static FILE_CACHE: Mutex<Option<FileCacheMap>> = Mutex::new(None);

/// Drop the resident parse cache. See `pi::release_caches` for why this
/// exists and who pays it back.
pub fn release_caches() {
    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    *cache = None;
}

/// One family member: extension id inside globalStorage + the Source it
/// bills to + the env var that overrides its root.
struct FamilyMember {
    ext_id: &'static str,
    source: Source,
    env_var: &'static str,
}

const FAMILY: [FamilyMember; 3] = [
    FamilyMember {
        ext_id: "saoudrizwan.claude-dev",
        source: Source::Cline,
        env_var: "CLINE_DATA_DIR",
    },
    FamilyMember {
        ext_id: "rooveterinaryinc.roo-cline",
        source: Source::RooCode,
        env_var: "ROO_DATA_DIR",
    },
    FamilyMember {
        ext_id: "kilocode.kilo-code",
        source: Source::Kilo,
        env_var: "KILO_DATA_DIR",
    },
];

/// VS Code-family editors we look under, per platform. Windows is not built
/// for today; the env vars above are the escape hatch for any layout we miss.
const EDITOR_ROOTS: [&str; 6] = [
    "Library/Application Support/Code/User/globalStorage",
    "Library/Application Support/Code - Insiders/User/globalStorage",
    "Library/Application Support/VSCodium/User/globalStorage",
    ".config/Code/User/globalStorage",
    ".config/Code - Insiders/User/globalStorage",
    ".config/VSCodium/User/globalStorage",
];

/// Every existing `tasks/` directory across the family, tagged with the
/// source it belongs to.
fn task_roots() -> Vec<(PathBuf, Source)> {
    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return Vec::new(),
    };
    let mut roots = Vec::new();
    for member in &FAMILY {
        if let Ok(custom) = std::env::var(member.env_var) {
            let trimmed = custom.trim();
            if !trimmed.is_empty() {
                // env 指向 globalStorage 根(与编辑器布局同形:<ext>/tasks/)。
                roots.push((
                    PathBuf::from(trimmed).join(member.ext_id).join("tasks"),
                    member.source,
                ));
                continue;
            }
        }
        for root in EDITOR_ROOTS {
            let dir = home.join(root).join(member.ext_id).join("tasks");
            if dir.is_dir() {
                roots.push((dir, member.source));
            }
        }
    }
    roots
}

/// All `ui_messages.json` files in a `tasks/` tree (one per task folder).
fn ui_message_files(tasks_dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = fs::read_dir(tasks_dir) else {
        return files;
    };
    for entry in entries.flatten() {
        let candidate = entry.path().join("ui_messages.json");
        if candidate.is_file() {
            files.push(candidate);
        }
    }
    files
}

/// Candidate log locations, for `tokenbuddy doctor`: presence is optional,
/// the doctor reports what exists and what does not.
pub fn log_paths() -> Vec<PathBuf> {
    task_roots().into_iter().map(|(p, _)| p).collect()
}

/// R84 — conversational text for context search, straight from the same
/// `tasks/` tree the usage collector reads. `api_conversation_history.json`
/// is an array of Anthropic Messages API objects (`{role, content}`); only
/// user/assistant *text* goes to the index — tool_use/tool_result/thinking
/// blocks are the exact repeat-cache noise the index exists to avoid, and a
/// user turn made only of tool_result blocks is a tool turn, not speech.
/// Cline wraps every user turn with `<task>` / `<environment_details>` /
/// `<system-reminder>` boilerplate; the wrappers are stripped so the same
/// environment dump never lands twice.
pub fn drain_messages(sink: &mut dyn FnMut(crate::context::ContextMessage)) {
    use crate::context::ContextMessage;

    /// A conversation file beyond this size is skipped whole: usage already
    /// came from ui_messages, and no transcript is worth an unbounded read.
    const HISTORY_CAP: u64 = 16 * 1024 * 1024;

    for (tasks_dir, source) in task_roots() {
        for dir in fs::read_dir(&tasks_dir).into_iter().flatten().flatten() {
            let history = dir.path().join("api_conversation_history.json");
            if !history.is_file() {
                continue;
            }
            let Ok(meta) = fs::metadata(&history) else {
                continue;
            };
            if meta.len() > HISTORY_CAP {
                continue;
            }
            let Ok(data) = fs::read_to_string(&history) else {
                continue;
            };
            // Borrow-parse: &RawValue avoids materializing the whole array
            // as one Value tree; each message is parsed and dropped in turn.
            let Ok(items) = serde_json::from_str::<Vec<&serde_json::value::RawValue>>(&data) else {
                continue;
            };

            let session_id = dir.file_name().to_string_lossy().to_string();
            // Task id is a millisecond epoch in every Cline generation; the
            // ui_messages sidecar's first ts and the file mtime are fallbacks.
            let timestamp = dir
                .file_name()
                .to_str()
                .and_then(|id| id.parse::<i64>().ok())
                .filter(|ms| *ms > 1_000_000_000_000)
                .map(|ms| ms / 1000)
                .or_else(|| file_mtime(&history))
                .unwrap_or(0);
            let project = scan_legacy_tags(&history).1.unwrap_or_default();

            for item in items {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(item.get()) else {
                    continue;
                };
                let role = match v.get("role").and_then(|r| r.as_str()) {
                    Some("user") => "user",
                    Some("assistant") => "assistant",
                    _ => continue,
                };
                let text =
                    text_of_content(v.get("content").unwrap_or(&serde_json::Value::Null), role);
                if !text.trim().is_empty() {
                    sink(ContextMessage {
                        source,
                        session_id: session_id.clone(),
                        role,
                        timestamp,
                        text,
                        project: project.clone(),
                        title: String::new(),
                    });
                }
            }
        }
    }
}

/// Join the text blocks of one message. `role` decides whether a
/// tool_result-only turn counts as speech (it never does — Anthropic's
/// convention makes those tool answers riding on the user channel).
fn text_of_content(content: &serde_json::Value, role: &str) -> String {
    match content {
        serde_json::Value::String(s) => strip_harness_wrappers(s),
        serde_json::Value::Array(blocks) => {
            let mut only_tool_results = true;
            let mut texts = Vec::new();
            for b in blocks {
                match b.get("type").and_then(|t| t.as_str()) {
                    Some("text") => {
                        only_tool_results = false;
                        if let Some(t) = b.get("text").and_then(|t| t.as_str()) {
                            texts.push(t.to_string());
                        }
                    }
                    Some("tool_result") | Some("tool_use") | Some("thinking") => {}
                    _ => {}
                }
            }
            if role == "user" && only_tool_results {
                return String::new();
            }
            strip_harness_wrappers(&texts.join("\n"))
        }
        _ => String::new(),
    }
}

/// Remove `<environment_details>…</environment_details>` and
/// `<system-reminder>…</system-reminder>` bodies, and unwrap `<task>…</task>`
/// keeping the words. Hand-rolled: no regex dep in the size budget.
fn strip_harness_wrappers(text: &str) -> String {
    let mut s = text.to_string();
    for tag in ["environment_details", "system-reminder"] {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        while let Some(start) = s.find(&open) {
            let Some(end_rel) = s[start..].find(&close) else {
                break;
            };
            s.replace_range(start..start + end_rel + close.len(), "");
        }
    }
    const TASK_OPEN: &str = "<task>";
    const TASK_CLOSE: &str = "</task>";
    if let Some(start) = s.find(TASK_OPEN) {
        let inner_start = start + TASK_OPEN.len();
        if let Some(end_rel) = s[inner_start..].find(TASK_CLOSE) {
            let inner = s[inner_start..inner_start + end_rel].to_string();
            s.replace_range(start..inner_start + end_rel + TASK_CLOSE.len(), &inner);
        }
    }
    s
}

/// The same, filtered to one family member (doctor diagnoses per source).
pub fn log_paths_for(source: Source) -> Vec<PathBuf> {
    task_roots()
        .into_iter()
        .filter(|(_, s)| *s == source)
        .map(|(p, _)| p)
        .collect()
}

/// Where this collector reads from, when that place exists on this machine.
/// Powers the dashboard's source-health panel and the first-run prompt.
pub fn log_path_for(source: Source) -> Option<PathBuf> {
    task_roots()
        .into_iter()
        .find(|(_, s)| *s == source)
        .map(|(p, _)| p)
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let roots = task_roots();
    if roots.is_empty() {
        return Ok(vec![]);
    }

    let mut files = Vec::new();
    for (dir, source) in &roots {
        for f in ui_message_files(dir) {
            files.push((f, *source));
        }
    }

    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let cache_map = cache.get_or_insert_with(HashMap::new);

    let mut all_records = Vec::new();
    let mut current_paths: HashSet<String> = HashSet::new();

    for (file_path, source) in &files {
        let path_str = file_path.to_string_lossy().to_string();
        current_paths.insert(path_str.clone());

        let mtime = std::fs::metadata(file_path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let needs_reparse = match cache_map.get(&path_str) {
            Some((cached_mtime, _)) => mtime > *cached_mtime,
            None => true,
        };

        if needs_reparse {
            let records = parse_task_file(file_path, *source).unwrap_or_default();
            cache_map.insert(path_str.clone(), (mtime, records));
        }

        if let Some((_, records)) = cache_map.get(&path_str) {
            all_records.extend(records.iter().cloned());
        }
    }

    cache_map.retain(|path, _| current_paths.contains(path));

    Ok(all_records)
}

/// One `say:"api_req_started"` entry = one API request = one TokenRecord.
/// Zero-token entries are skipped (error stubs carry no usage).
fn parse_task_file(path: &Path, source: Source) -> Result<Vec<TokenRecord>> {
    let data = fs::read_to_string(path)?;
    let entries: serde_json::Value = match serde_json::from_str(&data) {
        Ok(v) => v,
        Err(_) => return Ok(vec![]),
    };
    let Some(entries) = entries.as_array() else {
        return Ok(vec![]);
    };

    // The task folder name is the session id — stable, user-visible in the
    // extension, and unique per task.
    let session_id = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();

    // Legacy tags (<model>/<cwd> inside environment_details) come from one
    // bounded scan per file: model labels pre-4.x tasks, cwd is the project
    // for every task generation.
    let (legacy_model, cwd) = scan_legacy_tags(path);

    let mut records = Vec::new();
    for entry in entries {
        if entry.get("type").and_then(|t| t.as_str()) != Some("say")
            || entry.get("say").and_then(|s| s.as_str()) != Some("api_req_started")
        {
            continue;
        }
        // Payload is a JSON *string* in `text`.
        let payload: serde_json::Value = match entry
            .get("text")
            .and_then(|t| t.as_str())
            .and_then(|t| serde_json::from_str(t).ok())
        {
            Some(p) => p,
            None => continue,
        };
        let num = |k: &str| {
            payload
                .get(k)
                .and_then(|v| v.as_i64())
                .filter(|n| *n > 0)
                .unwrap_or(0) as u64
        };
        let (input, output, cache_read, cache_write) = (
            num("tokensIn"),
            num("tokensOut"),
            num("cacheReads"),
            num("cacheWrites"),
        );
        if input == 0 && output == 0 && cache_read == 0 && cache_write == 0 {
            continue;
        }

        // ts is epoch millis (number or numeric string); mtime fallback keeps
        // the row visible rather than parked in 1970.
        let ts_ms = entry
            .get("ts")
            .and_then(|ts| {
                ts.as_i64()
                    .or_else(|| ts.as_str().and_then(|s| s.parse::<i64>().ok()))
            })
            .unwrap_or_else(|| file_mtime(path).unwrap_or(0) * 1000);

        // Model identity: per-message modelInfo first (Cline 4.x), legacy
        // `<model>` tag as the fallback for older tasks.
        let model = entry
            .get("modelInfo")
            .and_then(|m| m.get("modelId"))
            .and_then(|m| m.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| legacy_model.clone())
            .unwrap_or_else(|| "unknown".to_string());

        records.push(TokenRecord {
            source,
            model,
            input_tokens: input,
            output_tokens: output,
            cache_read_tokens: cache_read,
            cache_creation_tokens: cache_write,
            timestamp: (ts_ms / 1000).max(0),
            session_id: Some(session_id.clone()),
            project: cwd.clone().unwrap_or_default(),
            duration_ms: None,
            ttft_ms: None,
            // `cost` is in the payload and deliberately not imported.
            credits: 0.0,
            context_ratio: 0.0,
            // Frozen key: task + millisecond + usage — two distinct requests
            // in the same task can only collide by sharing all four.
            record_id: Some(format!("{session_id}_{ts_ms}_{input}_{output}")),
            sidechain: false,
            merge_key: None,
        });
    }
    Ok(records)
}

/// First `<model>` / `<cwd>` tags from the sibling api_conversation_history.json,
/// found by streaming the file with a hard 2 MiB scan cap — the file holds the
/// whole conversation and must never reach the heap whole.
fn scan_legacy_tags(ui_messages_path: &Path) -> (Option<String>, Option<String>) {
    const SCAN_CAP: u64 = 2 * 1024 * 1024;
    let history = ui_messages_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("api_conversation_history.json");
    let Ok(file) = fs::File::open(&history) else {
        return (None, None);
    };
    let mut model = None;
    let mut cwd = None;
    let mut buf = String::new();
    let mut reader = BufReader::new(file).take(SCAN_CAP);
    let mut chunk = [0u8; 8192];
    loop {
        let n = reader.read(&mut chunk).unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.push_str(&String::from_utf8_lossy(&chunk[..n]));
        if model.is_none() {
            model = tag_value(&buf, "<model>");
        }
        if cwd.is_none() {
            cwd = tag_value(&buf, "<cwd>");
        }
        if model.is_some() && cwd.is_some() {
            break;
        }
    }
    (model, cwd)
}

/// First `<tag>…</tag>` value after `needle`, if both are present.
fn tag_value(text: &str, tag: &str) -> Option<String> {
    let start = text.find(tag)? + tag.len();
    let rest = &text[start..];
    let end = rest.find("</")?;
    let value = rest[..end].trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TEST_ENV_LOCK;

    /// 一个最小但形状正确的任务目录:ui_messages.json + 带环境块的历史。
    fn seed_task(root: &Path, ext_id: &str, task: &str, entries: &str, history: &str) -> PathBuf {
        let dir = root.join(ext_id).join("tasks").join(task);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("ui_messages.json"), entries).unwrap();
        if !history.is_empty() {
            std::fs::write(dir.join("api_conversation_history.json"), history).unwrap();
        }
        dir
    }

    fn entry(say: &str, text: &str, ts: &str, model_id: Option<&str>) -> String {
        let mut s = String::from(r#"{"type":"say","say":""#);
        s.push_str(say);
        s.push_str(r#"","text":"#);
        // to_string 产出的是合法 JSON 字符串字面量(自带引号与转义),原样嵌入。
        s.push_str(&serde_json::to_string(text).unwrap());
        s.push_str(r#","ts":"#);
        s.push_str(ts);
        if let Some(m) = model_id {
            s.push_str(r#","modelInfo":{"modelId":""#);
            s.push_str(m);
            s.push_str(r#"","providerId":"anthropic"}"#);
        }
        s.push('}');
        s
    }

    const PAYLOAD: &str =
        r#"{"cost":0.01,"tokensIn":1500,"tokensOut":300,"cacheReads":200,"cacheWrites":50}"#;

    /// 端到端:三兄弟各一任务——api_req_started 变记录,零值条目跳过,
    /// 非目标 say 不算,modelInfo 优先、legacy `<model>` 兜底,cwd 入 project。
    #[test]
    fn collects_all_three_family_members_from_globalstorage() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-cline-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("CLINE_DATA_DIR", &dir);
        std::env::set_var("ROO_DATA_DIR", &dir);
        std::env::set_var("KILO_DATA_DIR", &dir);

        let history = r#"{"messages":[{"role":"user","content":[{"type":"text","text":"<environment_details>\n<cwd>/Users/example/code/project</cwd>\n<model>claude-sonnet-4-6</model>\n</environment_details>"}]}]}"#;

        seed_task(
            &dir,
            "saoudrizwan.claude-dev",
            "task-a",
            &format!(
                "[{},{}]",
                entry("api_req_started", PAYLOAD, "1800000000123", None),
                entry(
                    "api_req_started",
                    r#"{"tokensIn":0,"tokensOut":0}"#,
                    "1",
                    Some("gpt-x")
                )
            ),
            history,
        );
        seed_task(
            &dir,
            "rooveterinaryinc.roo-cline",
            "task-b",
            &format!(
                "[{}]",
                entry("api_req_started", PAYLOAD, "1800000500000", Some("kimi-k2"))
            ),
            "",
        );
        seed_task(
            &dir,
            "kilocode.kilo-code",
            "task-c",
            &format!("[{}]", entry("user_feedback", "不应该被采集", "1", None)),
            "",
        );

        let mut records = collect_records().unwrap();
        records.sort_by(|a, b| a.source.as_str().cmp(b.source.as_str()));

        // cline 1 条(零值跳过)+ roocode 1 条;kilo 只有 user_feedback → 0。
        assert_eq!(records.len(), 2, "{records:?}");
        assert_eq!(records[0].source.as_str(), "cline");
        assert_eq!(records[1].source.as_str(), "roocode");

        let r = &records[0];
        assert_eq!(r.session_id.as_deref(), Some("task-a"));
        assert_eq!(r.input_tokens, 1500);
        assert_eq!(r.output_tokens, 300);
        assert_eq!(r.cache_read_tokens, 200);
        assert_eq!(r.cache_creation_tokens, 50);
        assert_eq!(r.timestamp, 1_800_000_000);
        // 无 modelInfo → legacy `<model>` 兜底;cwd 入 project。
        assert_eq!(r.model, "claude-sonnet-4-6");
        assert_eq!(r.project, "/Users/example/code/project");
        // 冻结键:task_ts毫秒_入_出。
        assert_eq!(
            r.record_id.as_deref(),
            Some("task-a_1800000000123_1500_300")
        );
        assert!(!r.sidechain);
        assert_eq!(r.credits, 0.0, "cost 是钱,不进口径");

        // modelInfo 优先于 legacy 标签。
        assert_eq!(records[1].model, "kimi-k2");
        assert_eq!(records[1].project, "", "无历史文件时 project 空");

        for v in ["CLINE_DATA_DIR", "ROO_DATA_DIR", "KILO_DATA_DIR"] {
            std::env::remove_var(v);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 毒行防御:坏 JSON / 缺 text / 字符串 ts / 空任务目录,静默跳过。
    #[test]
    fn malformed_entries_are_skipped_never_fatal() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-cline-poison-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("ROO_DATA_DIR", &dir);

        // 条目级毒化:text 不是 JSON → 该条跳过;零值条目跳过;好条目照收。
        // (Cline 自己写数组,文件级坏 JSON 见 task-q:整文件静默空。)
        let entries = format!(
            "[{},{}]",
            entry(
                "api_req_started",
                "not json{bad",
                "1800000999999",
                Some("glm-5")
            ),
            entry("api_req_started", PAYLOAD, "1800000999999", Some("glm-5"))
        );
        seed_task(&dir, "rooveterinaryinc.roo-cline", "task-p", &entries, "");
        seed_task(
            &dir,
            "rooveterinaryinc.roo-cline",
            "task-q",
            "not json at all",
            "",
        );
        let records = collect_records().unwrap();
        assert_eq!(records.len(), 1, "{records:?}");
        assert_eq!(records[0].timestamp, 1_800_000_999);
        assert_eq!(records[0].model, "glm-5");

        std::env::remove_var("ROO_DATA_DIR");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod drain_tests {
    use super::*;
    use crate::TEST_ENV_LOCK;

    /// 对话抽取:task 用户提示剥 <task> 壳保留正文、环境块整段剥、
    /// 纯 tool_result 的 user 轮不算发言、thinking/工具块不进索引、
    /// assistant 文本照收;时间戳取毫秒 taskId;source 随家族成员。
    #[test]
    fn drain_extracts_speech_stripping_harness_wrappers() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-cline-drain-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("KILO_DATA_DIR", &dir);

        let task_dir = dir
            .join("kilocode.kilo-code")
            .join("tasks")
            .join("1800000000123");
        std::fs::create_dir_all(&task_dir).unwrap();
        let history = serde_json::json!([
            {"role": "user", "content": "<task>\n修复 login 页面的 500\n</task>\n<environment_details>\n# VSCode 259 文件\n</environment_details>"},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "step 1 done"}]},
            {"role": "assistant", "content": [{"type": "thinking", "thinking": "内部草稿"}, {"type": "text", "text": "原因是 token 过期"}]},
            {"role": "assistant", "content": [{"type": "tool_use", "id": "t2", "name": "read_file", "input": {}}]},
            {"role": "system", "content": "系统重发不进"}
        ])
        .to_string();
        std::fs::write(task_dir.join("api_conversation_history.json"), history).unwrap();

        let mut msgs = Vec::new();
        drain_messages(&mut |m| msgs.push(m));
        assert_eq!(msgs.len(), 2, "只应有 task 提示+assistant 文本: {msgs:?}");

        let first = &msgs[0];
        assert_eq!(first.source.as_str(), "kilo");
        assert_eq!(first.role, "user");
        assert_eq!(first.session_id, "1800000000123");
        assert_eq!(first.timestamp, 1_800_000_000);
        assert!(
            first.text.contains("修复 login 页面的 500"),
            "{}",
            first.text
        );
        assert!(
            !first.text.contains("environment_details"),
            "{}",
            first.text
        );
        assert!(!first.text.contains("<task>"), "{}", first.text);

        assert_eq!(msgs[1].role, "assistant");
        assert_eq!(msgs[1].text, "原因是 token 过期");
        assert!(!msgs[1].text.contains("内部草稿"));

        std::env::remove_var("KILO_DATA_DIR");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 坏 JSON 静默空;缺历史文件的任务跳过;毒行不致命。
    #[test]
    fn drain_degrades_quietly_on_bad_files() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir =
            std::env::temp_dir().join(format!("tb-cline-drain-poison-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("CLINE_DATA_DIR", &dir);

        let bad = dir
            .join("saoudrizwan.claude-dev")
            .join("tasks")
            .join("task-bad");
        std::fs::create_dir_all(&bad).unwrap();
        std::fs::write(bad.join("api_conversation_history.json"), "not json").unwrap();
        // 无历史文件的任务:静默跳过。
        let empty = dir
            .join("saoudrizwan.claude-dev")
            .join("tasks")
            .join("task-empty");
        std::fs::create_dir_all(&empty).unwrap();

        let mut msgs = Vec::new();
        drain_messages(&mut |m| msgs.push(m));
        assert!(msgs.is_empty(), "{msgs:?}");

        std::env::remove_var("CLINE_DATA_DIR");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
