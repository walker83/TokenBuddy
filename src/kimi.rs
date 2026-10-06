//! Collector for Kimi CLI (kimi-cli, Moonshot).
//!
//! One `wire.jsonl` per session under `~/.kimi/sessions/<GROUP>/<UUID>/`.
//! Usage arrives as `{"timestamp": <float secs>, "message": {"type":
//! "StatusUpdate", "payload": {"message_id": …, "token_usage":
//! {"input_other", "output", "input_cache_read", "input_cache_creation"}}}}`
//! — a StatusUpdate for one LLM request is emitted repeatedly with running
//! totals, so the LAST update per `message_id` is the authoritative count
//! for that request (same reading as tokscale's kimi parser). The model
//! comes from `~/.kimi/config.json` (`"model"`), default `kimi-for-coding`.
//!
//! The kimi-code layout (`~/.kimi-code/.../agents/<AGENT>/wire.jsonl` with a
//! workspaces.json index) is a different beast and is deliberately not
//! attempted here; KIMI_DATA_DIR is the escape hatch for custom roots.

use crate::{file_mtime, FileCacheMap, Source, TokenRecord};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

static FILE_CACHE: Mutex<Option<FileCacheMap>> = Mutex::new(None);

/// Drop the resident parse cache. See `pi::release_caches`.
pub fn release_caches() {
    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    *cache = None;
}

const DEFAULT_MODEL: &str = "kimi-for-coding";

/// The kimi root holding `sessions/` and `config.json`.
fn kimi_dir() -> PathBuf {
    if let Ok(custom) = std::env::var("KIMI_DATA_DIR") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".kimi")
}

/// Where this collector reads from, when that place exists on this machine.
pub fn log_path() -> Option<PathBuf> {
    log_paths().into_iter().find(|p| p.exists())
}

/// Candidate log locations, for `tokenbuddy doctor`.
pub fn log_paths() -> Vec<PathBuf> {
    vec![kimi_dir().join("sessions")]
}

/// Every `sessions/*/*/wire.jsonl`, two levels deep (GROUP, then UUID).
fn wire_files() -> Vec<PathBuf> {
    let sessions = kimi_dir().join("sessions");
    let mut files = Vec::new();
    let Ok(groups) = fs::read_dir(&sessions) else {
        return files;
    };
    for group in groups.flatten() {
        let Ok(sessions_of_group) = fs::read_dir(group.path()) else {
            continue;
        };
        for session in sessions_of_group.flatten() {
            let wire = session.path().join("wire.jsonl");
            if wire.is_file() {
                files.push(wire);
            }
        }
    }
    files
}

/// Model name from the wire file's sibling-of-three `config.json`.
fn read_model(wire_path: &Path) -> String {
    // sessions/GROUP/UUID/wire.jsonl → root/config.json
    // sessions/GROUP/UUID/wire.jsonl:parent×3 到 sessions,再上一层是根。
    let Some(config) = wire_path
        .parent()
        .and_then(|uuid| uuid.parent())
        .and_then(|group| group.parent())
        .and_then(|sessions| sessions.parent())
        .map(|root| root.join("config.json"))
    else {
        return DEFAULT_MODEL.to_string();
    };
    let Ok(text) = fs::read_to_string(config) else {
        return DEFAULT_MODEL.to_string();
    };
    serde_json::from_str::<serde_json::Value>(&text)
        .ok()
        .and_then(|v| v.get("model").and_then(|m| m.as_str()).map(String::from))
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| DEFAULT_MODEL.to_string())
}

/// One record per distinct `message_id`: the last StatusUpdate wins (running
/// totals within one request), lines without a message id stand alone.
fn parse_wire_file(path: &Path) -> Result<Vec<TokenRecord>> {
    let model = read_model(path);
    let session_id = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();
    let fallback_ts = file_mtime(path).unwrap_or(0);

    struct Acc {
        tokens: (u64, u64, u64, u64),
        ts: i64,
    }
    // message_id → running-best (latest timestamp wins).
    let mut keyed: HashMap<String, Acc> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    let mut unkeyed: Vec<TokenRecord> = Vec::new();

    for line in BufReader::new(fs::File::open(path)?).lines() {
        let Ok(line) = line else { continue };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        if v.get("message")
            .and_then(|m| m.get("type"))
            .and_then(|t| t.as_str())
            != Some("StatusUpdate")
        {
            continue;
        }
        let usage = v
            .pointer("/message/payload/token_usage")
            .cloned()
            .unwrap_or_default();
        let num = |k: &str| {
            usage
                .get(k)
                .and_then(|x| x.as_i64())
                .filter(|n| *n > 0)
                .unwrap_or(0) as u64
        };
        let tokens = (
            num("input_other"),
            num("output"),
            num("input_cache_read"),
            num("input_cache_creation"),
        );
        if tokens.0 == 0 && tokens.1 == 0 && tokens.2 == 0 && tokens.3 == 0 {
            continue;
        }
        let ts = v
            .get("timestamp")
            .and_then(|t| t.as_f64())
            .map(|f| (f * 1000.0) as i64)
            .filter(|ms| *ms > 0)
            .map(|ms| ms / 1000)
            .unwrap_or(fallback_ts);
        let message_id = v
            .pointer("/message/payload/message_id")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_string();
        let record = TokenRecord {
            source: Source::Kimi,
            model: model.clone(),
            input_tokens: tokens.0,
            output_tokens: tokens.1,
            cache_read_tokens: tokens.2,
            cache_creation_tokens: tokens.3,
            timestamp: ts,
            session_id: Some(session_id.clone()),
            project: String::new(),
            duration_ms: None,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: None,
            sidechain: false,
            merge_key: None,
            request_count: 1,
        };
        if message_id.is_empty() {
            unkeyed.push(record);
        } else {
            match keyed.get_mut(&message_id) {
                // 同 message_id 的后续 StatusUpdate = 该请求的滚动累计,
                // 时间戳更新者为准(最终值)。
                Some(acc) if ts >= acc.ts => {
                    *acc = Acc { tokens, ts };
                }
                Some(_) => {}
                None => {
                    keyed.insert(message_id.clone(), Acc { tokens, ts });
                    order.push(message_id);
                }
            }
        }
    }

    let mut records = unkeyed;
    for id in order {
        let acc = &keyed[&id];
        let r = TokenRecord {
            source: Source::Kimi,
            model: model.clone(),
            input_tokens: acc.tokens.0,
            output_tokens: acc.tokens.1,
            cache_read_tokens: acc.tokens.2,
            cache_creation_tokens: acc.tokens.3,
            timestamp: acc.ts,
            session_id: Some(session_id.clone()),
            project: String::new(),
            duration_ms: None,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            // 冻结键:message_id 定位一次请求,session 前缀防跨会话撞车。
            record_id: Some(format!("{session_id}_{id}")),
            sidechain: false,
            merge_key: None,
            request_count: 1,
        };
        records.push(r);
    }
    Ok(records)
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let files = wire_files();
    if files.is_empty() {
        return Ok(vec![]);
    }

    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let cache_map = cache.get_or_insert_with(HashMap::new);

    let mut all_records = Vec::new();
    let mut current_paths: HashSet<String> = HashSet::new();

    for file_path in &files {
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
            let records = parse_wire_file(file_path).unwrap_or_default();
            cache_map.insert(path_str.clone(), (mtime, records));
        }

        if let Some((_, records)) = cache_map.get(&path_str) {
            all_records.extend(records.iter().cloned());
        }
    }

    cache_map.retain(|path, _| current_paths.contains(path));

    Ok(all_records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TEST_ENV_LOCK;

    const WIRE_LINE: &str = r#"{"timestamp":1800000000.42,"message":{"type":"StatusUpdate","payload":{"message_id":"msg-1","token_usage":{"input_other":1500,"output":300,"input_cache_read":200,"input_cache_creation":50}}}}"#;

    fn seed(root: &Path, group: &str, session: &str, lines: &[&str], model: Option<&str>) {
        let dir = root.join("sessions").join(group).join(session);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("wire.jsonl"), lines.join("\n")).unwrap();
        if let Some(m) = model {
            std::fs::write(root.join("config.json"), format!(r#"{{"model":"{m}"}}"#)).unwrap();
        }
    }

    /// 核心:同 message_id 的滚动 StatusUpdate 折叠为末值(每请求一条),
    /// 无 message_id 独立成行,config.json 提供模型名,毫秒→秒。
    #[test]
    fn rolling_status_updates_collapse_to_last_per_message_id() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-kimi-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("KIMI_DATA_DIR", &dir);

        // msg-1 两条:后到者时间戳更新、数值更大 → 取后值;
        // msg-2 一条:独立请求;零值行跳过;坏行跳过。
        let later = r#"{"timestamp":1800000100.0,"message":{"type":"StatusUpdate","payload":{"message_id":"msg-1","token_usage":{"input_other":3000,"output":900,"input_cache_read":400,"input_cache_creation":80}}}}"#;
        let second = r#"{"timestamp":1800000050.0,"message":{"type":"StatusUpdate","payload":{"message_id":"msg-2","token_usage":{"input_other":100,"output":20}}}}"#;
        let zero = r#"{"timestamp":1800000200.0,"message":{"type":"StatusUpdate","payload":{"message_id":"msg-3","token_usage":{"input_other":0,"output":0}}}}"#;
        seed(
            &dir,
            "g1",
            "sess-uuid",
            &["not json", WIRE_LINE, later, second, zero],
            Some("kimi-k2-thinking"),
        );

        let mut records = collect_records().unwrap();
        records.sort_by_key(|r| r.record_id.clone().unwrap_or_default());
        assert_eq!(records.len(), 2, "{records:?}");

        let first = &records[0];
        assert_eq!(first.source.as_str(), "kimi");
        assert_eq!(first.session_id.as_deref(), Some("sess-uuid"));
        assert_eq!(first.record_id.as_deref(), Some("sess-uuid_msg-1"));
        assert_eq!(first.input_tokens, 3000, "滚动累计取末值");
        assert_eq!(first.output_tokens, 900);
        assert_eq!(first.cache_read_tokens, 400);
        assert_eq!(first.cache_creation_tokens, 80);
        assert_eq!(first.timestamp, 1_800_000_100);
        assert_eq!(first.model, "kimi-k2-thinking");

        assert_eq!(records[1].record_id.as_deref(), Some("sess-uuid_msg-2"));
        assert_eq!(records[1].timestamp, 1_800_000_050);

        std::env::remove_var("KIMI_DATA_DIR");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// config.json 缺失 → 默认模型;坏行/缺目录静默。
    #[test]
    fn missing_config_falls_back_and_bad_files_degrade() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-kimi-def-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("KIMI_DATA_DIR", &dir);
        // 不写 config.json。
        seed(&dir, "g2", "sess2", &[WIRE_LINE], None);
        let records = collect_records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].model, "kimi-for-coding");

        std::env::remove_var("KIMI_DATA_DIR");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
