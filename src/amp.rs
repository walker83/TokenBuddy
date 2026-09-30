//! Collector for Amp (Sourcegraph).
//!
//! One JSON thread file per session under `~/.local/share/amp/threads/`.
//! The authoritative accounting lives in `usageLedger.events[]` — each event
//! is one billable request with `model`, RFC3339 `timestamp`, `credits`
//! (Amp's own raw unit, kept as credits like every source) and
//! `tokens{input, output, cacheReadInputTokens, cacheCreationInputTokens}`.
//! `messages[].usage` repeats the same requests; this collector reads ONLY
//! the ledger, so there is nothing to cross-match — the ledger is the thing
//! Amp itself bills from. Thread `created` (ms) and the file mtime are the
//! timestamp fallbacks, in that order.

use crate::{file_mtime, FileCacheMap, Source, TokenRecord};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

static FILE_CACHE: Mutex<Option<FileCacheMap>> = Mutex::new(None);

/// Drop the resident parse cache. See `pi::release_caches`.
pub fn release_caches() {
    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    *cache = None;
}

/// Amp CLI 遵循 XDG 约定,macOS 上也是 `~/.local/share/amp`;
/// 平台 data_dir 的变体一并探测,防发行版差异。
fn threads_dirs() -> Vec<PathBuf> {
    if let Ok(custom) = std::env::var("AMP_DATA_DIR") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return vec![PathBuf::from(trimmed)];
        }
    }
    let mut dirs = vec![dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local/share/amp/threads")];
    if let Some(data) = dirs::data_dir() {
        let candidate = data.join("amp/threads");
        if candidate != dirs[0] {
            dirs.push(candidate);
        }
    }
    dirs
}

/// Where this collector reads from, when that place exists on this machine.
pub fn log_path() -> Option<PathBuf> {
    log_paths().into_iter().find(|p| p.exists())
}

/// Candidate log locations, for `tokenbuddy doctor`.
pub fn log_paths() -> Vec<PathBuf> {
    threads_dirs()
}

fn thread_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    for dir in threads_dirs() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.extension().and_then(|e| e.to_str()) == Some("json") {
                files.push(p);
            }
        }
    }
    files
}

/// RFC3339 → 毫秒;0 = 不可用(调用方回退)。
fn parse_ts_ms(ts: Option<&str>) -> i64 {
    ts.and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|dt| dt.timestamp_millis())
        .filter(|ms| *ms != 0)
        .unwrap_or(0)
}

fn parse_thread_file(path: &Path) -> Result<Vec<TokenRecord>> {
    let data = fs::read_to_string(path)?;
    let Ok(thread) = serde_json::from_str::<serde_json::Value>(&data) else {
        return Ok(vec![]);
    };
    let thread_id = thread
        .get("id")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string()
        });
    let created_ms = thread.get("created").and_then(|v| v.as_i64()).unwrap_or(0);
    let mtime_ms = file_mtime(path).unwrap_or(0) * 1000;

    let Some(events) = thread
        .pointer("/usageLedger/events")
        .and_then(|e| e.as_array())
    else {
        return Ok(vec![]);
    };

    let mut records = Vec::new();
    for event in events {
        // 无 model 的事件没法归户,跳过(诚实缺格,不猜)。
        let Some(model) = event.get("model").and_then(|m| m.as_str()) else {
            continue;
        };
        let num = |k: &str| {
            event
                .pointer(&format!("/tokens/{k}"))
                .and_then(|v| v.as_i64())
                .filter(|n| *n > 0)
                .unwrap_or(0) as u64
        };
        let (input, output, cache_read, cache_write) = (
            num("input"),
            num("output"),
            num("cacheReadInputTokens"),
            num("cacheCreationInputTokens"),
        );
        if input == 0 && output == 0 && cache_read == 0 && cache_write == 0 {
            continue;
        }
        // RFC3339 → thread.created(ms) → mtime,与 tokscale 同序。
        let explicit = parse_ts_ms(event.get("timestamp").and_then(|t| t.as_str()));
        let ts_ms = if explicit != 0 {
            explicit
        } else if created_ms != 0 {
            created_ms
        } else {
            mtime_ms
        };
        let ts = (ts_ms.max(0) / 1000).max(0);
        records.push(TokenRecord {
            source: Source::Amp,
            model: model.to_string(),
            input_tokens: input,
            output_tokens: output,
            cache_read_tokens: cache_read,
            cache_creation_tokens: cache_write,
            timestamp: ts,
            session_id: Some(thread_id.clone()),
            project: String::new(),
            duration_ms: None,
            ttft_ms: None,
            // credits 是 Amp 自己的原始计量单位——照实入库,不折算钱。
            credits: event
                .get("credits")
                .and_then(|c| c.as_f64())
                .unwrap_or(0.0)
                .max(0.0),
            context_ratio: 0.0,
            // 冻结键:thread_毫秒_入_出——ledger 追加写,同一事件重读恒同键。
            record_id: Some(format!("{thread_id}_{ts_ms}_{input}_{output}")),
            sidechain: false,
            merge_key: None,
        });
    }
    Ok(records)
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let files = thread_files();
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
            let records = parse_thread_file(file_path).unwrap_or_default();
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

    /// 端到端:usageLedger 事件入账(RFC3339 时间戳、tokens、credits
    /// 照收),零 token 事件与缺 model 事件跳过,messages[].usage 不重复
    /// 计入(ledger 唯一),时间戳缺省回退 thread.created。
    #[test]
    fn ledger_events_bill_and_message_usage_is_ignored() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-amp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("AMP_DATA_DIR", &dir);

        let thread = serde_json::json!({
            "id": "T-abc",
            "created": 1_800_000_500_000i64,
            "messages": [
                {"role": "assistant", "messageId": 1,
                 "usage": {"model": "claude-sonnet-4-6", "inputTokens": 999,
                            "outputTokens": 999, "credits": 9.0}}
            ],
            "usageLedger": {"events": [
                {"timestamp": "2027-01-15T06:40:00Z", "model": "claude-sonnet-4-6",
                 "credits": 1.5,
                 "tokens": {"input": 1500, "output": 300,
                             "cacheReadInputTokens": 200, "cacheCreationInputTokens": 50}},
                {"model": "gpt-x",
                 "tokens": {"input": 0, "output": 0}},
                {"timestamp": "bad-ts", "model": "glm-5",
                 "tokens": {"input": 100, "output": 20}}
            ]}
        });
        std::fs::write(dir.join("T-abc.json"), thread.to_string()).unwrap();

        let records = collect_records().unwrap();
        // ledger 2 条入账(零 token 跳过),messages[].usage 的 999 不出现。
        assert_eq!(records.len(), 2, "{records:?}");

        let first = &records[0];
        assert_eq!(first.source.as_str(), "amp");
        assert_eq!(first.session_id.as_deref(), Some("T-abc"));
        assert_eq!(first.model, "claude-sonnet-4-6");
        assert_eq!(first.input_tokens, 1500);
        assert_eq!(first.output_tokens, 300);
        assert_eq!(first.cache_read_tokens, 200);
        assert_eq!(first.cache_creation_tokens, 50);
        assert_eq!(first.credits, 1.5, "credits 原始事实,不折算钱");
        // RFC3339 → 秒。
        // 2027-01-15T06:40:00Z 的 epoch 秒。
        assert_eq!(first.timestamp, 1_799_995_200);
        // 冻结键:thread_毫秒_入_出。
        assert_eq!(
            first.record_id.as_deref(),
            Some("T-abc_1799995200000_1500_300")
        );

        // 坏时间戳回退 thread.created(ms)。
        assert_eq!(records[1].model, "glm-5");
        assert_eq!(records[1].timestamp, 1_800_000_500);
        assert_eq!(records[1].input_tokens, 100, "messages[].usage 不得混入");

        std::env::remove_var("AMP_DATA_DIR");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 坏 JSON / 缺 ledger / 空目录:静默空。
    #[test]
    fn bad_files_degrade_to_empty() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-amp-poison-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("AMP_DATA_DIR", &dir);
        std::fs::write(dir.join("broken.json"), "not json").unwrap();
        std::fs::write(
            dir.join("noleadger.json"),
            serde_json::json!({"id": "x"}).to_string(),
        )
        .unwrap();
        let records = collect_records().unwrap();
        assert!(records.is_empty(), "{records:?}");
        std::env::remove_var("AMP_DATA_DIR");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
