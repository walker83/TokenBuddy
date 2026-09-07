//! Collector for the pi coding agent (https://github.com/badlogic/pi-mono).
//!
//! pi stores one JSONL file per session under `~/.pi/agent/sessions/<cwd>/<ts>_<uuid>.jsonl`.
//! The first line is a `type: "session"` record carrying the session id; every
//! assistant turn is a `type: "message"` record whose `message` object holds
//! `model` and `usage` with input / output / cacheRead / cacheWrite token counts.

use crate::{file_mtime, Source, TokenRecord};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

static FILE_CACHE: Mutex<Option<HashMap<String, (SystemTime, Vec<TokenRecord>)>>> = Mutex::new(None);

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let sessions_dir = get_pi_dir().join("agent").join("sessions");
    if !sessions_dir.exists() {
        return Ok(vec![]);
    }

    let jsonl_files = collect_session_files(&sessions_dir);
    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let cache_map = cache.get_or_insert_with(HashMap::new);

    let mut all_records = Vec::new();
    let mut current_paths: HashSet<String> = HashSet::new();

    for file_path in &jsonl_files {
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
            let records = parse_single_file(file_path).unwrap_or_default();
            cache_map.insert(path_str.clone(), (mtime, records));
        }

        if let Some((_, records)) = cache_map.get(&path_str) {
            all_records.extend(records.iter().cloned());
        }
    }

    cache_map.retain(|path, _| current_paths.contains(path));

    Ok(all_records)
}

fn get_pi_dir() -> PathBuf {
    if let Ok(custom) = std::env::var("PI_DIR") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".pi")
}

/// Sessions live one level deep: `sessions/<project-dir>/<session>.jsonl`.
fn collect_session_files(sessions_dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let entries = match fs::read_dir(sessions_dir) {
        Ok(e) => e,
        Err(_) => return files,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if let Ok(sub_entries) = fs::read_dir(&path) {
            for sub_entry in sub_entries.flatten() {
                let sub_path = sub_entry.path();
                if sub_path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                    files.push(sub_path);
                }
            }
        }
    }

    files
}

fn parse_single_file(file_path: &Path) -> Result<Vec<TokenRecord>> {
    let file = fs::File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut session_id: Option<String> = None;

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };

        if line.trim().is_empty() {
            continue;
        }

        let value: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };

        if session_id.is_none() && value.get("type").and_then(|t| t.as_str()) == Some("session") {
            session_id = value.get("id").and_then(|v| v.as_str()).map(|s| s.to_string());
        }

        if value.get("type").and_then(|t| t.as_str()) != Some("message") {
            continue;
        }

        let message = match value.get("message") {
            Some(m) => m,
            None => continue,
        };

        if message.get("role").and_then(|r| r.as_str()) != Some("assistant") {
            continue;
        }

        let usage = match message.get("usage") {
            Some(u) => u,
            None => continue,
        };

        let output_tokens = usage.get("output").and_then(|v| v.as_u64()).unwrap_or(0);
        if output_tokens == 0 {
            continue;
        }

        // Fall back to the file's mtime rather than 0: a 1970 timestamp sits
        // outside every time filter, so the row would be imported yet never
        // shown.
        let timestamp = value
            .get("timestamp")
            .and_then(|ts| ts.as_str())
            .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
            .map(|dt| dt.timestamp())
            .or_else(|| file_mtime(file_path))
            .unwrap_or(0);

        records.push(TokenRecord {
            source: Source::Pi,
            model: message
                .get("model")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            input_tokens: usage.get("input").and_then(|v| v.as_u64()).unwrap_or(0),
            output_tokens,
            cache_read_tokens: usage.get("cacheRead").and_then(|v| v.as_u64()).unwrap_or(0),
            cache_creation_tokens: usage.get("cacheWrite").and_then(|v| v.as_u64()).unwrap_or(0),
            timestamp,
            session_id: session_id.clone(),
            duration_ms: None,
            ttft_ms: None,
            credits: 0.0,
            record_id: None,
        });
    }

    Ok(records)
}

/// Conversational text for context search. Message entries carry
/// `{role, content, timestamp}`; content is either a string or an array of
/// typed parts whose `text` members hold the words.
pub fn collect_messages() -> Vec<crate::context::ContextMessage> {
    use crate::context::ContextMessage;

    let sessions_dir = get_pi_dir().join("agent").join("sessions");
    if !sessions_dir.exists() {
        return vec![];
    }

    let mut msgs = Vec::new();
    for file_path in collect_session_files(&sessions_dir) {
        let file = match fs::File::open(&file_path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let mut session_id = String::new();
        // sessions/<munged-cwd>/<ts>_<uuid>.jsonl — the directory is the project.
        let project = file_path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        for line_result in BufReader::new(file).lines() {
            let Ok(line) = line_result else { continue };
            let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else { continue };

            if session_id.is_empty() && value.get("type").and_then(|t| t.as_str()) == Some("session") {
                session_id = value
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
            }
            if value.get("type").and_then(|t| t.as_str()) != Some("message") {
                continue;
            }
            let Some(message) = value.get("message") else { continue };
            let role = match message.get("role").and_then(|r| r.as_str()) {
                Some("user") => "user",
                Some("assistant") => "assistant",
                _ => continue,
            };
            // pi stamps messages in epoch millis, unlike the RFC3339 lines.
            let timestamp = message
                .get("timestamp")
                .and_then(|ts| ts.as_i64())
                .map(|ms| ms / 1000)
                .or_else(|| file_mtime(&file_path))
                .unwrap_or(0);

            let text = match message.get("content") {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(serde_json::Value::Array(parts)) => parts
                    .iter()
                    .filter(|p| p.get("type").and_then(|t| t.as_str()) == Some("text"))
                    .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join("\n"),
                _ => String::new(),
            };
            if !text.trim().is_empty() {
                msgs.push(ContextMessage {
                    source: Source::Pi,
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
    msgs
}

// ============================================================
// Tool events (R1): pi persists tool output as `toolResult` role messages
// carrying `toolCallId` / `toolName` / `isError` — the cleanest of the seven
// sources. Calls whose result never arrives are not stored by pi, so there is
// no input fallback here by design.
// ============================================================

pub fn collect_tool_events() -> Vec<crate::tools::ToolEvent> {
    let sessions_dir = get_pi_dir().join("agent").join("sessions");
    if !sessions_dir.exists() {
        return vec![];
    }
    let mut evs = Vec::new();
    for file_path in collect_session_files(&sessions_dir) {
        let file = match fs::File::open(&file_path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let mut session_id = String::new();
        for line_result in BufReader::new(file).lines() {
            let Ok(line) = line_result else { continue };
            let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
            if session_id.is_empty() && value.get("type").and_then(|t| t.as_str()) == Some("session") {
                session_id = value.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            }
            if value.get("type").and_then(|t| t.as_str()) != Some("message") {
                continue;
            }
            let Some(message) = value.get("message") else { continue };
            if message.get("role").and_then(|r| r.as_str()) != Some("toolResult") {
                continue;
            }
            let Some(call_id) = message.get("toolCallId").and_then(|v| v.as_str()) else { continue };
            let timestamp = message
                .get("timestamp")
                .and_then(|ts| ts.as_i64())
                .map(|ms| ms / 1000)
                .or_else(|| file_mtime(&file_path))
                .unwrap_or(0);
            let text = match message.get("content") {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(serde_json::Value::Array(parts)) => parts
                    .iter()
                    .filter(|p| p.get("type").and_then(|t| t.as_str()) == Some("text"))
                    .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join("\n"),
                _ => String::new(),
            };
            if text.trim().is_empty() {
                continue;
            }
            evs.push(crate::tools::ToolEvent {
                source: Source::Pi,
                session_id: session_id.clone(),
                timestamp,
                tool_name: message
                    .get("toolName")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                block_key: call_id.to_string(),
                text,
                is_error: message.get("isError").and_then(|v| v.as_bool()).unwrap_or(false),
            });
        }
    }
    evs
}
