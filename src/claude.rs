use crate::file_mtime;
use crate::TokenRecord;
use crate::Source;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

static FILE_CACHE: Mutex<Option<HashMap<String, (SystemTime, Vec<TokenRecord>)>>> = Mutex::new(None);

struct ParsedAssistantUsage {
    #[allow(dead_code)]
    message_id: String,
    model: String,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
    stop_reason: Option<String>,
    timestamp: Option<String>,
    session_id: Option<String>,
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let claude_dir = get_claude_dir();
    let projects_dir = claude_dir.join("projects");

    if !projects_dir.exists() {
        return Ok(vec![]);
    }

    let jsonl_files = collect_jsonl_files(&projects_dir);
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

    // Clean up deleted files from cache
    cache_map.retain(|path, _| current_paths.contains(path));

    Ok(all_records)
}

fn get_claude_dir() -> PathBuf {
    if let Ok(custom) = std::env::var("CLAUDE_CONFIG_DIR") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".claude")
}

fn collect_jsonl_files(projects_dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let entries = match fs::read_dir(projects_dir) {
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
    let mut messages: HashMap<String, ParsedAssistantUsage> = HashMap::new();
    let mut current_session_id: Option<String> = None;

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

        if current_session_id.is_none() {
            if let Some(sid) = value.get("sessionId").and_then(|v| v.as_str()) {
                current_session_id = Some(sid.to_string());
            }
        }

        if value.get("type").and_then(|t| t.as_str()) != Some("assistant") {
            continue;
        }

        let message = match value.get("message") {
            Some(m) => m,
            None => continue,
        };

        let msg_id = match message.get("id").and_then(|v| v.as_str()) {
            Some(id) => id.to_string(),
            None => continue,
        };

        let usage = match message.get("usage") {
            Some(u) => u,
            None => continue,
        };

        let parsed = ParsedAssistantUsage {
            message_id: msg_id.clone(),
            model: message
                .get("model")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            input_tokens: usage.get("input_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
            output_tokens: usage.get("output_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
            cache_read_tokens: usage.get("cache_read_input_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
            cache_creation_tokens: usage.get("cache_creation_input_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
            stop_reason: message.get("stop_reason").and_then(|v| v.as_str()).map(|s| s.to_string()),
            timestamp: value.get("timestamp").and_then(|v| v.as_str()).map(|s| s.to_string()),
            session_id: current_session_id.clone(),
        };

        let should_replace = match messages.get(&msg_id) {
            None => true,
            Some(existing) => {
                if parsed.stop_reason.is_some() && existing.stop_reason.is_none() {
                    true
                } else if parsed.stop_reason.is_some() == existing.stop_reason.is_some() {
                    parsed.output_tokens > existing.output_tokens
                } else {
                    false
                }
            }
        };

        if should_replace {
            messages.insert(msg_id, parsed);
        }
    }

    let mut records = Vec::new();
    for msg in messages.values() {
        if msg.stop_reason.is_none() || msg.output_tokens == 0 {
            continue;
        }

        let timestamp = msg
            .timestamp
            .as_ref()
            .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
            .map(|dt| dt.timestamp())
            .or_else(|| file_mtime(file_path))
            .unwrap_or(0);

        records.push(TokenRecord {
            source: Source::Claude,
            model: msg.model.clone(),
            input_tokens: msg.input_tokens,
            output_tokens: msg.output_tokens,
            cache_read_tokens: msg.cache_read_tokens,
            cache_creation_tokens: msg.cache_creation_tokens,
            timestamp,
            session_id: msg.session_id.clone(),
            duration_ms: None,
            ttft_ms: None,
            credits: 0.0,
            record_id: None,
        });
    }

    Ok(records)
}

/// Conversational text for context search: user prompts and assistant replies
/// as they appear in the session JSONL. Tool results, tool_use blocks and
/// sidechain (subagent) transcripts are skipped — they are the re-sent cached
/// context that drowns real content.
pub fn drain_messages(sink: &mut dyn FnMut(crate::context::ContextMessage)) {
    let projects_dir = get_claude_dir().join("projects");
    if !projects_dir.exists() {
        return;
    }

    for file_path in collect_jsonl_files(&projects_dir) {
        for m in extract_messages_from_file(&file_path) {
            sink(m);
        }
    }
}

fn extract_messages_from_file(file_path: &Path) -> Vec<crate::context::ContextMessage> {
    use crate::context::ContextMessage;

    let mut msgs = Vec::new();
    let file = match fs::File::open(file_path) {
        Ok(f) => f,
        Err(_) => return msgs,
    };
    // projects/<munged-cwd>/<session>.jsonl — the directory is the project.
    let project = file_path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();
    let mut session_id = String::new();
    // Claude writes `{"type":"summary","summary":"…"}` lines naming the
    // session; that is the display title.
    let mut title = String::new();

    for line_result in BufReader::new(file).lines() {
        let Ok(line) = line_result else { continue };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else { continue };

        if session_id.is_empty() {
            if let Some(sid) = value.get("sessionId").and_then(|v| v.as_str()) {
                session_id = sid.to_string();
            }
        }
        if title.is_empty()
            && value.get("type").and_then(|t| t.as_str()) == Some("summary")
        {
            if let Some(s) = value.get("summary").and_then(|v| v.as_str()) {
                title = s.to_string();
            }
        }
        if value.get("isSidechain").and_then(|v| v.as_bool()) == Some(true) {
            continue;
        }
        let role = match value.get("type").and_then(|t| t.as_str()) {
            Some("user") => "user",
            Some("assistant") => "assistant",
            _ => continue,
        };
        let message = match value.get("message") {
            Some(m) => m,
            None => continue,
        };
        let timestamp = value
            .get("timestamp")
            .and_then(|v| v.as_str())
            .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
            .map(|dt| dt.timestamp())
            .or_else(|| file_mtime(file_path))
            .unwrap_or(0);

        let text = content_text(message.get("content"));
        if !text.trim().is_empty() {
            msgs.push(ContextMessage {
                source: Source::Claude,
                session_id: session_id.clone(),
                role,
                timestamp,
                text,
                project: project.clone(),
                title: title.clone(),
            });
        }
    }
    msgs
}

/// Claude `message.content` is either a plain string or an array of typed
/// parts; only the text parts carry conversational words.
fn content_text(content: Option<&serde_json::Value>) -> String {
    match content {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Array(parts)) => parts
            .iter()
            .filter(|p| p.get("type").and_then(|t| t.as_str()) == Some("text"))
            .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

// ============================================================
// Tool events (R1 of the context-search blueprint)
// ============================================================


/// Collect into a vector; the sync path uses [`drain_messages`] so a source's
/// messages are absorbed one at a time instead of all living at once.
pub fn collect_messages() -> Vec<crate::context::ContextMessage> {
    let mut msgs = Vec::new();
    drain_messages(&mut |m| msgs.push(m));
    msgs
}
