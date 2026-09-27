//! Collector for the WorkBuddy agent.
//!
//! WorkBuddy writes one JSON file per trace under `~/.workbuddy/traces/<pid>/`.
//! Usage is only aggregated at trace level: `trace.modelInfo` holds the summed
//! input / output / cached tokens over `callCount` model calls, so one trace
//! becomes one record here. `totalCachedTokens` is a subset of
//! `totalInputTokens` (`trace.totalTokens == input + output`), so the cached
//! share is split out into `cache_read_tokens` to match how the other
//! collectors treat prompt cache.
//!
//! Spans carry no usage fields, and traces without `modelInfo` made no model
//! calls, so those are skipped.

use crate::{file_mtime, FileCacheMap, Source, TokenRecord};
use anyhow::Result;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

static FILE_CACHE: Mutex<Option<FileCacheMap>> = Mutex::new(None);

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let traces_dir = get_workbuddy_dir().join("traces");
    if !traces_dir.exists() {
        return Ok(vec![]);
    }

    let trace_files = collect_trace_files(&traces_dir);

    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let cache_map = cache.get_or_insert_with(HashMap::new);

    let mut all_records = Vec::new();
    let mut current_paths: HashSet<String> = HashSet::new();

    for file_path in &trace_files {
        let path_str = file_path.to_string_lossy().to_string();
        current_paths.insert(path_str.clone());

        let mtime = fs::metadata(file_path)
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

fn get_workbuddy_dir() -> PathBuf {
    if let Ok(custom) = std::env::var("WORKBUDDY_DIR") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".workbuddy")
}

/// Traces live one level deep: `traces/<worker-pid>/trace_<id>.json`.
fn collect_trace_files(traces_dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let entries = match fs::read_dir(traces_dir) {
        Ok(e) => e,
        Err(_) => return files,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Ok(sub_entries) = fs::read_dir(&path) else {
            continue;
        };
        for sub_entry in sub_entries.flatten() {
            let sub_path = sub_entry.path();
            if sub_path.extension().and_then(|e| e.to_str()) == Some("json") {
                files.push(sub_path);
            }
        }
    }

    files
}

fn parse_single_file(file_path: &Path) -> Result<Vec<TokenRecord>> {
    let text = match fs::read_to_string(file_path) {
        Ok(t) => t,
        Err(_) => return Ok(vec![]),
    };
    let text = repair_lone_surrogates(&text);
    let value: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => return Ok(vec![]),
    };

    let trace = match value.get("trace") {
        Some(t) => t,
        None => return Ok(vec![]),
    };
    let model_info = match trace.get("modelInfo") {
        Some(m) => m,
        None => return Ok(vec![]),
    };

    let total_input = model_info
        .get("totalInputTokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let output_tokens = model_info
        .get("totalOutputTokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    if total_input + output_tokens == 0 {
        return Ok(vec![]);
    }
    let cache_read = model_info
        .get("totalCachedTokens")
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
        .min(total_input);

    // Fall back to the file's mtime rather than 0: a 1970 timestamp sits
    // outside every time filter, so the row would be imported yet never shown.
    let timestamp = trace
        .get("startedAt")
        .and_then(|ts| ts.as_str())
        .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| dt.timestamp())
        .or_else(|| file_mtime(file_path))
        .unwrap_or(0);

    let model = match model_info.get("models").and_then(|v| v.as_array()) {
        Some(models) if !models.is_empty() => models
            .iter()
            .filter_map(|m| m.as_str())
            .collect::<Vec<_>>()
            .join("+"),
        _ => "unknown".to_string(),
    };

    let record_id = trace
        .get("traceId")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Ok(vec![TokenRecord {
        source: Source::WorkBuddy,
        model,
        input_tokens: total_input - cache_read,
        output_tokens,
        cache_read_tokens: cache_read,
        cache_creation_tokens: 0,
        timestamp,
        session_id: trace
            .get("sessionId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        duration_ms: trace.get("duration").and_then(|v| v.as_u64()),
        ttft_ms: None,
        credits: 0.0,
        context_ratio: 0.0,
        record_id,
    }])
}

/// serde_json rejects unpaired UTF-16 surrogate escapes — a truncated emoji in a span
/// string kills the whole trace file — while the tool that wrote them tolerates it.
/// Replace each lone surrogate with U+FFFD; usage fields never live in those strings.
fn repair_lone_surrogates(text: &str) -> Cow<'_, str> {
    let b = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0;
    let mut repaired = false;

    while i < b.len() {
        if b[i] == b'\\' && i + 1 < b.len() && b[i + 1] == b'\\' {
            out.extend_from_slice(&b[i..i + 2]);
            i += 2;
            continue;
        }
        let Some(unit) = escape_at(b, i) else {
            out.push(b[i]);
            i += 1;
            continue;
        };
        let esc = &b[i..i + 6];
        let is_low = |u: u16| (0xDC00..=0xDFFF).contains(&u);
        let pair = (0xD800..=0xDBFF)
            .contains(&unit)
            .then_some(escape_at(b, i + 6))
            .flatten();
        if pair.is_some_and(is_low) {
            out.extend_from_slice(&b[i..i + 12]);
            i += 12;
        } else if is_low(unit) || (0xD800..=0xDBFF).contains(&unit) {
            out.extend_from_slice(b"\\ufffd");
            i += 6;
            repaired = true;
        } else {
            out.extend_from_slice(esc);
            i += 6;
        }
    }

    if repaired {
        Cow::Owned(
            String::from_utf8(out)
                .unwrap_or_else(|e| String::from_utf8_lossy(&e.into_bytes()).into_owned()),
        )
    } else {
        Cow::Borrowed(text)
    }
}

/// Code unit of a `\uXXXX` escape starting at `p`.
fn escape_at(b: &[u8], p: usize) -> Option<u16> {
    if p + 6 > b.len() || b[p] != b'\\' || b[p + 1] != b'u' {
        return None;
    }
    let hex = &b[p + 2..p + 6];
    if !hex.iter().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let s = std::str::from_utf8(hex).ok()?;
    u16::from_str_radix(s, 16).ok()
}

/// Conversational text for context search, read out of the generation spans.
///
/// Each generation span's `toolInput` embeds the full prompt — system block,
/// re-sent history and all — so the current turn's question is the *last*
/// `<user_query>` block, found from the end; earlier matches are the prompt
/// template quoting itself. Assistant replies sit in `toolOutput` as
/// chat.completion JSON (`choices[].message.content`, string content only —
/// tool-call turns carry `null`).
pub fn drain_messages(sink: &mut dyn FnMut(crate::context::ContextMessage)) {
    use crate::context::ContextMessage;

    let traces_dir = get_workbuddy_dir().join("traces");
    if !traces_dir.exists() {
        return;
    }

    for file_path in collect_trace_files(&traces_dir) {
        let Ok(text) = fs::read_to_string(&file_path) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&repair_lone_surrogates(&text))
        else {
            continue;
        };
        let Some(spans) = value.get("spans").and_then(|v| v.as_array()) else {
            continue;
        };
        let session_id = value
            .get("trace")
            .and_then(|t| t.get("sessionId").or_else(|| t.get("traceId")))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        for span in spans {
            if span.get("type").and_then(|t| t.as_str()) != Some("generation") {
                continue;
            }
            let timestamp = span
                .get("startedAt")
                .and_then(|ts| ts.as_str())
                .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
                .map(|dt| dt.timestamp())
                .or_else(|| file_mtime(&file_path))
                .unwrap_or(0);

            if let Some(query) = last_user_query(span.get("toolInput").and_then(|v| v.as_str())) {
                if !query.trim().is_empty() {
                    // WorkBuddy traces carry no working directory; the
                    // project stays unknown for this source.
                    sink(ContextMessage {
                        source: Source::WorkBuddy,
                        session_id: session_id.clone(),
                        role: "user",
                        timestamp,
                        text: unescape_prompt_literals(&query),
                        project: String::new(),
                        title: String::new(),
                    });
                }
            }

            if let Some(reply) = assistant_reply(span.get("toolOutput").and_then(|v| v.as_str())) {
                if !reply.trim().is_empty() {
                    sink(ContextMessage {
                        source: Source::WorkBuddy,
                        session_id: session_id.clone(),
                        role: "assistant",
                        timestamp,
                        text: unescape_prompt_literals(&reply),
                        project: String::new(),
                        title: String::new(),
                    });
                }
            }
        }
    }
}

/// The text of the final `<user_query>` block, scanning from the end so the
/// prompt template's own inline mention of the tag can never match. A block
/// missing its closing tag (truncated write) still yields its head.
fn last_user_query(tool_input: Option<&str>) -> Option<String> {
    let input = tool_input?;
    let open = input.rfind("<user_query>")? + "<user_query>".len();
    let body = match input[open..].find("</user_query>") {
        Some(close) => &input[open..open + close],
        None => &input[open..(open + 2000).min(input.len())],
    };
    // The template sometimes wraps the question with a leading newline.
    Some(body.trim().to_string())
}

/// Concatenated string contents of every choice in a chat.completion payload.
fn assistant_reply(tool_output: Option<&str>) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(tool_output?).ok()?;
    let items = match parsed {
        serde_json::Value::Array(items) => items,
        obj @ serde_json::Value::Object(_) => vec![obj],
        _ => return None,
    };
    let mut out = Vec::new();
    for item in items {
        for choice in item
            .get("choices")
            .and_then(|c| c.as_array())
            .into_iter()
            .flatten()
        {
            if let Some(content) = choice
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_str())
            {
                out.push(content.to_string());
            }
        }
    }
    Some(out.join("\n"))
}

/// The trace writer double-escapes its payload strings, so after JSON parsing
/// the text still holds literal `\n` / `\t` / `\"` sequences. Undo those so
/// snippets read as sentences instead of one long line.
fn unescape_prompt_literals(text: &str) -> String {
    if !text.contains('\\') {
        return text.to_string();
    }
    text.replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace("\\\"", "\"")
}

// ============================================================
// Tool events (R1): WorkBuddy traces record tool calls as `function` spans
// with `toolName` / `toolInput` / `toolOutput`. The output is a JSON envelope
// string (`{"content":..., "error":...}`) — kept raw; R2's synopsis layer is
// where it gets structured.
// ============================================================

/// Collect into a vector; the sync path uses [`drain_messages`] so a source's
/// messages are absorbed one at a time instead of all living at once.
pub fn collect_messages() -> Vec<crate::context::ContextMessage> {
    let mut msgs = Vec::new();
    drain_messages(&mut |m| msgs.push(m));
    msgs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> serde_json::Result<serde_json::Value> {
        serde_json::from_str(&repair_lone_surrogates(text))
    }

    #[test]
    fn lone_surrogates_become_replacement_char() {
        for bad in [r#"{"m":"x \ud83d y"}"#, r#"{"m":"x \udc00 y"}"#] {
            assert!(serde_json::from_str::<serde_json::Value>(bad).is_err());
            assert_eq!(parse(bad).unwrap()["m"], "x \u{FFFD} y");
        }
    }

    #[test]
    fn valid_escapes_pass_through() {
        for good in [
            r#"{"m":"😀 \ud83d\ude00 \u0041"}"#,
            r#"{"m":"literal \\ud83d text"}"#,
        ] {
            assert!(matches!(repair_lone_surrogates(good), Cow::Borrowed(_)));
            assert_eq!(
                parse(good).unwrap(),
                serde_json::from_str::<serde_json::Value>(good).unwrap()
            );
        }
    }
}
