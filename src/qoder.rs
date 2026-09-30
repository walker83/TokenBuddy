//! Collector for the Qoder coding agent.
//!
//! Qoder writes one JSONL per chat session under
//! `<home>/projects/<project-slug>/<session-uuid>.jsonl`. Every assistant turn
//! is a `type: "assistant"` record whose `message.usage` carries Claude-shaped
//! token fields plus an exact `credits` figure. The service masks the token
//! counts — they are always 0 — so spend is tracked through `credits` and the
//! token columns stay empty, which the cost engine reports as credit-derived
//! rather than price-estimated.
//!
//! Wall-clock per request only exists in the runtime log
//! (`<home>/logs/sessions/<slug>/<session>/segments/*.jsonl`, `turn.finished`
//! events), correlated back by `usage.request_id`.

use crate::{file_mtime, FileCacheMap, Source, TokenRecord};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

static FILE_CACHE: Mutex<Option<FileCacheMap>> = Mutex::new(None);

/// Drop the resident parse cache. The cache only exists to make a *second*
/// sync cheaper than the first; left in place it pins every record of every
/// session log in the heap for the life of the process, growing with total
/// history and eating the resident-memory budget the dashboard is measured
/// against. `store::sync` calls this once the parquet has been written, so
/// the saving is paid back only by whoever asks for the next sync.
pub fn release_caches() {
    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    *cache = None;
}

/// Where this collector reads from, when that place exists on this machine.
/// Powers the dashboard's source-health panel and the first-run prompt, so a
/// user with a missing or unmoved tool directory is told which one instead of
/// just seeing zeros.
pub fn log_path() -> Option<std::path::PathBuf> {
    get_qoder_homes().into_iter().next()
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let homes = get_qoder_homes();
    if homes.is_empty() {
        return Ok(vec![]);
    }

    let durations = collect_turn_durations(&homes);

    let mut jsonl_files = Vec::new();
    for home in &homes {
        jsonl_files.extend(collect_session_files(&home.join("projects")));
    }

    let mut cache = FILE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let cache_map = cache.get_or_insert_with(HashMap::new);

    let mut all_records = Vec::new();
    let mut current_paths: HashSet<String> = HashSet::new();

    for file_path in &jsonl_files {
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

    for record in all_records.iter_mut() {
        if let Some(request_id) = &record.record_id {
            record.duration_ms = durations.get(request_id).map(|d| *d as u64);
        }
    }

    Ok(all_records)
}

/// Candidate roots (CN and international builds), for `tokenbuddy doctor`.
pub fn log_paths() -> Vec<PathBuf> {
    match dirs::home_dir() {
        Some(home) => vec![home.join(".qoder-cn"), home.join(".qoder")],
        None => vec![],
    }
}

fn get_qoder_homes() -> Vec<PathBuf> {
    if let Ok(custom) = std::env::var("QODER_DIR") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            let path = PathBuf::from(trimmed);
            return if path.is_dir() { vec![path] } else { vec![] };
        }
    }

    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return vec![],
    };
    // `.qoder-cn` is the CN build, `.qoder` the international one; both are
    // read when present so a machine running either is covered.
    [".qoder-cn", ".qoder"]
        .iter()
        .map(|name| home.join(name))
        .filter(|p| p.join("projects").is_dir())
        .collect()
}

/// Session logs live one level deep: `projects/<project-slug>/<session>.jsonl`.
fn collect_session_files(projects_dir: &Path) -> Vec<PathBuf> {
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
        let Ok(sub_entries) = fs::read_dir(&path) else {
            continue;
        };
        for sub_entry in sub_entries.flatten() {
            let sub_path = sub_entry.path();
            if sub_path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                files.push(sub_path);
            }
        }
    }

    files
}

/// `request_id -> turn duration_ms` from the runtime logs. Rebuilt on every
/// sync; the log tree is a few MB and only read while parsing new sessions.
fn collect_turn_durations(homes: &[PathBuf]) -> HashMap<String, i64> {
    let mut durations = HashMap::new();
    for home in homes {
        collect_turn_durations_in(&home.join("logs").join("sessions"), &mut durations);
    }
    durations
}

/// Per-session activity counters from the same runtime logs the duration join
/// walks: `(model_responses, user_prompts, turns, tool_calls)`. Sessions that
/// never produce a billable record — BYOK, where Qoder reports no usage at
/// all — still log every event, so these counters are the only trace they
/// leave; the insights session leaderboard merges them instead of dropping
/// such sessions on the floor.
pub fn runtime_session_counts() -> HashMap<String, (u64, u64, u64, u64)> {
    let homes = get_qoder_homes();
    let mut out = HashMap::new();
    for home in &homes {
        count_runtime_in(&home.join("logs").join("sessions"), &mut out);
    }
    out
}

fn count_runtime_in(sessions_dir: &Path, out: &mut HashMap<String, (u64, u64, u64, u64)>) {
    let Ok(slugs) = fs::read_dir(sessions_dir) else {
        return;
    };
    for slug in slugs.flatten() {
        let Ok(sessions) = fs::read_dir(slug.path()) else {
            continue;
        };
        for session in sessions.flatten() {
            let session_dir = session.path();
            if !session_dir.is_dir() {
                continue;
            }
            let Some(sid) = session_dir.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let counter = out.entry(sid.to_string()).or_insert((0, 0, 0, 0));
            let Ok(segments) = fs::read_dir(session_dir.join("segments")) else {
                continue;
            };
            for segment in segments.flatten() {
                let path = segment.path();
                if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                    continue;
                }
                let Ok(file) = fs::File::open(&path) else {
                    continue;
                };
                for line in BufReader::new(file).lines().map_while(|l| l.ok()) {
                    if !line.contains("\"type\"") {
                        continue;
                    }
                    let Ok(t) = serde_json::from_str::<serde_json::Value>(&line) else {
                        continue;
                    };
                    match t.get("type").and_then(|v| v.as_str()) {
                        Some("model.response.completed") => counter.0 += 1,
                        Some("input.prompt.submitted") => counter.1 += 1,
                        Some("turn.finished") => counter.2 += 1,
                        Some("tool.requested") => counter.3 += 1,
                        _ => {}
                    }
                }
            }
        }
    }
}

fn collect_turn_durations_in(sessions_dir: &Path, out: &mut HashMap<String, i64>) {
    let Ok(entries) = fs::read_dir(sessions_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let session_dir = entry.path();
        if !session_dir.is_dir() {
            continue;
        }
        let Ok(sub_entries) = fs::read_dir(&session_dir) else {
            continue;
        };
        for sub_entry in sub_entries.flatten() {
            let turn_dir = sub_entry.path();
            if !turn_dir.is_dir() {
                continue;
            }
            let segments = match fs::read_dir(turn_dir.join("segments")) {
                Ok(s) => s,
                Err(_) => continue,
            };
            for segment in segments.flatten() {
                let path = segment.path();
                if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                    continue;
                }
                let Ok(file) = fs::File::open(&path) else {
                    continue;
                };
                for line in BufReader::new(file).lines().map_while(|l| l.ok()) {
                    if !line.contains("turn.finished") {
                        continue;
                    }
                    let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
                        continue;
                    };
                    if value.get("type").and_then(|t| t.as_str()) != Some("turn.finished") {
                        continue;
                    }
                    let data = match value.get("data") {
                        Some(d) => d,
                        None => continue,
                    };
                    let duration_ms = match data.get("duration_ms").and_then(|v| v.as_i64()) {
                        Some(d) => d,
                        None => continue,
                    };
                    let request_id = data
                        .get("client_request_id")
                        .or_else(|| value.get("request_id"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    if let Some(id) = request_id {
                        out.insert(id, duration_ms);
                    }
                }
            }
        }
    }
}

/// Qoder masks model ids to opaque keys (`qmodel`, `qfmodel`, …) in every
/// local log. The app's own model-config cache — QoderCN globalStorage
/// `aicoding.modelConfigs.cache.*`, read 2026-09 — maps them to display names
/// in plaintext; the table below is that catalog. Unknown keys (a newly
/// shipped model, or `byok:<profile-uuid>` for bring-your-own-key sessions)
/// pass through unchanged rather than being guessed.
fn demask_model(model: &str) -> String {
    const CATALOG: &[(&str, &str)] = &[
        ("qmodel_38max", "Qwen3.8-Max"),
        ("qmodel_latest", "Qwen3.7-Max"),
        ("qmodel", "Qwen3.7-Plus"),
        ("qfmodel", "Qwen3.8-Flash"),
        ("q37fmodel", "Qwen3.7-Flash"),
        ("dmodel", "DeepSeek-V4-Pro"),
        ("dfmodel", "DeepSeek-V4-Flash"),
        ("gmodel", "GLM-5.3"),
        ("gfmodel", "GLM-5.3-Flash"),
        ("gm51model", "GLM-5.2"),
        ("kmodel", "Kimi-K2.7-Code"),
        ("mmodel", "MiniMax-M2.7"),
    ];
    // Longest-key-first is implicit in table order: `qmodel_38max` must win
    // over the `qmodel` prefix.
    for (key, name) in CATALOG {
        if model == *key {
            return name.to_string();
        }
    }
    model.to_string()
}

fn parse_single_file(file_path: &Path) -> Result<Vec<TokenRecord>> {
    let file = fs::File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        if !line.contains("\"assistant\"") {
            continue;
        }

        let value: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if value.get("type").and_then(|t| t.as_str()) != Some("assistant") {
            continue;
        }

        let message = match value.get("message") {
            Some(m) => m,
            None => continue,
        };
        let usage = match message.get("usage") {
            Some(u) => u,
            None => continue,
        };

        let input_tokens = usage
            .get("input_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let output_tokens = usage
            .get("output_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let cache_read_tokens = usage
            .get("cache_read_input_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let cache_creation_tokens = usage
            .get("cache_creation_input_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let credits = usage.get("credits").and_then(|v| v.as_f64()).unwrap_or(0.0);
        // The only token-scale figure Qoder reports unmasked: fraction of the
        // context window this request consumed (0..=1). The token counts stay
        // zeroed server-side, so this is the "context fill" signal the
        // dashboard shows for the source.
        let context_ratio = usage
            .get("context_usage_ratio")
            .and_then(|v| v.as_f64())
            .filter(|r| (0.0..=1.0).contains(r))
            .unwrap_or(0.0);

        // Qoder marks free/cancelled turns non-billable with everything zeroed;
        // counting them would inflate request counts without any usage.
        let has_usage =
            input_tokens + output_tokens + cache_read_tokens + cache_creation_tokens > 0;
        let billable = usage
            .get("billable")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !(has_usage || (billable && credits > 0.0)) {
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

        let request_id = usage
            .get("request_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        records.push(TokenRecord {
            source: Source::Qoder,
            model: demask_model(
                message
                    .get("model")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown"),
            ),
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_creation_tokens,
            timestamp,
            session_id: value
                .get("sessionId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            project: String::new(),
            duration_ms: None,
            ttft_ms: None,
            credits,
            context_ratio,
            // The request id doubles as the dedupe key and the join key back to
            // the runtime log's per-turn duration.
            record_id: request_id,
            sidechain: false,
            merge_key: None,
        });
    }

    Ok(records)
}

/// Conversational text for context search. Qoder's session JSONL is
/// Claude-shaped (`type: user|assistant` records, `message.content` a string
/// or an array of typed parts), so extraction mirrors the claude collector.
pub fn drain_messages(sink: &mut dyn FnMut(crate::context::ContextMessage)) {
    use crate::context::ContextMessage;

    let homes = get_qoder_homes();
    if homes.is_empty() {
        return;
    }

    for home in &homes {
        for file_path in collect_session_files(&home.join("projects")) {
            let file = match fs::File::open(&file_path) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let fallback_session = file_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            // projects/<dir>/<session>.jsonl — the directory is the project
            // (munged; Qoder's carry a per-session date+hash suffix).
            let project = file_path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            let mut session_id = String::new();

            for line_result in BufReader::new(file).lines() {
                let Ok(line) = line_result else { continue };
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
                    continue;
                };

                if session_id.is_empty() {
                    session_id = value
                        .get("sessionId")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&fallback_session)
                        .to_string();
                }
                let role = match value.get("type").and_then(|t| t.as_str()) {
                    Some("user") => "user",
                    Some("assistant") => "assistant",
                    _ => continue,
                };
                let Some(message) = value.get("message") else {
                    continue;
                };
                let timestamp = value
                    .get("timestamp")
                    .and_then(|v| v.as_str())
                    .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
                    .map(|dt| dt.timestamp())
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
                    sink(ContextMessage {
                        source: Source::Qoder,
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

// ============================================================
// Tool events (R1): Qoder's session JSONL is Claude-shaped, so the same
// tool_use/tool_result pairing applies. Sources that never emit tool blocks
// simply yield nothing.
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
    use super::demask_model;

    /// The catalog is matched by exact key, so the `qmodel` entry must not
    /// swallow `qmodel_38max`, and anything not in the catalog — a future
    /// model key, or a BYOK profile id — comes back untouched instead of
    /// being mapped to a guess.
    #[test]
    fn masked_keys_map_to_catalog_display_names() {
        assert_eq!(demask_model("qmodel"), "Qwen3.7-Plus");
        assert_eq!(demask_model("qmodel_38max"), "Qwen3.8-Max");
        assert_eq!(demask_model("qfmodel"), "Qwen3.8-Flash");
        assert_eq!(demask_model("gmodel"), "GLM-5.3");
        assert_eq!(demask_model("kmodel"), "Kimi-K2.7-Code");
        assert_eq!(demask_model("byok:00000000-1111"), "byok:00000000-1111");
        assert_eq!(demask_model("zmodel-2099"), "zmodel-2099");
        assert_eq!(demask_model("unknown"), "unknown");
    }
}
