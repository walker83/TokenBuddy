//! Tool-event store — R1 of the context-search blueprint
//! (`docs/context-search-blueprint-2026-09-06.md`).
//!
//! The conversation index deliberately excludes tool I/O because agents re-send
//! prior tool output every turn; indexing turns would index the same cache N
//! times. This module indexes **blocks, not turns**: one tool call = one event,
//! keyed by `(source, session_id, block_key)` where `block_key` is the source's
//! own stable id (tool_use_id / callID / spanId / toolCallId). A block seen
//! again is idempotent under its FNV-1a doc id, so cross-sync re-collection
//! collapses into `id_dup_skipped`; blocks with *different* keys but *identical
//!* normalized output (the `(Bash completed with no output)` × 36 case)
//! collapse into `content_dup_skipped` — that rate is the real noise number
//! R1's acceptance gate reads.
//!
//! Storage is a second parquet cache (`~/.tokenbuddy/tools.parquet`) beside the
//! conversation one: same engine, same write pattern (tmp + rename), no new
//! query engine. Raw output is the T0 layer — nothing here feeds the in-memory
//! index yet; that is R2 (bounded synopsis).

use crate::mimo;
use crate::opencode;
use crate::zcode;
use crate::Source;
use anyhow::Result;
use arrow::array::{Array, BooleanBuilder, Int64Array, Int64Builder, RecordBatch, StringBuilder};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::collections::HashMap;
use std::path::Path;

/// Raw tool output can dwarf conversation text (Agent runs report in
/// tens of KB). This cap is the T0 storage bound; the R2 synopsis index
/// layer never sees more than its own budget regardless.
pub const MAX_TOOL_CHARS: usize = 32 * 1024;

/// One extracted tool call or result, before dedupe. `text` prefers the
/// output; sources that persist no output (pi only stores results alongside —
/// covered; a call with an empty result falls back to rendered input) keep the
/// rendered arguments, which are searchable signal in their own right.
#[derive(Debug, Clone)]
pub struct ToolEvent {
    pub source: Source,
    pub session_id: String,
    pub timestamp: i64,
    pub tool_name: String,
    /// The source's stable per-call id when it has one; callers fall back to
    /// `content_key_of` over the rendered text so dedupe still works.
    pub block_key: String,
    pub text: String,
    pub is_error: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ToolSyncStats {
    pub collected: u64,
    pub imported: u64,
    /// `(source, session, block_key)` already in the store — the idempotent
    /// re-collection path, expected to dominate from the second sync on.
    pub id_dup_skipped: u64,
    /// Same normalized output under a *different* block key — the repeated-
    /// cache noise this layer exists to collapse.
    pub content_dup_skipped: u64,
    pub total: u64,
}

use serde::Serialize;

/// Fallback block key for sources without per-call ids: same identity rule as
/// the conversation store (`context::doc_id_of`), minus one layer of nesting.
pub fn content_key_of(normalized_text: &str) -> String {
    format!("content:{}", crate::context::doc_id_of("t", "", normalized_text))
}

pub fn tool_doc_id_of(source: &str, session_id: &str, block_key: &str) -> i64 {
    crate::context::doc_id_of(source, session_id, block_key)
}

/// Slash-rooted paths with at least two segments (`/Users/walker/code/foo`,
/// but not `/tmp` or `/`), deduped, most-frequent-first, capped. Hand-rolled:
/// no regex dependency, and the token grammar is this narrow on purpose.
pub fn extract_file_paths(text: &str) -> Vec<String> {
    fn is_seg_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-' || b >= 0x80
    }
    let mut counts: HashMap<String, usize> = HashMap::new();
    let bytes = text.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] != b'/' {
            i += 1;
            continue;
        }
        // The slash must start a token: mid-date slashes ("2026/09/06") sit
        // between word bytes and never start a path.
        if i > 0 && is_seg_byte(bytes[i - 1]) {
            i += 1;
            continue;
        }
        let start = i;
        let mut segs = 0usize; // interior separators = segments - 1
        let mut prev_sep = false; // the leading slash itself is not a break
        let mut j = i;
        while j < bytes.len() {
            let b = bytes[j];
            if b == b'/' {
                if j > start {
                    if prev_sep {
                        break; // double slash — URL scheme / UNC, not a path
                    }
                    segs += 1;
                }
                prev_sep = true;
            } else if is_seg_byte(b) {
                prev_sep = false;
            } else {
                break;
            }
            j += 1;
        }
        if segs >= 1 && j - start >= 4 {
            if let Ok(s) = std::str::from_utf8(&bytes[start..j]) {
                *counts.entry(s.to_string()).or_insert(0) += 1;
            }
        }
        i = j.max(start + 1);
    }
    let mut paths: Vec<(String, usize)> = counts.into_iter().collect();
    paths.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    paths.into_iter().take(8).map(|(p, _)| p).collect()
}

fn truncate_chars(s: &str, max: usize) -> (String, bool) {
    if s.chars().count() <= max {
        return (s.to_string(), false);
    }
    (s.chars().take(max).collect(), true)
}

// ============================================================
// Parquet persistence (mirrors context.rs's cache pattern)
// ============================================================

fn tools_schema() -> SchemaRef {
    SchemaRef::from(Schema::new(vec![
        Field::new("doc_id", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("tool_name", DataType::Utf8, false),
        Field::new("is_error", DataType::Boolean, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("last_seen", DataType::Int64, false),
        Field::new("file_paths", DataType::Utf8, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("truncated", DataType::Boolean, false),
    ]))
}

struct ExistingRow {
    doc_id: i64,
    last_seen: i64,
}

fn read_existing(path: &Path) -> Result<Vec<ExistingRow>> {
    let mut rows = Vec::new();
    if !path.exists() {
        return Ok(rows);
    }
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let id_idx = schema
        .index_of("doc_id")
        .map_err(|e| anyhow::anyhow!("tools parquet has no doc_id column: {e}"))?;
    let ls_idx = schema
        .index_of("last_seen")
        .map_err(|e| anyhow::anyhow!("tools parquet has no last_seen column: {e}"))?;
    let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), [id_idx, ls_idx]);
    for batch in builder.with_projection(mask).build()? {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("doc_id is Int64");
        let lss = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("last_seen is Int64");
        for i in 0..batch.num_rows() {
            rows.push(ExistingRow { doc_id: ids.value(i), last_seen: lss.value(i) });
        }
    }
    Ok(rows)
}

fn events_to_batch(events: &[(i64, &ToolEvent, String, bool, i64)]) -> RecordBatch {
    // (doc_id, event, file_paths_joined, truncated, last_seen)
    let schema = tools_schema();
    let n = events.len();
    let mut id_b = Int64Builder::with_capacity(n);
    let mut src_b = StringBuilder::with_capacity(n, n * 10);
    let mut sid_b = StringBuilder::with_capacity(n, n * 40);
    let mut tool_b = StringBuilder::with_capacity(n, n * 16);
    let mut err_b = BooleanBuilder::with_capacity(n);
    let mut ts_b = Int64Builder::with_capacity(n);
    let mut ls_b = Int64Builder::with_capacity(n);
    let mut fp_b = StringBuilder::with_capacity(n, n * 80);
    let mut text_b = StringBuilder::with_capacity(n, n * 200);
    let mut trunc_b = BooleanBuilder::with_capacity(n);

    for (id, e, fps, truncated, last_seen) in events {
        id_b.append_value(*id);
        src_b.append_value(e.source.as_str());
        sid_b.append_value(&e.session_id);
        tool_b.append_value(&e.tool_name);
        err_b.append_value(e.is_error);
        ts_b.append_value(e.timestamp);
        ls_b.append_value(*last_seen);
        fp_b.append_value(fps);
        text_b.append_value(&e.text);
        trunc_b.append_value(*truncated);
    }

    RecordBatch::try_new(schema, vec![
        Arc::new(id_b.finish()) as Arc<dyn Array>,
        Arc::new(src_b.finish()),
        Arc::new(sid_b.finish()),
        Arc::new(tool_b.finish()),
        Arc::new(err_b.finish()),
        Arc::new(ts_b.finish()),
        Arc::new(ls_b.finish()),
        Arc::new(fp_b.finish()),
        Arc::new(text_b.finish()),
        Arc::new(trunc_b.finish()),
    ])
    .expect("tools record batch construction")
}

use std::sync::Arc;

fn write_parquet(path: &Path, batch: &RecordBatch) -> Result<()> {
    let tmp = path.with_extension("parquet.tmp");
    let file = std::fs::File::create(&tmp)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

/// Merge `events` into the tools parquet: id-level dupes bump `last_seen`
/// (the store is rewritten on merge anyway, so refreshing one column is the
/// same cost as appending), content-level dupes are counted and dropped.
pub fn sync_tools(path: &Path, clear: bool, events: Vec<ToolEvent>) -> Result<ToolSyncStats> {
    if clear && path.exists() {
        let _ = std::fs::remove_file(path);
    }
    let existing = read_existing(path)?;
    let mut last_seen_by_id: HashMap<i64, i64> =
        existing.into_iter().map(|r| (r.doc_id, r.last_seen)).collect();

    let mut stats = ToolSyncStats::default();
    let mut new_rows: Vec<(i64, ToolEvent, String, bool, i64)> = Vec::new();
    let mut run_content_keys: HashMap<i64, ()> = HashMap::new(); // content hash → seen this run
    let mut bumped: Vec<i64> = Vec::new(); // sorted+deduped before the binary_search below

    for e in &events {
        stats.collected += 1;
        let normalized = crate::context::normalize(&e.text);
        let doc_id = tool_doc_id_of(e.source.as_str(), &e.session_id, &e.block_key);
        if last_seen_by_id.contains_key(&doc_id) {
            stats.id_dup_skipped += 1;
            let ls = last_seen_by_id.get_mut(&doc_id).unwrap();
            if e.timestamp > *ls {
                *ls = e.timestamp;
                bumped.push(doc_id);
            }
            continue;
        }
        let content_key = fnv_content(e.source.as_str(), &e.session_id, &normalized);
        if run_content_keys.insert(content_key, ()).is_some() {
            stats.content_dup_skipped += 1;
            continue;
        }
        let (text, truncated) = truncate_chars(&e.text, MAX_TOOL_CHARS);
        let fps = extract_file_paths(&text).join("\u{1}");
        new_rows.push((doc_id, e.clone(), fps, truncated, e.timestamp));
        last_seen_by_id.insert(doc_id, e.timestamp);
        stats.imported += 1;
    }
    stats.total = last_seen_by_id.len() as u64;

    bumped.sort_unstable();
    bumped.dedup();

    if new_rows.is_empty() && bumped.is_empty() {
        return Ok(stats);
    }

    // Read the old file whole (the merge rewrite already pays this), refresh
    // last_seen for bumped ids, append the new rows, write atomically.
    let schema = tools_schema();
    let mut batches: Vec<RecordBatch> = Vec::new();
    if path.exists() && !bumped.is_empty() {
        let file = std::fs::File::open(path)?;
        for batch in ParquetRecordBatchReaderBuilder::try_new(file)?.build()? {
            let mut batch = batch?;
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("doc_id is Int64");
            let touched = (0..batch.num_rows()).any(|i| bumped.binary_search(&ids.value(i)).is_ok());
            if touched {
                let lss = batch
                    .column(6)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("last_seen is Int64");
                let mut new_ls = Int64Builder::with_capacity(batch.num_rows());
                for i in 0..batch.num_rows() {
                    let id = ids.value(i);
                    new_ls.append_value(last_seen_by_id.get(&id).copied().unwrap_or(lss.value(i)));
                }
                let mut columns = batch.columns().to_vec();
                columns[6] = Arc::new(new_ls.finish());
                batch = RecordBatch::try_new(schema.clone(), columns)?;
            }
            batches.push(batch);
        }
    } else if path.exists() {
        let file = std::fs::File::open(path)?;
        for batch in ParquetRecordBatchReaderBuilder::try_new(file)?.build()? {
            batches.push(batch?);
        }
    }
    if !new_rows.is_empty() {
        let refs: Vec<(i64, &ToolEvent, String, bool, i64)> = new_rows
            .iter()
            .map(|(id, e, fps, t, ls)| (*id, e, fps.clone(), *t, *ls))
            .collect();
        batches.push(events_to_batch(&refs));
    }
    if !batches.is_empty() {
        let merged = concat_batches(&schema, &batches)?;
        write_parquet(path, &merged)?;
    }
    Ok(stats)
}

fn fnv_content(source: &str, session_id: &str, normalized: &str) -> i64 {
    crate::context::doc_id_of(source, session_id, normalized)
}

// ============================================================
// Collection — opencode lineage (zcode / opencode / mimo)
// ============================================================

pub fn collect_all_tool_events() -> Vec<ToolEvent> {
    let mut evs = Vec::new();
    evs.extend(crate::claude::collect_tool_events());
    evs.extend(lineage_tool_events());
    evs.extend(crate::pi::collect_tool_events());
    evs.extend(crate::qoder::collect_tool_events());
    evs.extend(crate::workbuddy::collect_tool_events());
    evs
}

/// zcode / opencode / mimo share the part-table schema: `type: "tool"` parts
/// carry `callID`, `tool`, and `state.{status,input,output}`. A call's part
/// row is written once (not re-sent per turn), so the dedupe burden here is
/// cross-sync idempotency, not intra-log repetition.
fn lineage_tool_events() -> Vec<ToolEvent> {
    let mut evs = Vec::new();
    for (source, db_path) in [
        (Source::Zcode, zcode::db_path()),
        (Source::OpenCode, opencode::db_path()),
        (Source::Mimo, mimo::db_path()),
    ] {
        if !db_path.exists() {
            continue;
        }
        let ok = (|| -> Result<()> {
            let conn = rusqlite::Connection::open(&db_path)?;
            let mut stmt = conn.prepare(
                "SELECT m.session_id, m.time_created,
                        json_extract(p.data, '$.callID'),
                        json_extract(p.data, '$.tool'),
                        json_extract(p.data, '$.state.status'),
                        json_extract(p.data, '$.state.output'),
                        json_extract(p.data, '$.state.input')
                 FROM part p
                 JOIN message m ON m.id = p.message_id
                 WHERE json_extract(p.data, '$.type') = 'tool'",
            )?;
            let rows = stmt.query_map([], |row| {
                let sid: Option<String> = row.get(0)?;
                let created_ms: Option<i64> = row.get(1)?;
                let call_id: Option<String> = row.get(2)?;
                let tool: Option<String> = row.get(3)?;
                let status: Option<String> = row.get(4)?;
                let output: Option<String> = row.get(5)?;
                let input: Option<String> = row.get(6)?;
                Ok((sid.unwrap_or_default(), created_ms.unwrap_or(0), call_id, tool, status, output, input))
            })?;
            for row in rows {
                let (session_id, created_ms, call_id, tool, status, output, input) = row?;
                // Prefer the output; a call with no output yet still gets its
                // rendered arguments — the command line is the searchable part.
                let (text, from_output) = match output {
                    Some(o) if !o.trim().is_empty() => (o, true),
                    _ => match input {
                        Some(i) if !i.trim().is_empty() => (i, false),
                        _ => continue,
                    },
                };
                let status_l = status.unwrap_or_default().to_lowercase();
                let is_error = (!from_output && status_l == "error")
                    || (from_output
                        && (status_l.contains("error") || text.contains("<tool_use_error>")));
                evs.push(ToolEvent {
                    source,
                    session_id,
                    timestamp: created_ms / 1000,
                    tool_name: tool.unwrap_or_default(),
                    block_key: call_id.unwrap_or_else(|| content_key_of(&crate::context::normalize(&text))),
                    text,
                    is_error,
                });
            }
            Ok(())
        })();
        if let Err(e) = ok {
            eprintln!("[TokenBuddy] tools: {} extraction failed: {e}", source.as_str());
        }
    }
    evs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(source: Source, session: &str, key: &str, text: &str) -> ToolEvent {
        ToolEvent {
            source,
            session_id: session.to_string(),
            timestamp: 100,
            tool_name: "Bash".into(),
            block_key: key.into(),
            text: text.into(),
            is_error: false,
        }
    }

    fn tmp_path(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("tokenbuddy-tools-test-{tag}-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        dir.join("tools.parquet")
    }

    #[test]
    fn file_paths_need_two_segments_and_rank_by_frequency() {
        let paths = extract_file_paths(
            "read /Users/walker/code/foo/src/main.rs then /Users/walker/code/foo/src/main.rs again; /tmp stays out",
        );
        assert_eq!(paths[0], "/Users/walker/code/foo/src/main.rs");
        assert!(!paths.iter().any(|p| p == "/tmp"));
    }

    #[test]
    fn resync_is_idempotent_and_bumps_last_seen() {
        let path = tmp_path("idem");
        let _ = std::fs::remove_file(&path);
        let events = vec![
            ev(Source::Claude, "s1", "call_1", "output one"),
            ev(Source::Claude, "s1", "call_2", "output two"),
        ];
        let first = sync_tools(&path, false, events.clone()).unwrap();
        assert_eq!(first.imported, 2);
        assert_eq!(first.id_dup_skipped, 0);
        let later = ToolEvent { timestamp: 900, ..events[0].clone() };
        let second = sync_tools(&path, false, vec![events[1].clone(), later]).unwrap();
        assert_eq!(second.collected, 2);
        assert_eq!(second.imported, 0, "re-collection must not create docs");
        assert_eq!(second.id_dup_skipped, 2);
        assert_eq!(second.total, 2);
        // last_seen was refreshed for the bumped event
        let rows = read_existing(&path).unwrap();
        let row = rows.iter().find(|r| {
            r.doc_id == tool_doc_id_of("claude", "s1", "call_1")
        }).unwrap();
        assert_eq!(row.last_seen, 900);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn same_output_under_different_keys_collapses() {
        let path = tmp_path("content");
        let _ = std::fs::remove_file(&path);
        // 36 identical "(Bash completed with no output)" under distinct call
        // ids is exactly the noise this store exists to collapse.
        let events: Vec<ToolEvent> = (0..36)
            .map(|i| ev(Source::Claude, "s1", &format!("call_{i}"), "(Bash completed with no output)"))
            .collect();
        let stats = sync_tools(&path, false, events).unwrap();
        assert_eq!(stats.collected, 36);
        assert_eq!(stats.imported, 1, "one doc for the content");
        assert_eq!(stats.content_dup_skipped, 35);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn same_output_in_another_session_is_its_own_doc() {
        let path = tmp_path("session");
        let _ = std::fs::remove_file(&path);
        let events = vec![
            ev(Source::Claude, "s1", "call_1", "shared output"),
            ev(Source::Claude, "s2", "call_2", "shared output"),
        ];
        let stats = sync_tools(&path, false, events).unwrap();
        assert_eq!(stats.imported, 2, "session scopes the content key");
        let _ = std::fs::remove_file(&path);
    }
}

// ============================================================
// R2 · Bounded synopsis — the index-layer projection π(O10)
// ============================================================

/// Index-text budget for a tool doc. The raw output stays in tools.parquet
/// (T0); only this projection ever reaches the in-memory index, so the cap
/// directly bounds index RAM: B × event_count × 2 (text + lows) bytes.
pub const SYNOPSIS_MAX_CHARS: usize = 600;

/// Head/tail split of the elided middle: errors cluster at the tail, command
/// echo at the head (U-shaped value density — claude-mem's 60/30 heuristic,
/// rescaled for the smaller budget).
fn head_tail(text: &str, budget: usize) -> String {
    let total = text.chars().count();
    if total <= budget {
        return text.to_string();
    }
    let tail_n = budget * 2 / 5;
    let head_n = budget - tail_n;
    let head: String = text.chars().take(head_n).collect();
    let tail: String = text.chars().skip(total - tail_n).collect();
    format!("{head}\n[elided {} chars]\n{tail}", total - head_n - tail_n)
}

/// Sample a JSON document: keys at depth ≤2, array elements clamped to 3,
/// scalars truncated. Deterministic — no randomness, so the golden set never
/// wobbles between rebuilds (OpenViking's stable-sampling rule).
fn json_synopsis(text: &str, budget: usize) -> Option<String> {
    let trimmed = text.trim_start();
    if !trimmed.starts_with('{') && !trimmed.starts_with('[') {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(trimmed).ok()?;
    let mut out = String::new();
    fn walk(v: &serde_json::Value, depth: usize, budget: usize, out: &mut String) {
        if out.chars().count() >= budget {
            return;
        }
        match v {
            serde_json::Value::Object(map) => {
                for (k, val) in map.iter().take(12) {
                    if out.chars().count() >= budget {
                        return;
                    }
                    match val {
                        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                            out.push_str(k);
                            out.push_str(": ");
                            walk(val, depth + 1, budget, out);
                        }
                        scalar => {
                            let s = scalar.to_string();
                            let s = if s.chars().count() > 120 {
                                format!("{}…", s.chars().take(120).collect::<String>())
                            } else {
                                s
                            };
                            out.push_str(k);
                            out.push_str(": ");
                            out.push_str(&s);
                            out.push('\n');
                        }
                    }
                }
            }
            serde_json::Value::Array(items) => {
                out.push_str(&format!("[{} items] ", items.len()));
                for item in items.iter().take(3) {
                    if out.chars().count() >= budget {
                        return;
                    }
                    walk(item, depth + 1, budget, out);
                }
            }
            scalar => {
                let s = scalar.to_string();
                out.push_str(&s);
                out.push('\n');
            }
        }
    }
    walk(&value, 0, budget, &mut out);
    Some(out)
}

/// Stack-trace projection: the exception/type line, the first frames, and
/// every `caused by` / `Caused by` link in the chain.
fn stack_synopsis(text: &str, budget: usize) -> Option<String> {
    const MARKERS: [&str; 3] = ["panicked at", "Traceback (most recent call last)", "Exception in thread"];
    if !MARKERS.iter().any(|m| text.contains(m)) {
        return None;
    }
    // The discriminating value sits at BOTH ends: the raise site opens the
    // trace, but the actual exception message is the LAST lines (Python prints
    // frames first). Head-only projections made every Python traceback look
    // identical — R2's blind misses were exactly this.
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let frame_head = lines.iter().take(2);
    let tail_start = lines.len().saturating_sub(5);
    let frame_tail = lines[tail_start..].iter();
    let mut out = String::new();
    for line in frame_head.chain(frame_tail) {
        if out.chars().count() >= budget {
            break;
        }
        out.push_str(line.trim_end());
        out.push('\n');
    }
    Some(out)
}

/// Error-envelope projection for tool outputs that wrap the payload
/// (`<tool_use_error>…`, `{"error": …}`): the error line is the signal.
fn error_line(text: &str) -> Option<&str> {
    text.lines()
        .find(|l| l.contains("<tool_use_error>") || l.contains("\"error\"") || l.contains("Error:"))
}

/// The deterministic projection π for one tool output. Kind dispatch:
/// JSON → sampled keys; stack trace → frames + causes; otherwise head/tail
/// with an error-line pin when the output carries one. Never random.
pub fn synopsis(text: &str) -> String {
    let budget = SYNOPSIS_MAX_CHARS;
    if let Some(j) = json_synopsis(text, budget) {
        if !j.trim().is_empty() {
            return j;
        }
    }
    if let Some(s) = stack_synopsis(text, budget) {
        return s;
    }
    let projected = head_tail(text, budget);
    if let Some(err) = error_line(text) {
        // Pin even when the tail already kept it: the error is the query
        // signal for this doc, it goes first.
        if !projected.starts_with(err) {
            return format!("{err}\n{projected}");
        }
    }
    projected
}

#[cfg(test)]
mod synopsis_tests {
    use super::*;

    #[test]
    fn synopsis_is_deterministic_and_bounded() {
        let big = format!("head line\n{}\ntail error at the end", "x\n".repeat(2000));
        let a = synopsis(&big);
        let b = synopsis(&big);
        assert_eq!(a, b, "deterministic: golden set must not wobble");
        assert!(a.chars().count() <= SYNOPSIS_MAX_CHARS + 40); // elided marker overhead
        assert!(a.contains("head line") && a.contains("tail error"), "U-shaped keep");
    }

    #[test]
    fn json_outputs_sample_keys_and_clamp_arrays() {
        let j = r#"{"results":[{"id":1},{"id":2},{"id":3},{"id":4}],"total":4,"name":"westock"}"#;
        let s = synopsis(j);
        assert!(s.contains("total: 4") && s.contains("name:"), "scalars kept: {s}");
        assert!(s.contains("[4 items]"), "array count kept: {s}");
        assert!(!s.contains("\"id\":4"), "arrays clamped to 3: {s}");
    }

    #[test]
    fn stack_traces_keep_raise_site_and_tail_exception() {
        let t = format!(
            "Exit code 1\nTraceback (most recent call last):\n{}  TypeError: object of type 'NoneType' has no len()",
            "  File \"src/app.py\", line 10, in run\n   data = get(x)\n".repeat(30)
        );
        let s = synopsis(&t);
        assert!(s.contains("Traceback"), "raise site kept");
        assert!(s.contains("TypeError"), "tail exception kept — differentiates twins");
        assert!(!s.contains("line 10, in run\n   data = get(x)\n   data = get(x)"), "middle frames elided");
    }

    #[test]
    fn error_lines_get_pinned() {
        // Error buried in the tail region: head/tail keeps it but far from the
        // front — the pin must surface it first.
        let t = format!("ok line\n{}\n<tool_use_error>No such file: x", "fill\n".repeat(300));
        let s = synopsis(&t);
        assert!(s.starts_with("<tool_use_error>"), "error pinned to front: {s}");
    }
}
