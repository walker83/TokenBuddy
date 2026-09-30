//! Context search over the AI tools' conversation logs.
//!
//! The token store answers "how much"; this module answers "where did I see
//! that". Each collector extracts only conversational text (user prompts and
//! assistant replies) — the tool I/O and re-sent system context that dominate
//! raw logs stay out, which is what keeps search results from drowning in
//! cached repeats.
//!
//! Pipeline: collectors → normalize+hash dedupe → `~/.tokenbuddy/context.parquet` →
//! a compact in-memory gram index (ASCII tokens in a BTreeMap so queries can
//! match by prefix, CJK character bigrams in a HashMap; text lives compressed
//! in a zstd arena and decompresses per candidate, so the index holds no
//! per-doc String). Queries first decide which
//! terms carry information: a term covering more than 30% of the corpus is a
//! stopword by evidence, not by list. Candidates pool in three cascading
//! tiers — AND over the rarest content terms, bigram AND, idf-weighted OR —
//! then verify by substring scan on text lowercased once at build time, and
//! rank by idf-weighted coverage, hit density, exact-phrase bonus, role and
//! recency. Every search fills a `SearchTrace` (tier, pool sizes, per-stage
//! timings) that the server logs to stderr, returns in the response, and
//! appends as one JSON line to `~/.tokenbuddy/search-log.jsonl` (rotated past 5MB),
//! so query efficiency stays observable, tunable, and regressable against
//! real traffic. The index is rebuilt in a background thread after each
//! sync; the parquet file is the durable state, so a restart just re-reads
//! it.

use crate::{
    claude, cline, codex, gemini, hermes, mimo, minimax, opencode, pi, qoder, qwen, workbuddy,
    zcode, Source,
};
use anyhow::Result;
use arrow::array::{
    Array, BooleanArray, BooleanBuilder, Int64Array, Int64Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

/// Per-doc text kept in RAM and searched. Conversational messages rarely
/// exceed this; docs past it are stored short of their tail, and `truncated`
/// flags the hit in results.
const MAX_DOC_CHARS: usize = 16 * 1024;

/// Upper bound on candidates verified per query. Tier-3 pools arrive
/// idf-ranked so the cap drops the least-promising docs first; tier-1/2
/// pools beyond it keep the newest docs.
const MAX_VERIFY_CANDIDATES: usize = 8_000;

/// A term covering more than this fraction of the corpus carries no routing
/// information — a stopword by evidence, not by list. It sits out the
/// hard-AND pools, and its ~0 idf keeps it from moving ranked results or
/// leaking into the highlight set.
const HIGH_DF_RATIO: f64 = 0.3;

/// Verbose queries drive the candidate pool with only this many rarest
/// content terms — demanding that every word of a sentence co-occur is how
/// vague queries used to fall through every tier into noise.
const POOL_TERMS_MAX: usize = 3;

/// Refuse to build an index over a runaway corpus; stats surfaces the cap.
const MAX_INDEX_DOCS: usize = 1_000_000;

/// R4: below this many tier-1 AND hits, backfill the pool with idf-ranked OR
/// candidates. A smaller AND is a precision win only if it is not an accident
/// of one skewed term's document frequency.
const SOFT_AND_MIN_POOL: usize = 50;

/// R5 click-feedback weight (O7 simplified): score += β·ln(1+clicks). Log
/// damping keeps a heavily-clicked doc from immune-to-anything status.
/// β calibrated on the golden set (R5): 0.15 needed ~300 clicks to lift a
/// backfill-rescued doc past an echo-heavy top-10; 0.30 crosses with ~10 —
/// clicks are user preference among content-equivalent docs, and 20 clicks
/// (+0.90) stays under the phrase bonus (1.5).
const CLICK_BETA: f64 = 0.30;

/// Backfill tail cap: verify is a substring scan per candidate, so an
/// unbounded backfill trades the recall win for a 20× latency hit. Top-2000
/// by summed idf keeps the docs any query term can justify.
const SOFT_AND_BACKFILL_MAX: usize = 2_000;

/// ASCII tokens at least this long also match by prefix ("zcod" hits
/// "zcode"); shorter tokens match exactly only, or a one-letter query would
/// union nearly every posting in the index.
const PREFIX_MIN_CHARS: usize = 3;

/// One conversational message extracted from a source log, before dedupe.
/// `project` is the working directory the session ran in — a real path for
/// the opencode-lineage stores, a munged directory name for the rest, empty
/// when the source logs carry nothing (WorkBuddy). See `project_label`.
/// `title` is the session's display name when the source stores one
/// (opencode-lineage `session.title`, Claude summaries); the index falls
/// back to the first user message for sources that do not.
#[derive(Debug, Clone)]
pub struct ContextMessage {
    pub source: Source,
    pub session_id: String,
    pub role: &'static str, // "user" | "assistant"
    pub timestamp: i64,
    pub text: String,
    pub project: String,
    pub title: String,
}

/// Display label for a project: home-relative where the raw value is a real
/// path; munged storage names (`-Users-walker-code-foo`, pi's trailing
/// `--`) keep their dash form because the escaping cannot be reversed
/// unambiguously (`data-ai` may hide `data/ai` or `data.ai`).
pub fn project_label(project: &str) -> String {
    let trimmed = project.trim_matches('-');
    if trimmed.is_empty() {
        return String::new();
    }
    if let Some(home) = dirs::home_dir() {
        let home = home.to_string_lossy();
        if let Some(rest) = project.strip_prefix(home.as_ref()) {
            return rest.trim_start_matches('/').to_string();
        }
        let user = home.trim_end_matches('/').rsplit('/').next().unwrap_or("");
        let munged_prefix = format!("Users-{user}-");
        if let Some(rest) = trimmed.strip_prefix(&munged_prefix) {
            // Qoder nests per-session dirs: `-Users-x-Documents-Q-<date>-<hash>`
            if let Some(stripped) = strip_session_suffix(rest) {
                return stripped;
            }
            return rest.to_string();
        }
    }
    trimmed.to_string()
}

/// Qoder's project dirs carry a per-session `<...>-YYYY-MM-DD-<8 hex>` tail;
/// peel it so sessions of one working directory share one label.
fn strip_session_suffix(label: &str) -> Option<String> {
    // "-YYYY-MM-DD-XXXXXXXX" is 20 ASCII bytes at the tail.
    if label.len() <= 20 || !label.is_ascii() {
        return None;
    }
    let n = label.len();
    let date = &label[n - 19..n - 9];
    let hash = &label[n - 8..];
    let date_ok = date.len() == 10
        && date.as_bytes()[4] == b'-'
        && date.as_bytes()[7] == b'-'
        && date[..4].bytes().all(|b| b.is_ascii_digit())
        && date[5..7].bytes().all(|b| b.is_ascii_digit())
        && date[8..].bytes().all(|b| b.is_ascii_digit());
    let hash_ok = hash.len() == 8 && hash.bytes().all(|b| b.is_ascii_hexdigit());
    if date_ok && hash_ok && label.as_bytes()[n - 20] == b'-' {
        return Some(label[..n - 20].to_string());
    }
    None
}

/// A deduped document — what one parquet row / one index entry holds.
#[derive(Debug, Clone, Serialize)]
pub struct ContextDoc {
    pub doc_id: i64,
    pub source: String,
    pub session_id: String,
    pub role: String,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SyncStats {
    pub collected: u64,
    pub imported: u64,
    pub skipped: u64,
    pub total_docs: u64,
}

// ============================================================
// Normalization + dedupe key
// ============================================================

/// Lowercase with whitespace runs collapsed to one space. This is the dedupe
/// identity, so two restatements of the same cached context block collapse
/// regardless of wrapping or capitalization.
pub fn normalize(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_space = true; // swallows the leading run
    for ch in text.chars() {
        if ch.is_whitespace() {
            if !in_space {
                out.push(' ');
                in_space = true;
            }
        } else {
            in_space = false;
            out.extend(ch.to_lowercase());
        }
    }
    while out.ends_with(' ') {
        out.pop();
    }
    out
}

/// FNV-1a 64-bit: stable across runs and versions, unlike `DefaultHasher`,
/// because doc_ids persisted in parquet must keep their meaning.
fn fnv1a(bytes: &[u8]) -> i64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h as i64
}

/// Dedupe key for a message: same text inside one session collapses; the same
/// text asked in a different session stays (its own timestamp is real signal).
pub fn doc_id_of(source: &str, session_id: &str, normalized: &str) -> i64 {
    fnv1a(format!("{source}\u{1}{session_id}\u{1}{normalized}").as_bytes())
}

// ============================================================
// Collection
// ============================================================

/// True if `name` is a table or view in the connected database.
/// Cheap one-row probe against sqlite_master — used to pick between
/// OpenCode's v1 (`message`/`part`) and v2 (`session_message`) schemas
/// without a per-version dependency. Shared between the token collector
/// (`opencode.rs`) and this context search collector.
pub(crate) fn sqlite_table_exists(conn: &rusqlite::Connection, name: &str) -> bool {
    let mut stmt = match conn.prepare("SELECT 1 FROM sqlite_master WHERE name = ?1 LIMIT 1") {
        Ok(s) => s,
        Err(_) => return false,
    };
    stmt.exists(rusqlite::params![name]).unwrap_or(false)
}

/// zcode / opencode / mimo share the opencode lineage schema: `message` rows
/// carry role + visibility in a JSON `data` column, `part` rows carry the
/// text under `type: "text"`. Hidden messages (background notifications) are
/// filtered when the field is present. One call per source so the sync can
/// absorb and drop each source's messages instead of holding all of them.
fn drain_lineage(source: Source, db_path: &std::path::Path, sink: &mut dyn FnMut(ContextMessage)) {
    {
        if !db_path.exists() {
            return;
        }
        let ok = (|| -> Result<()> {
            // Read-write open like the token collectors: a read-only open
            // fails while the writer's WAL is not checkpointed. SELECTs only.
            // `session.directory` is the real working directory and
            // `session.title` the display name — together they answer "which
            // project was this in" and "which conversation is this".
            let conn = rusqlite::Connection::open(db_path)?;

            // OpenCode shipped two SQLite shapes. v1 has `part` rows
            // carrying text bodies and `session` for metadata. v2 (>= 2026)
            // drops both: message bodies are flattened into
            // `session_message.data.content[]` as `{type:"text", text:"..."}`
            // entries, and `session` became `session_v2`. Detect at runtime
            // so both stay supported. See PR feat/opencode-v2-on-gitea.
            // `session.directory` is the real working directory and
            // `session.title` the display name — together they answer "which
            // project was this in" and "which conversation is this".
            let sql = if crate::context::sqlite_table_exists(&conn, "part") {
                // v1: text lives on `part`, joined to `message` and `session`.
                "SELECT m.session_id, m.time_created, \
                        json_extract(m.data, '$.role'), \
                        json_extract(p.data, '$.text'), \
                        s.directory, \
                        s.title \
                 FROM part p \
                 JOIN message m ON m.id = p.message_id \
                 JOIN session s ON s.id = m.session_id \
                 WHERE json_extract(p.data, '$.type') = 'text' \
                   AND COALESCE(json_extract(m.data, '$.semantics.uiVisibility'), 'visible') != 'hidden'"
            } else if crate::context::sqlite_table_exists(&conn, "session_message") {
                // v2: text comes from one of two places per row type:
                //   - user messages: `data.text` is the prompt string directly.
                //   - assistant messages: `data.content[]` is an array of
                //     {type, text} parts where `type='text'` is the visible
                //     reply. Reasoning and tool parts are skipped to keep
                //     the search corpus the human-visible conversation.
                // `json_each` flattens the content array so we can pick the
                // text parts with a regular WHERE clause. Hidden-row
                // filtering is omitted because v2 doesn't expose a
                // uiVisibility field.
                "SELECT m.session_id, s.time_created, \
                        m.type, \
                        COALESCE( \
                            json_extract(m.data, '$.text'), \
                            json_extract(je.value, '$.text')), \
                        s.directory, \
                        s.title \
                 FROM session_message m \
                 JOIN session_v2 s ON s.id = m.session_id, \
                      json_each(m.data, '$.content') je \
                 WHERE ( \
                       (m.type = 'user' AND json_extract(m.data, '$.text') IS NOT NULL) \
                    OR (m.type = 'assistant' \
                        AND json_extract(je.value, '$.type') = 'text' \
                        AND json_extract(je.value, '$.text') IS NOT NULL) \
                 )"
            } else {
                // Neither schema present — nothing to extract from this source.
                return Ok(());
            };
            let mut stmt = conn.prepare(sql)?;
            let rows = stmt.query_map([], |row| {
                let session_id: String = row.get::<_, Option<String>>(0)?.unwrap_or_default();
                let created_ms: Option<i64> = row.get(1)?;
                let role: Option<String> = row.get(2)?;
                let text: Option<String> = row.get(3)?;
                let project: Option<String> = row.get(4)?;
                let title: Option<String> = row.get(5)?;
                Ok((
                    session_id,
                    created_ms.unwrap_or(0),
                    role,
                    text,
                    project,
                    title,
                ))
            })?;
            for row in rows {
                let (session_id, created_ms, role, text, project, title) = row?;
                let Some(text) = text else { continue };
                if text.trim().is_empty() {
                    continue;
                }
                let role = match role.as_deref() {
                    Some("user") => "user",
                    Some("assistant") => "assistant",
                    _ => continue,
                };
                sink(ContextMessage {
                    source,
                    session_id,
                    role,
                    timestamp: created_ms / 1000,
                    text,
                    project: project.unwrap_or_default(),
                    title: title.unwrap_or_default(),
                });
            }
            Ok(())
        })();
        if let Err(e) = ok {
            eprintln!(
                "[TokenBuddy] context: {} extraction failed: {e}",
                source.as_str()
            );
        }
    }
}

// ============================================================
// Parquet persistence
// ============================================================

fn context_schema() -> SchemaRef {
    SchemaRef::from(Schema::new(vec![
        Field::new("doc_id", DataType::Int64, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("project", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("truncated", DataType::Boolean, false),
    ]))
}

/// `docs` entries: (doc_id, message, text stored short of its tail?)
fn docs_to_batch(docs: &[(i64, ContextMessage, bool)]) -> RecordBatch {
    let schema = context_schema();
    let n = docs.len();
    let mut id_b = Int64Builder::with_capacity(n);
    let mut src_b = StringBuilder::with_capacity(n, n * 10);
    let mut sid_b = StringBuilder::with_capacity(n, n * 40);
    let mut role_b = StringBuilder::with_capacity(n, n * 10);
    let mut ts_b = Int64Builder::with_capacity(n);
    let mut text_b = StringBuilder::with_capacity(n, n * 200);
    let mut proj_b = StringBuilder::with_capacity(n, n * 60);
    let mut title_b = StringBuilder::with_capacity(n, n * 80);
    let mut trunc_b = BooleanBuilder::with_capacity(n);

    for (id, m, truncated) in docs {
        id_b.append_value(*id);
        src_b.append_value(m.source.as_str());
        sid_b.append_value(&m.session_id);
        role_b.append_value(m.role);
        ts_b.append_value(m.timestamp);
        text_b.append_value(&m.text);
        proj_b.append_value(&m.project);
        title_b.append_value(&m.title);
        trunc_b.append_value(*truncated);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_b.finish()) as Arc<dyn Array>,
            Arc::new(src_b.finish()),
            Arc::new(sid_b.finish()),
            Arc::new(role_b.finish()),
            Arc::new(ts_b.finish()),
            Arc::new(text_b.finish()),
            Arc::new(proj_b.finish()),
            Arc::new(title_b.finish()),
            Arc::new(trunc_b.finish()),
        ],
    )
    .expect("context record batch construction")
}

fn read_existing_doc_ids(path: &Path) -> Result<HashSet<i64>> {
    let mut ids = HashSet::new();
    if !path.exists() {
        return Ok(ids);
    }
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema().clone();
    let idx = schema
        .index_of("doc_id")
        .map_err(|e| anyhow::anyhow!("context parquet has no doc_id column: {e}"))?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), [idx]);
    for batch in builder.with_projection(mask).build()? {
        let batch = batch?;
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("doc_id is Int64");
        for i in 0..arr.len() {
            ids.insert(arr.value(i));
        }
    }
    Ok(ids)
}

/// Test-only writer: production sync streams through `SyncSink` instead, but
/// the golden-set tests build an arbitrary parquet in one shot.
#[cfg(test)]
fn write_context_parquet(path: &Path, batch: &RecordBatch) -> Result<()> {
    let tmp = path.with_extension("parquet.tmp");
    let file = std::fs::File::create(&tmp)?;
    let mut writer =
        parquet::arrow::arrow_writer::ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

/// A parquet written before a column was added (`project`, then `title`)
/// cannot merge with the current schema; the file is a cache over the source
/// logs, so "delete and re-import" is the whole migration. Absent file →
/// vacuously fine.
fn parquet_is_current_schema(path: &Path) -> bool {
    let Ok(file) = std::fs::File::open(path) else {
        return true;
    };
    match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(builder) => {
            let schema = builder.schema();
            schema.index_of("project").is_ok() && schema.index_of("title").is_ok()
        }
        Err(_) => true, // unreadable: let the later full read surface the error
    }
}

/// Run one collector's drain with a panic guard.
///
/// The drainers return nothing, so a malformed log surfaces as a panic rather
/// than an `Err`. Left unguarded, one bad file in one tool's history took the
/// whole index down with it and the search box went permanently empty — a
/// failure the user could neither see nor work around. Catching per source
/// keeps the other seven sources searchable and prints which one gave up.
fn drain_guarded<F: FnOnce()>(name: &str, f: F) {
    // R102: a switched-off source costs nothing here either — the search
    // index follows the same switches as the ledger, or a disabled source
    // would keep quietly paying the heaviest bill in the house.
    if !crate::quota::source_enabled(name) {
        return;
    }
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).is_err() {
        eprintln!("[TokenBuddy] context collection failed for {name}; other sources kept");
    }
}

/// Sources the sync drains conversation text for, in drain order. The
/// coverage block of `/api/context/stats` is derived from this, so a source
/// missing here reads as `not_supported` instead of silently looking like
/// "nothing to say" in search results (issue #23).
pub const DRAINED_SOURCES: &[&str] = &[
    "claude",
    "zcode",
    "opencode",
    "mimo",
    "pi",
    "cline",
    "qoder",
    "workbuddy",
    "minimax",
    "hermes",
    "qwen",
    "gemini",
    "codex",
];

/// Collect from every source, dedupe against `path`, and append the new docs.
/// `clear` rebuilds from scratch (the caller owns snapshotting, mirroring
/// Incremental collect+dedupe over the live sources, one source at a time:
/// each source's message vector is absorbed and dropped before the next
/// collector runs, so the transient peak is the largest single source, not
/// every source's logs at once.
pub fn sync_context(path: &Path, clear: bool) -> Result<SyncStats> {
    let mut sink = SyncSink::open(path, clear)?;
    {
        let mut push = |m: ContextMessage| sink.absorb_one(m);
        drain_guarded("claude", || claude::drain_messages(&mut push));
        drain_guarded("zcode", || {
            drain_lineage(Source::Zcode, &zcode::db_path(), &mut push)
        });
        drain_guarded("opencode", || {
            drain_lineage(Source::OpenCode, &opencode::db_path(), &mut push)
        });
        drain_guarded("mimo", || {
            drain_lineage(Source::Mimo, &mimo::db_path(), &mut push)
        });
        drain_guarded("pi", || pi::drain_messages(&mut push));
        // R84:Cline 家族对话(api_conversation_history.json 的文本块,
        // 环境包裹/工具块/纯 tool_result 轮次全部剥离)。
        drain_guarded("cline", || cline::drain_messages(&mut push));
        drain_guarded("qoder", || qoder::drain_messages(&mut push));
        drain_guarded("workbuddy", || workbuddy::drain_messages(&mut push));
        drain_guarded("minimax", || minimax::drain_messages(&mut push));
        drain_guarded("hermes", || hermes::drain_messages(&mut push));
        // issue #23:qwen/gemini/codex 的 drain_messages 早已实现并公告,
        // 却从未接进生产清单——账本能统计、搜索静默空。接线后与 Source
        // 枚举的承诺对齐;新的采集器必须同时进 DRAINED_SOURCES 与这里的
        // 清单,guard 测试会拦住只写其一的回归。
        drain_guarded("qwen", || qwen::drain_messages(&mut push));
        drain_guarded("gemini", || gemini::drain_messages(&mut push));
        drain_guarded("codex", || codex::drain_messages(&mut push));
    }
    sink.finish(path)
}

/// Dedupe + writeback state shared by the streaming sync and the testable
/// [`sync_messages`] core.
struct SyncSink {
    existing: std::collections::HashSet<i64>,
    new_docs: Vec<(i64, ContextMessage, bool)>,
    collected: u64,
}

impl SyncSink {
    fn open(path: &Path, clear: bool) -> Result<Self> {
        if path.exists() && (clear || !parquet_is_current_schema(path)) {
            if !clear {
                eprintln!("[TokenBuddy] context: legacy parquet schema, rebuilding");
            }
            let _ = std::fs::remove_file(path);
        }
        Ok(Self {
            existing: read_existing_doc_ids(path)?,
            new_docs: Vec::new(),
            collected: 0,
        })
    }

    fn absorb(&mut self, messages: Vec<ContextMessage>) {
        for m in messages {
            self.absorb_one(m);
        }
    }

    fn absorb_one(&mut self, m: ContextMessage) {
        self.collected += 1;
        let (text, truncated) = truncate_chars(&m.text, MAX_DOC_CHARS);
        let id = doc_id_of(m.source.as_str(), &m.session_id, &normalize(&text));
        if !self.existing.insert(id) {
            return;
        }
        self.new_docs
            .push((id, ContextMessage { text, ..m }, truncated));
    }

    fn finish(self, path: &Path) -> Result<SyncStats> {
        let imported = self.new_docs.len() as u64;
        if !self.new_docs.is_empty() {
            // Stream old rows into the rewritten file one batch at a time —
            // a read-everything-then-concat merge held two copies of the
            // corpus in RAM and set the process's peak.
            let new_batch = docs_to_batch(&self.new_docs);
            let tmp = path.with_extension("parquet.tmp");
            let out = std::fs::File::create(&tmp)?;
            let mut writer =
                parquet::arrow::arrow_writer::ArrowWriter::try_new(out, new_batch.schema(), None)?;
            if path.exists() {
                let file = std::fs::File::open(path)?;
                let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
                for batch in reader {
                    writer.write(&batch?)?;
                }
            }
            writer.write(&new_batch)?;
            writer.close()?;
            std::fs::rename(tmp, path)?;
        }

        Ok(SyncStats {
            collected: self.collected,
            imported,
            skipped: self.collected - imported,
            total_docs: self.existing.len() as u64,
        })
    }
}

/// The testable core of [`sync_context`]: same contract, caller-supplied
/// messages.
pub fn sync_messages(path: &Path, clear: bool, messages: Vec<ContextMessage>) -> Result<SyncStats> {
    let mut sink = SyncSink::open(path, clear)?;
    sink.absorb(messages);
    sink.finish(path)
}

fn truncate_chars(s: &str, max: usize) -> (String, bool) {
    if s.chars().count() <= max {
        return (s.to_string(), false);
    }
    (s.chars().take(max).collect(), true)
}

// ============================================================
// Index
// ============================================================

#[derive(Debug, Clone, Serialize)]
pub struct IndexStats {
    pub docs: usize,
    pub ascii_terms: usize,
    pub cjk_grams: usize,
    pub approx_bytes: usize,
    pub built_at: i64,
    pub build_ms: u64,
    pub oldest: Option<i64>,
    pub newest: Option<i64>,
    pub by_source: HashMap<String, usize>,
    /// Distinct project labels → doc counts, what the dashboard filter is
    /// built from. Docs without a project (WorkBuddy) are not listed.
    pub by_project: HashMap<String, usize>,
    pub truncated_build: bool,
}

/// Role bytes stored per doc; the string forms live in `ROLE_NAMES`.
const ROLE_USER: u8 = 0;
const ROLE_ASSISTANT: u8 = 1;
const ROLE_DIGEST: u8 = 2;
const ROLE_NAMES: [&str; 3] = ["user", "assistant", "session_digest"];

/// 32 bytes, all inline — the pre-interning `DocMeta` carried three `String`s
/// per doc and the strings, not the metadata, were what set the index's
/// floor. Sources, sessions and projects are interned; text lives compressed
/// in `text_arena`.
struct DocMeta {
    doc_id: i64,
    timestamp: i64,
    session: u32,
    len_chars: u32,
    /// Index into `projects`.
    project: u32,
    source: u8,
    role: u8,
    truncated: bool,
}

/// One posting list inside `postings`: byte range of its delta-varint
/// encoding, with the entry count (the document frequency) kept separately —
/// varints are variable width, so decoding stops at `len` entries.
#[derive(Clone, Copy)]
struct Span {
    off: u32,
    len: u32,
}

/// Doc indices are ascending per term, so each posting encodes as the gap to
/// its predecessor — one byte for the small gaps that dominate — keeping the
/// whole posting arena a fraction of its u32 size.
fn encode_deltas(postings: &[u32], out: &mut Vec<u8>) {
    let mut prev = 0u32;
    for &d in postings {
        let mut gap = d.wrapping_sub(prev);
        prev = d;
        loop {
            let b = (gap & 0x7f) as u8;
            gap >>= 7;
            if gap == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        }
    }
}

/// Inverse of [`encode_deltas`]; repeated doc indices (ascii tokens that
/// occur twice in one doc) encode as zero-gap and decode back unchanged.
fn decode_deltas(bytes: &[u8], entries: u32) -> Vec<u32> {
    let mut out = Vec::with_capacity(entries as usize);
    let (mut prev, mut i) = (0u32, 0usize);
    while i < bytes.len() && out.len() < entries as usize {
        let (mut gap, mut shift) = (0u32, 0u32);
        loop {
            let Some(b) = bytes.get(i) else { return out };
            i += 1;
            gap |= ((b & 0x7f) as u32) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        prev = prev.wrapping_add(gap);
        out.push(prev);
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TermKind {
    Ascii,
    /// CJK character bigram — the CJK tier. A lone CJK char that can form
    /// no bigram arrives as a df-0 Gram so the pool falls to a full scan.
    Gram,
}

/// One query term with its corpus-wide document frequency resolved up front.
/// `df == 0` means the index never saw it; the BM25-style `idf` is ~0 for
/// corpus-wide terms, which is what neutralizes stopwords without a list.
struct QueryTerm {
    text: String,
    kind: TermKind,
    df: usize,
    idf: f64,
    /// Ascii terms ≥ `PREFIX_MIN_CHARS` match every vocab entry with that
    /// prefix; the merged, deduped postings are kept here so pool building
    /// does not recompute the range scan. Empty for the CJK tiers, whose
    /// postings live in the maps and are looked up by `kind`.
    postings: Vec<u32>,
}

/// Immutable, shareable search index over the context parquet.
///
/// Memory layout: one zstd-compressed text arena, one flat `Vec<u32>`
/// posting arena sliced by `Span`s, and interned doc metadata — the whole
/// index holds no per-doc `String` at all. A full index over the real corpus
/// sits well under 100 MB resident; the arenas are what verification and
/// snippet extraction decompress from, one doc at a time.
pub struct ContextIndex {
    docs: Vec<DocMeta>,
    /// Per-doc zstd frames concatenated; `text_spans[i]` is doc i's range.
    text_arena: Vec<u8>,
    text_spans: Vec<Span>,
    /// All posting lists concatenated as delta-varints, ascending within
    /// each span.
    postings: Vec<u8>,
    /// Lowercased ASCII-ish tokens → posting span.
    ascii: BTreeMap<Box<str>, Span>,
    /// CJK character bigrams → posting span.
    cjk: HashMap<Box<str>, Span>,
    /// (source id, session id) → display name: the tool's own title when it
    /// stores one, else the first user message.
    titles: HashMap<(u8, u32), Box<str>>,
    /// Every vocab entry's df, ascending — the relative stopword guard reads
    /// the whole distribution, not just one term's df.
    df_hist: Vec<u32>,
    /// Interning tables: ids in `DocMeta` index into these.
    source_names: Vec<Box<str>>,
    sessions: Vec<Box<str>>,
    projects: Vec<Box<str>>,
    stats: IndexStats,
}

fn is_cjk(ch: char) -> bool {
    matches!(ch,
        '\u{3040}'..='\u{30FF}'   // kana
        | '\u{3400}'..='\u{4DBF}' // CJK ext A
        | '\u{4E00}'..='\u{9FFF}' // CJK unified
        | '\u{AC00}'..='\u{D7AF}' // hangul syllables
        | '\u{F900}'..='\u{FAFF}' // CJK compat
    )
}

fn is_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

/// Split text into indexable grams, in two tiers:
/// - ASCII runs become whole lowercase tokens (`read_existing_doc_ids` stays
///   one token, split on `_` only in queries the same way);
/// - CJK runs contribute overlapping character bigrams — "文搜" inside
///   "上下文搜索" finds its docs, word boundaries and jieba's 55 MB
///   dictionary alike. Substring verification at query time is the
///   precision layer, so nothing needs a word tier.
///
/// Everything else is a separator; query and target split identically.
pub fn grams_of(text: &str) -> (Vec<String>, Vec<String>) {
    let mut tokens = Vec::new();
    let mut bigrams = Vec::new();
    let lower = text.to_lowercase();
    let mut word = String::new();
    let mut cjk_run = String::new();

    fn flush_word(word: &mut String, tokens: &mut Vec<String>) {
        if !word.is_empty() {
            tokens.push(std::mem::take(word));
        }
    }
    fn flush_cjk(run: &mut String, bigrams: &mut Vec<String>) {
        if run.is_empty() {
            return;
        }
        let chars: Vec<char> = run.chars().collect();
        for w in chars.windows(2) {
            bigrams.push(w.iter().collect::<String>());
        }
        run.clear();
    }

    for ch in lower.chars() {
        if is_token_char(ch) {
            flush_cjk(&mut cjk_run, &mut bigrams);
            word.push(ch);
        } else if is_cjk(ch) {
            flush_word(&mut word, &mut tokens);
            cjk_run.push(ch);
        } else {
            flush_word(&mut word, &mut tokens);
            flush_cjk(&mut cjk_run, &mut bigrams);
        }
    }
    flush_word(&mut word, &mut tokens);
    flush_cjk(&mut cjk_run, &mut bigrams);
    (tokens, bigrams)
}

/// Intersection of two ascending, deduped posting lists.
fn and_merge(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut next = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                next.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    next
}

/// Content fingerprint for near-duplicate suppression: gram hashes of the
/// normalized text. Two hits where one's shingles are mostly contained in
/// the other's read as "the agent restating the human" — kept once.
fn shingles_of(text: &str) -> HashSet<u64> {
    let (tokens, bigrams) = grams_of(&normalize(text));
    tokens
        .into_iter()
        .chain(bigrams)
        .map(|s| fnv1a(s.as_bytes()) as u64)
        .collect()
}

/// A candidate hit is a near-duplicate of a kept one when at least this
/// share of its shingles already appear in the kept text. Same-topic but
/// genuinely different replies sit around 0.3–0.45; restatements and quote
///heavy answers clear 0.55 easily.
const DUPLICATE_CONTAINMENT: f64 = 0.55;
/// Shingles below this count never flag duplicates — a one-liner quote is
/// not a restatement.
const MIN_SHINGLES_FOR_DUP: usize = 12;

/// zstd level for per-doc text frames in the arena. Level 3 measured a 1.6×
/// ratio on the real corpus (code-heavy conversation text compresses poorly);
/// higher levels bought almost nothing for the extra build seconds.
const TEXT_ZSTD_LEVEL: i32 = 3;
/// Scratch state for one index build, in two streaming passes over the
/// parquet: pass 1 sizes every term's posting list, pass 2 fills a single
/// pre-sized u32 bucket arena (one large allocation the OS takes back when
/// the build ends) and compresses doc text into its arena. Raw text and
/// per-term `Vec`s never both live in RAM at scale.
struct IndexBuilder {
    docs: Vec<DocMeta>,
    text_arena: Vec<u8>,
    text_spans: Vec<Span>,
    /// term string → term id, shared by both passes.
    terms: HashMap<Box<str>, u32>,
    /// per term: posting-list length (pass 1) then arena offset (finalize).
    counts: Vec<u32>,
    /// Ascii-kind terms by id, so finalize can build the ordered BTreeMap
    /// without re-tokenizing.
    ascii_ids: HashSet<u32>,
    source_ids: HashMap<Box<str>, u8>,
    source_names: Vec<Box<str>>,
    session_ids: HashMap<Box<str>, u32>,
    sessions: Vec<Box<str>>,
    project_ids: HashMap<Box<str>, u32>,
    projects: Vec<Box<str>>,
    titles: HashMap<(u8, u32), Box<str>>,
    /// First user message per session — digest text and the title fallback.
    first_user: HashMap<(u8, u32), Box<str>>,
}

impl IndexBuilder {
    fn new() -> Self {
        Self {
            docs: Vec::new(),
            text_arena: Vec::new(),
            text_spans: Vec::new(),
            terms: HashMap::new(),
            counts: Vec::new(),
            ascii_ids: HashSet::new(),
            source_ids: HashMap::new(),
            source_names: Vec::new(),
            session_ids: HashMap::new(),
            sessions: Vec::new(),
            project_ids: HashMap::new(),
            projects: Vec::new(),
            titles: HashMap::new(),
            first_user: HashMap::new(),
        }
    }

    fn intern_source(&mut self, s: &str) -> u8 {
        if let Some(&id) = self.source_ids.get(s) {
            return id;
        }
        let id = self.source_names.len() as u8;
        self.source_names.push(s.into());
        self.source_ids.insert(s.into(), id);
        id
    }

    fn intern_session(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.session_ids.get(s) {
            return id;
        }
        let id = self.sessions.len() as u32;
        self.sessions.push(s.into());
        self.session_ids.insert(s.into(), id);
        id
    }

    fn intern_project(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.project_ids.get(s) {
            return id;
        }
        let id = self.projects.len() as u32;
        self.projects.push(s.into());
        self.project_ids.insert(s.into(), id);
        id
    }

    /// Pass 1: size the posting lists. Tokenization mirrors `count_doc` —
    /// ascii tokens count per occurrence, CJK bigrams dedupe per doc — so the
    /// pass-2 cursor writes never overrun.
    fn count_doc(&mut self, text: &str) {
        let (tokens, bigrams) = grams_of(text);
        let tid = |b: &mut Self, t: Box<str>| -> u32 {
            let n = b.terms.len() as u32;
            let id = *b.terms.entry(t).or_insert(n);
            if id == n {
                b.counts.push(0);
            }
            id
        };
        for t in tokens {
            let id = tid(self, t.into_boxed_str());
            self.ascii_ids.insert(id);
            self.counts[id as usize] += 1;
        }
        let mut seen_grams = HashSet::new();
        for g in bigrams {
            if seen_grams.insert(g.clone()) {
                let id = tid(self, g.into_boxed_str());
                self.counts[id as usize] += 1;
            }
        }
    }

    /// Pass 2: append one doc — posting writes into the bucket arena slices
    /// handed in by the caller (`cursors` advance per term), original-case
    /// text compressed into its arena. `idx` must equal the doc's final
    /// position, i.e. docs arrive in the same order pass 1 saw them.
    #[allow(clippy::too_many_arguments)] // private two-pass builder, callers all in this file
    fn push_doc(
        &mut self,
        doc_id: i64,
        timestamp: i64,
        source: u8,
        session: u32,
        project: u32,
        role: u8,
        truncated: bool,
        text: &str,
        buckets: &mut [u32],
        cursors: &mut [u32],
    ) {
        let idx = self.docs.len() as u32;
        let (tokens, bigrams) = grams_of(text);
        let write = |tid: u32, buckets: &mut [u32], cursors: &mut [u32]| {
            let c = &mut cursors[tid as usize];
            let slot = *c;
            *c += 1;
            buckets[slot as usize] = idx;
        };
        for t in tokens {
            let id = self.terms[t.as_str()];
            write(id, buckets, cursors);
        }
        let mut seen_grams = HashSet::new();
        for g in bigrams {
            if seen_grams.insert(g.clone()) {
                let id = self.terms[g.as_str()];
                write(id, buckets, cursors);
            }
        }
        let compressed = zstd::bulk::compress(text.as_bytes(), TEXT_ZSTD_LEVEL)
            .unwrap_or_else(|_| text.as_bytes().to_vec());
        let span = Span {
            off: self.text_arena.len() as u32,
            len: compressed.len() as u32,
        };
        self.text_arena.extend_from_slice(&compressed);
        self.text_spans.push(span);
        self.docs.push(DocMeta {
            doc_id,
            timestamp,
            session,
            len_chars: text.chars().count() as u32,
            project,
            source,
            role,
            truncated,
        });
    }

    /// Finalize into the compact `ContextIndex`: each term's bucket interval
    /// sorts ascending, delta-varint-encodes into the posting arena, and
    /// records its `Span`; the bucket arena drops on return.
    #[allow(clippy::too_many_arguments)] // private finalize step, single call site
    fn finish(
        self,
        mut buckets: Vec<u32>,
        offsets: Vec<u32>,
        build_ms: u64,
        truncated_build: bool,
        oldest: Option<i64>,
        newest: Option<i64>,
        by_source: HashMap<String, usize>,
        by_project: HashMap<String, usize>,
    ) -> ContextIndex {
        let n_terms = self.counts.len();
        let mut postings: Vec<u8> = Vec::new();
        let mut ascii: BTreeMap<Box<str>, Span> = BTreeMap::new();
        let mut cjk: HashMap<Box<str>, Span> =
            HashMap::with_capacity(n_terms - self.ascii_ids.len());
        for (term, &tid) in &self.terms {
            let (off, len) = (
                offsets[tid as usize] as usize,
                self.counts[tid as usize] as usize,
            );
            let interval = &mut buckets[off..off + len];
            interval.sort_unstable();
            let span = Span {
                off: postings.len() as u32,
                len: len as u32,
            };
            encode_deltas(interval, &mut postings);
            if self.ascii_ids.contains(&tid) {
                ascii.insert(term.clone(), span);
            } else {
                cjk.insert(term.clone(), span);
            }
        }

        let mut df_hist: Vec<u32> = ascii.values().chain(cjk.values()).map(|s| s.len).collect();
        df_hist.sort_unstable();

        // Live-bytes estimate of everything the index holds at rest.
        let ascii_key_bytes: usize = ascii.keys().map(|k| k.len()).sum();
        let cjk_key_bytes: usize = cjk.keys().map(|k| k.len()).sum();
        let approx_bytes = postings.len()
            + self.text_arena.len()
            + self.text_spans.len() * std::mem::size_of::<Span>()
            + (ascii.len() + cjk.len())
                * (std::mem::size_of::<Span>() + 3 * std::mem::size_of::<usize>())
            + ascii_key_bytes
            + cjk_key_bytes
            + self.docs.len() * std::mem::size_of::<DocMeta>()
            + df_hist.len() * 4
            + self.sessions.iter().map(|s| s.len()).sum::<usize>()
            + self.projects.iter().map(|s| s.len()).sum::<usize>();

        let stats = IndexStats {
            docs: self.docs.len(),
            ascii_terms: ascii.len(),
            cjk_grams: cjk.len(),
            approx_bytes,
            built_at: crate::now_ts(),
            build_ms,
            oldest,
            newest,
            by_source,
            by_project,
            truncated_build,
        };

        ContextIndex {
            docs: self.docs,
            text_arena: self.text_arena,
            text_spans: self.text_spans,
            postings,
            ascii,
            cjk,
            titles: self.titles,
            df_hist,
            source_names: self.source_names,
            sessions: self.sessions,
            projects: self.projects,
            stats,
        }
    }
}

impl ContextIndex {
    /// Build from the context parquet. Missing file → an empty, usable index.
    /// Two streaming passes: pass 1 sizes every posting list (and derives the
    /// session digests, so their terms are sized too); pass 2 fills one
    /// pre-sized bucket arena and compresses text — the index never holds
    /// raw corpus text or per-term `Vec`s.
    pub fn build(path: &Path) -> Result<Self> {
        let t0 = Instant::now();
        let mut b = IndexBuilder::new();
        let mut truncated_build = false;
        // (doc count, max ts, has_user, first project id) per session.
        let mut sess_stat: HashMap<(u8, u32), (usize, i64, bool, u32)> = HashMap::new();
        // Rows pass 1 accepted — pass 2 must stop at the same row under the
        // MAX_INDEX_DOCS cap.
        let mut pass1_docs = 0usize;

        // R5: a context.parquet that cannot be opened is moved aside instead
        // of failing every build — it is derived data, fully re-collectable
        // from the source logs via a rebuild.
        let corrupt = path.exists()
            && std::fs::File::open(path)
                .and_then(|f| {
                    ParquetRecordBatchReaderBuilder::try_new(f)
                        .map_err(|e| std::io::Error::other(e.to_string()))
                })
                .is_err();
        if corrupt {
            let aside = path.with_file_name(format!(
                "context.corrupt-{}.parquet",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
            ));
            match std::fs::rename(path, &aside) {
                Ok(()) => eprintln!(
                    "[TokenBuddy] context.parquet 无法读取——已移至 {};重建索引将重新采集对话",
                    aside.display()
                ),
                Err(re) => eprintln!("[TokenBuddy] context.parquet 无法读取且移不开({re})"),
            }
        }
        if path.exists() && !corrupt {
            // ---- Pass 1: term sizes, session stats, digests, interning ----
            let file = std::fs::File::open(path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
            'p1: for batch in reader {
                let batch = batch?;
                let n = batch.num_rows();
                let ids = int_col(&batch, "doc_id");
                let sources = string_col(&batch, "source");
                let sids = string_col(&batch, "session_id");
                let roles = string_col(&batch, "role");
                let tss = int_col(&batch, "timestamp");
                let texts_col = string_col(&batch, "text");
                let projects_col = string_col(&batch, "project");
                let titles_col = string_col(&batch, "title");
                let truncs = bool_col(&batch, "truncated");
                let (
                    Some(_ids),
                    Some(sources),
                    Some(sids),
                    Some(roles),
                    Some(tss),
                    Some(texts_col),
                    Some(_truncs),
                ) = (ids, sources, sids, roles, tss, texts_col, truncs)
                else {
                    continue;
                };
                for i in 0..n {
                    if pass1_docs >= MAX_INDEX_DOCS {
                        truncated_build = true;
                        break 'p1;
                    }
                    let (text, _) = truncate_chars(texts_col.value(i), MAX_DOC_CHARS);
                    let source = b.intern_source(sources.value(i));
                    let session = b.intern_session(sids.value(i));
                    let project_id =
                        b.intern_project(projects_col.map(|c| c.value(i)).unwrap_or(""));
                    let role = if roles.value(i) == "user" {
                        ROLE_USER
                    } else {
                        ROLE_ASSISTANT
                    };
                    let ts = tss.value(i);
                    let title = titles_col.map(|c| c.value(i)).unwrap_or("");
                    if !title.is_empty() {
                        // Last non-empty title wins: tools rename sessions as
                        // they go, and parquet order is roughly chronological.
                        b.titles.insert(
                            (source, session),
                            truncate_chars(title, 80).0.into_boxed_str(),
                        );
                    }
                    b.count_doc(&text);
                    let key = (source, session);
                    let e = sess_stat.entry(key).or_insert((
                        0usize,
                        0i64,
                        role == ROLE_USER,
                        project_id,
                    ));
                    e.0 += 1;
                    if ts > e.1 {
                        e.1 = ts;
                    }
                    if role == ROLE_USER && !b.first_user.contains_key(&key) {
                        let preview = text.split_whitespace().collect::<Vec<_>>().join(" ");
                        b.first_user
                            .insert(key, truncate_chars(&preview, 160).0.into_boxed_str());
                    }
                    pass1_docs += 1;
                }
            }
        }

        // Session digests: one deterministic summary doc per session with
        // enough activity — indexed (so a session's own task words route to
        // it) but popped out of ranked results at query time. Derived here so
        // their terms are part of pass 1's sizing.
        let digest_inputs: Vec<((u8, u32), usize, i64, u32)> = sess_stat
            .iter()
            .filter(|(_, &(count, _, has_user, _))| count >= 4 && has_user)
            .map(|(&k, &(count, max_ts, _, project_id))| (k, count, max_ts, project_id))
            .collect();
        let mut digests: Vec<(i64, i64, u8, u32, u32, String)> = Vec::new();
        for ((source, session), count, max_ts, project_id) in digest_inputs {
            let Some(preview) = b.first_user.get(&(source, session)) else {
                continue;
            };
            let mut digest = String::from(preview.as_ref());
            digest.push_str(&format!("\n消息: {count}"));
            let doc_id = doc_id_of(
                b.source_names[source as usize].as_ref(),
                b.sessions[session as usize].as_ref(),
                "session-digest",
            );
            b.count_doc(&digest);
            digests.push((doc_id, max_ts, source, session, project_id, digest));
        }

        // ---- Bucket arena: one large allocation, one interval per term ----
        let total: u32 = b.counts.iter().map(|&c| c as u64).sum::<u64>() as u32;
        let mut offsets: Vec<u32> = Vec::with_capacity(b.counts.len() + 1);
        let mut acc = 0u32;
        for &c in &b.counts {
            offsets.push(acc);
            acc += c;
        }
        offsets.push(acc);
        let mut buckets: Vec<u32> = vec![0u32; total as usize];
        let mut cursors: Vec<u32> = offsets[..offsets.len() - 1].to_vec();

        if path.exists() {
            // ---- Pass 2: fill buckets, compress text ----
            let file = std::fs::File::open(path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
            let mut seen = 0usize;
            'p2: for batch in reader {
                let batch = batch?;
                let n = batch.num_rows();
                let ids = int_col(&batch, "doc_id");
                let sources = string_col(&batch, "source");
                let sids = string_col(&batch, "session_id");
                let roles = string_col(&batch, "role");
                let tss = int_col(&batch, "timestamp");
                let texts_col = string_col(&batch, "text");
                let projects_col = string_col(&batch, "project");
                let truncs = bool_col(&batch, "truncated");
                let (
                    Some(ids),
                    Some(sources),
                    Some(sids),
                    Some(roles),
                    Some(tss),
                    Some(texts_col),
                    Some(truncs),
                ) = (ids, sources, sids, roles, tss, texts_col, truncs)
                else {
                    continue;
                };
                for i in 0..n {
                    if seen >= pass1_docs {
                        break 'p2;
                    }
                    seen += 1;
                    let (text, _) = truncate_chars(texts_col.value(i), MAX_DOC_CHARS);
                    let source = b.source_ids[sources.value(i)];
                    let session = b.session_ids[sids.value(i)];
                    let project_id = b.project_ids[projects_col.map(|c| c.value(i)).unwrap_or("")];
                    let role = if roles.value(i) == "user" {
                        ROLE_USER
                    } else {
                        ROLE_ASSISTANT
                    };
                    b.push_doc(
                        ids.value(i),
                        tss.value(i),
                        source,
                        session,
                        project_id,
                        role,
                        truncs.value(i),
                        &text,
                        &mut buckets,
                        &mut cursors,
                    );
                }
            }
        }
        for (doc_id, max_ts, source, session, project_id, digest) in digests {
            b.push_doc(
                doc_id,
                max_ts,
                source,
                session,
                project_id,
                ROLE_DIGEST,
                false,
                &digest,
                &mut buckets,
                &mut cursors,
            );
        }

        let mut by_source: HashMap<String, usize> = HashMap::new();
        let mut by_project: HashMap<String, usize> = HashMap::new();
        let mut oldest: Option<i64> = None;
        let mut newest: Option<i64> = None;
        for d in &b.docs {
            if d.role == ROLE_DIGEST {
                continue;
            }
            *by_source
                .entry(b.source_names[d.source as usize].to_string())
                .or_default() += 1;
            if !b.projects[d.project as usize].is_empty() {
                *by_project
                    .entry(project_label(&b.projects[d.project as usize]))
                    .or_default() += 1;
            }
            if d.timestamp > 0 {
                oldest = Some(oldest.map_or(d.timestamp, |o| o.min(d.timestamp)));
                newest = Some(newest.map_or(d.timestamp, |n| n.max(d.timestamp)));
            }
        }

        // Sessions no source titled get their first user message as the
        // display name — the same anchor a person would scan for.
        let fallback_titles: Vec<((u8, u32), String)> = b
            .first_user
            .keys()
            .filter(|k| !b.titles.contains_key(k))
            .map(|k| (*k, truncate_chars(&b.first_user[k], 60).0))
            .collect();
        for (k, v) in fallback_titles {
            b.titles.insert(k, v.into_boxed_str());
        }

        Ok(b.finish(
            buckets,
            offsets,
            t0.elapsed().as_millis() as u64,
            truncated_build,
            oldest,
            newest,
            by_source,
            by_project,
        ))
    }

    pub fn stats(&self) -> &IndexStats {
        &self.stats
    }

    fn session_title(&self, meta: &DocMeta) -> String {
        self.titles
            .get(&(meta.source, meta.session))
            .map(|s| s.to_string())
            .unwrap_or_default()
    }

    /// Decode one posting list from the delta-varint arena.
    fn decode_postings(&self, span: Span) -> Vec<u32> {
        decode_deltas(&self.postings[span.off as usize..], span.len)
    }

    /// Decompress one doc's original-case text from the arena. A zstd frame
    /// per doc keeps this a single slice + call; if compression ever failed
    /// at build time the span holds the raw bytes, so decompression errors
    /// fall back to reading the span directly.
    fn doc_text(&self, idx: usize) -> String {
        let span = self.text_spans[idx];
        let raw = &self.text_arena[span.off as usize..(span.off + span.len) as usize];
        match zstd::bulk::decompress(raw, MAX_DOC_CHARS * 4) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(_) => String::from_utf8_lossy(raw).into_owned(),
        }
    }

    /// One session's messages around an anchor doc — "take me back to that
    /// conversation". This is the index view: deduped within the session
    /// and capped at `MAX_DOC_CHARS`, so a repeated message shows once and
    /// tails of huge messages stay cut; each message is further capped at
    /// `VIEW_MSG_CHARS` for transport. Widening `around` pages through the
    /// session from the frontend.
    /// Resolve one doc's `(source, session_id)`. `/api/docs` promises that a
    /// search hit's doc_id alone locates its conversation (issue #12 Bug 3);
    /// the HTTP handler uses this when the caller passes only doc_id.
    pub fn doc_session(&self, doc_id: i64) -> Option<(String, String)> {
        let i = self.docs.iter().position(|d| d.doc_id == doc_id)?;
        Some((
            self.source_names[self.docs[i].source as usize].to_string(),
            self.sessions[self.docs[i].session as usize].to_string(),
        ))
    }

    pub fn session_view(
        &self,
        source: &str,
        session_id: &str,
        anchor_doc_id: i64,
        around: usize,
    ) -> Option<SessionContext> {
        const VIEW_MSG_CHARS: usize = 1500;
        let around = around.clamp(1, 50);
        // Interned ids make the filter integer comparisons.
        let src_id = self
            .source_names
            .iter()
            .position(|s| s.as_ref() == source)? as u8;
        let sess_id = self
            .sessions
            .iter()
            .position(|s| s.as_ref() == session_id)? as u32;
        let mut idxs: Vec<usize> = (0..self.docs.len())
            .filter(|&i| {
                self.docs[i].source == src_id
                    && self.docs[i].session == sess_id
                    && self.docs[i].role != ROLE_DIGEST
            })
            .collect();
        if idxs.is_empty() {
            return None;
        }
        idxs.sort_unstable_by_key(|&i| (self.docs[i].timestamp, self.docs[i].doc_id));
        let anchor = idxs
            .iter()
            .position(|&i| self.docs[i].doc_id == anchor_doc_id)
            .unwrap_or(0);
        let start = anchor.saturating_sub(around);
        let end = (anchor + around + 1).min(idxs.len());
        let messages = idxs[start..end]
            .iter()
            .map(|&i| {
                let m = &self.docs[i];
                let (text, _) = truncate_chars(&self.doc_text(i), VIEW_MSG_CHARS);
                SessionMessage {
                    role: ROLE_NAMES[m.role as usize],
                    timestamp: m.timestamp,
                    text,
                    matched: m.doc_id == anchor_doc_id,
                }
            })
            .collect();
        Some(SessionContext {
            source: source.to_string(),
            session_id: session_id.to_string(),
            project: project_label(&self.projects[self.docs[idxs[anchor]].project as usize]),
            title: self.session_title(&self.docs[idxs[anchor]]),
            total: idxs.len(),
            anchor_pos: anchor,
            window_start: start,
            messages,
        })
    }

    /// BM25-style idf: 0 for absent terms, ~0 for corpus-wide ones.
    fn idf_of(&self, df: usize) -> f64 {
        if df == 0 {
            return 0.0;
        }
        let n = self.docs.len() as f64;
        let x = (n - df as f64 + 0.5) / (df as f64 + 0.5);
        (1.0 + x).ln()
    }

    /// Query terms with corpus document frequency resolved per term. These
    /// drive the pool stages, verification and highlighting; the split
    /// mirrors the index-side gram split.
    fn query_terms(&self, query: &str) -> Vec<QueryTerm> {
        let (tokens, bigrams) = grams_of(query);
        let mut terms: Vec<QueryTerm> = Vec::new();
        for t in tokens {
            if terms
                .iter()
                .any(|x| x.kind == TermKind::Ascii && x.text == t)
            {
                continue;
            }
            // ASCII df counts prefix matches, not one vocab entry, so a
            // "zcod" query knows how many docs its expansion really covers.
            let mut merged: Vec<u32> = Vec::new();
            if t.chars().count() >= PREFIX_MIN_CHARS {
                let end = format!("{t}\u{10FFFF}").into_boxed_str();
                for (_, span) in self.ascii.range(t.clone().into_boxed_str()..end) {
                    merged.extend(self.decode_postings(*span));
                }
                merged.sort_unstable();
                merged.dedup();
            } else if let Some(span) = self.ascii.get(t.as_str()) {
                merged.extend(self.decode_postings(*span));
            }
            let df = merged.len();
            terms.push(QueryTerm {
                idf: self.idf_of(df),
                text: t,
                kind: TermKind::Ascii,
                df,
                postings: merged,
            });
        }
        for g in bigrams {
            if terms
                .iter()
                .any(|x| x.kind == TermKind::Gram && x.text == g)
            {
                continue;
            }
            let df = self.cjk.get(g.as_str()).map_or(0, |s| s.len as usize);
            terms.push(QueryTerm {
                idf: self.idf_of(df),
                text: g,
                kind: TermKind::Gram,
                df,
                postings: Vec::new(),
            });
        }
        // A lone CJK char forms no bigram and matches no vocab entry, but its
        // docs are still findable by substring: emit it as a df-0 Gram so the
        // pool stage takes the verified full scan.
        if terms.is_empty() {
            let cjk_chars: Vec<char> = query.chars().filter(|c| is_cjk(*c)).collect();
            if cjk_chars.len() == 1 {
                terms.push(QueryTerm {
                    text: cjk_chars[0].to_string(),
                    kind: TermKind::Gram,
                    df: 0,
                    idf: 0.0,
                    postings: Vec::new(),
                });
            }
        }
        terms
    }

    /// Terms that actually route a query: seen in the corpus, and below the
    /// `HIGH_DF_RATIO` stopword line.
    fn content_terms<'a>(&self, terms: &'a [QueryTerm]) -> Vec<&'a QueryTerm> {
        let cutoff = HIGH_DF_RATIO * self.docs.len() as f64;
        let n_vocab = self.df_hist.len();
        terms
            .iter()
            .filter(|t| {
                if t.df == 0 || (t.df as f64) > cutoff {
                    return false;
                }
                // R2 relative guard: a term in the top 0.1% of the df
                // distribution is corpus glue even when the absolute line
                // hasn't caught up with growth (tool layer tripled N).
                if n_vocab > 0 && t.df > 64 {
                    let greater = n_vocab - self.df_hist.partition_point(|&d| (d as usize) <= t.df);
                    if (greater as f64) / (n_vocab as f64) <= 0.001 {
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    /// Terms verification scans and scores: content terms when there are
    /// any, else every term the index knows, else every term at all — a
    /// lone char that only ever lives inside longer indexed words («税»
    /// against 税务 docs) has df=0 but its docs are still findable by
    /// substring, which is exactly what verification is for.
    fn scoring_terms<'a>(&self, terms: &'a [QueryTerm]) -> Vec<&'a QueryTerm> {
        let content = self.content_terms(terms);
        if !content.is_empty() {
            return content;
        }
        let known: Vec<&QueryTerm> = terms.iter().filter(|t| t.df > 0).collect();
        if known.is_empty() {
            return terms.iter().collect();
        }
        known
    }

    /// Candidate docs and the tier that produced them:
    /// 1. hard AND over the rarest content terms (ASCII tokens) — at most
    ///    `POOL_TERMS_MAX` of them, so verbose queries stop demanding that
    ///    every word of a sentence co-occur;
    /// 2. hard AND over content character bigrams — the CJK tier, catching
    ///    fragments that span any word boundary;
    /// 3. ranked OR by summed idf across all terms — the vague/typo tier.
    ///
    /// A precise query (≤ `POOL_TERMS_MAX` content terms) keeps the hard
    /// ASCII miss: a token nothing contains empties the pool outright rather
    /// than letting soft CJK matches backfill noise. Verbose queries soften
    /// it — recall wins once the query is a sentence.
    fn candidate_pool(&self, terms: &[QueryTerm]) -> (Vec<u32>, u8, usize) {
        // A lone CJK char ("税" against docs that only ever say 税务) forms
        // no bigram: a verified full scan is the only honest answer.
        if terms.len() == 1 && terms[0].kind == TermKind::Gram && terms[0].text.chars().count() == 1
        {
            return ((0..self.docs.len() as u32).collect(), 1, 0);
        }

        let content = self.content_terms(terms);
        let verbose = content.len() > POOL_TERMS_MAX;
        if !verbose && terms.iter().any(|t| t.kind == TermKind::Ascii && t.df == 0) {
            return (Vec::new(), 0, 0);
        }

        let postings_of = |t: &QueryTerm| -> Vec<u32> {
            let span = match t.kind {
                TermKind::Ascii => return t.postings.clone(),
                TermKind::Gram => self.cjk.get(t.text.as_str()),
            };
            span.map(|s| self.decode_postings(*s)).unwrap_or_default()
        };

        // Tier 1: AND over the rarest content terms — ASCII tokens and CJK
        // bigrams alike, since both split identically on query and doc side.
        let mut drive: Vec<&QueryTerm> = content.to_vec();
        if drive.len() > POOL_TERMS_MAX {
            drive.sort_by_key(|t| t.df); // rarest first; stable for ties
            drive.truncate(POOL_TERMS_MAX);
        }
        if !drive.is_empty() {
            let mut per_term: Vec<Vec<u32>> = drive.iter().map(|t| postings_of(t)).collect();
            per_term.sort_by_key(|v| v.len());
            let mut acc = per_term.remove(0);
            for other in &per_term {
                if acc.is_empty() {
                    break;
                }
                acc = and_merge(&acc, other);
            }
            if !acc.is_empty() {
                // R4 soft-AND backfill: a tiny AND pool is brittle — one rare
                // term skewed by a corner of the corpus (evaluator sessions,
                // one noisy project) silently drops every doc missing just
                // that term. Union the AND with the idf-OR pool (already
                // best-first) so ranking, not the pool boundary, decides.
                let mut backfill = 0usize;
                if acc.len() < SOFT_AND_MIN_POOL {
                    let seen: HashSet<u32> = acc.iter().copied().collect();
                    // Collect non-AND candidates with their summed idf, keep
                    // the best SOFT_AND_BACKFILL_MAX — an unbounded tail made
                    // verify scan 8k docs and p95 hit 722ms (R4 first run).
                    let mut weights: HashMap<u32, f64> = HashMap::new();
                    for t in &content {
                        for &d in postings_of(t).iter() {
                            if !seen.contains(&d) {
                                *weights.entry(d).or_insert(0.0) += t.idf;
                            }
                        }
                    }
                    let mut tail: Vec<(u32, f64)> = weights.into_iter().collect();
                    tail.sort_by(|a, b| {
                        b.1.partial_cmp(&a.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then(a.0.cmp(&b.0))
                    });
                    tail.truncate(SOFT_AND_BACKFILL_MAX);
                    backfill = tail.len();
                    acc.extend(tail.into_iter().map(|(d, _)| d));
                }
                return (acc, 1, backfill);
            }
        }

        // Tier 3 (tier 2 stays reserved for the retired jieba word tier,
        // so historical search-log traces keep their meaning): sum idf over every term's postings, best first. Left
        // untruncated so the caller can record the true union size in the
        // trace before applying MAX_VERIFY_CANDIDATES.
        let bump: Vec<&QueryTerm> = if content.is_empty() {
            terms.iter().filter(|t| t.df > 0).collect()
        } else {
            content
        };
        let mut weights: HashMap<u32, f64> = HashMap::new();
        for t in bump {
            for &d in &postings_of(t) {
                *weights.entry(d).or_insert(0.0) += t.idf;
            }
        }
        let mut scored: Vec<(u32, f64)> = weights.into_iter().collect();
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        (scored.into_iter().map(|(d, _)| d).collect(), 3, 0)
    }

    pub fn search(&self, query: &str, filter: &SearchFilter) -> SearchResponse {
        self.search_with_clicks(query, filter, &HashMap::new())
    }

    pub fn search_with_clicks(
        &self,
        query: &str,
        filter: &SearchFilter,
        clicks: &HashMap<i64, u64>,
    ) -> SearchResponse {
        let t0 = Instant::now();
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return SearchResponse {
                query: String::new(),
                elapsed_ms: 0,
                candidates: 0,
                results: vec![],
                terms: vec![],
                session_headers: vec![],
                trace: SearchTrace::default(),
                project_matched: Vec::new(),
            };
        }

        // R21: pull field filters and exclusions out of the query text; what
        // remains is what the tokenizer sees. Syntax overrides URL params.
        let parsed_query = parse_query_syntax(trimmed);
        let merged = SearchFilter {
            source: parsed_query.source.as_deref().or(filter.source),
            role: parsed_query.role.as_deref().or(filter.role),
            project: parsed_query.project.as_deref().or(filter.project),
            session: parsed_query.session.as_deref().or(filter.session),
            since: parsed_query
                .days
                .map(|d| crate::now_ts() - d * 86_400)
                .or(filter.since),
            limit: filter.limit,
            exclude_sessions: filter.exclude_sessions,
        };
        let filter = &merged;
        let trimmed = if parsed_query.free_text.is_empty() {
            trimmed
        } else {
            &parsed_query.free_text
        };

        let t_terms = Instant::now();
        let terms = self.query_terms(trimmed);
        let terms_us = t_terms.elapsed().as_micros() as u64;

        let t_pool = Instant::now();
        let (pool, tier, backfill) = self.candidate_pool(&terms);
        let pool_from = pool.len();
        let mut pool = pool;
        if pool.len() > MAX_VERIFY_CANDIDATES {
            if tier == 3 {
                // Already idf-best-first; drop the tail.
                pool.truncate(MAX_VERIFY_CANDIDATES);
            } else {
                // Ascending doc order would keep the oldest; a pool this big
                // from an AND means every member matched everything asked,
                // so recency is the only tiebreaker left.
                pool.sort_by(|a, b| {
                    self.docs[*b as usize]
                        .timestamp
                        .cmp(&self.docs[*a as usize].timestamp)
                });
                pool.truncate(MAX_VERIFY_CANDIDATES);
            }
        }
        let pool_us = t_pool.elapsed().as_micros() as u64;

        let scoring = self.scoring_terms(&terms);
        let idf_sum: f64 = scoring.iter().map(|t| t.idf).sum();
        let normalized_query = normalize(trimmed);
        let now = crate::now_ts();
        let mut hits: Vec<SearchHit> = Vec::new();
        let mut hit_shingles: Vec<HashSet<u64>> = Vec::new();
        let mut candidates = 0usize;

        let t_verify = Instant::now();
        // R21: normalized `-word` needles; `contains(slice)` is a single
        // pass over the haystack for all of them.
        let exclusion_needles: Vec<String> = parsed_query
            .excluded
            .iter()
            .map(|w| normalize(w))
            .filter(|w| !w.is_empty())
            .collect();
        // Filters resolve to interned ids once, so the per-candidate checks
        // below are integer comparisons. A source absent from the interning
        // table matches zero docs — an early empty, not a dropped filter.
        // Case-insensitive so `source:ZCODE` in query syntax and the HTTP
        // param behave the same as the lowercase canonical form (issue #30
        // Bug 5).
        let src_filter = match filter.source {
            None => None,
            Some(s) => match self
                .source_names
                .iter()
                .position(|x| x.as_ref().eq_ignore_ascii_case(s))
            {
                Some(p) => Some(p),
                None => {
                    return SearchResponse {
                        query: trimmed.to_string(),
                        elapsed_ms: t0.elapsed().as_millis() as u64,
                        candidates: 0,
                        results: vec![],
                        terms: vec![],
                        session_headers: vec![],
                        trace: SearchTrace::default(),
                        project_matched: Vec::new(),
                    };
                }
            },
        };
        let role_filter = filter.role.map(|r| {
            if r == "user" {
                ROLE_USER
            } else {
                ROLE_ASSISTANT
            }
        });
        // issue #26:project 过滤接受尾段——`local-token-compute` 命中
        // `code/local-token-compute` 与 `code-local-token-compute` 两种键形
        // (munged 键不可逆,先按尾段归并,不动存储)。命中的键回给调用方,
        // 一个都没命中时 `project_matched` 为空 = 键拼错,不是「没聊过」。
        let project_filter: Option<(Vec<u32>, Vec<String>)> = filter.project.map(|p| {
            let mut ids: Vec<u32> = Vec::new();
            let mut labels: Vec<String> = Vec::new();
            for (i, raw) in self.projects.iter().enumerate() {
                let label = project_label(raw);
                if label == p
                    || label.ends_with(&format!("/{p}"))
                    || label.ends_with(&format!("-{p}"))
                {
                    ids.push(i as u32);
                    if !labels.contains(&label) {
                        labels.push(label);
                    }
                }
            }
            (ids, labels)
        });
        let excluded: Vec<u32> = filter
            .exclude_sessions
            .iter()
            .filter_map(|s| self.sessions.iter().position(|x| x.as_ref() == *s))
            .map(|p| p as u32)
            .collect();
        for &idx in &pool {
            let meta = &self.docs[idx as usize];
            if let Some(s) = src_filter {
                if meta.source as usize != s {
                    continue;
                }
            }
            if let Some(r) = role_filter {
                if meta.role != r {
                    continue;
                }
            }
            if let Some((ids, _)) = &project_filter {
                if !ids.contains(&meta.project) {
                    continue;
                }
            }
            if let Some(since) = filter.since {
                if meta.timestamp < since {
                    continue;
                }
            }
            if let Some(sess) = filter.session {
                // 前缀匹配:面板里复制半截 id 也能命中;空结果仍是诚实空。
                if !self.sessions[meta.session as usize].starts_with(sess) {
                    continue;
                }
            }
            if excluded.contains(&meta.session) {
                continue;
            }
            candidates += 1;

            // Text decompresses per candidate — one doc at a time instead of
            // the whole corpus twice (original + lowercased) at rest.
            let text = self.doc_text(idx as usize);
            let lower = text.to_lowercase();
            let mut matched_any = false;
            let mut matched_idf = 0.0f64;
            let mut total_occurrences = 0u32;
            let mut first_pos: Option<usize> = None;

            for t in &scoring {
                let mut occurrences = 0u32;
                let mut from = 0usize;
                while let Some(pos) = lower[from..].find(t.text.as_str()) {
                    let abs = from + pos;
                    occurrences += 1;
                    if first_pos.map_or(true, |p| abs < p) {
                        first_pos = Some(abs);
                    }
                    from = abs + t.text.len();
                    if occurrences >= 64 || from >= lower.len() {
                        break;
                    }
                }
                if occurrences > 0 {
                    matched_any = true;
                    matched_idf += t.idf;
                    total_occurrences = total_occurrences.saturating_add(occurrences);
                }
            }
            if !matched_any {
                continue;
            }
            // R21: `-word` exclusions — the text is already decompressed
            // here, so the check is free at this point.
            if exclusion_needles
                .iter()
                .any(|needle| lower.contains(needle.as_str()))
            {
                continue;
            }

            let coverage = matched_idf / idf_sum.max(1e-9);
            let kb = (meta.len_chars as f64 / 1024.0).clamp(0.25, 64.0);
            let density = ((total_occurrences as f64 / kb) / 6.0).min(1.0);
            let phrase = if !normalized_query.is_empty() && lower.contains(&normalized_query) {
                1.0
            } else {
                0.0
            };
            let role_w = match meta.role {
                ROLE_USER => 0.9,
                // Navigation aid: matched via the session's own task words.
                ROLE_DIGEST => 0.45,
                _ => 0.5,
            };
            let age_days = ((now - meta.timestamp).max(0) / 86_400) as f64;
            let recency = 0.5f64.powf(age_days / 30.0);

            // Phrase hits are the strongest single signal and must not be
            // diluted by coverage on long docs; density edges out coverage
            // there for the same reason.
            let mut score = 1.5 * coverage + 1.0 * density + 1.5 * phrase + role_w + 0.4 * recency;
            // O7 simplified: clicked docs come back. Log damping — the 50th
            // click on one doc must not make it unmovable.
            if let Some(c) = clicks.get(&meta.doc_id) {
                if *c > 0 {
                    score += CLICK_BETA * (*c as f64).ln_1p();
                }
            }

            let snippet = extract_snippet(&text, &lower, first_pos.unwrap_or(0));
            let shingles = shingles_of(&text);
            hits.push(SearchHit {
                doc_id_str: meta.doc_id.to_string(),
                source: self.source_names[meta.source as usize].to_string(),
                session_id: self.sessions[meta.session as usize].to_string(),
                role: ROLE_NAMES[meta.role as usize],
                timestamp: meta.timestamp,
                score,
                snippet,
                project: project_label(&self.projects[meta.project as usize]),
                title: self.session_title(meta),
                truncated: meta.truncated,
            });
            hit_shingles.push(shingles);
        }
        let verify_us = t_verify.elapsed().as_micros() as u64;
        let matched = hits.len();

        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // R3 (O8 simplified): digests pop out of the ranked list into
        // `session_headers`; a session whose digest matched the query gets a
        // flat boost on its content hits — the digest is evidence the whole
        // session is about what was asked, not just one doc.
        let mut digest_sessions: HashMap<String, f64> = HashMap::new();
        let mut session_headers: Vec<SearchHit> = Vec::new();
        hits.retain(|h| {
            if h.role == ROLE_NAMES[ROLE_DIGEST as usize] {
                digest_sessions.insert(h.session_id.clone(), h.score);
                session_headers.push(h.clone());
                false
            } else {
                true
            }
        });
        if !digest_sessions.is_empty() {
            for h in hits.iter_mut() {
                if digest_sessions.contains_key(&h.session_id) {
                    h.score += 0.25;
                }
            }
            hits.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        // Near-duplicate suppression + human-voice top-up. Agents quote the
        // human's words back at length, so assistant docs outrank and outnumber
        // the person's own messages and the result list reads as AI-only
        // echoes. Two passes fix that:
        //   1. fold: within one session drop assistant docs mostly contained
        //      in an already-kept doc; user docs fold only onto other user
        //      docs, at a stricter bar;
        //   2. top-up: any session whose survivors are all assistant hits
        //      gets its best-scoring user hit (the person's actual words)
        //      inserted right after the session's first survivor.
        let mut scored: Vec<Option<(SearchHit, HashSet<u64>)>> = hits
            .into_iter()
            .zip(hit_shingles)
            .map(|(h, s)| Some((h, s)))
            .collect();
        scored.sort_by(|a, b| {
            let (a, b) = (a.as_ref().unwrap(), b.as_ref().unwrap());
            b.0.score
                .partial_cmp(&a.0.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut kept: Vec<SearchHit> = Vec::with_capacity(scored.len());
        let mut kept_shingles: Vec<HashSet<u64>> = Vec::with_capacity(scored.len());
        let mut deduped = 0usize;
        for slot in scored.iter_mut() {
            let Some((hit, cand)) = slot.take() else {
                continue;
            };
            // Tool docs are never fold candidates: they are the origin of the
            // output, and a user/assistant message quoting it is the echo. The
            // echo's shingle set contains the synopsis's — without this rule
            // the R2 blind misses fold away behind their own quotes.
            if hit.role == "tool" {
                kept.push(hit);
                kept_shingles.push(cand);
                continue;
            }
            // Assistant docs fold aggressively: restatement and quote-heavy
            // answers are the noise. A person repeating themselves is worth
            // seeing, so user docs fold only onto other user docs, stricter.
            let containment_floor = if hit.role == "user" {
                0.7
            } else {
                DUPLICATE_CONTAINMENT
            };
            let dup_pos = if cand.len() >= MIN_SHINGLES_FOR_DUP {
                kept_shingles.iter().zip(kept.iter()).position(|(ks, k)| {
                    k.session_id == hit.session_id
                        && ks.intersection(&cand).count() as f64 / cand.len() as f64
                            >= containment_floor
                })
            } else {
                None
            };
            match dup_pos {
                Some(pos) => {
                    deduped += 1;
                    // The human's wording wins the slot: an assistant doc
                    // scoring above the message it restates is pure density
                    // (echo + lead-in), so swap the user hit into its place.
                    if hit.role == "user" && kept[pos].role != "user" {
                        kept[pos] = hit;
                        kept_shingles[pos] = cand;
                    }
                }
                None => {
                    kept.push(hit);
                    kept_shingles.push(cand);
                }
            }
        }

        // Top-up pass: a session whose survivors are all assistant hits gets
        // the human's own words back — the latest user doc at or before the
        // session's first kept hit, i.e. the message that likely triggered
        // that reply, whether or not it shared the query terms. Insertions
        // happen back-to-front so recorded positions stay valid.
        let mut injected: HashSet<String> = HashSet::new();
        let mut insertions: Vec<(usize, SearchHit)> = Vec::new();
        for pos in 0..kept.len() {
            let sid = kept[pos].session_id.clone();
            if injected.contains(&sid)
                || kept.iter().any(|k| k.session_id == sid && k.role == "user")
            {
                continue;
            }
            let anchor = &kept[pos];
            let (src, before_ts, base_score, doc_id) = (
                anchor.source.as_str(),
                anchor.timestamp,
                anchor.score,
                anchor.doc_id_str.clone(),
            );
            let src_id = self.source_names.iter().position(|x| x.as_ref() == src);
            let sess_id = self.sessions.iter().position(|x| x.as_ref() == sid);
            let (Some(src_id), Some(sess_id)) = (src_id, sess_id) else {
                continue;
            };
            let mut best: Option<usize> = None;
            for (i, d) in self.docs.iter().enumerate() {
                if d.source as usize == src_id
                    && d.session as usize == sess_id
                    && d.role == ROLE_USER
                    && d.timestamp <= before_ts
                    && best.map_or(true, |b| self.docs[b].timestamp <= d.timestamp)
                {
                    best = Some(i);
                }
            }
            let Some(ui) = best else { continue };
            let meta = &self.docs[ui];
            if meta.doc_id.to_string() == doc_id {
                continue;
            }
            let text = self.doc_text(ui);
            let lower = text.to_lowercase();
            injected.insert(sid);
            insertions.push((
                pos + 1,
                SearchHit {
                    doc_id_str: meta.doc_id.to_string(),
                    source: self.source_names[meta.source as usize].to_string(),
                    session_id: self.sessions[meta.session as usize].to_string(),
                    role: ROLE_NAMES[meta.role as usize],
                    timestamp: meta.timestamp,
                    score: base_score - 0.01,
                    snippet: extract_snippet(&text, &lower, 0),
                    project: project_label(&self.projects[meta.project as usize]),
                    title: self.session_title(meta),
                    truncated: meta.truncated,
                },
            ));
        }
        for (pos, hit) in insertions.into_iter().rev() {
            kept.insert(pos, hit);
        }
        hits = kept;
        hits.truncate(filter.limit.max(1));
        let returned = hits.len();

        session_headers.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        SearchResponse {
            query: trimmed.to_string(),
            elapsed_ms: t0.elapsed().as_millis() as u64,
            candidates,
            results: hits,
            session_headers,
            terms: scoring.iter().map(|t| t.text.clone()).collect(),
            trace: SearchTrace {
                tier,
                pool: pool.len(),
                pool_from,
                verified: candidates,
                matched,
                returned,
                deduped,
                terms_total: terms.len(),
                terms_content: self.content_terms(&terms).len(),
                backfill,
                terms_us,
                pool_us,
                verify_us,
                total_ms: t0.elapsed().as_millis() as u64,
            },
            project_matched: project_filter.map(|(_, l)| l).unwrap_or_default(),
        }
    }
}

fn string_col<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a arrow::array::StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>())
}

fn int_col<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Int64Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
}

fn bool_col<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a BooleanArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
}

/// ±window around the first hit, expanded from whitespace boundaries so words
/// are not cut in half.
fn extract_snippet(text: &str, lower: &str, pos: usize) -> String {
    const BEFORE: usize = 90;
    const AFTER: usize = 170;

    let clamped = pos.min(lower.len());
    let char_start = lower[..clamped].chars().count();
    let total_chars = text.chars().count();
    let start = char_start.saturating_sub(BEFORE);
    let end = (char_start + AFTER).min(total_chars);

    let mut snippet = String::new();
    if start > 0 {
        snippet.push('…');
    }
    snippet.push_str(
        text.chars()
            .skip(start)
            .take(end - start)
            .collect::<String>()
            .trim_start(),
    );
    if end < total_chars {
        snippet.push('…');
    }
    snippet
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionMessage {
    pub role: &'static str,
    pub timestamp: i64,
    pub text: String,
    /// True on the doc the search anchored to.
    pub matched: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionContext {
    pub source: String,
    pub session_id: String,
    pub project: String,
    /// Session display name (tool title or first user message).
    pub title: String,
    /// Messages in the whole (indexed, deduped) session.
    pub total: usize,
    /// 0-based position of the anchor message within the session.
    pub anchor_pos: usize,
    /// 0-based position of `messages[0]` within the session.
    pub window_start: usize,
    pub messages: Vec<SessionMessage>,
}

// ============================================================
// Query types + shared state for the server
// ============================================================

#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    /// Serialized as a string: i64 exceeds JavaScript's safe integer range,
    /// and this id round-trips back to `/api/context/session`.
    #[serde(rename = "doc_id")]
    pub doc_id_str: String,
    pub source: String,
    pub session_id: String,
    pub role: &'static str,
    pub timestamp: i64,
    pub score: f64,
    pub snippet: String,
    /// Project display label ("" when the source logs carry none).
    pub project: String,
    /// Session display name: the tool's title or the first user message.
    pub title: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub elapsed_ms: u64,
    pub candidates: usize,
    pub results: Vec<SearchHit>,
    /// Lowercased content terms the client should highlight in `snippet`.
    /// Stopword-tier terms (df past `HIGH_DF_RATIO`) are excluded, so a
    /// vague query no longer highlights every「的」in every snippet.
    pub terms: Vec<String>,
    /// R3: matched session digests, popped out of `results` — the dashboard
    /// renders these as session headers ("this whole session is about it"),
    /// never as result rows competing with content.
    #[serde(default)]
    pub session_headers: Vec<SearchHit>,
    /// Per-query efficiency trace, mirrored to the server log.
    pub trace: SearchTrace,
    /// issue #26:`project=` 过滤命中的实际键(尾段匹配可能同时命中多个
    /// 形状)。为空表示过滤条件没匹配到任何已知键——调用方据此区分
    /// 「这个工程没聊过」与「project 键拼错」。
    #[serde(default)]
    pub project_matched: Vec<String>,
}

/// What happened inside one search: which tier produced the candidate pool,
/// how large the pool was before and after the verify cap, and how long each
/// stage took. Logged for every query and returned in the response so the
/// dashboard shows it and tuning decisions have numbers behind them.
#[derive(Debug, Clone, Serialize, Default)]
pub struct SearchTrace {
    /// 0 = empty pool, 1 = term AND (or lone-char full scan), 2 = bigram
    /// AND, 3 = idf-weighted OR.
    pub tier: u8,
    /// Candidates surviving the verify cap.
    pub pool: usize,
    /// Pool size before the cap (tier-3 union can dwarf it).
    pub pool_from: usize,
    /// Candidates that passed the source/role/time filters.
    pub verified: usize,
    /// Verified docs where at least one scoring term actually matched.
    pub matched: usize,
    /// Same-session hits folded as near-duplicates (agent restating the
    /// human) after ranking.
    pub deduped: usize,
    /// R5: OR-backfill docs unioned into a soft-AND pool — the dial that
    /// bought +18pp recall at a latency cost, so it stays observable.
    #[serde(default)]
    pub backfill: usize,
    /// Hits returned after the limit.
    pub returned: usize,
    pub terms_total: usize,
    /// Terms below the stopword line — what actually drove the query.
    pub terms_content: usize,
    pub terms_us: u64,
    pub pool_us: u64,
    pub verify_us: u64,
    pub total_ms: u64,
}

/// R21 — query syntax, parsed once and merged into the filter the search
/// already speaks:
///
/// * `source:claude`, `project:foo`, `role:user`, `days:7` — field filters
///   (they override the URL parameters when both are present);
/// * `-word` — results containing the word (normalized substring) drop out;
/// * bare quotes on a free token are stripped; phrase matching itself rides
///   the existing verbatim-substring verification.
///
/// Unknown `key:value` tokens stay in the free text: a user searching for
/// e.g. a literal URL with a colon must not lose it to the parser.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ParsedQuery {
    pub free_text: String,
    pub source: Option<String>,
    pub project: Option<String>,
    pub role: Option<String>,
    pub days: Option<i64>,
    pub excluded: Vec<String>,
    /// R91 — `session:<id>` 前缀过滤:从收据/会话表拿到 id 后一步跳进对话。
    pub session: Option<String>,
}

pub fn parse_query_syntax(query: &str) -> ParsedQuery {
    let mut parsed = ParsedQuery::default();
    let mut free: Vec<&str> = Vec::new();
    for token in query.split_whitespace() {
        if let Some((key, value)) = token.split_once(':') {
            if !value.is_empty() {
                match key {
                    "source" => {
                        parsed.source = Some(value.to_string());
                        continue;
                    }
                    "project" => {
                        parsed.project = Some(value.to_string());
                        continue;
                    }
                    "session" => {
                        parsed.session = Some(value.to_string());
                        continue;
                    }
                    "role" => {
                        parsed.role = Some(value.to_string());
                        continue;
                    }
                    "days" => {
                        if let Ok(n) = value.parse::<i64>() {
                            if n > 0 {
                                parsed.days = Some(n);
                                continue;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        if let Some(rest) = token.strip_prefix('-') {
            if !rest.is_empty() && !rest.contains(':') {
                parsed.excluded.push(rest.to_string());
                continue;
            }
        }
        let bare = token.trim_matches('"');
        if !bare.is_empty() {
            free.push(bare);
        }
    }
    parsed.free_text = free.join(" ");
    parsed
}

#[derive(Default)]
pub struct SearchFilter<'a> {
    pub source: Option<&'a str>,
    pub role: Option<&'a str>,
    /// Project display label, as produced by `project_label` and listed in
    /// `IndexStats.by_project` (the dropdown values).
    pub project: Option<&'a str>,
    /// R91 — session-id prefix filter (receipts/insights rows jump here).
    pub session: Option<&'a str>,
    /// Inclusive lower bound on `timestamp` (epoch seconds); None = unbounded.
    pub since: Option<i64>,
    pub limit: usize,
    /// Sessions to drop from results (R3): the evaluator's own live session
    /// is part of the corpus and pollutes queries it is actively debugging
    /// with — pass it here to measure everyone else.
    pub exclude_sessions: &'a [&'a str],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Empty,
    Syncing,
    Building,
    Ready,
    Failed,
}

/// R5: the search-quality panel's numbers (see `ContextHandle::quality`).
#[derive(Debug, Clone, Serialize)]
pub struct BadQuery {
    pub ts: i64,
    pub query: String,
    pub returned: usize,
    pub tier: u8,
}

#[derive(Debug, Clone, Serialize)]
pub struct QualityStats {
    pub total: usize,
    pub zero_return: usize,
    pub tier3: usize,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub bad: Vec<BadQuery>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContextStatus {
    pub phase: Phase,
    pub detail: Option<String>,
    pub last_sync: Option<SyncStats>,
}

pub struct ContextHandle {
    parquet_path: PathBuf,
    /// `search-log.jsonl` next to the parquet: one line per query, so real
    /// traffic accumulates as raw material for regression cases and tuning.
    search_log_path: PathBuf,
    /// R5: doc_id → click count (O7). Survives rebuilds — clicks are user
    /// signal about docs, the index is just their current representation.
    clicks_path: PathBuf,
    clicks: RwLock<HashMap<i64, u64>>,
    inner: RwLock<Inner>,
    /// Serializes [`ContextHandle::sync_and_build`]. A run reads the stored
    /// doc ids once as its dedupe baseline, so two overlapping runs — the
    /// boot-time background build and a `/api/sync`-triggered refresh — would
    /// each import the whole corpus and double every row.
    build_lock: Mutex<()>,
}

struct Inner {
    index: Option<Arc<ContextIndex>>,
    status: ContextStatus,
    /// The corpus on disk has grown since this index was built. The index is
    /// still served — it is a subset, not a lie — but the next search rebuilds.
    stale: bool,
    /// Epoch seconds of the last search that actually touched the index. Drives
    /// the idle unload that hands the memory back.
    last_used: i64,
}

/// How long the search index may sit unused before it is dropped, in seconds.
///
/// Long enough that a person reading the dashboard all afternoon never notices
/// (and never pays to rebuild), short enough that closing the laptop lid
/// eventually returns the memory instead of pinning it until restart.
const IDLE_UNLOAD_SECS: i64 = 15 * 60;

/// Rotate the search log once it passes this size; the previous generation
/// moves to `.jsonl.1`, so a burst of queries never loses everything.
const SEARCH_LOG_MAX_BYTES: u64 = 5 * 1024 * 1024;

/// One `search-log.jsonl` line. The filters are the effective ones (post
/// parsing); `trace` is the same struct the API returns.
#[derive(Serialize)]
struct SearchLogEntry<'a> {
    ts: i64,
    query: &'a str,
    source: Option<&'a str>,
    role: Option<&'a str>,
    project: Option<&'a str>,
    since: Option<i64>,
    limit: usize,
    elapsed_ms: u64,
    returned: usize,
    trace: &'a SearchTrace,
}

/// Append one line to the JSONL search log, rotating the old file away when
/// it passes `rotate_over` bytes. Failures are the caller's to report; the
/// check-before-append race with concurrent searches only ever costs a
/// slightly larger file.
fn append_search_line(path: &Path, line: &str, rotate_over: u64) -> std::io::Result<()> {
    use std::io::Write as _;
    if let Ok(md) = std::fs::metadata(path) {
        if md.len() > rotate_over {
            let _ = std::fs::rename(path, path.with_extension("jsonl.1"));
        }
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(f, "{line}")
}

impl ContextHandle {
    pub fn new(parquet_path: PathBuf) -> Self {
        let search_log_path = parquet_path.with_file_name("search-log.jsonl");
        let clicks_path = parquet_path.with_file_name("clicks.json");
        let clicks = Self::load_clicks(&clicks_path);
        Self {
            parquet_path,
            search_log_path,
            clicks_path,
            clicks: RwLock::new(clicks),
            inner: RwLock::new(Inner {
                index: None,
                status: ContextStatus {
                    phase: Phase::Empty,
                    detail: None,
                    last_sync: None,
                },
                stale: false,
                last_used: 0,
            }),
            build_lock: Mutex::new(()),
        }
    }

    fn load_clicks(path: &Path) -> HashMap<i64, u64> {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str::<HashMap<String, u64>>(&s).ok())
            .map(|m| {
                m.into_iter()
                    .filter_map(|(k, v)| k.parse::<i64>().ok().map(|k| (k, v)))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// R5 nightly-harvest numbers, served for the dashboard's quality panel:
    /// the funnel over every logged query, plus the worst offenders. Reading
    /// the whole log is fine at the 5MB rotation cap.
    pub fn quality(&self) -> QualityStats {
        let mut entries: Vec<(i64, String, usize, u8, u64)> = Vec::new();
        if let Ok(text) = std::fs::read_to_string(&self.search_log_path) {
            for line in text.lines() {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                let ts = v.get("ts").and_then(|x| x.as_i64()).unwrap_or(0);
                let q = v
                    .get("query")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let returned = v.get("returned").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
                let tier = v
                    .get("trace")
                    .and_then(|t| t.get("tier"))
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0) as u8;
                let ms = v.get("elapsed_ms").and_then(|x| x.as_u64()).unwrap_or(0);
                entries.push((ts, q, returned, tier, ms));
            }
        }
        entries.sort_by_key(|e| e.0);
        let total = entries.len();
        let zero_return = entries.iter().filter(|e| e.2 == 0).count();
        let tier3 = entries.iter().filter(|e| e.3 == 3).count();
        let mut lats: Vec<u64> = entries.iter().map(|e| e.4).collect();
        lats.sort_unstable();
        let p = |f: f64| -> u64 {
            if lats.is_empty() {
                return 0;
            }
            lats[((lats.len() as f64 - 1.0) * f).round() as usize]
        };
        let bad: Vec<BadQuery> = entries
            .iter()
            .filter(|e| e.2 == 0 || e.3 == 3)
            .rev()
            .take(10)
            .map(|e| BadQuery {
                ts: e.0,
                query: e.1.clone(),
                returned: e.2,
                tier: e.3,
            })
            .collect();
        QualityStats {
            total,
            zero_return,
            tier3,
            p50_ms: p(0.5),
            p95_ms: p(0.95),
            bad,
        }
    }

    /// One user click on a result: bump its count in RAM and on disk. The
    /// write is tmp+rename like every other durable artifact here.
    pub fn record_click(&self, doc_id: i64) -> u64 {
        let mut clicks = self.clicks.write().unwrap_or_else(|e| e.into_inner());
        let c = clicks.entry(doc_id).or_insert(0);
        *c += 1;
        let n = *c;
        let snapshot: std::collections::BTreeMap<String, u64> =
            clicks.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        drop(clicks);
        let body = serde_json::to_string(&snapshot).unwrap_or_default();
        let tmp = self.clicks_path.with_extension("json.tmp");
        if std::fs::write(&tmp, body).is_ok() {
            let _ = std::fs::rename(&tmp, &self.clicks_path);
        }
        n
    }

    fn set_phase(&self, phase: Phase, detail: Option<String>) {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        inner.status.phase = phase;
        inner.status.detail = detail;
    }

    /// Record that the index was actually read, so the idle unload knows.
    fn mark_used(&self) {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        inner.last_used = crate::now_ts();
    }

    /// Is the loaded index safe to serve without rebuilding?
    fn index_usable(&self) -> bool {
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        inner.index.is_some() && !inner.stale
    }

    /// Flag the in-memory index as possibly behind the corpus (R104): the
    /// caller just synced, and source logs may hold conversations the index
    /// has never seen. The next warm rebuilds; a build that imports nothing
    /// keeps the current index and clears the flag.
    pub fn mark_stale(&self) {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if inner.index.is_some() {
            inner.stale = true;
        }
    }

    /// Guarantee a usable index, building it if this is the first search of
    /// the session (or the corpus moved on since the last build). Blocking on
    /// purpose: the caller asked to search, and a few seconds of build is a far
    /// better answer than an empty result set.
    pub fn ensure_index(&self) -> Result<()> {
        if self.index_usable() {
            self.mark_used();
            return Ok(());
        }
        // `sync_and_build` takes the build lock itself and re-checks staleness
        // under it, so two racing first searches cost one build, not two.
        self.sync_and_build(false)?;
        self.mark_used();
        Ok(())
    }

    /// Start a build in the background if one is needed, and return at once.
    ///
    /// Called when the user opens the search view: by the time they finish
    /// typing a query the index is usually already there, and a user who only
    /// ever looks at statistics never triggers it at all.
    pub fn warm_if_needed(self: &Arc<Self>) {
        if self.index_usable() {
            return;
        }
        let me = Arc::clone(self);
        std::thread::spawn(move || {
            if let Err(e) = me.ensure_index() {
                eprintln!("[TokenBuddy] context warm-up failed: {e}");
            }
        });
    }

    /// Hand the index memory back if nobody has searched for a while.
    ///
    /// Driven by a timer in `main`, so an idle dashboard returns to its
    /// statistics-only footprint without the user having to restart anything.
    /// The parquet is untouched: the next search rebuilds from it.
    pub fn unload_if_idle(&self) -> bool {
        self.unload_if_idle_after(IDLE_UNLOAD_SECS)
    }

    /// [`Self::unload_if_idle`] with the threshold supplied, so the lifecycle
    /// can be tested without waiting out a real idle window.
    pub fn unload_if_idle_after(&self, idle_secs: i64) -> bool {
        let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let Some(_) = inner.index.as_ref() else {
            return false;
        };
        if inner.last_used == 0 || crate::now_ts() - inner.last_used < idle_secs {
            return false;
        }
        if matches!(inner.status.phase, Phase::Syncing | Phase::Building) {
            return false;
        }
        let docs = inner
            .index
            .as_ref()
            .map(|i| i.stats().docs)
            .unwrap_or_default();
        inner.index = None;
        inner.stale = false;
        inner.status.phase = Phase::Empty;
        inner.status.detail = None;
        eprintln!(
            "[TokenBuddy] context index idle for {IDLE_UNLOAD_SECS}s, unloaded ({docs} docs)"
        );
        true
    }

    /// Incremental collect+dedupe of conversations, then rebuild the index
    /// and swap it in. `clear` drops the stored parquet first (full rebuild).
    pub fn sync_and_build(&self, clear: bool) -> Result<SyncStats> {
        // Held for the whole run so overlapping refreshes queue up instead of
        // each importing the corpus against the same stale dedupe baseline.
        let _build_guard = self.build_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.set_phase(Phase::Syncing, None);
        if clear {
            let _ = std::fs::remove_file(&self.parquet_path);
        }
        // One-time cleanup: the tool-output store this path fed was removed
        // from the index (it tripled index memory for content the search
        // already finds in the conversation docs); drop the derived file too.
        let _ = std::fs::remove_file(self.parquet_path.with_file_name("tools.parquet"));
        let stats = match sync_context(&self.parquet_path, false) {
            Ok(s) => s,
            Err(e) => {
                self.set_phase(Phase::Failed, Some(e.to_string()));
                return Err(e);
            }
        };
        // Nothing new was collected, so the index on disk already describes the
        // corpus exactly. Rebuilding it would be a full two-pass pass over every
        // turn to produce the same bytes — and the build is the one phase whose
        // transient allocations set the process high-water mark, which the OS
        // then keeps. Skipping it is what stops a run of syncs (a dashboard
        // refresh button is easy to hold down) from walking the footprint up.
        if !clear && stats.imported == 0 && self.index_is_loaded() {
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
            inner.status.last_sync = Some(stats.clone());
            inner.status.phase = Phase::Ready;
            inner.status.detail = None;
            inner.stale = false;
            return Ok(stats);
        }
        self.set_phase(Phase::Building, None);
        {
            // Drop the old index before building the replacement: the OS does
            // not return freed small allocations, so peak — not steady-state —
            // is what sets the process footprint. Search reports the building
            // phase for the few seconds this takes.
            let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
            inner.index = None;
        }
        match ContextIndex::build(&self.parquet_path) {
            Ok(index) => {
                let mut inner = self.inner.write().unwrap_or_else(|e| e.into_inner());
                inner.index = Some(Arc::new(index));
                inner.status.last_sync = Some(stats.clone());
                inner.status.phase = Phase::Ready;
                inner.status.detail = None;
                inner.stale = false;
                inner.last_used = crate::now_ts();
                Ok(stats)
            }
            Err(e) => {
                self.set_phase(Phase::Failed, Some(e.to_string()));
                Err(e)
            }
        }
    }

    pub fn status(&self) -> ContextStatus {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .status
            .clone()
    }

    /// Whether a searchable index is currently loaded. An index that failed to
    /// build is not "loaded" even though the corpus is unchanged, so the
    /// no-op-sync shortcut must not strand the user with an empty search box.
    fn index_is_loaded(&self) -> bool {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .index
            .is_some()
    }

    pub fn index_stats(&self) -> Option<IndexStats> {
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        inner.index.as_ref().map(|i| i.stats().clone())
    }

    /// Resolve one doc's `(source, session_id)` so /api/context/session can
    /// take a bare doc_id from a search hit (issue #12 Bug 3).
    pub fn doc_session(&self, doc_id: i64) -> Result<Option<(String, String)>> {
        // A doc lookup presupposes a built index, same as session_view.
        self.ensure_index()?;
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let Some(index) = &inner.index else {
            anyhow::bail!("上下文索引尚未就绪（{}）", inner.status.phase_desc());
        };
        Ok(index.doc_session(doc_id))
    }

    /// Session messages around one hit, for the dashboard's "view in
    /// conversation" expansion. `Ok(None)` = the caller named a session the
    /// index doesn't have (a client mistake → the handler answers 400);
    /// `Err` = the index itself isn't ready (a server state → 500).
    pub fn session_view(
        &self,
        source: &str,
        session_id: &str,
        anchor_doc_id: i64,
        around: usize,
    ) -> Result<Option<SessionContext>> {
        // Expanding a hit is a search action: the index has to be there.
        self.ensure_index()?;
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let Some(index) = &inner.index else {
            anyhow::bail!("上下文索引尚未就绪（{}）", inner.status.phase_desc());
        };
        Ok(index.session_view(source, session_id, anchor_doc_id, around))
    }

    pub fn search(&self, query: &str, filter: &SearchFilter) -> Result<SearchResponse> {
        // First search of the session pays for the build; after that this is a
        // couple of atomic reads on an already-usable index.
        self.ensure_index()?;
        let inner = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let Some(index) = &inner.index else {
            anyhow::bail!("上下文索引尚未就绪（{}）", inner.status.phase_desc());
        };
        let clicks = self
            .clicks
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let resp = index.search_with_clicks(query, filter, &clicks);
        self.log_search(query, filter, &resp);
        // One compact line per query: tier, pool funnel and stage timings —
        // the numbers that tell whether the next tuning move is pool-side
        // (tier 3 with a huge pool) or verify-side (verify µs >> pool µs).
        let t = &resp.trace;
        let q: String = query.split_whitespace().collect::<Vec<_>>().join(" ");
        let q: String = q.chars().take(60).collect();
        eprintln!(
            "[TokenBuddy] ctx-search q={q:?} tier={} terms={}/{} pool={}/{} verified={} matched={} dedup={} ret={} {}ms [terms {}µs pool {}µs verify {}µs]",
            t.tier,
            t.terms_content,
            t.terms_total,
            t.pool,
            t.pool_from,
            t.verified,
            t.matched,
            t.deduped,
            t.returned,
            resp.elapsed_ms,
            t.terms_us,
            t.pool_us,
            t.verify_us
        );
        Ok(resp)
    }

    /// Durable twin of the stderr line: one JSON line per query in
    /// `search-log.jsonl`. Real queries pile up there as the corpus for
    /// future regression cases and "which tiers are slow" analysis; jq the
    /// file and the queries people actually typed are all there.
    fn log_search(&self, query: &str, filter: &SearchFilter, resp: &SearchResponse) {
        let entry = SearchLogEntry {
            ts: crate::now_ts(),
            query,
            source: filter.source,
            role: filter.role,
            project: filter.project,
            since: filter.since,
            limit: filter.limit,
            elapsed_ms: resp.elapsed_ms,
            returned: resp.results.len(),
            trace: &resp.trace,
        };
        let Ok(line) = serde_json::to_string(&entry) else {
            return;
        };
        if let Err(e) = append_search_line(&self.search_log_path, &line, SEARCH_LOG_MAX_BYTES) {
            eprintln!("[TokenBuddy] ctx-search log write failed: {e}");
        }
    }
}

impl ContextStatus {
    fn phase_desc(&self) -> String {
        String::from(match &self.phase {
            Phase::Empty => "尚未构建",
            Phase::Syncing => "正在采集",
            Phase::Building => "正在建索引",
            Phase::Ready => "就绪",
            Phase::Failed => "失败",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// R102: with every source switched off, a context sync must collect
    /// nothing — even on a machine whose agent logs are really there (this
    /// is the point of the switch: the index build is the heaviest walk).
    #[test]
    fn context_sync_with_all_sources_disabled_collects_nothing() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("tb_ctx_all_off");
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("TOKENBUDDY_HOME", &dir);
        let cfg = crate::quota::QuotaConfig {
            alert: None,
            collectors: vec![],
            disabled_sources: crate::SOURCE_NAMES.iter().map(|s| s.to_string()).collect(),
        };
        crate::quota::save_config(&cfg).unwrap();

        let path = dir.join("context.parquet");
        let stats = sync_context(&path, true).unwrap();
        assert_eq!(
            stats.collected, 0,
            "gated drains must feed the sink nothing"
        );

        std::env::remove_var("TOKENBUDDY_HOME");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn normalize_collapses_whitespace_and_case() {
        assert_eq!(
            normalize("  Hello   WORLD \n\t again "),
            "hello world again"
        );
        assert_eq!(normalize("上下文  搜索"), "上下文 搜索");
    }

    #[test]
    fn doc_id_is_stable_and_session_scoped() {
        let a = doc_id_of("claude", "s1", "hello world");
        let b = doc_id_of("claude", "s1", "hello world");
        let c = doc_id_of("claude", "s2", "hello world");
        let d = doc_id_of("zcode", "s1", "hello world");
        assert_eq!(a, b);
        assert_ne!(a, c, "same text in another session is its own doc");
        assert_ne!(a, d, "same text from another source is its own doc");
    }

    #[test]
    fn grams_handle_mixed_script() {
        let (tokens, bigrams) = grams_of("修复 Context 搜索bug");
        assert_eq!(tokens, vec!["context", "bug"]);
        assert_eq!(bigrams, vec!["修复", "搜索"]);
    }

    #[test]
    fn grams_cut_cjk_into_bigrams() {
        let (_, bigrams) = grams_of("重新安装依赖");
        assert_eq!(bigrams, vec!["重新", "新安", "安装", "装依", "依赖"]);
    }

    #[test]
    fn grams_keep_snake_case_whole_and_lowercase() {
        let (tokens, bigrams) = grams_of("read_existing_doc_ids 上下文");
        assert_eq!(tokens, vec!["read_existing_doc_ids"]);
        assert_eq!(bigrams, vec!["上下", "下文"]);
    }

    #[test]
    fn grams_of_lone_cjk_char_yields_nothing() {
        let (tokens, bigrams) = grams_of("税");
        assert!(tokens.is_empty() && bigrams.is_empty());
    }

    #[test]
    fn index_build_and_search_roundtrip() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("tokenbuddy_ctx_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("context.parquet");

        let msgs = vec![
            ContextMessage {
                source: Source::Claude,
                session_id: "s1".into(),
                role: "user",
                timestamp: 1_700_000_000,
                text: "帮我看看 parquet 写入为什么慢".into(),
                project: String::new(),
                title: String::new(),
            },
            ContextMessage {
                source: Source::Claude,
                session_id: "s1".into(),
                role: "assistant",
                timestamp: 1_700_000_100,
                text: "parquet 写入慢的原因是每行都 flush，改成批量写就好了".into(),
                project: String::new(),
                title: String::new(),
            },
            ContextMessage {
                source: Source::Zcode,
                session_id: "s2".into(),
                role: "user",
                timestamp: 1_700_100_000,
                text: "帮我看看 parquet 写入为什么慢".into(),
                project: String::new(),
                title: String::new(),
            },
            // Same text twice in one session → one doc.
            ContextMessage {
                source: Source::Zcode,
                session_id: "s2".into(),
                role: "assistant",
                timestamp: 1_700_100_200,
                text: "帮我看看 parquet 写入为什么慢".into(),
                project: String::new(),
                title: String::new(),
            },
        ];
        let mut existing = HashSet::new();
        let mut docs = Vec::new();
        for m in msgs {
            let text = m.text.clone();
            let (text, truncated) = truncate_chars(&text, MAX_DOC_CHARS);
            let id = doc_id_of(m.source.as_str(), &m.session_id, &normalize(&text));
            if existing.insert(id) {
                docs.push((id, ContextMessage { text, ..m }, truncated));
            }
        }
        write_context_parquet(&path, &docs_to_batch(&docs))?;

        let index = ContextIndex::build(&path)?;
        assert_eq!(index.stats().docs, 3, "in-session duplicate collapsed");

        let resp = index.search(
            "parquet 写入",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(resp.results.len(), 3, "every doc mentions both terms");
        assert!(resp
            .results
            .iter()
            .all(|h| h.snippet.contains("parquet") || h.snippet.contains("写入")));
        // The exact phrase is present in three docs → phrase bonus lifts the
        // best match to the top and keeps the order stable.
        assert!(resp.results[0].score >= resp.results.last().unwrap().score);
        assert!((1..=3).contains(&resp.trace.tier));
        assert!(resp.trace.pool >= resp.trace.returned);

        // AND semantics: an ASCII term nothing contains empties the pool even
        // though a CJK gram would soft-match.
        let resp = index.search(
            "写入 nonexistentword",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert!(resp.results.is_empty());

        // role filter
        let resp = index.search(
            "parquet",
            &SearchFilter {
                role: Some("user"),
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(resp.results.len(), 2);
        assert!(resp.results.iter().all(|h| h.role == "user"));

        // source filter
        let resp = index.search(
            "parquet",
            &SearchFilter {
                source: Some("zcode"),
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].source, "zcode");

        // source filter — case-insensitive (issue #30 Bug 5): `ZCODE` must
        // see the same corpus as `zcode`, not a silently empty one.
        let resp = index.search(
            "parquet",
            &SearchFilter {
                source: Some("ZCODE"),
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(resp.results.len(), 1, "uppercase source still matches");
        assert_eq!(resp.results[0].source, "zcode");

        // single CJK char query falls back to the full-scan pool
        let resp = index.search(
            "税",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert!(resp.results.is_empty(), "char absent from corpus");

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[test]
    fn bigram_tier_catches_fragments_that_words_miss() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("tokenbuddy_ctx_frag_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("context.parquet");

        let docs = vec![(
            1i64,
            ContextMessage {
                source: Source::Zcode,
                session_id: "s1".into(),
                role: "user",
                timestamp: 1_700_000_000,
                text: "上下文搜索的目标是不遗漏".into(),
                project: String::new(),
                title: String::new(),
            },
            false,
        )];
        write_context_parquet(&path, &docs_to_batch(&docs))?;
        let index = ContextIndex::build(&path)?;

        // "下文搜" spans the 上下文|搜索 word boundary: no doc-side jieba
        // word equals it, so the word tier must soft-miss and the bigram
        // tier (下文 + 文搜) has to carry the recall.
        let resp = index.search(
            "下文搜",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(resp.results.len(), 1, "cross-boundary fragment must hit");
        assert!(resp.results[0].snippet.contains("上下文搜索"));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[test]
    fn stopwords_stay_out_of_pools_and_highlights() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("tokenbuddy_ctx_stop_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("context.parquet");

        // 10 docs: 「的」 in 60% of them — past the HIGH_DF_RATIO line —
        // 权限 in two. A verbose sentence query must surface 权限 docs, not
        // the 「的」 majority, and must not hand 「的」 to the highlighter.
        let mut docs = Vec::new();
        let mut id = 0i64;
        let mut add = |docs: &mut Vec<(i64, ContextMessage, bool)>, text: &str| {
            id += 1;
            docs.push((
                id,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: format!("s{id}"),
                    role: if id % 2 == 0 { "assistant" } else { "user" },
                    timestamp: 1_700_000_000 + id * 1000,
                    text: text.into(),
                    project: String::new(),
                    title: String::new(),
                },
                false,
            ));
        };
        for i in 0..6 {
            add(&mut docs, &format!("这是第{i}批无关的记录，大家看一下"));
        }
        add(&mut docs, "权限配置在哪里改");
        add(&mut docs, "还是权限的问题，权限不够用");
        add(&mut docs, "今天天气不错");
        add(&mut docs, "明天继续干活");
        write_context_parquet(&path, &docs_to_batch(&docs))?;
        let index = ContextIndex::build(&path)?;

        let resp = index.search(
            "上次讨论的权限问题怎么解决的",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert!(!resp.results.is_empty(), "content terms must still hit");
        assert!(
            resp.results.iter().all(|h| h.snippet.contains("权限")),
            "only 权限 docs may rank, got: {:?}",
            resp.results
                .iter()
                .map(|h| h.snippet.clone())
                .collect::<Vec<_>>()
        );
        assert!(
            !resp.terms.contains(&"的".to_string()),
            "stopword leaked into highlight terms"
        );
        assert!(resp.trace.terms_content >= 1);

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[test]
    fn verbose_query_pools_on_rarest_terms_only() -> Result<()> {
        let dir =
            std::env::temp_dir().join(format!("tokenbuddy_ctx_verbose_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("context.parquet");

        // 19 docs. The query has four content terms (失败 df=1, 讨论 df=2,
        // 权限 df=3, 部署 df=4 — all under the 30% line), which makes it
        // verbose: the pool ANDs only the rarest three. docA carries
        // 失败/讨论/权限 but not 部署, so a full AND would find nothing.
        let mut docs = Vec::new();
        let mut id = 0i64;
        let mut add = |docs: &mut Vec<(i64, ContextMessage, bool)>, text: &str| {
            id += 1;
            docs.push((
                id,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: format!("s{id}"),
                    role: "user",
                    timestamp: 1_700_000_000 + id * 1000,
                    text: text.into(),
                    project: String::new(),
                    title: String::new(),
                },
                false,
            ));
        };
        add(&mut docs, "线上又失败了，先讨论，权限怎么给");
        add(&mut docs, "明天再讨论这个方案");
        add(&mut docs, "权限不足的报错");
        add(&mut docs, "权限校验没过");
        add(&mut docs, "部署脚本跑通了");
        add(&mut docs, "部署到线上去");
        add(&mut docs, "部署手册要更新");
        add(&mut docs, "部署完成了");
        for i in 0..11 {
            add(&mut docs, &format!("第{i}天天气不错适合干活"));
        }
        write_context_parquet(&path, &docs_to_batch(&docs))?;
        let index = ContextIndex::build(&path)?;

        let resp = index.search(
            "权限 部署 讨论 失败",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(resp.trace.tier, 1, "rarest-3 AND should carry the pool");
        assert!(
            !resp.results.is_empty(),
            "docA must be findable without 部署"
        );
        assert!(resp.results[0].snippet.contains("失败"));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[test]
    fn snippet_window_stays_on_char_boundaries() {
        // 100 leading chars guarantee the window opens before the hit, so the
        // leading ellipsis is expected.
        let text = "x".repeat(100) + "上下文搜索的目标行在这里" + &"y".repeat(300);
        let lower = text.to_lowercase();
        let pos = lower.find("上下文").unwrap();
        let snippet = extract_snippet(&text, &lower, pos);
        assert!(snippet.contains("上下文搜索"));
        assert!(snippet.starts_with('…'));
        assert!(snippet.ends_with('…'));
        // A hit near the head of a short doc gets no ellipses at all.
        let snippet = extract_snippet("短的上下文搜索", "短的上下文搜索", 3);
        assert_eq!(snippet, "短的上下文搜索");
    }

    #[test]
    fn prefix_query_matches_partial_tokens() -> Result<()> {
        let dir =
            std::env::temp_dir().join(format!("tokenbuddy_ctx_prefix_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("context.parquet");

        let docs = vec![(
            1i64,
            ContextMessage {
                source: Source::Zcode,
                session_id: "s1".into(),
                role: "assistant",
                timestamp: 1_700_000_000,
                text: "the zcode collector reads model_usage".into(),
                project: String::new(),
                title: String::new(),
            },
            false,
        )];
        write_context_parquet(&path, &docs_to_batch(&docs))?;
        let index = ContextIndex::build(&path)?;

        let resp = index.search(
            "zcod",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(resp.results.len(), 1, "prefix of a 5-char token matches");
        let resp = index.search(
            "zt",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert!(resp.results.is_empty(), "short tokens match exactly only");

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }
}

/// Regression suite for search behavior: each test pins one observable
/// promise of the tier/pool/scoring pipeline on a tiny synthetic corpus, so
/// future tuning (weights, thresholds, tier order) that silently changes
/// behavior fails here instead of in front of a user. The `search-log.jsonl`
/// entries real traffic accumulates are the raw material for growing this
/// suite — a query that misbehaves in the log becomes a fixture here.
#[cfg(test)]
mod search_regression {
    use super::*;

    /// Build an index from `(timestamp_seconds, text)` docs. Source is
    /// Zcode, roles alternate user/assistant by position, sessions are
    /// unique — nothing dedupes, every row is a doc. Returns the index and
    /// its temp dir (caller removes it).
    fn reg_index(name: &str, docs: &[(i64, &str)]) -> (ContextIndex, PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("tokenbuddy_ctx_reg_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("context.parquet");
        let rows = docs
            .iter()
            .enumerate()
            .map(|(i, (ts, text))| {
                (
                    (i + 1) as i64,
                    ContextMessage {
                        source: Source::Zcode,
                        session_id: format!("s{i}"),
                        role: if i % 2 == 0 { "user" } else { "assistant" },
                        timestamp: *ts,
                        text: (*text).into(),
                        project: String::new(),
                        title: String::new(),
                    },
                    false,
                )
            })
            .collect::<Vec<_>>();
        write_context_parquet(&path, &docs_to_batch(&rows)).unwrap();
        (ContextIndex::build(&path).unwrap(), dir)
    }

    /// R104: mark_stale flags a loaded index for rebuild; a rebuild that
    /// imports nothing keeps the current index and clears the flag.
    #[test]
    fn mark_stale_flags_loaded_index_and_rebuild_clears_it() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = crate::unique_test_dir("ctx_stale");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("context.parquet");
        let rows = (0i64..2)
            .map(|i| {
                (
                    i + 1,
                    ContextMessage {
                        source: Source::Zcode,
                        session_id: "s".into(),
                        role: "user",
                        timestamp: 1_700_000_000 + i,
                        text: format!("陈旧标记测试 {i}"),
                        project: String::new(),
                        title: String::new(),
                    },
                    false,
                )
            })
            .collect::<Vec<_>>();
        write_context_parquet(&path, &docs_to_batch(&rows)).unwrap();

        let handle = ContextHandle::new(path.clone());
        // No index yet: mark_stale is a no-op (the warm builds from scratch
        // anyway, and a bare flag must not make search block for nothing).
        handle.mark_stale();
        assert!(handle.index_stats().is_none());

        // Load an index directly — same object the build path would store.
        let index = ContextIndex::build(&path).unwrap();
        {
            let mut inner = handle.inner.write().unwrap_or_else(|e| e.into_inner());
            inner.index = Some(std::sync::Arc::new(index));
        }
        assert!(handle.search("陈旧标记", &SearchFilter::default()).is_ok());

        // A sync happened: the loaded index is now flagged, not served.
        handle.mark_stale();
        assert!(
            handle.index_stats().is_some(),
            "flag must not drop the index"
        );

        // The warm path (ensure_index) rebuilds; importing nothing new keeps
        // the corpus bytes and hands back a usable index.
        handle.ensure_index().unwrap();
        assert!(!handle
            .search("陈旧标记", &SearchFilter::default())
            .unwrap()
            .results
            .is_empty());

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Lazy building only pays off if the index really does leave. Pin the
    /// whole lifecycle: build → not idle → unload → corpus intact → rebuild
    /// on demand. A regression that keeps the index resident is exactly the
    /// memory growth this design exists to prevent.
    #[test]
    fn index_is_built_on_demand_and_unloaded_when_idle() {
        let dir =
            std::env::temp_dir().join(format!("tokenbuddy_ctx_lifecycle_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("context.parquet");

        let handle = ContextHandle::new(path.clone());
        // Nothing is built until something asks — a reader of statistics never
        // pays for the index at all.
        assert!(handle.index_stats().is_none(), "index must not exist yet");
        assert!(!handle.unload_if_idle_after(0), "nothing to unload");

        let rows = (0..4)
            .map(|i| {
                (
                    (i + 1) as i64,
                    ContextMessage {
                        source: Source::Zcode,
                        session_id: format!("s{i}"),
                        role: if i % 2 == 0 { "user" } else { "assistant" },
                        timestamp: 1_700_000_000 + i as i64,
                        text: format!("生命周期测试第 {i} 条"),
                        project: String::new(),
                        title: String::new(),
                    },
                    false,
                )
            })
            .collect::<Vec<_>>();
        write_context_parquet(&path, &docs_to_batch(&rows)).unwrap();

        // The corpus exists on disk but is still not loaded.
        assert!(handle.index_stats().is_none());

        // The first search is what builds it.
        handle.ensure_index().unwrap();
        assert!(handle.index_stats().is_some());
        assert!(!handle
            .search("生命周期", &SearchFilter::default())
            .unwrap()
            .results
            .is_empty());

        // Freshly used: the idle timer must not fire.
        assert!(!handle.unload_if_idle_after(IDLE_UNLOAD_SECS));

        // Idle past the threshold: the heap goes back, the parquet does not.
        assert!(handle.unload_if_idle_after(0));
        assert!(handle.index_stats().is_none());
        assert!(path.exists(), "the corpus must survive the unload");

        // And the next search simply rebuilds it.
        assert!(!handle
            .search("生命周期", &SearchFilter::default())
            .unwrap()
            .results
            .is_empty());
        assert!(handle.index_stats().is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn search(index: &ContextIndex, q: &str) -> SearchResponse {
        index.search(
            q,
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        )
    }

    #[test]
    fn query_syntax_parse_extracts_fields_exclusions_and_free_text() {
        let p = parse_query_syntax("source:claude days:7 -崩溃 panic 発生 \"exact phrase\"");
        assert_eq!(p.source.as_deref(), Some("claude"));
        assert_eq!(p.days, Some(7));
        assert_eq!(p.excluded, vec!["崩溃".to_string()]);
        assert_eq!(p.free_text, "panic 発生 exact phrase");
        assert_eq!(p.project, None);

        // Unknown key:value stays free text — a URL must not lose its colon.
        let p = parse_query_syntax("https://example.com/a:1");
        assert_eq!(p.source, None);
        assert_eq!(p.free_text, "https://example.com/a:1");

        // Invalid days is left as free text, not silently accepted.
        let p = parse_query_syntax("days:x hello");
        assert_eq!(p.days, None);
        assert_eq!(p.free_text, "days:x hello");

        let p = parse_query_syntax("-only-exclusion");
        assert_eq!(p.excluded, vec!["only-exclusion".to_string()]);
        assert!(p.free_text.is_empty());
    }

    #[test]
    fn query_syntax_filters_flow_into_search() {
        // Three docs across two sources and one with the excluded word.
        let (index, _dir) = reg_index_proj(
            "syntax",
            &[
                (
                    1_788_874_500,
                    "rust lifetime issues in generic code",
                    "proj-a",
                ),
                (
                    1_788_874_600,
                    "rust lifetime issues again but blocked",
                    "proj-a",
                ),
                (1_788_874_700, "rust lifetimes explained clearly", "proj-b"),
            ],
        );

        // -blocked removes the second doc only.
        let resp = search(&index, "rust lifetime -blocked");
        assert_eq!(resp.results.len(), 2, "excluded doc drops out");
        assert!(resp
            .results
            .iter()
            .all(|h| !h.snippet.to_lowercase().contains("blocked")));

        // source: filter — all corpus docs are zcode, so a foreign source
        // answers nothing while the bare query answers.
        let bare = search(&index, "rust lifetime");
        assert!(!bare.results.is_empty());
        let foreign = index.search(
            "rust lifetime source:claude",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert!(foreign.results.is_empty(), "source: filter applies");

        // days:300 keeps the (recent) corpus; days:0-style tiny windows do not.
        let recent = index.search(
            "rust lifetime days:300",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(recent.results.len(), bare.results.len());
        let ancient = index.search(
            "rust lifetime days:1",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert!(ancient.results.is_empty(), "1-day window predates the docs");

        // R91 session: 前缀过滤——s1 只命中第 2 篇(s0/s2 是 user 轮,
        // 文本里也带 rust lifetime,数量断言避免巧合漏检)。
        let p = parse_query_syntax("rust lifetime session:s1");
        assert_eq!(p.session.as_deref(), Some("s1"));
        let s1 = index.search(
            "rust lifetime session:s1",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(s1.results.len(), 1, "session 前缀应只留 s1 的文档");
        // 半截前缀也能命中(收据面板复制短 id 的场景)。
        let half = index.search(
            "rust lifetime session:s",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(
            half.results.len(),
            bare.results.len(),
            "公共前缀 s 命中全部"
        );
        // 不存在的会话 = 诚实空,不是忽略过滤器。
        let none = index.search(
            "rust lifetime session:zzz",
            &SearchFilter {
                limit: 10,
                ..Default::default()
            },
        );
        assert!(none.results.is_empty(), "session: fail-closed");
    }

    /// Like `reg_index`, with an explicit project per doc.
    fn reg_index_proj(name: &str, docs: &[(i64, &str, &str)]) -> (ContextIndex, PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("tokenbuddy_ctx_regp_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("context.parquet");
        let rows = docs
            .iter()
            .enumerate()
            .map(|(i, (ts, text, project))| {
                (
                    (i + 1) as i64,
                    ContextMessage {
                        source: Source::Zcode,
                        session_id: format!("s{i}"),
                        role: if i % 2 == 0 { "user" } else { "assistant" },
                        timestamp: *ts,
                        text: (*text).into(),
                        project: (*project).into(),
                        title: String::new(),
                    },
                    false,
                )
            })
            .collect::<Vec<_>>();
        write_context_parquet(&path, &docs_to_batch(&rows)).unwrap();
        (ContextIndex::build(&path).unwrap(), dir)
    }

    #[test]
    fn project_labels_strip_home_and_munged_prefixes() {
        let home = dirs::home_dir().unwrap();
        let user = home
            .to_string_lossy()
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap()
            .to_string();
        assert_eq!(project_label(""), "");
        assert_eq!(
            project_label(&format!("{}/code/foo", home.display())),
            "code/foo"
        );
        assert_eq!(project_label(&format!("{}/code", home.display())), "code");
        assert_eq!(
            project_label(&format!("-Users-{user}-code-bar")),
            "code-bar"
        );
        assert_eq!(
            project_label(&format!("--Users-{user}-code-baz--")),
            "code-baz"
        );
        // Qoder's per-session date+hash tail collapses to one label.
        assert_eq!(
            project_label(&format!(
                "-Users-{user}-Documents-Qoder-2026-08-28-a42def97"
            )),
            "Documents-Qoder"
        );
        // Unknown shapes pass through trimmed.
        assert_eq!(project_label("/srv/shared"), "/srv/shared");
    }

    #[test]
    fn project_filter_and_hit_labels() {
        let home = dirs::home_dir().unwrap().to_string_lossy().to_string();
        let alpha = format!("{}/code/alpha", home);
        let beta = format!("{}/code/beta", home);
        let texts: Vec<(i64, &str, String)> = vec![
            (1, "权限配置说明在文档里", alpha.clone()),
            (2, "权限校验逻辑看入口文件", alpha.clone()),
            (3, "权限相关的旧讨论在另一处", beta.clone()),
            (4, "今天天气不错适合干活", alpha.clone()),
        ];
        let texts: Vec<(i64, &str, &str)> =
            texts.iter().map(|(a, b, c)| (*a, *b, c.as_str())).collect();
        let (index, dir) = reg_index_proj("project_filter", &texts);

        // Labels surface on hits and in stats.
        let resp = search(&index, "权限");
        assert!(resp
            .results
            .iter()
            .all(|h| h.project == "code/alpha" || h.project == "code/beta"));
        assert_eq!(index.stats().by_project.get("code/alpha"), Some(&3));
        assert_eq!(index.stats().by_project.get("code/beta"), Some(&1));

        // The filter narrows to one project's docs.
        let resp = index.search(
            "权限",
            &SearchFilter {
                project: Some("code/beta"),
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].project, "code/beta");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn session_view_windows_around_anchor() {
        let home = dirs::home_dir().unwrap().to_string_lossy().to_string();
        let alpha = format!("{}/code/alpha", home);
        let dir = std::env::temp_dir().join(format!("tokenbuddy_ctx_sess_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("context.parquet");
        let steps = ["一", "二", "三", "四", "五", "六", "七"];
        let mut rows = Vec::new();
        for (i, step) in steps.iter().enumerate() {
            rows.push((
                (i + 1) as i64,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: "s1".into(),
                    role: if i % 2 == 0 { "user" } else { "assistant" },
                    timestamp: 1_700_000_000 + i as i64 * 60,
                    text: format!("权限讨论第{step}步继续"),
                    project: alpha.clone(),
                    title: String::new(),
                },
                false,
            ));
        }
        // Same keyword in another session: must never leak into s1's window.
        rows.push((
            8,
            ContextMessage {
                source: Source::Zcode,
                session_id: "s2".into(),
                role: "user",
                timestamp: 1_700_100_000,
                text: "权限讨论在别的会话".into(),
                project: alpha.clone(),
                title: String::new(),
            },
            false,
        ));
        write_context_parquet(&path, &docs_to_batch(&rows)).unwrap();
        let index = ContextIndex::build(&path).unwrap();

        // The anchor doc_id comes from a real search hit — the same flow the
        // dashboard uses (hit → session_view).
        let resp = search(&index, "权限讨论");
        let hit = resp
            .results
            .iter()
            .find(|h| h.session_id == "s1" && h.snippet.contains("第四步"))
            .expect("anchor hit must be searchable");
        let anchor_id = hit.doc_id_str.parse::<i64>().unwrap();
        let view = index
            .session_view("zcode", "s1", anchor_id, 2)
            .expect("session must exist");
        assert_eq!(view.total, 7);
        assert_eq!(view.anchor_pos, 3);
        assert_eq!(view.window_start, 1);
        assert_eq!(view.messages.len(), 5, "±2 around position 3");
        assert_eq!(view.messages.iter().filter(|m| m.matched).count(), 1);
        assert!(view.messages.iter().all(|m| m.text.contains("权限讨论")));
        assert_eq!(view.project, "code/alpha");

        // A wide window is clamped, not padded.
        let view = index.session_view("zcode", "s1", anchor_id, 50).unwrap();
        assert_eq!(view.messages.len(), 7);

        // Unknown session → None.
        assert!(index.session_view("zcode", "nope", anchor_id, 5).is_none());

        // Issue #12 Bug 3: a bare doc_id must resolve its (source, session)
        // pair — that's the /api/docs contract for one-parameter drill-down.
        assert_eq!(
            index.doc_session(anchor_id),
            Some(("zcode".into(), "s1".into()))
        );
        assert_eq!(index.doc_session(-1), None);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn session_titles_prefer_explicit_then_first_user_message() {
        // Two sessions built explicitly so title provenance is unambiguous.
        let dir = std::env::temp_dir().join(format!("tokenbuddy_ctx_title_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("context.parquet");
        let rows = vec![
            (
                1,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: "s1".into(),
                    role: "assistant",
                    timestamp: 1,
                    text: "先回答第一个问题".into(),
                    project: String::new(),
                    title: "旧标题".into(),
                },
                false,
            ),
            (
                2,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: "s1".into(),
                    role: "user",
                    timestamp: 2,
                    text: "权限配置在哪里".into(),
                    project: String::new(),
                    title: "最终标题".into(),
                },
                false,
            ),
            (
                3,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: "s2".into(),
                    role: "assistant",
                    timestamp: 3,
                    text: "没有标题的会话".into(),
                    project: String::new(),
                    title: String::new(),
                },
                false,
            ),
            (
                4,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: "s2".into(),
                    role: "user",
                    timestamp: 4,
                    text: "部署\n失败了\n怎么排查".into(),
                    project: String::new(),
                    title: String::new(),
                },
                false,
            ),
        ];
        write_context_parquet(&path, &docs_to_batch(&rows)).unwrap();
        let index = ContextIndex::build(&path).unwrap();

        // Last non-empty explicit title wins.
        let resp = search(&index, "权限");
        assert!(resp.results.iter().all(|h| h.title == "最终标题"));
        // No explicit title → first user message, whitespace collapsed.
        let resp = search(&index, "部署");
        assert!(resp
            .results
            .iter()
            .all(|h| h.title == "部署 失败了 怎么排查"));
        // Session view carries the same title.
        let hit = resp.results[0].clone();
        let view = index
            .session_view(
                "zcode",
                &hit.session_id,
                hit.doc_id_str.parse::<i64>().unwrap(),
                5,
            )
            .unwrap();
        assert_eq!(view.title, "部署 失败了 怎么排查");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn legacy_parquet_without_project_rebuilds() -> Result<()> {
        let dir =
            std::env::temp_dir().join(format!("tokenbuddy_ctx_legacy_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("context.parquet");

        // A pre-project parquet: schema without the project column.
        let legacy_schema = SchemaRef::from(Schema::new(vec![
            Field::new("doc_id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let mut id_b = Int64Builder::new();
        let mut text_b = StringBuilder::new();
        id_b.append_value(1);
        text_b.append_value("旧格式的文档");
        let legacy = RecordBatch::try_new(
            legacy_schema,
            vec![Arc::new(id_b.finish()), Arc::new(text_b.finish())],
        )?;
        write_context_parquet(&path, &legacy)?;
        assert!(!parquet_is_current_schema(&path));

        let stats = sync_messages(
            &path,
            false,
            vec![ContextMessage {
                source: Source::Zcode,
                session_id: "s1".into(),
                role: "user",
                timestamp: 1_700_000_000,
                text: "新格式的文档".into(),
                project: "/tmp/proj".into(),
                title: String::new(),
            }],
        )?;
        assert_eq!(
            stats.total_docs, 1,
            "legacy rows must be discarded, not merged"
        );
        assert!(parquet_is_current_schema(&path));

        let index = ContextIndex::build(&path)?;
        assert_eq!(index.stats().docs, 1);
        assert_eq!(index.stats().by_project.get("/tmp/proj"), Some(&1));

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }

    #[test]
    fn verbose_query_softens_missing_ascii_term() {
        // Four content terms make the query verbose, so a bogus ASCII token
        // soft-misses instead of emptying the pool — the opposite of the
        // precise-query hard miss pinned in the roundtrip test.
        let texts: Vec<(i64, &str)> = vec![
            (1, "权限 部署 讨论 失败"),
            (2, "第一天天气不错适合干活"),
            (3, "第二天天气不错适合干活"),
            (4, "第三天天气不错适合干活"),
            (5, "第四天天气不错适合干活"),
            (6, "第五天天气不错适合干活"),
        ];
        let (index, dir) = reg_index("verbose_ascii", &texts);
        let resp = search(&index, "权限 部署 讨论 失败 zzznotaword");
        assert_eq!(resp.trace.tier, 1);
        assert!(
            !resp.results.is_empty(),
            "verbose query must not die on a missing token"
        );
        assert!(resp.results[0].snippet.contains("权限"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn all_stopword_query_keeps_recall_via_fallback() {
        // Every doc is 100% 「的」 docs — no content terms exist. The
        // fallback scoring set must still rank and return them.
        let texts: Vec<(i64, &str)> = (0..6)
            .map(|i| {
                (
                    1 + i,
                    if i % 2 == 0 {
                        "好的知道了记录"
                    } else {
                        "谁的文件发过了"
                    },
                )
            })
            .collect();
        let (index, dir) = reg_index("all_stop", &texts);
        let resp = search(&index, "的");
        assert_eq!(
            resp.trace.terms_content, 0,
            "nothing is below the stopword line"
        );
        assert_eq!(
            resp.results.len(),
            6,
            "fallback must still return every match"
        );
        assert_eq!(resp.trace.tier, 1, "lone-char query is a full-scan pool");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn lone_cjk_char_full_scan_matches_inside_longer_words() {
        // The index only ever cut 税务 as a word, so 「税」 has df=0 — the
        // lone-char full scan plus substring verification is what finds it.
        let (index, dir) = reg_index(
            "lone_char",
            &[(1, "税务申报流程"), (2, "今天天气不错"), (3, "部署完成")],
        );
        let resp = search(&index, "税");
        assert_eq!(resp.results.len(), 1);
        assert!(resp.results[0].snippet.contains("税务"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn ascii_stopword_leaves_and_pool_to_content_terms() {
        // "the" covers 60% of the corpus — an ASCII stopword by evidence. It
        // must not join the AND, and it must not leak into highlight terms.
        let mut texts: Vec<(i64, String)> = (0..6)
            .map(|i| {
                (
                    1 + i as i64,
                    format!("the daily report volume {i} looks fine"),
                )
            })
            .collect();
        texts.push((7, "parquet schema definition".into()));
        texts.push((8, "parquet writer tuning".into()));
        texts.push((9, "完全无关的中文内容第一条".into()));
        texts.push((10, "完全无关的中文内容第二条".into()));
        let texts: Vec<(i64, &str)> = texts.iter().map(|(a, b)| (*a, b.as_str())).collect();
        let (index, dir) = reg_index("ascii_stop", &texts);
        let resp = search(&index, "the parquet");
        assert_eq!(resp.trace.tier, 1);
        assert_eq!(resp.results.len(), 2);
        assert!(resp.results.iter().all(|h| h.snippet.contains("parquet")));
        assert!(
            !resp.terms.contains(&"the".to_string()),
            "stopword leaked into highlights"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn phrase_bonus_lifts_adjacent_match_over_scattered() {
        // Same two content terms in both docs; only docA has them adjacent.
        // Position the two docs on even indexes so both are user-role and
        // the role weight cannot explain the ranking.
        let mut texts: Vec<(i64, String)> = vec![(1, "the quick brown fox jumps over".into())];
        for i in 0..7 {
            texts.push((2 + i, format!("第{i}天天气不错适合干活记录")));
        }
        texts.push((9, "brown and quick updates".into()));
        texts.push((10, "第十一项完全无关内容".into()));
        let texts: Vec<(i64, &str)> = texts.iter().map(|(a, b)| (*a, b.as_str())).collect();
        let (index, dir) = reg_index("phrase", &texts);
        let resp = search(&index, "quick brown");
        assert_eq!(resp.results.len(), 2);
        assert!(
            resp.results[0].snippet.contains("fox"),
            "adjacent phrase must outrank the scattered mention: {:?}",
            resp.results
                .iter()
                .map(|h| h.snippet.clone())
                .collect::<Vec<_>>()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn tier3_orders_by_idf_weight() {
        // No doc carries both content terms, so the pool falls to tier 3.
        // 权限 (df=1) carries more idf than 部署 (df=3): its doc ranks first
        // even though another doc says 部署 three times.
        let mut texts: Vec<(i64, String)> = vec![(1, "权限不足需要申请".into())];
        for (i, t) in ["部署 部署 部署完成", "部署手册要更新", "部署脚本跑通了"]
            .iter()
            .enumerate()
        {
            texts.push((2 + i as i64, (*t).into()));
        }
        for i in 0..8 {
            texts.push((5 + i, format!("第{i}天天气不错适合干活")));
        }
        let texts: Vec<(i64, &str)> = texts.iter().map(|(a, b)| (*a, b.as_str())).collect();
        let (index, dir) = reg_index("tier3", &texts);
        let resp = search(&index, "权限 部署");
        assert_eq!(resp.trace.tier, 3, "AND tiers must find nothing here");
        assert!(
            resp.results[0].snippet.contains("权限"),
            "rarest-term doc must rank first: {:?}",
            resp.results
                .iter()
                .map(|h| h.snippet.clone())
                .collect::<Vec<_>>()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn since_filter_applies_before_verification() {
        let texts: Vec<(i64, &str)> = vec![
            (1_000, "权限第一批"),
            (2_000, "权限第二批"),
            (3_000, "权限第三批"),
            (4_000, "权限第四批"),
        ];
        let (index, dir) = reg_index("since", &texts);
        let resp = index.search(
            "权限",
            &SearchFilter {
                since: Some(2_000),
                limit: 10,
                ..Default::default()
            },
        );
        assert_eq!(
            resp.trace.verified, 3,
            "time filter should cut the pool before verify"
        );
        assert_eq!(resp.results.len(), 3);
        assert!(resp.results.iter().all(|h| h.timestamp >= 2_000));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn session_without_user_voice_gets_it_topped_up() {
        // Both sessions rank assistant-only for the query (their humans
        // never said the query words). The top-up pass must still surface
        // each session's human line — the user doc just before the ranked
        // reply — right after that reply, so person and agent read as a pair.
        let dir = std::env::temp_dir().join(format!("tokenbuddy_ctx_topup_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("context.parquet");
        let rows = vec![
            (
                1,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: "s1".into(),
                    role: "user",
                    timestamp: 1_700_000_000,
                    text: "帮我看看怎么回事".into(),
                    project: String::new(),
                    title: String::new(),
                },
                false,
            ),
            (
                2,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: "s1".into(),
                    role: "assistant",
                    timestamp: 1_700_000_060,
                    text: "部署失败的原因是脚本里写死了旧主机名，改成变量读取就好了，另外部署失败时重试逻辑也要补上".into(),
                    project: String::new(),
                    title: String::new(),
                },
                false,
            ),
            (
                3,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: "s2".into(),
                    role: "user",
                    timestamp: 1_700_100_000,
                    text: "今天天气不错".into(),
                    project: String::new(),
                    title: String::new(),
                },
                false,
            ),
            (
                4,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: "s2".into(),
                    role: "assistant",
                    timestamp: 1_700_100_060,
                    text: "部署失败告警已经处理完了，失败的根因是磁盘写满，部署失败重试也配置好了".into(),
                    project: String::new(),
                    title: String::new(),
                },
                false,
            ),
        ];
        write_context_parquet(&path, &docs_to_batch(&rows)).unwrap();
        let index = ContextIndex::build(&path).unwrap();

        let resp = search(&index, "部署失败");
        let roles: Vec<(&str, &str)> = resp
            .results
            .iter()
            .map(|h| (h.session_id.as_str(), h.role))
            .collect();
        assert!(
            roles.contains(&("s1", "user")) && roles.contains(&("s2", "user")),
            "each session's human line must be surfaced next to its reply: {roles:?}"
        );
        for sid in ["s1", "s2"] {
            let positions: Vec<usize> = resp
                .results
                .iter()
                .enumerate()
                .filter(|(_, h)| h.session_id == sid)
                .map(|(i, _)| i)
                .collect();
            assert_eq!(positions.len(), 2, "{sid}: one reply + one human line");
            assert_eq!(
                resp.results[positions[0]].role, "assistant",
                "{sid}: the ranked reply leads"
            );
            assert_eq!(
                resp.results[positions[0] + 1].role,
                "user",
                "{sid}: the triggering human line sits right after it"
            );
        }
        // s1's topped-up line is the actual pre-reply human message.
        let s1_user = resp
            .results
            .iter()
            .find(|h| h.session_id == "s1" && h.role == "user")
            .unwrap();
        assert!(s1_user.snippet.contains("帮我看看怎么回事"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn agent_echo_of_user_message_folds_into_one_hit() {
        // One session: the human asks, the "assistant" replies by restating
        // the question almost verbatim plus a short lead-in. Both contain
        // the query words; the reply must fold into the human's hit.
        // A second session with the same question stays — a repeat across
        // sessions is real signal, and one session of it remains as anchor.
        let dir = std::env::temp_dir().join(format!("tokenbuddy_ctx_echo_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("context.parquet");
        let question = "权限校验失败的时候日志里应该怎么排查，网关的权限校验失败会打印什么日志";
        let reply = format!("好的，权限校验失败的时候日志里应该这样排查：先看网关的权限校验失败日志打印位置，{question}");
        let rows = vec![
            (
                1,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: "s1".into(),
                    role: "user",
                    timestamp: 1_700_000_000,
                    text: question.into(),
                    project: String::new(),
                    title: String::new(),
                },
                false,
            ),
            (
                2,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: "s1".into(),
                    role: "assistant",
                    timestamp: 1_700_000_060,
                    text: reply,
                    project: String::new(),
                    title: String::new(),
                },
                false,
            ),
            (
                3,
                ContextMessage {
                    source: Source::Zcode,
                    session_id: "s2".into(),
                    role: "user",
                    timestamp: 1_700_100_000,
                    text: question.into(),
                    project: String::new(),
                    title: String::new(),
                },
                false,
            ),
        ];
        write_context_parquet(&path, &docs_to_batch(&rows)).unwrap();
        let index = ContextIndex::build(&path).unwrap();

        let resp = search(&index, "权限校验失败 日志");
        assert_eq!(
            resp.trace.deduped,
            1,
            "the restating reply must fold: {:?}",
            resp.results
                .iter()
                .map(|h| (h.role, h.snippet.clone()))
                .collect::<Vec<_>>()
        );
        assert_eq!(resp.results.len(), 2, "one per session survives");
        let sessions: Vec<&str> = resp.results.iter().map(|h| h.session_id.as_str()).collect();
        assert!(sessions.contains(&"s1") && sessions.contains(&"s2"));
        assert!(
            resp.results.iter().all(|h| h.role == "user"),
            "every survivor is the human's wording, not its echo: {:?}",
            resp.results
                .iter()
                .map(|h| (h.role, h.session_id.as_str()))
                .collect::<Vec<_>>()
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn trace_invariants_hold_across_query_shapes() {
        let texts: Vec<(i64, String)> = vec![
            "the quick brown fox jumps over".into(),
            "权限不足需要申请，部署也失败了".into(),
            "好的知道了记录".into(),
            "部署 部署 部署完成".into(),
            "税务申报流程说明".into(),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, t)| (1_700_000_000 + i as i64 * 3600, t))
        .collect();
        let texts: Vec<(i64, &str)> = texts.iter().map(|(a, b)| (*a, b.as_str())).collect();
        let (index, dir) = reg_index("invariants", &texts);
        for q in [
            "权限 部署",
            "the fox",
            "的",
            "税",
            "zzz 量子纠缠不存在",
            "quick",
            "",
        ] {
            let resp = search(&index, q);
            let t = &resp.trace;
            assert!(t.tier <= 3, "{q:?}: tier {}", t.tier);
            assert!(
                t.pool <= t.pool_from,
                "{q:?}: pool {} > {}",
                t.pool,
                t.pool_from
            );
            assert!(
                t.verified <= t.pool,
                "{q:?}: verified {} > pool {}",
                t.verified,
                t.pool
            );
            assert!(
                t.matched <= t.verified,
                "{q:?}: matched {} > verified {}",
                t.matched,
                t.verified
            );
            assert!(
                t.returned <= t.matched,
                "{q:?}: returned {} > matched {}",
                t.returned,
                t.matched
            );
            assert!(
                t.terms_content <= t.terms_total,
                "{q:?}: content {}",
                t.terms_content
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn search_log_appends_and_rotates() {
        let dir =
            std::env::temp_dir().join(format!("tokenbuddy_ctx_logrot_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("search-log.jsonl");

        append_search_line(&path, &"a".repeat(30), 40).unwrap();
        append_search_line(&path, &"b".repeat(30), 40).unwrap();
        // File is now ~62 bytes > 40: the third write rotates it away.
        append_search_line(&path, "last", 40).unwrap();

        let rotated = dir.join("search-log.jsonl.1");
        assert!(rotated.exists(), "old log must rotate to .1");
        assert_eq!(
            std::fs::read_to_string(&rotated).unwrap().lines().count(),
            2
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "last\n");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn handle_writes_search_log_jsonl() {
        let dir =
            std::env::temp_dir().join(format!("tokenbuddy_ctx_loghandle_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let handle = ContextHandle::new(dir.join("context.parquet"));
        let resp = SearchResponse {
            query: "回归".into(),
            elapsed_ms: 3,
            candidates: 4,
            results: vec![],
            terms: vec![],
            session_headers: vec![],
            project_matched: Vec::new(),
            trace: SearchTrace {
                tier: 1,
                pool: 4,
                pool_from: 4,
                verified: 4,
                matched: 2,
                deduped: 1,
                returned: 2,
                terms_total: 5,
                terms_content: 3,
                terms_us: 10,
                pool_us: 5,
                verify_us: 50,
                total_ms: 3,
                backfill: 0,
            },
        };
        handle.log_search(
            "回归查询",
            &SearchFilter {
                limit: 30,
                ..Default::default()
            },
            &resp,
        );
        let line = std::fs::read_to_string(dir.join("search-log.jsonl")).unwrap();
        let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(v["query"], "回归查询");
        assert_eq!(v["limit"], 30);
        assert_eq!(v["trace"]["tier"], 1);
        assert_eq!(v["trace"]["pool_from"], 4);
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod drained_guard_tests {
    use super::DRAINED_SOURCES;

    /// issue #23 的守门断言:实现了 drain 却没接进 sync_context 的源是
    /// 「只在单测里跑过的死代码」。每个账本源要么在 DRAINED_SOURCES(=生产
    /// drain 清单的镜像),要么在下面这张显式 not_supported 白名单里——
    /// 两者皆无即失败,新增采集器漏接线会被这条测试拦下。
    #[test]
    fn every_ledger_source_is_indexed_or_whitelisted() {
        const NOT_SUPPORTED: &[&str] = &["roocode", "kilo", "kimi", "amp"];
        for name in crate::SOURCE_NAMES {
            assert!(
                DRAINED_SOURCES.contains(name) || NOT_SUPPORTED.contains(name),
                "源 {name} 既不在 sync_context 的 drain 清单也不在 not_supported 白名单——账本能统计、搜索却静默空(issue #23)"
            );
        }
        for name in DRAINED_SOURCES {
            assert!(
                crate::SOURCE_NAMES.contains(name),
                "DRAINED_SOURCES 里的 {name} 不是合法账本源"
            );
        }
    }
}
