use crate::{claude, hermes, mimo, minimax, opencode, pi, qoder, workbuddy, zcode, TokenRecord};
use anyhow::Result;
use arrow::array::{
    Array, Float64Array, Float64Builder, Int64Array, Int64Builder, RecordBatch, StringArray,
    StringBuilder,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// How many pre-rebuild snapshots `sync_full` retains.
const SNAPSHOT_KEEP: usize = 5;

/// One collector's failure during a sync round. Sync is fault-tolerant per
/// source: a locked or corrupt tool database costs the user that source's
/// rows, not every other source's, and says so here instead of failing the
/// whole run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceError {
    pub source: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    pub claude_imported: u32,
    pub claude_skipped: u32,
    pub claude_files: u32,
    pub opencode_imported: u32,
    pub opencode_skipped: u32,
    pub mimo_imported: u32,
    pub mimo_skipped: u32,
    pub zcode_imported: u32,
    pub zcode_skipped: u32,
    pub pi_imported: u32,
    pub pi_skipped: u32,
    pub qoder_imported: u32,
    pub qoder_skipped: u32,
    pub workbuddy_imported: u32,
    pub workbuddy_skipped: u32,
    pub minimax_imported: u32,
    pub minimax_skipped: u32,
    pub hermes_imported: u32,
    pub hermes_skipped: u32,
    pub codex_imported: u32,
    pub codex_skipped: u32,
    pub gemini_imported: u32,
    pub gemini_skipped: u32,
    pub qwen_imported: u32,
    pub qwen_skipped: u32,
    /// Sources that failed this round; empty on a clean run.
    pub errors: Vec<SourceError>,
    /// Rows the store held before a full rebuild. A rebuild re-derives from
    /// whatever logs still exist, so this is the number history can shrink
    /// by — reported so the UI never claims a rebuild "added" rows while the
    /// total quietly fell.
    pub previous_total: Option<u64>,
    /// Rows the store holds now.
    pub total_after: u64,
    /// Wall-clock cost of the round, milliseconds.
    pub duration_ms: u64,
    /// `incremental` or `full`, as the caller asked for it.
    pub mode: String,
}

/// What one collector can see on this machine, for the dashboard's health
/// panel: present or not, and where it looked.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceHealth {
    pub id: String,
    pub label: String,
    pub present: bool,
    pub path: Option<String>,
}

/// Wall-clock facts about the last completed sync, persisted next to the
/// parquet so "上次同步 3 分钟前" survives a restart instead of being
/// reconstructed from nothing on the dashboard.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncState {
    #[serde(default)]
    pub last_sync_at: Option<i64>,
    #[serde(default)]
    pub last_sync_mode: Option<String>,
    #[serde(default)]
    pub last_sync_imported: Option<u32>,
    #[serde(default)]
    pub last_sync_duration_ms: Option<u64>,
    #[serde(default)]
    pub last_sync_errors: Vec<SourceError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub total_requests: u64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_cache_read_tokens: u64,
    pub total_cache_creation_tokens: u64,
    pub total_tokens: u64,
    /// Raw host-reported credits for the window. This is the exact consumption
    /// figure for sources whose token counts are masked (Qoder).
    pub total_credits: f64,
    /// Mean context-window fill over reporting requests in the whole window.
    pub avg_context_ratio: Option<f64>,
    pub by_source: Vec<SourceRow>,
    pub by_model: Vec<ModelRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRow {
    pub source: String,
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    /// Credits billed by the tool itself in the window.
    pub credits: f64,
    /// Mean context-window fill over the requests that reported one
    /// (Qoder-only today); `None` when no request in the window did.
    pub avg_context_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRow {
    pub source: String,
    pub model: String,
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub total_tokens: u64,
    /// Credits billed by the tool itself for this (source, model) pair.
    pub credits: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineBucket {
    pub label: String,
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub total_tokens: u64,
    pub claude_tokens: u64,
    pub opencode_tokens: u64,
    pub mimo_tokens: u64,
    pub zcode_tokens: u64,
    pub pi_tokens: u64,
    pub qoder_tokens: u64,
    pub workbuddy_tokens: u64,
    pub minimax_tokens: u64,
    pub hermes_tokens: u64,
}

/// Per-model comparison row used by the `/api/models` report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelStat {
    pub model: String,
    /// Normalized family label (e.g. `sonnet`, `glm`, `qwen`) for grouping.
    pub family: String,
    /// Sources (claude/opencode/...) that issued requests to this model.
    pub sources: Vec<String>,
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub total_tokens: u64,
    /// Credits billed by the tool itself for this model.
    pub credits: f64,
    pub avg_tokens_per_req: f64,
    pub avg_duration_ms: Option<f64>,
    pub p95_duration_ms: Option<f64>,
    pub avg_ttft_ms: Option<f64>,
    pub p95_ttft_ms: Option<f64>,
    /// Aggregate throughput: total output tokens / total duration seconds.
    pub tokens_per_sec: Option<f64>,
    pub cache_hit_rate: Option<f64>,
    /// Hosts (Fleet aggregation only) that issued requests to this model;
    /// empty for the single-machine endpoint, and skipped in that case so
    /// the local JSON shape stays byte-identical to before Fleet.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<String>,
}

/// Response shape for `/api/models`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelComparison {
    /// Sorted by total_tokens descending.
    pub models: Vec<ModelStat>,
}

// --- Fleet (multi-machine aggregation over pulled host parquets) ---

/// Response shape for `/api/fleet/summary`: fleet-wide totals, one row per
/// host, and the host × source matrix behind the fleet heatmap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetSummary {
    pub totals: FleetTotals,
    /// Sorted by host id; only hosts whose parquet is pulled locally appear.
    pub by_host: Vec<FleetHostRow>,
    /// One cell per (host, source) with any activity in the window.
    pub host_source: Vec<FleetHostSourceCell>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetTotals {
    /// Hosts contributing to this window (after the host filter).
    pub hosts: usize,
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub total_tokens: u64,
    pub credits: f64,
    /// cache_read / (input + cache_read) across the fleet, same convention
    /// as the dashboard's local cache-hit figure.
    pub cache_hit_rate: Option<f64>,
    pub avg_context_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetHostRow {
    pub host: String,
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub total_tokens: u64,
    pub credits: f64,
    pub avg_context_ratio: Option<f64>,
    /// Sources seen on this host in the window (for quick scanning).
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetHostSourceCell {
    pub host: String,
    pub source: String,
    pub requests: u64,
    pub total_tokens: u64,
    pub credits: f64,
}

/// Response shape for `/api/fleet/metrics`: the local latency/cache panel
/// re-keyed by host instead of source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetMetrics {
    pub by_host: Vec<FleetHostMetrics>,
    /// Same totals shape as the single-machine `/api/metrics`.
    pub totals: TotalsMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetHostMetrics {
    pub host: String,
    pub requests: u64,
    pub avg_duration_ms: Option<f64>,
    pub avg_ttft_ms: Option<f64>,
    pub cache_hit_rate: Option<f64>,
    pub output_input_ratio: Option<f64>,
    pub avg_input_per_req: f64,
    pub avg_output_per_req: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metrics {
    /// Per-source aggregate metrics. The `*_cache_hit_rate` fields are
    /// fractions in [0.0, 1.0]; missing (None) when the source has no cache
    /// or no requests in the filter window.
    pub by_source: Vec<SourceMetrics>,
    /// Aggregate over all sources in the filter window.
    pub totals: TotalsMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMetrics {
    pub source: String,
    pub requests: u64,
    pub avg_duration_ms: Option<f64>,
    pub avg_ttft_ms: Option<f64>,
    pub cache_hit_rate: Option<f64>,
    /// output_tokens / input_tokens; None when input_tokens == 0.
    pub output_input_ratio: Option<f64>,
    pub avg_input_per_req: f64,
    pub avg_output_per_req: f64,
    pub avg_cache_read_per_req: f64,
    /// Mean context-window fill over reporting requests (Qoder-only today);
    /// `None` when no request reported one.
    pub avg_context_ratio: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TotalsMetrics {
    pub requests: u64,
    pub avg_duration_ms: Option<f64>,
    pub avg_ttft_ms: Option<f64>,
    pub cache_hit_rate: Option<f64>,
    pub output_input_ratio: Option<f64>,
    pub avg_input_per_req: f64,
    pub avg_output_per_req: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heatmap {
    /// Each cell is `(row_label, col_label, value)`. Row / col labels are
    /// either `model` (model name) or `source` depending on `mode`.
    pub rows: Vec<String>,
    pub cols: Vec<String>,
    /// value[i][j] is the metric for row i × col j. None means "no data".
    pub values: Vec<Vec<Option<f64>>>,
    pub metric: String, // "total_tokens" | "requests" | "avg_duration_ms"
    pub mode: String,   // "model_x_source" | "model_x_day"
    pub max_value: f64,
    pub day_labels: Vec<String>, // populated only for model_x_day
}

// --- Insights (deep analysis) ---

/// One China-local hour-of-day slot of the work-rhythm histogram.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourBucket {
    /// 0..=23, China-local hour.
    pub hour: u32,
    pub tokens: u64,
    pub requests: u64,
}

/// Daily cache efficiency point: input-side hit rate plus the raw token
/// volume the cache absorbed that day.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheDay {
    /// `%Y-%m-%d`, China-local.
    pub label: String,
    pub cache_hit_rate: Option<f64>,
    pub cache_read_tokens: u64,
    pub input_tokens: u64,
}

/// One session aggregated across its requests. Sessions come from whatever
/// `session_id` the source log carried; rows without one still count toward
/// every other panel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRow {
    pub source: String,
    pub session_id: String,
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub total_tokens: u64,
    pub credits: f64,
    pub avg_context_ratio: Option<f64>,
    /// First request epoch seconds; 0 when unknown.
    pub first_ts: i64,
    /// Last request epoch seconds; 0 when unknown.
    pub last_ts: i64,
    /// User prompts submitted in the session, from the source's runtime log
    /// (Qoder). `None` when the source logs no such events.
    pub user_prompts: Option<u64>,
    /// Completed turns, from the runtime log. `None` when unavailable.
    pub turns: Option<u64>,
    /// Tool invocations requested, from the runtime log. `None` when
    /// unavailable.
    pub tool_calls: Option<u64>,
    /// True when the session has runtime-log activity but no billable record
    /// at all — a Qoder BYOK session, where Qoder reports no usage. Token and
    /// credit columns are genuinely absent, not merely zero.
    pub from_runtime: bool,
}

/// Daily mean of the source-reported context-window fill (Qoder today).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextFillDay {
    /// `%Y-%m-%d`, China-local.
    pub label: String,
    pub avg_ratio: Option<f64>,
    /// Requests that reported a ratio that day.
    pub requests: u64,
}

/// Response shape for `/api/insights` — the deep-analysis panels in one pass.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Insights {
    /// Always 24 entries, ordered by hour.
    pub rhythm: Vec<HourBucket>,
    /// Ordered by day ascending; days with no data are absent.
    pub cache_trend: Vec<CacheDay>,
    /// Raw sum of cache_read tokens in the window — the input the cache
    /// absorbed. A fact, not an extrapolation.
    pub cache_served_tokens: u64,
    /// Top sessions by total_tokens descending, at most `limit` entries.
    pub sessions: Vec<SessionRow>,
    /// Ordered by day ascending; only days with at least one reporting
    /// request appear.
    pub context_fill_trend: Vec<ContextFillDay>,
    /// Mean ratio over every reporting request in the window; `None` when the
    /// window has none (every source but Qoder today).
    pub context_fill_avg: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimelineMode {
    Hourly,
    Daily,
    Weekly,
    Monthly,
}

// --- Parquet helpers (arrow 58) ---

fn parquet_schema() -> SchemaRef {
    SchemaRef::from(Schema::new(vec![
        Field::new("source", DataType::Utf8, false),
        Field::new("model", DataType::Utf8, false),
        Field::new("input_tokens", DataType::Int64, false),
        Field::new("output_tokens", DataType::Int64, false),
        Field::new("cache_read_tokens", DataType::Int64, false),
        Field::new("cache_creation_tokens", DataType::Int64, false),
        Field::new("credits", DataType::Float64, false),
        // Fraction of the context window one request consumed, as reported by
        // the source (Qoder). 0.0 = source does not report it.
        Field::new("context_ratio", DataType::Float64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("session_id", DataType::Utf8, true),
        // R14: project attribution from the source log (cwd / workspace
        // directory / project slug). Null for rows collected before the
        // column existed and for sources that do not say.
        Field::new("project", DataType::Utf8, true),
        Field::new("message_id", DataType::Utf8, false),
        Field::new("duration_ms", DataType::Int64, true),
        Field::new("ttft_ms", DataType::Int64, true),
    ]))
}

pub(crate) fn records_to_batch(records: &[(String, TokenRecord)]) -> RecordBatch {
    let s = parquet_schema();
    let n = records.len();
    let mut source_b = StringBuilder::with_capacity(n, n * 10);
    let mut model_b = StringBuilder::with_capacity(n, n * 40);
    let mut input_b = Int64Builder::with_capacity(n);
    let mut output_b = Int64Builder::with_capacity(n);
    let mut cache_read_b = Int64Builder::with_capacity(n);
    let mut cache_write_b = Int64Builder::with_capacity(n);
    let mut credits_b = Float64Builder::with_capacity(n);
    let mut ratio_b = Float64Builder::with_capacity(n);
    let mut ts_b = Int64Builder::with_capacity(n);
    let mut sid_b = StringBuilder::with_capacity(n, n * 40);
    let mut proj_b = StringBuilder::with_capacity(n, n * 60);
    let mut mid_b = StringBuilder::with_capacity(n, n * 50);
    let mut dur_b = Int64Builder::with_capacity(n);
    let mut ttft_b = Int64Builder::with_capacity(n);

    for (mid, r) in records {
        source_b.append_value(r.source.as_str());
        model_b.append_value(&r.model);
        input_b.append_value(r.input_tokens as i64);
        output_b.append_value(r.output_tokens as i64);
        cache_read_b.append_value(r.cache_read_tokens as i64);
        cache_write_b.append_value(r.cache_creation_tokens as i64);
        credits_b.append_value(r.credits);
        ratio_b.append_value(r.context_ratio);
        ts_b.append_value(r.timestamp);
        match &r.session_id {
            Some(s) => sid_b.append_value(s),
            None => sid_b.append_null(),
        }
        if r.project.is_empty() {
            proj_b.append_null();
        } else {
            proj_b.append_value(&r.project);
        }
        mid_b.append_value(mid);
        match r.duration_ms {
            Some(d) => dur_b.append_value(d as i64),
            None => dur_b.append_null(),
        }
        match r.ttft_ms {
            Some(d) => ttft_b.append_value(d as i64),
            None => ttft_b.append_null(),
        }
    }

    RecordBatch::try_new(
        s,
        vec![
            Arc::new(source_b.finish()) as Arc<dyn Array>,
            Arc::new(model_b.finish()),
            Arc::new(input_b.finish()),
            Arc::new(output_b.finish()),
            Arc::new(cache_read_b.finish()),
            Arc::new(cache_write_b.finish()),
            Arc::new(credits_b.finish()),
            Arc::new(ratio_b.finish()),
            Arc::new(ts_b.finish()),
            Arc::new(sid_b.finish()),
            Arc::new(proj_b.finish()),
            Arc::new(mid_b.finish()),
            Arc::new(dur_b.finish()),
            Arc::new(ttft_b.finish()),
        ],
    )
    .expect("record batch construction")
}

fn read_existing_ids(path: &Path) -> Result<HashSet<String>> {
    if !path.exists() {
        return Ok(HashSet::new());
    }
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Project only the message_id column to avoid reading the entire parquet file
    let schema = builder.schema().clone();
    let msg_id_idx = schema
        .index_of("message_id")
        .map_err(|e| anyhow::anyhow!("No message_id column: {e}"))?;
    let parquet_schema = builder.parquet_schema().clone();
    let mask = ProjectionMask::roots(&parquet_schema, [msg_id_idx]);

    let reader = builder.with_projection(mask).build()?;
    let mut ids = HashSet::new();
    for batch in reader {
        let batch = batch?;
        let col = batch.column(0);
        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..arr.len() {
            ids.insert(arr.value(i).to_string());
        }
    }
    Ok(ids)
}

pub(crate) fn write_parquet(path: &Path, batch: &RecordBatch) -> Result<()> {
    let tmp = path.with_extension("parquet.tmp");
    let file = std::fs::File::create(&tmp)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

/// Run one collector, degrading to "no rows plus a reported error" instead of
/// aborting the round. Without this, one locked tool database voided the
/// import of every other source — the failure mode that makes a local
/// analytics tool look broken for reasons the dashboard cannot explain.
fn collect_or_report<F>(collect: F, name: &str, errors: &mut Vec<SourceError>) -> Vec<TokenRecord>
where
    F: FnOnce() -> Result<Vec<TokenRecord>>,
{
    match collect() {
        Ok(records) => records,
        Err(e) => {
            // `{e:#}` walks the whole anyhow context chain: a bare "database
            // is locked" does not say which database, and the panel this
            // lands in is the user's only clue.
            let message = format!("{e:#}");
            eprintln!("[TokenBuddy] {name} collection failed: {message}");
            errors.push(SourceError {
                source: name.to_string(),
                message,
            });
            Vec::new()
        }
    }
}

/// Rows this round added, summed across every source. Named once so the CLI,
/// the dashboard status line and the persisted state can never quote three
/// different numbers for the same run.
pub fn imported_total(result: &SyncResult) -> u32 {
    result.claude_imported
        + result.opencode_imported
        + result.mimo_imported
        + result.zcode_imported
        + result.pi_imported
        + result.qoder_imported
        + result.workbuddy_imported
        + result.minimax_imported
        + result.hermes_imported
        + result.codex_imported
        + result.gemini_imported
        + result.qwen_imported
}

/// Row count straight from the parquet footer, without reading any column
/// data. 0 when the file does not exist yet.
/// True when the parquet file at `path` opens far enough to read its footer.
fn parquet_opens(path: &Path) -> bool {
    std::fs::File::open(path)
        .ok()
        .and_then(|file| ParquetRecordBatchReaderBuilder::try_new(file).ok())
        .is_some()
}

/// Newest `data.<stamp>.snap.parquet` in `dir` that actually opens. The
/// timestamped names sort lexicographically == chronologically, so the last
/// readable one is the freshest usable backup.
fn newest_readable_snapshot(dir: &Path) -> Option<PathBuf> {
    let mut snaps: Vec<PathBuf> = std::fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            let n = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
            n.starts_with("data.") && n.ends_with(".snap.parquet")
        })
        .collect();
    snaps.sort();
    snaps.into_iter().rev().find(|p| parquet_opens(p))
}

/// R5: if `path` exists but does not open as parquet, restore the newest
/// readable snapshot over it; with no usable snapshot, move the corrupt file
/// aside (kept for forensics) so the store starts fresh instead of failing
/// on every launch. Missing and healthy files pass untouched.
fn repair_corrupt_parquet(path: &Path) {
    if !path.exists() || parquet_opens(path) {
        return;
    }
    eprintln!(
        "[TokenBuddy] {} 无法读取(损坏或截断),尝试从快照恢复",
        path.display()
    );
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    if let Some(snap) = newest_readable_snapshot(dir) {
        let tmp = path.with_extension("parquet.recover");
        if std::fs::copy(&snap, &tmp).is_ok() && std::fs::rename(&tmp, path).is_ok() {
            eprintln!(
                "[TokenBuddy] 已从 {} 恢复 {} 行",
                snap.display(),
                parquet_row_count(path)
            );
            return;
        }
    }
    let aside = path.with_file_name(format!("data.corrupt-{}.parquet", crate::now_ts()));
    match std::fs::rename(path, &aside) {
        Ok(()) => eprintln!(
            "[TokenBuddy] 没有可读快照——损坏文件已移至 {};以空账本启动,来源日志仍在,可重新采集",
            aside.display()
        ),
        Err(e) => eprintln!("[TokenBuddy] 损坏文件无法移开({e}),将以损坏状态继续尝试"),
    }
}

fn parquet_row_count(path: &Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    std::fs::File::open(path)
        .ok()
        .and_then(|file| ParquetRecordBatchReaderBuilder::try_new(file).ok())
        .map(|builder| builder.metadata().file_metadata().num_rows().max(0) as u64)
        .unwrap_or(0)
}

fn sync_to_parquet(path: &Path, clear_existing: bool) -> Result<SyncResult> {
    let started = std::time::Instant::now();
    let mut errors: Vec<SourceError> = Vec::new();

    // Collected, absorbed and released one source at a time. Holding all eight
    // result vectors — plus every collector's own parse cache — at once is what
    // set a sync's high-water mark in the hundreds of MB on a real corpus; the
    // peak is now bounded by the largest *single* source.
    let mut existing: HashSet<String> = if clear_existing {
        HashSet::new()
    } else {
        read_existing_ids(path)?
    };

    let mut new_records: Vec<(String, TokenRecord)> = Vec::new();

    // Key formulas are frozen per source: rows already in `data.parquet` were
    // written with them, so changing one would re-import history as duplicates.
    // A collector that ships its own stable id (`record_id`) always wins.
    let (claude_imported, claude_skipped) = {
        let claude_records = collect_or_report(claude::collect_records, "claude", &mut errors);
        let counted = absorb(&claude_records, &mut existing, &mut new_records, |r| {
            format!("cl_{}_{}", r.timestamp, r.input_tokens)
        });
        // Free this source's parse cache before the next one allocates —
        // left alone, the caches alone add up to the whole corpus.
        claude::release_caches();
        counted
    };
    let (opencode_imported, opencode_skipped) = {
        let opencode_records =
            collect_or_report(opencode::collect_records, "opencode", &mut errors);
        let counted = absorb(&opencode_records, &mut existing, &mut new_records, |r| {
            format!("oc_{}_{}", r.timestamp, r.input_tokens)
        });
        // Free this source's parse cache before the next one allocates —
        // left alone, the caches alone add up to the whole corpus.
        opencode::release_caches();
        counted
    };
    let (mimo_imported, mimo_skipped) = {
        let mimo_records = collect_or_report(mimo::collect_records, "mimo", &mut errors);
        let counted = absorb(&mimo_records, &mut existing, &mut new_records, |r| {
            format!("mi_{}_{}", r.timestamp, r.input_tokens)
        });
        // Free this source's parse cache before the next one allocates —
        // left alone, the caches alone add up to the whole corpus.
        mimo::release_caches();
        counted
    };
    let (zcode_imported, zcode_skipped) = {
        let zcode_records = collect_or_report(zcode::collect_records, "zcode", &mut errors);
        let counted = absorb(&zcode_records, &mut existing, &mut new_records, |r| {
            format!(
                "zc_{}_{}_{}",
                r.session_id.as_deref().unwrap_or(""),
                r.timestamp,
                r.input_tokens
            )
        });
        // Free this source's parse cache before the next one allocates —
        // left alone, the caches alone add up to the whole corpus.
        zcode::release_caches();
        counted
    };
    let (pi_imported, pi_skipped) = {
        let pi_records = collect_or_report(pi::collect_records, "pi", &mut errors);
        let counted = absorb(&pi_records, &mut existing, &mut new_records, |r| {
            format!(
                "pi_{}_{}_{}",
                r.session_id.as_deref().unwrap_or(""),
                r.timestamp,
                r.input_tokens
            )
        });
        // Free this source's parse cache before the next one allocates —
        // left alone, the caches alone add up to the whole corpus.
        pi::release_caches();
        counted
    };
    // Qoder masks every token count, so the timestamp+tokens fallback key would
    // collapse whole sessions into one row; its request id is the key instead.
    let (qoder_imported, qoder_skipped) = {
        let qoder_records = collect_or_report(qoder::collect_records, "qoder", &mut errors);
        let counted = absorb(&qoder_records, &mut existing, &mut new_records, |r| {
            format!(
                "qo_{}_{}",
                r.session_id.as_deref().unwrap_or(""),
                r.timestamp
            )
        });
        // Free this source's parse cache before the next one allocates —
        // left alone, the caches alone add up to the whole corpus.
        qoder::release_caches();
        counted
    };
    let (workbuddy_imported, workbuddy_skipped) = {
        let workbuddy_records =
            collect_or_report(workbuddy::collect_records, "workbuddy", &mut errors);
        let counted = absorb(&workbuddy_records, &mut existing, &mut new_records, |r| {
            format!(
                "wb_{}_{}",
                r.session_id.as_deref().unwrap_or(""),
                r.timestamp
            )
        });
        // Free this source's parse cache before the next one allocates —
        // left alone, the caches alone add up to the whole corpus.
        workbuddy::release_caches();
        counted
    };
    // MiniMax ships a provider responseId on every assistant line, so
    // `absorb` uses it via `record_id`; the session key below only covers
    // lines where the id is missing.
    let (minimax_imported, minimax_skipped) = {
        let minimax_records = collect_or_report(minimax::collect_records, "minimax", &mut errors);
        let counted = absorb(&minimax_records, &mut existing, &mut new_records, |r| {
            format!(
                "mx_{}_{}",
                r.session_id.as_deref().unwrap_or(""),
                r.timestamp
            )
        });
        // Free this source's parse cache before the next one allocates —
        // left alone, the caches alone add up to the whole corpus.
        minimax::release_caches();
        counted
    };
    // Hermes is the one replace-per-sync source: its rows are aggregates the
    // gateway revises in place as a session grows, so a stored key would
    // freeze the first-imported sums. Drop the previous `hermes_*` keys so
    // every live row re-imports, and remember whether the file actually held
    // any — the rewrite below must also run when the collector now yields
    // nothing (the gateway pruned its history) so the bill can shrink with
    // the source. `hermes_skipped` therefore stays 0: replacement, not
    // skipping, is how an updated aggregate stays honest.
    let he_stale = {
        let before = existing.len();
        existing.retain(|k| !k.starts_with("hermes_"));
        before != existing.len()
    };
    let (hermes_imported, hermes_skipped) = {
        let hermes_records = collect_or_report(hermes::collect_records, "hermes", &mut errors);
        let counted = absorb(&hermes_records, &mut existing, &mut new_records, |r| {
            format!(
                "hm_{}_{}",
                r.session_id.as_deref().unwrap_or(""),
                r.timestamp
            )
        });
        // Free this source's parse cache before the next one allocates —
        // left alone, the caches alone add up to the whole corpus.
        hermes::release_caches();
        counted
    };
    // Codex events carry a stable per-response record id, so the fallback
    // key below is never used; it exists to satisfy the absorb signature.
    let (codex_imported, codex_skipped) = {
        let codex_records = collect_or_report(crate::codex::collect_records, "codex", &mut errors);
        let counted = absorb(&codex_records, &mut existing, &mut new_records, |r| {
            format!("cx_{}_{}", r.timestamp, r.input_tokens)
        });
        crate::codex::release_caches();
        counted
    };
    let (gemini_imported, gemini_skipped) = {
        let gemini_records =
            collect_or_report(crate::gemini::collect_records, "gemini", &mut errors);
        let counted = absorb(&gemini_records, &mut existing, &mut new_records, |r| {
            format!("ge_{}_{}", r.timestamp, r.input_tokens)
        });
        crate::gemini::release_caches();
        counted
    };
    let (qwen_imported, qwen_skipped) = {
        let qwen_records = collect_or_report(crate::qwen::collect_records, "qwen", &mut errors);
        let counted = absorb(&qwen_records, &mut existing, &mut new_records, |r| {
            format!("qw_{}_{}", r.timestamp, r.input_tokens)
        });
        crate::qwen::release_caches();
        counted
    };

    if !new_records.is_empty() || he_stale {
        let new_batch = records_to_batch(&new_records);
        let s = parquet_schema();
        // A full rebuild replaces the file, so it must not merge the stale rows
        // still on disk — `existing` was emptied, so every record is "new" here.
        let merged = if path.exists() && !clear_existing {
            let file = std::fs::File::open(path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
            let mut batches: Vec<RecordBatch> = reader
                .into_iter()
                .map(|b| align_batch(&b?, &s))
                .collect::<Result<_>>()?;
            // The old snapshot's hermes rows are superseded by this round's —
            // filter them out so replacement does not turn into duplication.
            if he_stale {
                for batch in batches.iter_mut() {
                    *batch = without_message_prefix(batch, "hermes_");
                }
                batches.retain(|b| b.num_rows() > 0);
            }
            if batches.is_empty() {
                new_batch
            } else {
                let mut all = batches;
                all.push(new_batch);
                concat_batches(&s, &all)?
            }
        } else {
            new_batch
        };
        write_parquet(path, &merged)?;
    }

    let total_after = parquet_row_count(path);

    Ok(SyncResult {
        claude_imported,
        claude_skipped,
        claude_files: 0,
        opencode_imported,
        opencode_skipped,
        mimo_imported,
        mimo_skipped,
        zcode_imported,
        zcode_skipped,
        pi_imported,
        pi_skipped,
        qoder_imported,
        qoder_skipped,
        workbuddy_imported,
        workbuddy_skipped,
        minimax_imported,
        minimax_skipped,
        hermes_imported,
        hermes_skipped,
        codex_imported,
        codex_skipped,
        gemini_imported,
        gemini_skipped,
        qwen_imported,
        qwen_skipped,
        errors,
        previous_total: None,
        total_after,
        duration_ms: started.elapsed().as_millis() as u64,
        mode: if clear_existing {
            "full"
        } else {
            "incremental"
        }
        .to_string(),
    })
}

/// Append the records of one source that aren't in `existing` yet to
/// `new_records`, returning `(imported, skipped)`.
///
/// `existing` is taken mutably because keys are inserted as they are claimed:
/// without that, two records in the *same* batch sharing a key would both land
/// in `new_records` and be written as duplicate rows. That matters most during
/// `sync_full`, where `existing` starts empty and every collected record is
/// therefore "new".
fn absorb<F: Fn(&TokenRecord) -> String>(
    records: &[TokenRecord],
    existing: &mut HashSet<String>,
    new_records: &mut Vec<(String, TokenRecord)>,
    key_of: F,
) -> (u32, u32) {
    let mut imported = 0;
    let mut skipped = 0;
    for r in records {
        let mid = r
            .record_id
            .clone()
            .map(|id| format!("{}_{}", r.source.as_str(), id))
            .unwrap_or_else(|| key_of(r));
        if existing.insert(mid.clone()) {
            imported += 1;
            new_records.push((mid, r.clone()));
        } else {
            skipped += 1;
        }
    }
    (imported, skipped)
}

/// Copy `batch`, dropping every row whose `message_id` starts with `prefix`.
///
/// Hermes replacement is the only caller: the arrow meta-crate ships without
/// compute features (binary-size budget), so there is no filter kernel to
/// reach for — the fixed schema is walked with plain builders instead.
fn without_message_prefix(batch: &RecordBatch, prefix: &str) -> RecordBatch {
    let mid = batch
        .column_by_name("message_id")
        .expect("message_id column exists")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("message_id is utf8");
    let keep: Vec<usize> = (0..batch.num_rows())
        .filter(|&i| !mid.value(i).starts_with(prefix))
        .collect();
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(batch.num_columns());
    for field in batch.schema_ref().fields() {
        let col = batch
            .column_by_name(field.name())
            .expect("field and column agree");
        let filtered: Arc<dyn Array> = match field.data_type() {
            DataType::Utf8 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8 column");
                let mut b = StringBuilder::with_capacity(keep.len(), batch.num_rows() * 10);
                for &i in &keep {
                    b.append_value(arr.value(i));
                }
                Arc::new(b.finish())
            }
            DataType::Int64 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 column");
                let mut b = Int64Builder::with_capacity(keep.len());
                for &i in &keep {
                    if arr.is_null(i) {
                        b.append_null();
                    } else {
                        b.append_value(arr.value(i));
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("float64 column");
                let mut b = Float64Builder::with_capacity(keep.len());
                for &i in &keep {
                    if arr.is_null(i) {
                        b.append_null();
                    } else {
                        b.append_value(arr.value(i));
                    }
                }
                Arc::new(b.finish())
            }
            other => panic!("without_message_prefix: unexpected column type {other:?}"),
        };
        columns.push(filtered);
    }
    RecordBatch::try_new(batch.schema(), columns).expect("rebuild of an aligned batch shape")
}

/// Pad columns that an older `data.parquet` predates so its batches can be
/// concatenated with rows written under the current schema.
fn align_batch(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let columns: Vec<Arc<dyn Array>> = schema
        .fields()
        .iter()
        .map(|field| {
            batch
                .column_by_name(field.name())
                .cloned()
                .unwrap_or_else(|| default_column(field.data_type(), n))
        })
        .collect();
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

fn default_column(data_type: &DataType, n: usize) -> Arc<dyn Array> {
    match data_type {
        DataType::Float64 => Arc::new(Float64Array::from(vec![0.0f64; n])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![0i64; n])),
        _ => {
            let mut b = StringBuilder::new();
            for _ in 0..n {
                b.append_null();
            }
            Arc::new(b.finish())
        }
    }
}

// --- Store ---

/// R8 — window facts: the subscription-quota view built from local facts
/// only. No spend estimates, no outbound calls: how much went through in the
/// currently-open 5h window and the trailing week, what the user's own 28-day
/// window peak looks like (P90 + max), and where the open window stands
/// against that reference at the current burn rate.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WindowFact {
    /// Epoch seconds the open window started (first request after the
    /// previous one expired).
    pub window_start: i64,
    pub window_tokens: u64,
    pub window_requests: u64,
    /// tokens/hour across the open window so far.
    pub burn_per_hour: f64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct WindowsFacts {
    /// Newest row in the store — "now" as far as the data can see.
    pub data_now: i64,
    /// The 5h window currently open, if any request falls inside it.
    pub open_window: Option<WindowFact>,
    /// Last 168h of tokens, mirroring a weekly limit's rolling shape.
    pub week_tokens: u64,
    pub week_requests: u64,
    /// 28 days of 5h windows: the 90th-percentile and the max sum. These are
    /// the user's own historical rhythm — a reference scale, not a quota.
    pub p90_5h_tokens: u64,
    pub max_5h_tokens: u64,
    /// open window tokens as a fraction of P90 (None when P90 is 0).
    pub p90_ratio: Option<f64>,
    /// Hours until the open window reaches P90 at the current burn rate.
    /// Negative = already past it. None = no burn yet.
    pub hours_to_p90: Option<f64>,
}

/// The subscription window this tool mirrors: five hours, first request opens
/// the next one.
pub const WINDOW_SECS: i64 = 5 * 3600;
/// How far back window history reaches for the P90 reference.
pub const WINDOW_HISTORY_DAYS: i64 = 28;

/// Segment rows (ascending (ts, tokens)) into 5h windows: a window opens at
/// its first request and expires WINDOW_SECS later; the next request after
/// expiry opens the following one. Returns (start_ts, tokens, requests) per
/// window, ascending.
/// Typed accessor for a named Int64 column of a batch.
fn int_col_named<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    let idx = b.schema().index_of(name)?;
    b.column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("{name} is not Int64"))
}

pub fn segment_windows(rows: &[(i64, u64)]) -> Vec<(i64, u64, u64)> {
    let mut out: Vec<(i64, u64, u64)> = Vec::new();
    for &(ts, tokens) in rows {
        match out.last_mut() {
            Some((start, sum, count)) if ts - *start < WINDOW_SECS => {
                *sum += tokens;
                *count += 1;
            }
            _ => out.push((ts, tokens, 1)),
        }
    }
    out
}

/// P90 over window sums, linear interpolation between adjacent order
/// statistics (the usual default so small samples still move the number).
fn percentile90(sums: &mut [u64]) -> u64 {
    if sums.is_empty() {
        return 0;
    }
    sums.sort_unstable();
    let n = sums.len();
    let rank = 0.9 * (n - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    let v = sums[lo] as f64 + (sums[hi] as f64 - sums[lo] as f64) * (rank - lo as f64);
    v as u64
}

/// R11 — daily usage anomalies via weekday-stratified robust z-scores.
/// Daily token volume has strong weekday periodicity, so each day is
/// compared only against same-weekday history (trailing 8 weeks). The
/// modified z-score `0.6745·(x−median)/MAD` (Iglewicz & Hoaglin) is robust
/// to the outliers it hunts: a spike never inflates its own baseline.
/// |z| > 3.5 is the standard flag threshold.
pub const ANOMALY_Z: f64 = 3.5;
/// Same-weekday samples required before a day can be judged.
pub const ANOMALY_MIN_BASELINE: usize = 3;
/// How far back the audit reaches.
pub const ANOMALY_HISTORY_DAYS: usize = 56;

#[derive(Debug, Clone, serde::Serialize)]
pub struct AnomalyDay {
    pub date: String,
    pub tokens: u64,
    pub baseline_median: u64,
    pub modified_z: f64,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
pub struct AnomalyReport {
    pub checked_days: usize,
    pub flagged: Vec<AnomalyDay>,
}

pub fn detect_daily_anomalies(days: &[(String, u64)]) -> AnomalyReport {
    use chrono::Datelike;
    let mut report = AnomalyReport {
        checked_days: days.len(),
        flagged: Vec::new(),
    };
    if days.len() <= ANOMALY_MIN_BASELINE {
        return report;
    }
    let weekday = |label: &str| {
        chrono::NaiveDate::parse_from_str(label, "%Y-%m-%d")
            .map(|d| d.weekday().num_days_from_monday())
            .ok()
    };

    for (i, (label, tokens)) in days.iter().enumerate() {
        let Some(wd) = weekday(label) else { continue };
        // Same-weekday samples strictly before this day, within the window.
        let history: Vec<u64> = days[..i]
            .iter()
            .filter(|(l, _)| weekday(l) == Some(wd))
            .map(|(_, t)| *t)
            .collect();
        if history.len() < ANOMALY_MIN_BASELINE {
            continue;
        }
        let mut sorted = history.clone();
        sorted.sort_unstable();
        let median = sorted[sorted.len() / 2] as f64;
        let mut devs: Vec<f64> = history.iter().map(|t| (*t as f64 - median).abs()).collect();
        devs.sort_by(|a, b| a.total_cmp(b));
        let mad = devs[devs.len() / 2];
        // Zero MAD with a nonzero deviation is itself the strongest signal.
        let z = if mad == 0.0 {
            if *tokens as f64 != median {
                f64::INFINITY
            } else {
                0.0
            }
        } else {
            0.6745 * (*tokens as f64 - median).abs() / mad
        };
        if z > ANOMALY_Z {
            report.flagged.push(AnomalyDay {
                date: label.clone(),
                tokens: *tokens,
                baseline_median: median as u64,
                modified_z: if z.is_infinite() { f64::MAX } else { z },
            });
        }
    }
    report
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PivotRow {
    pub project: String,
    pub model: String,
    pub tokens: u64,
    pub requests: u64,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
pub struct Pivot {
    pub rows: Vec<PivotRow>,
}

pub struct Store {
    parquet_path: PathBuf,
}

/// One source's footprint in the ledger (doctor's "did anything land" view).
pub struct SourceLedgerStat {
    pub source: String,
    pub rows: u64,
    pub latest_ts: i64,
}

impl Store {
    pub fn open() -> Result<Self> {
        let base = get_store_dir();
        std::fs::create_dir_all(&base)?;

        // Restrict permissions to owner-only (rwx------)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(metadata) = std::fs::metadata(&base) {
                let mut perms = metadata.permissions();
                perms.set_mode(0o700);
                let _ = std::fs::set_permissions(&base, perms);
            }
        }

        let parquet_path = base.join("data.parquet");

        // Migrate old DataFusion parquet if needed
        let old_df_path = base.join("df").join("token_records.parquet");
        if old_df_path.exists() && !parquet_path.exists() {
            eprintln!("[TokenBuddy] Migrating old DataFusion parquet to unified path");
            let _ = std::fs::rename(&old_df_path, &parquet_path);
        }

        // One-time cleanup: the DuckDB engine file outlived the engine —
        // every query now aggregates straight from the parquet.
        let _ = std::fs::remove_file(base.join("tokenbuddy.duckdb"));
        // R5: a corrupt store must never become a crash loop. A file that
        // cannot even be opened is repaired from the newest readable
        // snapshot, or moved aside for a fresh start — either way the server
        // comes up and reports what happened.
        repair_corrupt_parquet(&parquet_path);
        migrate_parquet_schema(&parquet_path)?;

        Ok(Self { parquet_path })
    }

    pub fn record_count(&self) -> Result<u64> {
        Ok(parquet_row_count(&self.parquet_path))
    }

    /// Per-source ledger presence for doctor: how many rows actually landed
    /// and how fresh the newest is. "The collector can read the logs" and
    /// "rows reached the ledger" are different facts — dedup and format
    /// misses are silent zeroes on the path between them.
    pub fn query_source_ledger_stats(&self) -> Result<Vec<SourceLedgerStat>> {
        let batches = if !self.parquet_path.exists() {
            vec![]
        } else {
            rust_read_agg_columns(&self.parquet_path)?
        };
        let mut acc: HashMap<String, (u64, i64)> = HashMap::new();
        for batch in &batches {
            for i in 0..batch.num_rows() {
                let src = col_str(batch, "source", i);
                let ts = col_i64(batch, "timestamp", i);
                let e = acc.entry(src.to_string()).or_insert((0, 0));
                e.0 += 1;
                if ts > e.1 {
                    e.1 = ts;
                }
            }
        }
        let mut out: Vec<SourceLedgerStat> = acc
            .into_iter()
            .map(|(source, (rows, latest_ts))| SourceLedgerStat {
                source,
                rows,
                latest_ts,
            })
            .collect();
        out.sort_by(|a, b| b.rows.cmp(&a.rows).then(a.source.cmp(&b.source)));
        Ok(out)
    }

    /// Directory holding `data.parquet`, `state.json` and the snapshots.
    fn base_dir(&self) -> PathBuf {
        self.parquet_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."))
    }

    /// Last completed sync, as recorded on disk. A missing or unreadable file
    /// reads as "never synced" rather than an error — the dashboard has to
    /// render on a first run, before anything has ever been written.
    pub fn state(&self) -> SyncState {
        std::fs::read_to_string(self.base_dir().join("state.json"))
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default()
    }

    /// Persist sync facts. Best effort by design: failing to write a
    /// bookkeeping file must not fail a sync whose data is already safe.
    fn write_state(&self, mode: &str, result: &SyncResult) {
        let state = SyncState {
            last_sync_at: Some(crate::now_ts()),
            last_sync_mode: Some(mode.to_string()),
            last_sync_imported: Some(imported_total(result)),
            last_sync_duration_ms: Some(result.duration_ms),
            last_sync_errors: result.errors.clone(),
        };
        match serde_json::to_vec_pretty(&state) {
            Ok(raw) => {
                if let Err(e) = std::fs::write(self.base_dir().join("state.json"), raw) {
                    eprintln!("[TokenBuddy] cannot write state.json: {e}");
                }
            }
            Err(e) => eprintln!("[TokenBuddy] cannot serialize state.json: {e}"),
        }
    }

    /// Per-collector presence, for the source-health panel and the first-run
    /// prompt. Pure filesystem checks — cheap enough to call on every load.
    pub fn source_status(&self) -> Vec<SourceHealth> {
        let probes: [(&str, &str, Option<PathBuf>); 12] = [
            ("claude", "Claude Code", claude::log_path()),
            ("codex", "Codex CLI", crate::codex::log_path()),
            ("gemini", "Gemini CLI", crate::gemini::log_path()),
            ("qwen", "Qwen Code", crate::qwen::log_path()),
            ("zcode", "ZCode", zcode::log_path()),
            ("qoder", "Qoder", qoder::log_path()),
            ("workbuddy", "WorkBuddy", workbuddy::log_path()),
            ("minimax", "MiniMax Code", minimax::log_path()),
            ("hermes", "Hermes Agent", hermes::log_path()),
            ("opencode", "OpenCode", opencode::log_path()),
            ("mimo", "Mimo", mimo::log_path()),
            ("pi", "Pi", pi::log_path()),
        ];
        probes
            .into_iter()
            .map(|(id, label, path)| SourceHealth {
                id: id.to_string(),
                label: label.to_string(),
                present: path.is_some(),
                path: path.map(|p| p.to_string_lossy().into_owned()),
            })
            .collect()
    }

    pub fn sync(&self) -> Result<SyncResult> {
        let result = sync_to_parquet(&self.parquet_path, false)?;
        self.write_state("incremental", &result);
        Ok(result)
    }

    /// Where this store's aggregate parquet lives (fleet push reads it).
    pub fn parquet_path(&self) -> &Path {
        &self.parquet_path
    }

    /// Rebuild from the collectors only. Rows whose source log has since been
    /// rotated away cannot be re-derived, so this drops history by design —
    /// the previous file is kept as a timestamped snapshot to make that
    /// recoverable.
    /// The snapshot is restored if the rebuild fails. `data.parquet` has
    /// already been moved aside by then, so letting the error propagate would
    /// leave the store looking empty — and `record_count()` would report 0 —
    /// while the data is in fact sitting intact in the snapshot file.
    pub fn sync_full(&self) -> Result<SyncResult> {
        eprintln!("[TokenBuddy] sync_full: rebuilding from scratch...");
        let snapshot = self.snapshot_parquet()?;
        // Counted off the snapshot, which is the store exactly as it was a
        // moment ago — the only way the UI can tell the user that a rebuild
        // shrank their history instead of implying it grew.
        let previous_total = snapshot.as_ref().map(|p| parquet_row_count(p));
        match sync_to_parquet(&self.parquet_path, true) {
            Ok(mut result) => {
                result.previous_total = previous_total;
                let lost = previous_total
                    .map(|before| before.saturating_sub(result.total_after))
                    .unwrap_or(0);
                if lost > 0 {
                    eprintln!(
                        "[TokenBuddy] sync_full: {lost} row(s) could not be re-derived from \
                         surviving logs (snapshot kept for recovery)"
                    );
                }
                let kept = self.prune_snapshots(SNAPSHOT_KEEP)?;
                if kept > 0 {
                    eprintln!("[TokenBuddy] sync_full: pruned {kept} old snapshot(s)");
                }
                self.write_state("full", &result);
                Ok(result)
            }
            Err(e) => {
                if let Some(snap) = snapshot {
                    eprintln!(
                        "[TokenBuddy] sync_full failed ({e}); restoring {}",
                        snap.display()
                    );
                    if let Err(restore) = std::fs::rename(&snap, &self.parquet_path) {
                        eprintln!(
                            "[TokenBuddy] sync_full: restore failed, data is still at {}: {restore}",
                            snap.display()
                        );
                    }
                }
                Err(e)
            }
        }
    }

    /// Move the current `data.parquet` aside, returning where it went — or
    /// `None` when there was no file to preserve.
    fn snapshot_parquet(&self) -> Result<Option<PathBuf>> {
        if !self.parquet_path.exists() {
            return Ok(None);
        }
        let stamp = chrono::Local::now().format("%Y%m%d-%H%M%S");
        let name = format!("data.{}.snap.parquet", stamp);
        let dest = self.parquet_path.with_file_name(name);
        std::fs::rename(&self.parquet_path, &dest)?;
        eprintln!(
            "[TokenBuddy] sync_full: snapshot kept at {}",
            dest.display()
        );
        Ok(Some(dest))
    }

    /// Keep only the newest `keep` snapshots; returns how many were removed.
    fn prune_snapshots(&self, keep: usize) -> Result<usize> {
        let dir = self
            .parquet_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let mut snaps: Vec<PathBuf> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                let n = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
                n.starts_with("data.") && n.ends_with(".snap.parquet")
            })
            .collect();
        // Timestamped names sort lexicographically == chronologically.
        snaps.sort();
        let mut removed = 0usize;
        if snaps.len() > keep {
            for path in snaps.iter().take(snaps.len() - keep) {
                if std::fs::remove_file(path).is_ok() {
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    pub fn query_summary(
        &self,
        source: Option<&str>,
        model: Option<&str>,
        date_start: Option<i64>,
        date_end: Option<i64>,
    ) -> Result<Summary> {
        let summary = if !self.parquet_path.exists() {
            empty_summary()
        } else {
            let batches = rust_read_agg_columns(&self.parquet_path)?;
            rust_compute_summary(&batches, source, model, date_start, date_end)?
        };
        Ok(summary)
    }

    pub fn query_timeline(
        &self,
        mode: TimelineMode,
        source: Option<&str>,
        model: Option<&str>,
        date_start: Option<i64>,
        date_end: Option<i64>,
    ) -> Result<Vec<TimelineBucket>> {
        let buckets = if !self.parquet_path.exists() {
            vec![]
        } else {
            let batches = rust_read_agg_columns(&self.parquet_path)?;
            rust_compute_timeline(&batches, mode, source, model, date_start, date_end)?
        };
        Ok(buckets)
    }

    /// Aggregate metrics (cache hit rate, avg duration, TTFT, output/input
    /// ratio, average tokens per request) for each source plus combined totals.
    pub fn query_metrics(
        &self,
        source: Option<&str>,
        model: Option<&str>,
        date_start: Option<i64>,
        date_end: Option<i64>,
    ) -> Result<Metrics> {
        // Latency and cache metrics need per-row percentiles, so this scans the
        // parquet directly instead of going through SQL.
        let batches = rust_read_agg_columns(&self.parquet_path)?;
        rust_compute_metrics(&batches, source, model, date_start, date_end)
    }

    /// Deep-analysis panels: hour-of-day rhythm, daily cache efficiency,
    /// top sessions and the context-window fill trend. One pass over the
    /// parquet; `session_limit` caps the session leaderboard (default 20).
    /// After the parquet pass, Qoder runtime-log counts (prompts / turns /
    /// tool calls) are merged into its sessions, and sessions that exist
    /// only in the runtime log — BYOK, billed by nobody — are appended so
    /// they stay visible instead of vanishing for having no billable row.
    pub fn query_insights(
        &self,
        source: Option<&str>,
        model: Option<&str>,
        date_start: Option<i64>,
        date_end: Option<i64>,
        session_limit: usize,
    ) -> Result<Insights> {
        let batches = rust_read_with_sessions(&self.parquet_path)?;
        let mut insights = rust_compute_insights(&batches, source, model, date_start, date_end)?;

        let runtime = crate::qoder::runtime_session_counts();
        let qoder_filtered = source.is_some_and(|s| s != "qoder");
        if !runtime.is_empty() {
            for row in insights.sessions.iter_mut() {
                if row.source == "qoder" {
                    if let Some(&(responses, prompts, turns, tools)) =
                        runtime.get(row.session_id.as_str())
                    {
                        // Runtime responses also count sidechain subagent
                        // calls, so the billable `requests` figure from the
                        // parquet stays authoritative when both exist.
                        let _ = responses;
                        row.user_prompts = Some(prompts);
                        row.turns = Some(turns);
                        row.tool_calls = Some(tools);
                    }
                }
            }
            if !qoder_filtered {
                for (sid, &(responses, prompts, turns, tools)) in runtime.iter() {
                    if insights
                        .sessions
                        .iter()
                        .any(|s| s.source == "qoder" && s.session_id == *sid)
                    {
                        continue;
                    }
                    if responses == 0 && prompts == 0 && turns == 0 && tools == 0 {
                        continue;
                    }
                    insights.sessions.push(SessionRow {
                        source: "qoder".to_string(),
                        session_id: sid.clone(),
                        requests: responses,
                        input_tokens: 0,
                        output_tokens: 0,
                        cache_read_tokens: 0,
                        cache_creation_tokens: 0,
                        total_tokens: 0,
                        credits: 0.0,
                        avg_context_ratio: None,
                        first_ts: 0,
                        last_ts: 0,
                        user_prompts: Some(prompts),
                        turns: Some(turns),
                        tool_calls: Some(tools),
                        from_runtime: true,
                    });
                }
                insights.sessions.sort_by(|a, b| {
                    b.total_tokens
                        .cmp(&a.total_tokens)
                        .then(b.credits.total_cmp(&a.credits))
                        .then(b.tool_calls.unwrap_or(0).cmp(&a.tool_calls.unwrap_or(0)))
                        .then(a.source.cmp(&b.source))
                        .then(a.session_id.cmp(&b.session_id))
                });
            }
        }
        // The compute pass deliberately leaves the list untruncated so the
        // runtime merge can reorder it; the cap applies to the final result
        // whichever branch ran.
        insights.sessions.truncate(session_limit.max(1));
        Ok(insights)
    }

    /// Heatmap of model × source or model × day, configurable metric.
    /// `metric` ∈ { "total_tokens", "requests", "avg_duration_ms" }.
    /// `mode` ∈ { "model_x_source", "model_x_day" }.
    pub fn query_heatmap(
        &self,
        mode: &str,
        metric: &str,
        source: Option<&str>,
        model: Option<&str>,
        date_start: Option<i64>,
        date_end: Option<i64>,
    ) -> Result<Heatmap> {
        let batches = rust_read_agg_columns(&self.parquet_path)?;
        rust_compute_heatmap(&batches, mode, metric, source, model, date_start, date_end)
    }

    /// Per-model comparison report: volume, latency percentiles, throughput
    /// and cache efficiency for every model in the filter window.
    /// Aggregates in memory for the same reason as
    /// `query_metrics`: the per-row percentiles are cheaper than SQL.
    /// R8: subscription-window facts from local rows only. Reads
    /// (timestamp, four token columns) for the last 28 days and segments
    /// them into 5h windows. See [`WindowsFacts`].
    pub fn query_windows(&self) -> Result<WindowsFacts> {
        const HISTORY: i64 = WINDOW_HISTORY_DAYS * 86_400;
        let empty = || WindowsFacts {
            data_now: 0,
            open_window: None,
            week_tokens: 0,
            week_requests: 0,
            p90_5h_tokens: 0,
            max_5h_tokens: 0,
            p90_ratio: None,
            hours_to_p90: None,
        };
        if !self.parquet_path.exists() {
            return Ok(empty());
        }
        let batches = rust_read_agg_columns(&self.parquet_path)?;
        let mut rows: Vec<(i64, u64)> = Vec::new();
        for b in &batches {
            let ts = b
                .column(b.schema().index_of("timestamp")?)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("timestamp is i64");
            let inp = int_col_named(b, "input_tokens")?;
            let out = int_col_named(b, "output_tokens")?;
            let cr = int_col_named(b, "cache_read_tokens")?;
            let cw = int_col_named(b, "cache_creation_tokens")?;
            for i in 0..b.num_rows() {
                let t = inp.value(i).max(0) as u64
                    + out.value(i).max(0) as u64
                    + cr.value(i).max(0) as u64
                    + cw.value(i).max(0) as u64;
                rows.push((ts.value(i), t));
            }
        }
        if rows.is_empty() {
            return Ok(empty());
        }
        rows.sort_by_key(|(ts, _)| *ts);
        let data_now = rows.last().map(|(ts, _)| *ts).unwrap_or(0);
        let horizon = data_now - HISTORY;

        let windows = segment_windows(&rows);
        let recent: Vec<(i64, u64, u64)> = windows
            .iter()
            .copied()
            .filter(|(start, _, _)| *start >= horizon)
            .collect();

        // Trailing week = requests in [data_now - 168h, data_now].
        let week_start = data_now - 168 * 3600;
        let week: (u64, u64) = rows
            .iter()
            .filter(|(ts, _)| *ts >= week_start)
            .fold((0, 0), |(t, c), (_, tok)| (t + tok, c + 1));

        let open = recent.last().copied();
        let open_window = open.map(|(start, tokens, requests)| {
            let hours = ((data_now - start).max(1)) as f64 / 3600.0;
            WindowFact {
                window_start: start,
                window_tokens: tokens,
                window_requests: requests,
                burn_per_hour: tokens as f64 / hours,
            }
        });

        let mut sums: Vec<u64> = recent.iter().map(|(_, t, _)| *t).collect();
        let p90 = percentile90(&mut sums);
        let max = sums.last().copied().unwrap_or(0);
        let (p90_ratio, hours_to_p90) = match (&open_window, p90) {
            (Some(w), p) if p > 0 => {
                let ratio = Some(w.window_tokens as f64 / p as f64);
                let hours = if w.burn_per_hour > 0.0 {
                    Some((p as f64 - w.window_tokens as f64) / w.burn_per_hour)
                } else {
                    None
                };
                (ratio, hours)
            }
            (Some(_), 0) => (None, None),
            _ => (None, None),
        };

        Ok(WindowsFacts {
            data_now,
            open_window,
            week_tokens: week.0,
            week_requests: week.1,
            p90_5h_tokens: p90,
            max_5h_tokens: max,
            p90_ratio,
            hours_to_p90,
        })
    }

    /// R11: audit the last 56 days for daily usage anomalies. Days are
    /// bucketed on China-local midnight (the same labels the dashboard uses).
    pub fn query_anomalies(&self) -> Result<AnomalyReport> {
        if !self.parquet_path.exists() {
            return Ok(AnomalyReport::default());
        }
        let batches = rust_read_agg_columns(&self.parquet_path)?;
        let horizon = crate::cn_midnight(ANOMALY_HISTORY_DAYS as i64);
        let mut by_day: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
        for b in &batches {
            let ts = b
                .column(b.schema().index_of("timestamp")?)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("timestamp is i64");
            let inp = int_col_named(b, "input_tokens")?;
            let out = int_col_named(b, "output_tokens")?;
            let cr = int_col_named(b, "cache_read_tokens")?;
            let cw = int_col_named(b, "cache_creation_tokens")?;
            for i in 0..b.num_rows() {
                let t = ts.value(i);
                if t < horizon {
                    continue;
                }
                *by_day.entry(crate::cn_day_label(t)).or_insert(0) += (inp.value(i).max(0)
                    + out.value(i).max(0)
                    + cr.value(i).max(0)
                    + cw.value(i).max(0))
                    as u64;
            }
        }
        let days: Vec<(String, u64)> = by_day.into_iter().collect();
        Ok(detect_daily_anomalies(&days))
    }

    /// R14: project × model attribution grid. Only rows whose source log
    /// named a project appear; the caller decides how to present the blank
    /// slice. Sorted by tokens descending.
    pub fn query_pivot(&self, date_start: Option<i64>, date_end: Option<i64>) -> Result<Pivot> {
        let mut grid: std::collections::HashMap<(String, String), (u64, u64)> =
            std::collections::HashMap::new();
        if self.parquet_path.exists() {
            let batches = rust_read_agg_columns(&self.parquet_path)?;
            for b in &batches {
                let ts = b
                    .column(b.schema().index_of("timestamp")?)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("timestamp is i64");
                let proj = b
                    .column(b.schema().index_of("project")?)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("project is utf8");
                let model = b
                    .column(b.schema().index_of("model")?)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("model is utf8");
                let inp = int_col_named(b, "input_tokens")?;
                let out = int_col_named(b, "output_tokens")?;
                let cr = int_col_named(b, "cache_read_tokens")?;
                let cw = int_col_named(b, "cache_creation_tokens")?;
                for i in 0..b.num_rows() {
                    if let Some(s) = date_start {
                        if ts.value(i) < s {
                            continue;
                        }
                    }
                    if let Some(e) = date_end {
                        if ts.value(i) >= e {
                            continue;
                        }
                    }
                    let project = if proj.is_null(i) { "" } else { proj.value(i) };
                    if project.is_empty() {
                        continue;
                    }
                    let tokens = (inp.value(i).max(0)
                        + out.value(i).max(0)
                        + cr.value(i).max(0)
                        + cw.value(i).max(0)) as u64;
                    let e = grid
                        .entry((project.to_string(), model.value(i).to_string()))
                        .or_insert((0, 0));
                    e.0 += tokens;
                    e.1 += 1;
                }
            }
        }
        let mut rows: Vec<PivotRow> = grid
            .into_iter()
            .map(|((project, model), (tokens, requests))| PivotRow {
                project,
                model,
                tokens,
                requests,
            })
            .collect();
        rows.sort_by_key(|r| std::cmp::Reverse(r.tokens));
        Ok(Pivot { rows })
    }

    pub fn query_models(
        &self,
        source: Option<&str>,
        model: Option<&str>,
        date_start: Option<i64>,
        date_end: Option<i64>,
    ) -> Result<ModelComparison> {
        let batches = rust_read_agg_columns(&self.parquet_path)?;
        rust_compute_models(&batches, source, model, date_start, date_end)
    }

    /// Hosts with locally pulled fleet data, sorted for a stable UI order.
    pub fn fleet_hosts(&self) -> Vec<String> {
        fleet_paths().into_iter().map(|(h, _)| h).collect()
    }

    /// Fleet-wide totals plus per-host rows and the host × source matrix.
    /// The pulled parquets are read with the usual aggregation columns plus
    /// an in-memory `host` column, so the single-machine aggregators do all
    /// the arithmetic; a host filter just narrows which files are read.
    pub fn query_fleet_summary(
        &self,
        host: Option<&str>,
        source: Option<&str>,
        model: Option<&str>,
        date_start: Option<i64>,
        date_end: Option<i64>,
    ) -> Result<FleetSummary> {
        query_fleet_summary_in(&fleet_base(), host, source, model, date_start, date_end)
    }

    /// Fleet-wide metrics keyed by host. Per host, the same aggregation as
    /// the local endpoint runs over that host's batches; the totals run
    /// over all of them combined.
    pub fn query_fleet_metrics(
        &self,
        host: Option<&str>,
        source: Option<&str>,
        model: Option<&str>,
        date_start: Option<i64>,
        date_end: Option<i64>,
    ) -> Result<FleetMetrics> {
        query_fleet_metrics_in(&fleet_base(), host, source, model, date_start, date_end)
    }

    /// Fleet-wide per-model comparison; identical semantics to the local
    /// one, with the hosts that used each model attached.
    pub fn query_fleet_models(
        &self,
        host: Option<&str>,
        source: Option<&str>,
        model: Option<&str>,
        date_start: Option<i64>,
        date_end: Option<i64>,
    ) -> Result<ModelComparison> {
        let base = fleet_base();
        let combined: Vec<RecordBatch> = read_fleet_batches(&base, host)?
            .iter()
            .flat_map(|(_, batches)| batches.iter().cloned())
            .collect();
        rust_compute_models(&combined, source, model, date_start, date_end)
    }
}

fn empty_summary() -> Summary {
    Summary {
        total_requests: 0,
        total_input_tokens: 0,
        total_output_tokens: 0,
        total_cache_read_tokens: 0,
        total_cache_creation_tokens: 0,
        total_tokens: 0,
        total_credits: 0.0,
        avg_context_ratio: None,
        by_source: vec![],
        by_model: vec![],
    }
}

/// Rewrite `data.parquet` when it was written by an older schema so the
/// aggregators, which name every column, work before the next sync.
fn migrate_parquet_schema(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let want = parquet_schema();
    let batches = rust_read_parquet(path)?;
    let outdated = batches.iter().any(|b| {
        b.schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .ne(want.fields().iter().map(|f| f.name()))
    });
    if !outdated {
        return Ok(());
    }
    eprintln!("[TokenBuddy] Migrating data.parquet to the current schema");
    let aligned: Vec<RecordBatch> = batches
        .iter()
        .map(|b| align_batch(b, &want))
        .collect::<Result<_>>()?;
    write_parquet(path, &concat_batches(&want, &aligned)?)
}

fn get_store_dir() -> PathBuf {
    crate::data_dir()
}

// ============================================================
// Rust parquet store — pure Rust aggregation, no SQL engine
// ============================================================

fn rust_read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
    if !path.exists() {
        return Ok(vec![]);
    }
    let file = std::fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Read only the columns the in-memory aggregators consume. `session_id` and
/// `message_id` are by far the widest fields and nothing in metrics, heatmap or
/// model comparison reads them, so projecting them out is where the memory
/// actually goes — a date filter applied after the full read would not.
fn rust_read_agg_columns(path: &Path) -> Result<Vec<RecordBatch>> {
    if !path.exists() {
        return Ok(vec![]);
    }
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let keep: Vec<usize> = builder
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !matches!(f.name().as_str(), "session_id" | "message_id"))
        .map(|(i, _)| i)
        .collect();
    let mask = ProjectionMask::roots(builder.parquet_schema(), keep);
    let reader = builder.with_projection(mask).build()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Like [`rust_read_agg_columns`] but keeps `session_id` for the session
/// leaderboard; only `message_id` (the widest, still-unused column) is
/// projected out. Only the insights endpoint needs it.
fn rust_read_with_sessions(path: &Path) -> Result<Vec<RecordBatch>> {
    if !path.exists() {
        return Ok(vec![]);
    }
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let keep: Vec<usize> = builder
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name() != "message_id")
        .map(|(i, _)| i)
        .collect();
    let mask = ProjectionMask::roots(builder.parquet_schema(), keep);
    let reader = builder.with_projection(mask).build()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

fn rust_compute_insights(
    batches: &[RecordBatch],
    source_filter: Option<&str>,
    model_filter: Option<&str>,
    ds: Option<i64>,
    de: Option<i64>,
) -> Result<Insights> {
    #[derive(Default)]
    struct CacheAcc {
        inp: u64,
        cr: u64,
    }
    #[derive(Default)]
    struct FillAcc {
        sum: f64,
        n: u64,
    }
    #[derive(Default)]
    struct SessionAcc {
        requests: u64,
        inp: u64,
        out: u64,
        cr: u64,
        cw: u64,
        credits: f64,
        ratio_sum: f64,
        ratio_n: u64,
        first_ts: i64,
        last_ts: i64,
    }

    let mut rhythm = vec![
        HourBucket {
            hour: 0,
            tokens: 0,
            requests: 0
        };
        24
    ];
    // BTreeMap keeps both day series ordered by label without a sort pass.
    let mut cache_days: std::collections::BTreeMap<String, CacheAcc> = Default::default();
    let mut fill_days: std::collections::BTreeMap<String, FillAcc> = Default::default();
    let mut sessions: HashMap<(String, String), SessionAcc> = HashMap::new();
    let mut cache_served_total: u64 = 0;

    for batch in batches {
        let n = batch.num_rows();
        for i in 0..n {
            let src = col_str(batch, "source", i);
            let mdl = col_str(batch, "model", i);
            let ts = col_i64(batch, "timestamp", i);
            if !row_passes(src, mdl, ts, source_filter, model_filter, ds, de) {
                continue;
            }
            let inp = col_i64(batch, "input_tokens", i) as u64;
            let out = col_i64(batch, "output_tokens", i) as u64;
            let cr = col_i64(batch, "cache_read_tokens", i) as u64;
            let cw = col_i64(batch, "cache_creation_tokens", i) as u64;
            let credits = col_f64(batch, "credits", i);
            let ratio = col_f64(batch, "context_ratio", i);
            let total = inp + out + cr + cw;

            // Hour-of-day in China time: the day boundary logic does not
            // matter here, only the local clock hour.
            let hour = (((ts + crate::CN_OFFSET_SECS).rem_euclid(86_400)) / 3_600) as usize;
            rhythm[hour].tokens += total;
            rhythm[hour].requests += 1;

            let day = crate::cn_day_label(ts);
            let c = cache_days.entry(day.clone()).or_default();
            c.inp += inp;
            c.cr += cr;
            cache_served_total += cr;

            if ratio > 0.0 {
                let f = fill_days.entry(day).or_default();
                f.sum += ratio;
                f.n += 1;
            }

            if let Some(sid) = batch
                .column_by_name("session_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .filter(|a| !a.is_null(i))
                .map(|a| a.value(i))
            {
                if !sid.is_empty() {
                    let s = sessions
                        .entry((src.to_string(), sid.to_string()))
                        .or_default();
                    s.requests += 1;
                    s.inp += inp;
                    s.out += out;
                    s.cr += cr;
                    s.cw += cw;
                    s.credits += credits;
                    if ratio > 0.0 {
                        s.ratio_sum += ratio;
                        s.ratio_n += 1;
                    }
                    if s.first_ts == 0 || ts < s.first_ts {
                        s.first_ts = ts;
                    }
                    if ts >= s.last_ts {
                        s.last_ts = ts;
                    }
                }
            }
        }
    }

    // The buckets were pre-allocated with hour=0; stamp the real hour in
    // (array index) so the serialized field is meaningful, not just position.
    for (i, b) in rhythm.iter_mut().enumerate() {
        b.hour = i as u32;
    }

    let mut session_rows: Vec<SessionRow> = sessions
        .into_iter()
        .map(|((source, session_id), s)| SessionRow {
            source,
            session_id,
            requests: s.requests,
            input_tokens: s.inp,
            output_tokens: s.out,
            cache_read_tokens: s.cr,
            cache_creation_tokens: s.cw,
            total_tokens: s.inp + s.out + s.cr + s.cw,
            credits: s.credits,
            avg_context_ratio: if s.ratio_n == 0 {
                None
            } else {
                Some(s.ratio_sum / s.ratio_n as f64)
            },
            first_ts: s.first_ts,
            last_ts: s.last_ts,
            user_prompts: None,
            turns: None,
            tool_calls: None,
            from_runtime: false,
        })
        .collect();
    // Credits break the token ties so masked sources (Qoder) still rank by
    // their real consumption instead of dropping to the bottom on 0 tokens;
    // tool activity is the last resort so BYOK sessions (no billing at all,
    // only runtime-log counts) sort above long-dead empty rows.
    session_rows.sort_by(|a, b| {
        b.total_tokens
            .cmp(&a.total_tokens)
            .then(b.credits.total_cmp(&a.credits))
            .then(b.tool_calls.unwrap_or(0).cmp(&a.tool_calls.unwrap_or(0)))
            .then(a.source.cmp(&b.source))
            .then(a.session_id.cmp(&b.session_id))
    });

    let cache_trend: Vec<CacheDay> = cache_days
        .into_iter()
        .map(|(label, a)| CacheDay {
            cache_hit_rate: if a.inp + a.cr > 0 {
                Some(a.cr as f64 / (a.inp + a.cr) as f64)
            } else {
                None
            },
            cache_read_tokens: a.cr,
            input_tokens: a.inp,
            label,
        })
        .collect();

    let context_fill_trend: Vec<ContextFillDay> = fill_days
        .into_iter()
        .map(|(label, f)| ContextFillDay {
            avg_ratio: Some(f.sum / f.n as f64),
            requests: f.n,
            label,
        })
        .collect();
    let fill_total: (f64, u64) = context_fill_trend.iter().fold((0.0, 0), |(s, n), d| {
        (
            s + d.avg_ratio.unwrap_or(0.0) * d.requests as f64,
            n + d.requests,
        )
    });
    let context_fill_avg = if fill_total.1 > 0 {
        Some(fill_total.0 / fill_total.1 as f64)
    } else {
        None
    };

    Ok(Insights {
        rhythm,
        cache_trend,
        cache_served_tokens: cache_served_total,
        sessions: session_rows,
        context_fill_trend,
        context_fill_avg,
    })
}

#[inline]
fn col_str<'a>(batch: &'a RecordBatch, name: &str, i: usize) -> &'a str {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .map(|a| a.value(i))
        .unwrap_or("")
}

#[inline]
fn col_i64_opt(batch: &RecordBatch, name: &str, i: usize) -> Option<i64> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .and_then(|a| if a.is_null(i) { None } else { Some(a.value(i)) })
}

#[inline]
fn col_i64(batch: &RecordBatch, name: &str, i: usize) -> i64 {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(i))
        .unwrap_or(0)
}

#[inline]
fn col_f64(batch: &RecordBatch, name: &str, i: usize) -> f64 {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .map(|a| a.value(i))
        .unwrap_or(0.0)
}

// --- Summary (totals + by source + by source×model) ---

/// Row filter shared by the engine-free aggregations: source equality,
/// substring model match (what the dashboard's model filter sends), and the
/// [start, end) timestamp window the HTTP filters describe.
#[inline]
fn row_passes(
    src: &str,
    mdl: &str,
    ts: i64,
    source: Option<&str>,
    model: Option<&str>,
    ds: Option<i64>,
    de: Option<i64>,
) -> bool {
    if let Some(s) = source {
        if src != s {
            return false;
        }
    }
    if let Some(m) = model {
        if !mdl.contains(m) {
            return false;
        }
    }
    if let Some(d) = ds {
        if ts < d {
            return false;
        }
    }
    if let Some(d) = de {
        if ts >= d {
            return false;
        }
    }
    true
}

#[derive(Default)]
struct SumAcc {
    requests: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
    credits: f64,
    ratio_sum: f64,
    ratio_count: u64,
}

impl SumAcc {
    #[inline]
    fn add(&mut self, inp: u64, out: u64, cr: u64, cw: u64, credits: f64, ratio: f64) {
        self.requests += 1;
        self.input_tokens += inp;
        self.output_tokens += out;
        self.cache_read_tokens += cr;
        self.cache_creation_tokens += cw;
        self.credits += credits;
        if ratio > 0.0 {
            self.ratio_sum += ratio;
            self.ratio_count += 1;
        }
    }
    fn total_tokens(&self) -> u64 {
        self.input_tokens + self.output_tokens + self.cache_read_tokens + self.cache_creation_tokens
    }
    fn avg_context_ratio(&self) -> Option<f64> {
        if self.ratio_count == 0 {
            None
        } else {
            Some(self.ratio_sum / self.ratio_count as f64)
        }
    }
}

fn rust_compute_summary(
    batches: &[RecordBatch],
    source_filter: Option<&str>,
    model_filter: Option<&str>,
    ds: Option<i64>,
    de: Option<i64>,
) -> Result<Summary> {
    let mut total = SumAcc::default();
    // BTreeMap: the API contract is rows ordered by source, matching the
    // ORDER BY the SQL path had.
    let mut by_source: std::collections::BTreeMap<String, SumAcc> =
        std::collections::BTreeMap::new();
    let mut by_model: HashMap<(String, String), SumAcc> = HashMap::new();

    for batch in batches {
        let n = batch.num_rows();
        for i in 0..n {
            let src = col_str(batch, "source", i);
            let mdl = col_str(batch, "model", i);
            let ts = col_i64(batch, "timestamp", i);
            if !row_passes(src, mdl, ts, source_filter, model_filter, ds, de) {
                continue;
            }
            let inp = col_i64(batch, "input_tokens", i) as u64;
            let out = col_i64(batch, "output_tokens", i) as u64;
            let cr = col_i64(batch, "cache_read_tokens", i) as u64;
            let cw = col_i64(batch, "cache_creation_tokens", i) as u64;
            let credits = col_f64(batch, "credits", i);
            let ratio = col_f64(batch, "context_ratio", i);
            total.add(inp, out, cr, cw, credits, ratio);
            by_source
                .entry(src.to_string())
                .or_default()
                .add(inp, out, cr, cw, credits, ratio);
            by_model
                .entry((src.to_string(), mdl.to_string()))
                .or_default()
                .add(inp, out, cr, cw, credits, ratio);
        }
    }

    let mut model_rows: Vec<ModelRow> = by_model
        .into_iter()
        .map(|((source, model), a)| ModelRow {
            source,
            model,
            requests: a.requests,
            input_tokens: a.input_tokens,
            output_tokens: a.output_tokens,
            cache_read_tokens: a.cache_read_tokens,
            cache_creation_tokens: a.cache_creation_tokens,
            total_tokens: a.total_tokens(),
            credits: a.credits,
        })
        .collect();
    // Total-volume desc, the SQL path's ORDER BY; the tie-break keeps the
    // order stable where the engine never promised one.
    model_rows.sort_by(|a, b| {
        b.total_tokens
            .cmp(&a.total_tokens)
            .then(a.source.cmp(&b.source))
            .then(a.model.cmp(&b.model))
    });

    Ok(Summary {
        total_requests: total.requests,
        total_input_tokens: total.input_tokens,
        total_output_tokens: total.output_tokens,
        total_cache_read_tokens: total.cache_read_tokens,
        total_cache_creation_tokens: total.cache_creation_tokens,
        total_tokens: total.total_tokens(),
        total_credits: total.credits,
        avg_context_ratio: total.avg_context_ratio(),
        by_source: by_source
            .into_iter()
            .map(|(source, a)| SourceRow {
                source,
                requests: a.requests,
                input_tokens: a.input_tokens,
                output_tokens: a.output_tokens,
                cache_read_tokens: a.cache_read_tokens,
                cache_creation_tokens: a.cache_creation_tokens,
                credits: a.credits,
                avg_context_ratio: a.avg_context_ratio(),
            })
            .collect(),
        by_model: model_rows,
    })
}

// --- Timeline buckets ---

/// The per-source split the timeline carries, in `TimelineBucket` field
/// order; the index into `Acc.src` comes from `TIMELINE_SOURCES`.
const TIMELINE_SOURCES: [&str; 9] = [
    "claude",
    "opencode",
    "mimo",
    "zcode",
    "pi",
    "qoder",
    "workbuddy",
    "minimax",
    "hermes",
];

#[derive(Default)]
struct TimelineAcc {
    requests: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
    src: [u64; 9],
}

/// Bucket label for one granularity, matching the SQL expressions the
/// engine-free path replaced: the epoch shifts to China time before
/// formatting so buckets break at 00:00 CST, and weekly follows strftime
/// `%W` — week 1 opens at the year's first Monday, earlier days are week 00.
fn bucket_label(mode: TimelineMode, ts: i64) -> String {
    use chrono::{Datelike, Timelike};
    let shifted = ts + crate::CN_OFFSET_SECS;
    let t = chrono::DateTime::from_timestamp(shifted, 0)
        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).expect("epoch is valid"))
        .naive_utc();
    match mode {
        TimelineMode::Hourly => format!("{} {:02}:00", t.format("%Y-%m-%d"), t.hour()),
        TimelineMode::Daily => t.format("%Y-%m-%d").to_string(),
        TimelineMode::Weekly => {
            let yday = t.ordinal0() as i64; // 0-based day of year
            let wday_m = t.weekday().num_days_from_monday() as i64;
            let week = (yday - wday_m + 7) / 7;
            format!("{}-W{:02}", t.year(), week)
        }
        TimelineMode::Monthly => t.format("%Y-%m").to_string(),
    }
}

fn rust_compute_timeline(
    batches: &[RecordBatch],
    mode: TimelineMode,
    source_filter: Option<&str>,
    model_filter: Option<&str>,
    ds: Option<i64>,
    de: Option<i64>,
) -> Result<Vec<TimelineBucket>> {
    let mut map: HashMap<String, TimelineAcc> = HashMap::new();
    for batch in batches {
        let n = batch.num_rows();
        for i in 0..n {
            let src = col_str(batch, "source", i);
            let mdl = col_str(batch, "model", i);
            let ts = col_i64(batch, "timestamp", i);
            if !row_passes(src, mdl, ts, source_filter, model_filter, ds, de) {
                continue;
            }
            let inp = col_i64(batch, "input_tokens", i) as u64;
            let out = col_i64(batch, "output_tokens", i) as u64;
            let cr = col_i64(batch, "cache_read_tokens", i) as u64;
            let cw = col_i64(batch, "cache_creation_tokens", i) as u64;
            let slot = TIMELINE_SOURCES.iter().position(|s| *s == src);
            let acc = map.entry(bucket_label(mode, ts)).or_default();
            acc.requests += 1;
            acc.input_tokens += inp;
            acc.output_tokens += out;
            acc.cache_read_tokens += cr;
            acc.cache_creation_tokens += cw;
            if let Some(k) = slot {
                acc.src[k] += inp + out + cr + cw;
            }
        }
    }
    // Label sort == chronological sort for every granularity's format.
    let mut labeled: Vec<(String, TimelineAcc)> = map.into_iter().collect();
    labeled.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(labeled
        .into_iter()
        .map(|(label, a)| TimelineBucket {
            label,
            requests: a.requests,
            input_tokens: a.input_tokens,
            output_tokens: a.output_tokens,
            cache_read_tokens: a.cache_read_tokens,
            cache_creation_tokens: a.cache_creation_tokens,
            total_tokens: a.input_tokens
                + a.output_tokens
                + a.cache_read_tokens
                + a.cache_creation_tokens,
            claude_tokens: a.src[0],
            opencode_tokens: a.src[1],
            mimo_tokens: a.src[2],
            zcode_tokens: a.src[3],
            pi_tokens: a.src[4],
            qoder_tokens: a.src[5],
            workbuddy_tokens: a.src[6],
            minimax_tokens: a.src[7],
            hermes_tokens: a.src[8],
        })
        .collect())
}

// --- Per-source aggregate metrics ---

struct SourceAcc {
    requests: u64,
    dur_sum: u128,
    dur_count: u64,
    ttft_sum: u128,
    ttft_count: u64,
    inp_sum: u128,
    out_sum: u128,
    cache_read_sum: u128,
    cache_in_total: u128, // cache_read_sum + inp_sum (input-side tokens)
    ratio_sum: f64,
    ratio_count: u64,
}

impl SourceAcc {
    fn new() -> Self {
        Self {
            requests: 0,
            dur_sum: 0,
            dur_count: 0,
            ttft_sum: 0,
            ttft_count: 0,
            inp_sum: 0,
            out_sum: 0,
            cache_read_sum: 0,
            cache_in_total: 0,
            ratio_sum: 0.0,
            ratio_count: 0,
        }
    }
    fn avg_duration(&self) -> Option<f64> {
        if self.dur_count == 0 {
            None
        } else {
            Some(self.dur_sum as f64 / self.dur_count as f64)
        }
    }
    fn avg_ttft(&self) -> Option<f64> {
        if self.ttft_count == 0 {
            None
        } else {
            Some(self.ttft_sum as f64 / self.ttft_count as f64)
        }
    }
    fn cache_hit_rate(&self) -> Option<f64> {
        if self.cache_in_total == 0 {
            None
        } else {
            Some(self.cache_read_sum as f64 / self.cache_in_total as f64)
        }
    }
    fn out_in_ratio(&self) -> Option<f64> {
        if self.inp_sum == 0 {
            None
        } else {
            Some(self.out_sum as f64 / self.inp_sum as f64)
        }
    }
    fn avg_input(&self) -> f64 {
        if self.requests == 0 {
            0.0
        } else {
            self.inp_sum as f64 / self.requests as f64
        }
    }
    fn avg_output(&self) -> f64 {
        if self.requests == 0 {
            0.0
        } else {
            self.out_sum as f64 / self.requests as f64
        }
    }
    fn avg_cache_read(&self) -> f64 {
        if self.requests == 0 {
            0.0
        } else {
            self.cache_read_sum as f64 / self.requests as f64
        }
    }
    fn avg_context_ratio(&self) -> Option<f64> {
        if self.ratio_count == 0 {
            None
        } else {
            Some(self.ratio_sum / self.ratio_count as f64)
        }
    }
}

// Internal aggregation helper on the hot read path; the argument list mirrors
// the parquet column order, a params struct would add a construction cost per
// row for no clarity gain.
#[allow(clippy::too_many_arguments)]
fn acc_record(
    acc: &mut SourceAcc,
    src: &str,
    mdl: &str,
    ts: i64,
    inp: u64,
    out: u64,
    cr: u64,
    cw: u64,
    dur: Option<i64>,
    ttft: Option<i64>,
    ratio: f64,
    source_filter: Option<&str>,
    model_filter: Option<&str>,
    ds: Option<i64>,
    de: Option<i64>,
) -> bool {
    if let Some(s) = source_filter {
        if src != s {
            return false;
        }
    }
    if let Some(m) = model_filter {
        if !mdl.contains(m) {
            return false;
        }
    }
    if let Some(d) = ds {
        if ts < d {
            return false;
        }
    }
    if let Some(d) = de {
        if ts >= d {
            return false;
        }
    }
    acc.requests += 1;
    acc.inp_sum += inp as u128;
    acc.out_sum += out as u128;
    acc.cache_read_sum += cr as u128;
    acc.cache_in_total += (cr + inp) as u128;
    if let Some(d) = dur {
        if d >= 0 {
            acc.dur_sum += d as u128;
            acc.dur_count += 1;
        }
    }
    if let Some(t) = ttft {
        if t >= 0 {
            acc.ttft_sum += t as u128;
            acc.ttft_count += 1;
        }
    }
    if ratio > 0.0 {
        acc.ratio_sum += ratio;
        acc.ratio_count += 1;
    }
    let _ = cw; // not used in current metrics
    true
}

fn finalize_metrics(map: HashMap<String, SourceAcc>) -> Vec<SourceMetrics> {
    let mut rows: Vec<SourceMetrics> = map
        .into_iter()
        .map(|(src, a)| SourceMetrics {
            source: src,
            requests: a.requests,
            avg_duration_ms: a.avg_duration(),
            avg_ttft_ms: a.avg_ttft(),
            cache_hit_rate: a.cache_hit_rate(),
            output_input_ratio: a.out_in_ratio(),
            avg_input_per_req: a.avg_input(),
            avg_output_per_req: a.avg_output(),
            avg_cache_read_per_req: a.avg_cache_read(),
            avg_context_ratio: a.avg_context_ratio(),
        })
        .collect();
    rows.sort_by(|a, b| a.source.cmp(&b.source));
    rows
}

fn combine_totals(map: &HashMap<String, SourceAcc>) -> TotalsMetrics {
    let mut t = SourceAcc::new();
    for a in map.values() {
        t.requests += a.requests;
        t.inp_sum += a.inp_sum;
        t.out_sum += a.out_sum;
        t.cache_read_sum += a.cache_read_sum;
        t.cache_in_total += a.cache_in_total;
        t.dur_sum += a.dur_sum;
        t.dur_count += a.dur_count;
        t.ttft_sum += a.ttft_sum;
        t.ttft_count += a.ttft_count;
        t.ratio_sum += a.ratio_sum;
        t.ratio_count += a.ratio_count;
    }
    TotalsMetrics {
        requests: t.requests,
        avg_duration_ms: t.avg_duration(),
        avg_ttft_ms: t.avg_ttft(),
        cache_hit_rate: t.cache_hit_rate(),
        output_input_ratio: t.out_in_ratio(),
        avg_input_per_req: t.avg_input(),
        avg_output_per_req: t.avg_output(),
    }
}

fn rust_compute_metrics(
    batches: &[RecordBatch],
    source_filter: Option<&str>,
    model_filter: Option<&str>,
    ds: Option<i64>,
    de: Option<i64>,
) -> Result<Metrics> {
    let mut map: HashMap<String, SourceAcc> = HashMap::new();
    for batch in batches {
        let n = batch.num_rows();
        for i in 0..n {
            let src = col_str(batch, "source", i);
            let mdl = col_str(batch, "model", i);
            let ts = col_i64(batch, "timestamp", i);
            let inp = col_i64(batch, "input_tokens", i) as u64;
            let out = col_i64(batch, "output_tokens", i) as u64;
            let cr = col_i64(batch, "cache_read_tokens", i) as u64;
            let cw = col_i64(batch, "cache_creation_tokens", i) as u64;
            let dur = col_i64_opt(batch, "duration_ms", i);
            let ttft = col_i64_opt(batch, "ttft_ms", i);
            let ratio = col_f64(batch, "context_ratio", i);
            let acc = map.entry(src.to_string()).or_insert_with(SourceAcc::new);
            acc_record(
                acc,
                src,
                mdl,
                ts,
                inp,
                out,
                cr,
                cw,
                dur,
                ttft,
                ratio,
                source_filter,
                model_filter,
                ds,
                de,
            );
        }
    }
    let totals = combine_totals(&map);
    Ok(Metrics {
        by_source: finalize_metrics(map),
        totals,
    })
}

// --- Heatmap (model × source or model × day) ---

fn rust_compute_heatmap(
    batches: &[RecordBatch],
    mode: &str,
    metric: &str,
    source_filter: Option<&str>,
    model_filter: Option<&str>,
    ds: Option<i64>,
    de: Option<i64>,
) -> Result<Heatmap> {
    // Use a HashMap keyed on (model, source) or (model, day).
    struct Acc {
        count: u64,
        dur_sum: f64,
        dur_count: u64,
        total_tokens: u64,
    }

    let mut map: HashMap<(String, String), Acc> = HashMap::new();
    let mut models: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut cols: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut days: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for batch in batches {
        let n = batch.num_rows();
        for i in 0..n {
            let src = col_str(batch, "source", i);
            let mdl = col_str(batch, "model", i);
            let ts = col_i64(batch, "timestamp", i);
            let inp = col_i64(batch, "input_tokens", i) as u64;
            let out = col_i64(batch, "output_tokens", i) as u64;
            let cr = col_i64(batch, "cache_read_tokens", i) as u64;
            let cw = col_i64(batch, "cache_creation_tokens", i) as u64;
            if let Some(s) = source_filter {
                if src != s {
                    continue;
                }
            }
            if let Some(m) = model_filter {
                if !mdl.contains(m) {
                    continue;
                }
            }
            if let Some(d) = ds {
                if ts < d {
                    continue;
                }
            }
            if let Some(d) = de {
                if ts >= d {
                    continue;
                }
            }

            let dur = col_i64_opt(batch, "duration_ms", i);
            let ttft = col_i64_opt(batch, "ttft_ms", i);
            let total = inp + out + cr + cw;

            let day = crate::cn_day_label(ts);

            let (col_key, row_key) = match mode {
                "model_x_day" => (day.clone(), mdl.to_string()),
                _ => (src.to_string(), mdl.to_string()), // model_x_source
            };

            let acc = map
                .entry((row_key.clone(), col_key.clone()))
                .or_insert(Acc {
                    count: 0,
                    dur_sum: 0.0,
                    dur_count: 0,
                    total_tokens: 0,
                });
            acc.count += 1;
            acc.total_tokens += total;
            if let Some(d) = dur {
                if d >= 0 {
                    acc.dur_sum += d as f64;
                    acc.dur_count += 1;
                }
            }
            let _ = ttft;

            models.insert(row_key);
            cols.insert(col_key.clone());
            if mode == "model_x_day" {
                days.insert(col_key);
            }
        }
    }

    let row_labels: Vec<String> = if mode == "model_x_day" {
        models.into_iter().collect()
    } else {
        // For model_x_source, sort rows by total token volume desc to put heavy hitters on top.
        let mut totals: HashMap<String, u64> = HashMap::new();
        for ((mdl, _), acc) in &map {
            *totals.entry(mdl.clone()).or_insert(0) += acc.total_tokens;
        }
        let mut v: Vec<_> = totals.into_iter().collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        v.into_iter().map(|(k, _)| k).collect()
    };

    let mut col_labels: Vec<String> = cols.into_iter().collect();
    if mode == "model_x_day" {
        col_labels.sort();
    } else {
        // Order: the sources this tool collects, then any others alphabetically.
        let order = |s: &str| match s {
            "claude" => 0,
            "opencode" => 1,
            "mimo" => 2,
            "zcode" => 3,
            "pi" => 4,
            "qoder" => 5,
            "workbuddy" => 6,
            "minimax" => 7,
            "hermes" => 8,
            _ => 9,
        };
        col_labels.sort_by_key(|c| (order(c), c.clone()));
    }

    let mut values: Vec<Vec<Option<f64>>> = Vec::new();
    let mut max_value: f64 = 0.0;
    for row in &row_labels {
        let mut row_vals = Vec::new();
        for col in &col_labels {
            let v = map
                .get(&(row.clone(), col.clone()))
                .and_then(|a| match metric {
                    "requests" => Some(a.count as f64),
                    "avg_duration_ms" => {
                        if a.dur_count > 0 {
                            Some(a.dur_sum / a.dur_count as f64)
                        } else {
                            None
                        }
                    }
                    "total_tokens" => Some(a.total_tokens as f64),
                    _ => Some(a.total_tokens as f64),
                });
            if let Some(x) = v {
                if x > max_value {
                    max_value = x;
                }
            }
            row_vals.push(v);
        }
        values.push(row_vals);
    }

    Ok(Heatmap {
        rows: row_labels,
        cols: col_labels.clone(),
        values,
        metric: metric.to_string(),
        mode: mode.to_string(),
        max_value,
        day_labels: if mode == "model_x_day" {
            col_labels
        } else {
            vec![]
        },
    })
}

// --- Per-model comparison (shared by local and fleet queries) ---

fn rust_compute_models(
    batches: &[RecordBatch],
    source_filter: Option<&str>,
    model_filter: Option<&str>,
    ds: Option<i64>,
    de: Option<i64>,
) -> Result<ModelComparison> {
    struct ModelAcc {
        sources: std::collections::BTreeSet<String>,
        /// Only populated when batches carry the injected fleet `host`
        /// column; local queries leave it empty.
        hosts: std::collections::BTreeSet<String>,
        requests: u64,
        inp: u64,
        out: u64,
        cr: u64,
        cw: u64,
        credits: f64,
        durs: Vec<u64>,
        ttfts: Vec<u64>,
        /// Sum of (output_tokens, duration_ms) pairs for aggregate
        /// throughput; both values only count rows that have a duration.
        out_with_dur: u64,
        dur_ms_with_out: u64,
    }

    let mut map: HashMap<String, ModelAcc> = HashMap::new();
    for batch in batches {
        let n = batch.num_rows();
        for i in 0..n {
            let src = col_str(batch, "source", i);
            let mdl = col_str(batch, "model", i);
            let ts = col_i64(batch, "timestamp", i);
            let inp = col_i64(batch, "input_tokens", i) as u64;
            let out = col_i64(batch, "output_tokens", i) as u64;
            let cr = col_i64(batch, "cache_read_tokens", i) as u64;
            let cw = col_i64(batch, "cache_creation_tokens", i) as u64;
            let credits = col_f64(batch, "credits", i);

            if !row_passes(src, mdl, ts, source_filter, model_filter, ds, de) {
                continue;
            }

            let acc = map.entry(mdl.to_string()).or_insert_with(|| ModelAcc {
                sources: Default::default(),
                hosts: Default::default(),
                requests: 0,
                inp: 0,
                out: 0,
                cr: 0,
                cw: 0,
                credits: 0.0,
                durs: Vec::new(),
                ttfts: Vec::new(),
                out_with_dur: 0,
                dur_ms_with_out: 0,
            });
            acc.requests += 1;
            acc.sources.insert(src.to_string());
            let host = col_str(batch, "host", i);
            if !host.is_empty() {
                acc.hosts.insert(host.to_string());
            }
            acc.inp += inp;
            acc.out += out;
            acc.cr += cr;
            acc.cw += cw;
            acc.credits += credits;
            if let Some(d) = col_i64_opt(batch, "duration_ms", i) {
                if d >= 0 {
                    acc.durs.push(d as u64);
                    acc.out_with_dur += out;
                    acc.dur_ms_with_out += d as u64;
                }
            }
            if let Some(t) = col_i64_opt(batch, "ttft_ms", i) {
                if t >= 0 {
                    acc.ttfts.push(t as u64);
                }
            }
        }
    }

    fn percentile(sorted: &[u64], p: f64) -> Option<f64> {
        if sorted.is_empty() {
            return None;
        }
        let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        Some(sorted[idx.min(sorted.len() - 1)] as f64)
    }

    let mut models: Vec<ModelStat> = map
        .into_iter()
        .map(|(mdl, a)| {
            let total_tokens = a.inp + a.out + a.cr + a.cw;
            let mut durs = a.durs;
            durs.sort_unstable();
            let mut ttfts = a.ttfts;
            ttfts.sort_unstable();
            let avg = |v: &[u64]| {
                if v.is_empty() {
                    None
                } else {
                    Some(v.iter().sum::<u64>() as f64 / v.len() as f64)
                }
            };
            ModelStat {
                family: crate::model_family(&mdl),
                sources: a.sources.into_iter().collect(),
                hosts: a.hosts.into_iter().collect(),
                requests: a.requests,
                input_tokens: a.inp,
                output_tokens: a.out,
                cache_read_tokens: a.cr,
                cache_creation_tokens: a.cw,
                total_tokens,
                credits: a.credits,
                avg_tokens_per_req: if a.requests > 0 {
                    total_tokens as f64 / a.requests as f64
                } else {
                    0.0
                },
                avg_duration_ms: avg(&durs),
                p95_duration_ms: percentile(&durs, 0.95),
                avg_ttft_ms: avg(&ttfts),
                p95_ttft_ms: percentile(&ttfts, 0.95),
                tokens_per_sec: if a.dur_ms_with_out > 0 {
                    Some(a.out_with_dur as f64 / (a.dur_ms_with_out as f64 / 1000.0))
                } else {
                    None
                },
                cache_hit_rate: if a.cr + a.inp > 0 {
                    Some(a.cr as f64 / (a.cr + a.inp) as f64)
                } else {
                    None
                },
                model: mdl,
            }
        })
        .collect();

    models.sort_by_key(|m| std::cmp::Reverse(m.total_tokens));

    Ok(ModelComparison { models })
}

// --- Fleet reads (pulled host parquets under ~/.tokenbuddy/fleet/) ---

fn fleet_base() -> PathBuf {
    crate::data_dir().join("fleet")
}

/// (host, parquet path) for every host pulled by fleet-sync, sorted by host.
/// A host directory only counts once its data.parquet has fully landed.
pub fn fleet_paths_in(base: &Path) -> Vec<(String, PathBuf)> {
    let mut out: Vec<(String, PathBuf)> = std::fs::read_dir(base)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let path = e.path().join("data.parquet");
            let host = e.file_name().to_str()?.to_string();
            path.exists().then_some((host, path))
        })
        .collect();
    out.sort();
    out
}

pub fn fleet_paths() -> Vec<(String, PathBuf)> {
    fleet_paths_in(&fleet_base())
}

/// Stamp every row of a pulled host parquet with an in-memory `host` column,
/// so the shared aggregators can group and filter by machine without the
/// column ever existing on disk.
fn with_host_column(batch: &RecordBatch, host: &str) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new("host", DataType::Utf8, false)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![host; n])));
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Read the pulled hosts (or just `host_filter`'s) with the aggregation
/// columns plus the injected host column: one (host, batches) pair per host.
fn read_fleet_batches(
    base: &Path,
    host_filter: Option<&str>,
) -> Result<Vec<(String, Vec<RecordBatch>)>> {
    let mut per_host = Vec::new();
    for (host, path) in fleet_paths_in(base) {
        if let Some(want) = host_filter {
            if host != want {
                continue;
            }
        }
        let batches = rust_read_agg_columns(&path)?
            .iter()
            .map(|b| with_host_column(b, &host))
            .collect::<Result<Vec<_>>>()?;
        per_host.push((host, batches));
    }
    Ok(per_host)
}

pub fn query_fleet_summary_in(
    base: &Path,
    host: Option<&str>,
    source: Option<&str>,
    model: Option<&str>,
    ds: Option<i64>,
    de: Option<i64>,
) -> Result<FleetSummary> {
    let per_host = read_fleet_batches(base, host)?;
    let mut combined: Vec<RecordBatch> = Vec::new();
    let mut by_host: Vec<FleetHostRow> = Vec::with_capacity(per_host.len());
    let mut host_source: Vec<FleetHostSourceCell> = Vec::new();

    for (host_name, batches) in &per_host {
        let s = rust_compute_summary(batches, source, model, ds, de)?;
        for src in &s.by_source {
            host_source.push(FleetHostSourceCell {
                host: host_name.clone(),
                source: src.source.clone(),
                requests: src.requests,
                total_tokens: src.input_tokens
                    + src.output_tokens
                    + src.cache_read_tokens
                    + src.cache_creation_tokens,
                credits: src.credits,
            });
        }
        by_host.push(FleetHostRow {
            host: host_name.clone(),
            requests: s.total_requests,
            input_tokens: s.total_input_tokens,
            output_tokens: s.total_output_tokens,
            cache_read_tokens: s.total_cache_read_tokens,
            cache_creation_tokens: s.total_cache_creation_tokens,
            total_tokens: s.total_tokens,
            credits: s.total_credits,
            avg_context_ratio: s.avg_context_ratio,
            sources: s.by_source.iter().map(|r| r.source.clone()).collect(),
        });
        combined.extend(batches.iter().cloned());
    }

    let totals_acc = rust_compute_summary(&combined, source, model, ds, de)?;
    let input_side = totals_acc.total_input_tokens + totals_acc.total_cache_read_tokens;
    let totals = FleetTotals {
        hosts: by_host.len(),
        requests: totals_acc.total_requests,
        input_tokens: totals_acc.total_input_tokens,
        output_tokens: totals_acc.total_output_tokens,
        cache_read_tokens: totals_acc.total_cache_read_tokens,
        cache_creation_tokens: totals_acc.total_cache_creation_tokens,
        total_tokens: totals_acc.total_tokens,
        credits: totals_acc.total_credits,
        cache_hit_rate: (input_side > 0)
            .then(|| totals_acc.total_cache_read_tokens as f64 / input_side as f64),
        avg_context_ratio: totals_acc.avg_context_ratio,
    };
    Ok(FleetSummary {
        totals,
        by_host,
        host_source,
    })
}

pub fn query_fleet_metrics_in(
    base: &Path,
    host: Option<&str>,
    source: Option<&str>,
    model: Option<&str>,
    ds: Option<i64>,
    de: Option<i64>,
) -> Result<FleetMetrics> {
    let per_host = read_fleet_batches(base, host)?;
    let mut by_host = Vec::with_capacity(per_host.len());
    let mut combined: Vec<RecordBatch> = Vec::new();
    for (host_name, batches) in &per_host {
        let m = rust_compute_metrics(batches, source, model, ds, de)?;
        by_host.push(FleetHostMetrics {
            host: host_name.clone(),
            requests: m.totals.requests,
            avg_duration_ms: m.totals.avg_duration_ms,
            avg_ttft_ms: m.totals.avg_ttft_ms,
            cache_hit_rate: m.totals.cache_hit_rate,
            output_input_ratio: m.totals.output_input_ratio,
            avg_input_per_req: m.totals.avg_input_per_req,
            avg_output_per_req: m.totals.avg_output_per_req,
        });
        combined.extend(batches.iter().cloned());
    }
    let totals = rust_compute_metrics(&combined, source, model, ds, de)?.totals;
    Ok(FleetMetrics { by_host, totals })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Source;

    /// R11: a flat weekday rhythm with one 10x spike flags exactly that day;
    /// short baselines never judge.
    #[test]
    fn anomaly_detection_flags_spikes_only() {
        let mut days: Vec<(String, u64)> = Vec::new();
        // Five Mondays, ~1000 tokens each; the sixth explodes.
        let base = chrono::NaiveDate::from_ymd_opt(2026, 7, 6).unwrap(); // a Monday
        for w in 0..5 {
            days.push((
                (base + chrono::Duration::weeks(w))
                    .format("%Y-%m-%d")
                    .to_string(),
                1000, // identical rhythm → MAD 0, the spike still flags
            ));
        }
        let spike_date = (base + chrono::Duration::weeks(5))
            .format("%Y-%m-%d")
            .to_string();
        days.push((spike_date.clone(), 10_000));

        let report = detect_daily_anomalies(&days);
        assert_eq!(report.checked_days, 6);
        assert_eq!(report.flagged.len(), 1, "only the spike flags");
        assert_eq!(report.flagged[0].date, spike_date);
        assert_eq!(report.flagged[0].baseline_median, 1000);
    }

    /// Weekday stratification: 10x above *Saturday* history is not judged by
    /// Monday's quiet rhythm.
    #[test]
    fn anomaly_baselines_are_weekday_stratified() {
        let monday = chrono::NaiveDate::from_ymd_opt(2026, 7, 6).unwrap();
        let saturday = chrono::NaiveDate::from_ymd_opt(2026, 7, 11).unwrap();
        let mut days: Vec<(String, u64)> = Vec::new();
        for w in 0..4 {
            days.push((
                (monday + chrono::Duration::weeks(w))
                    .format("%Y-%m-%d")
                    .to_string(),
                100_u64,
            ));
            days.push((
                (saturday + chrono::Duration::weeks(w))
                    .format("%Y-%m-%d")
                    .to_string(),
                9_000_u64,
            ));
        }
        let report = detect_daily_anomalies(&days);
        assert!(report.flagged.is_empty(), "each weekday judged by itself");
    }

    /// R8: 5h window segmentation — a window expires WINDOW_SECS after its
    /// first request; the next request opens a new one.
    #[test]
    fn segment_windows_groups_by_five_hour_expiry() {
        let five_h = WINDOW_SECS;
        let rows = vec![
            (1000, 100),
            (2000, 50),
            (1000 + five_h + 1, 7), // previous window expired → new one
            (1000 + five_h + 2, 3),
        ];
        let w = segment_windows(&rows);
        assert_eq!(w, vec![(1000, 150, 2), (1000 + five_h + 1, 10, 2)]);
    }

    #[test]
    fn percentile90_interpolates_and_handles_tiny_samples() {
        assert_eq!(percentile90(&mut []), 0);
        assert_eq!(percentile90(&mut [42]), 42);
        // [10, 20, 30, 100]: rank = 0.9*3 = 2.7 → 30 + 0.7*(100-30) = 79
        assert_eq!(percentile90(&mut [30, 10, 100, 20]), 79);
    }

    /// R8 end to end on a temp store: open window, week totals and the P90
    /// reference come out of the same parquet the dashboard reads.
    #[test]
    fn query_windows_reports_open_window_and_p90_reference() {
        let dir = std::env::temp_dir().join(format!("tb-win-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.parquet");

        let five_h = WINDOW_SECS;
        let base = 1_800_000_000;
        let mut records: Vec<(String, TokenRecord)> = Vec::new();
        let push = |ts: i64, tokens: u64, records: &mut Vec<(String, TokenRecord)>| {
            let r = TokenRecord {
                source: Source::Claude,
                model: "m".into(),
                input_tokens: tokens,
                output_tokens: 0,
                cache_read_tokens: 0,
                cache_creation_tokens: 0,
                timestamp: ts,
                session_id: None,
                project: String::new(),
                duration_ms: None,
                ttft_ms: None,
                credits: 0.0,
                context_ratio: 0.0,
                record_id: None,
                merge_key: None,
            };
            records.push((format!("k{ts}_{tokens}"), r));
        };
        push(base, 500, &mut records);
        push(base + 3600, 100, &mut records);
        push(base + five_h + 60, 200, &mut records);
        push(base - 29 * 86_400, 1_000_000, &mut records);
        let batch = records_to_batch(&records);
        write_parquet(&path, &batch).unwrap();

        let store = Store {
            parquet_path: path.clone(),
        };
        let facts = store.query_windows().expect("facts");

        assert_eq!(facts.data_now, base + five_h + 60);
        let open = facts.open_window.expect("window B is open");
        assert_eq!(open.window_start, base + five_h + 60);
        assert_eq!(open.window_tokens, 200);
        assert_eq!(facts.week_tokens, 800);
        assert_eq!(facts.week_requests, 3);
        assert_eq!(facts.max_5h_tokens, 600);
        // rank 0.9 over [200, 600] → 200 + 0.9*400 = 560
        assert_eq!(facts.p90_5h_tokens, 560);
        assert!(facts.p90_ratio.unwrap() < 1.0);
        assert!(facts.hours_to_p90.unwrap() > 0.0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn sample_records(n: usize) -> Vec<(String, TokenRecord)> {
        (0..n)
            .map(|i| {
                let r = TokenRecord {
                    source: Source::Claude,
                    model: "test-model".into(),
                    input_tokens: 10,
                    output_tokens: 5,
                    cache_read_tokens: 0,
                    cache_creation_tokens: 0,
                    timestamp: 1_788_874_500 + i as i64,
                    session_id: Some(format!("s{i}")),
                    project: String::new(),
                    duration_ms: Some(100),
                    ttft_ms: None,
                    credits: 0.0,
                    context_ratio: 0.0,
                    record_id: Some(format!("r{i}")),
                    merge_key: None,
                };
                (format!("k{i}"), r)
            })
            .collect()
    }

    /// R5: a corrupt data.parquet with a readable snapshot is restored from
    /// it; with no snapshot it is moved aside so the store starts fresh.
    #[test]
    fn corrupt_parquet_is_repaired_from_snapshot_or_moved_aside() {
        let dir = std::env::temp_dir().join(format!("tb-repair-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let data = dir.join("data.parquet");

        // Case 1: no snapshot — corrupt file is moved aside, path is free.
        std::fs::write(&data, b"definitely not parquet").unwrap();
        repair_corrupt_parquet(&data);
        assert!(!data.exists(), "corrupt file must be moved aside");
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            1,
            "kept for forensics"
        );

        // Case 2: a readable snapshot exists — it is copied over the store.
        let snap = dir.join("data.20260901-000000.snap.parquet");
        write_parquet(&snap, &records_to_batch(&sample_records(3))).unwrap();
        std::fs::write(&data, b"garbage again").unwrap();
        repair_corrupt_parquet(&data);
        assert!(parquet_opens(&data), "store must open after repair");
        assert_eq!(parquet_row_count(&data), 3, "restored snapshot rows");

        // Case 3: healthy file — untouched.
        repair_corrupt_parquet(&data);
        assert_eq!(parquet_row_count(&data), 3);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// R5: the newest readable snapshot wins — an older one is skipped, and a
    /// corrupt newest one is skipped in favour of an older readable one.
    #[test]
    fn newest_readable_snapshot_prefers_newest_that_opens() {
        let dir = std::env::temp_dir().join(format!("tb-snap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let old = dir.join("data.20260901-000000.snap.parquet");
        write_parquet(&old, &records_to_batch(&sample_records(1))).unwrap();
        // A newer snapshot that is itself corrupt must not be selected.
        let new_corrupt = dir.join("data.20260902-000000.snap.parquet");
        std::fs::write(&new_corrupt, b"junk").unwrap();

        let picked = newest_readable_snapshot(&dir).expect("old snapshot is readable");
        assert_eq!(picked, old);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn timeline_labels_match_the_sql_formats() {
        // 2026-09-08 21:35 CST (1_788_874_500 is 13:35 UTC).
        let ts = 1_788_874_500;
        assert_eq!(bucket_label(TimelineMode::Hourly, ts), "2026-09-08 21:00");
        assert_eq!(bucket_label(TimelineMode::Daily, ts), "2026-09-08");
        assert_eq!(bucket_label(TimelineMode::Weekly, ts), "2026-W36");
        assert_eq!(bucket_label(TimelineMode::Monthly, ts), "2026-09");
        // Buckets break at 00:00 CST, not UTC midnight: 15:59 UTC is still
        // 23:59 of the same CST day, 16:00 UTC opens the next one.
        assert_eq!(
            bucket_label(TimelineMode::Daily, 1_788_883_199),
            bucket_label(TimelineMode::Daily, ts)
        );
        assert_ne!(
            bucket_label(TimelineMode::Daily, 1_788_883_200),
            bucket_label(TimelineMode::Daily, ts)
        );
    }

    #[test]
    fn weekly_labels_follow_strftime_w_semantics_across_year_edges() {
        // %W: week 1 opens at the year's first Monday; earlier days are
        // week 00 of that calendar year. 2027-01-01 is a Friday → W00.
        let jan1_2027 = 1_798_761_600 + 12 * 3600;
        assert_eq!(bucket_label(TimelineMode::Weekly, jan1_2027), "2027-W00");
        // 2024-01-01 was a Monday → week 1 immediately.
        let jan1_2024 = 1_704_067_200 + 12 * 3600;
        assert_eq!(bucket_label(TimelineMode::Weekly, jan1_2024), "2024-W01");
        // 2026-01-01 was a Thursday → W00; first Monday 2026-01-05 → W01.
        let jan1_2026 = 1_767_225_600 + 12 * 3600;
        let jan5_2026 = 1_767_571_200 + 12 * 3600;
        assert_eq!(bucket_label(TimelineMode::Weekly, jan1_2026), "2026-W00");
        assert_eq!(bucket_label(TimelineMode::Weekly, jan5_2026), "2026-W01");
    }

    fn rec(source: Source, ts: i64, input: u64, record_id: Option<&str>) -> TokenRecord {
        TokenRecord {
            source,
            model: "test-model".to_string(),
            input_tokens: input,
            output_tokens: 1,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            timestamp: ts,
            session_id: Some("s1".to_string()),
            project: String::new(),
            duration_ms: None,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: record_id.map(|s| s.to_string()),
            merge_key: None,
        }
    }

    fn claude_key(r: &TokenRecord) -> String {
        format!("cl_{}_{}", r.timestamp, r.input_tokens)
    }

    /// One-pass sanity over the insights panels: CST hour bucketing, daily
    /// cache hit rate, session aggregation with the context fill the masked
    /// source reports.
    #[test]
    fn insights_bucket_hours_days_sessions_and_context_fill() {
        // 2026-09-08 13:35 UTC == 21:35 CST: the rhythm histogram must file
        // this under hour 21, not 13.
        let ts = 1_788_874_500;
        let mut q1 = rec(Source::Qoder, ts, 0, Some("q1"));
        q1.credits = 2.0;
        q1.context_ratio = 0.5;
        q1.session_id = Some("sess-q".to_string());
        let mut q2 = rec(Source::Qoder, ts + 120, 0, Some("q2"));
        q2.credits = 1.0;
        q2.context_ratio = 0.25;
        q2.session_id = Some("sess-q".to_string());
        let mut cl = rec(Source::Claude, ts + 60, 100, Some("c1"));
        cl.output_tokens = 50;
        cl.cache_read_tokens = 100;
        cl.session_id = Some("sess-c".to_string());

        let batch = records_to_batch(&[
            ("m1".to_string(), q1),
            ("m2".to_string(), q2),
            ("m3".to_string(), cl),
        ]);
        let d = rust_compute_insights(&[batch], None, None, None, None).unwrap();

        assert_eq!(d.rhythm.len(), 24);
        assert_eq!(d.rhythm[21].hour, 21, "hour field must carry the CST hour");
        // claude 100+50+100; the two qoder rows carry no tokens, but the
        // helper's default output_tokens=1 still counts → 250 + 2.
        assert_eq!(d.rhythm[21].tokens, 252);
        assert_eq!(d.rhythm[21].requests, 3);
        assert_eq!(d.rhythm[13].requests, 0, "UTC hour must not be used");

        assert_eq!(d.cache_trend.len(), 1);
        let day = &d.cache_trend[0];
        assert_eq!(day.label, "2026-09-08");
        // Input side 100 fresh + 100 cached → hit rate exactly 1/2.
        assert_eq!(day.cache_hit_rate, Some(0.5));
        assert_eq!(d.cache_served_tokens, 100);

        assert_eq!(d.sessions.len(), 2);
        // Tokens first, credits as the tiebreak for masked rows.
        assert_eq!(d.sessions[0].source, "claude");
        assert_eq!(d.sessions[1].source, "qoder");
        let qs = &d.sessions[1];
        assert_eq!(qs.requests, 2);
        assert_eq!(qs.credits, 3.0);
        assert_eq!(qs.avg_context_ratio, Some(0.375));
        assert_eq!(qs.first_ts, ts);
        assert_eq!(qs.last_ts, ts + 120);
        // The parquet path never fabricates runtime counts; the merge in
        // `query_insights` fills them, and flags BYOK-only rows instead.
        assert_eq!(qs.user_prompts, None);
        assert_eq!(qs.tool_calls, None);
        assert!(!qs.from_runtime);

        assert_eq!(d.context_fill_trend.len(), 1);
        assert_eq!(d.context_fill_trend[0].requests, 2);
        assert_eq!(d.context_fill_avg, Some(0.375));
    }

    /// Two records in the *same* batch resolving to the same key must not both
    /// be appended. `existing` used to be read-only here, so during
    /// `sync_full` — where it starts empty — genuine duplicates were written.
    #[test]
    fn absorb_dedupes_within_a_single_batch() {
        let records = vec![
            rec(Source::Claude, 1_700_000_000, 100, None),
            rec(Source::Claude, 1_700_000_000, 100, None),
            rec(Source::Claude, 1_700_000_000, 200, None),
        ];
        let mut existing = HashSet::new();
        let mut new_records = Vec::new();
        let (imported, skipped) = absorb(&records, &mut existing, &mut new_records, claude_key);

        assert_eq!((imported, skipped), (2, 1));
        assert_eq!(new_records.len(), 2);
    }

    /// Hermes replacement filters superseded rows out of the old parquet by
    /// `message_id` prefix; rows from every other source and every nullable
    /// column must survive the copy untouched.
    #[test]
    fn without_message_prefix_drops_only_prefixed_rows() {
        let mut updated = rec(Source::Hermes, 1_700_000_000, 500, None);
        updated.duration_ms = Some(4_200);
        let mut replaced = rec(Source::Hermes, 1_700_000_500, 300, None);
        replaced.duration_ms = None;
        let other = rec(Source::Claude, 1_700_001_000, 100, None);
        let batch = records_to_batch(&[
            ("hermes_s1|m|a|b|c|t".to_string(), updated),
            ("hermes_s1|m|a|b|c|t2".to_string(), replaced),
            ("cl_1700001000_100".to_string(), other),
        ]);

        let filtered = without_message_prefix(&batch, "hermes_");
        assert_eq!(filtered.num_rows(), 1);
        let src = filtered
            .column_by_name("source")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dur = filtered
            .column_by_name("duration_ms")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let inp = filtered
            .column_by_name("input_tokens")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(src.value(0), "claude");
        assert_eq!(inp.value(0), 100);
        assert!(dur.is_null(0), "other sources keep their null duration");

        // Filtering the other side leaves both hermes rows: the prefix is
        // what names the superseded set, everything else survives verbatim.
        let kept = without_message_prefix(&batch, "cl_");
        assert_eq!(kept.num_rows(), 2);
        let src2 = kept
            .column_by_name("source")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dur2 = kept
            .column_by_name("duration_ms")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(src2.value(0), "hermes");
        assert_eq!(src2.value(1), "hermes");
        assert_eq!(dur2.value(0), 4_200);
        assert!(
            dur2.is_null(1),
            "the superseded row's null duration is preserved"
        );
    }

    /// A collector's own `record_id` wins over the fallback key and is
    /// namespaced by source, so two sources sharing an id cannot collide.
    #[test]
    fn absorb_prefers_record_id_and_namespaces_by_source() {
        let records = vec![
            rec(Source::Zcode, 1_700_000_000, 100, Some("abc")),
            rec(Source::Pi, 1_700_000_000, 100, Some("abc")),
        ];
        let mut existing = HashSet::new();
        let mut new_records = Vec::new();
        let (imported, skipped) = absorb(&records, &mut existing, &mut new_records, claude_key);

        assert_eq!((imported, skipped), (2, 0));
        let keys: Vec<String> = new_records.iter().map(|(k, _)| k.clone()).collect();
        assert!(keys.contains(&"zcode_abc".to_string()));
        assert!(keys.contains(&"pi_abc".to_string()));
    }

    /// Pins the known weakness of the fallback key for sources that ship no
    /// `record_id`: same second + same input count collapses two distinct
    /// requests into one, and the second is dropped silently. This is the
    /// behaviour that makes `claude`/`opencode`/`mimo` under-count — kept
    /// visible rather than quietly assumed away.
    #[test]
    fn weak_fallback_key_collapses_same_second_same_input() {
        let records = vec![
            rec(Source::Claude, 1_700_000_000, 100, None),
            rec(Source::Claude, 1_700_000_000, 100, None),
        ];
        let mut existing = HashSet::new();
        let mut new_records = Vec::new();
        let (imported, skipped) = absorb(&records, &mut existing, &mut new_records, claude_key);

        assert_eq!(
            (imported, skipped),
            (1, 1),
            "the second distinct request was silently dropped"
        );
    }

    /// A key already present on disk is skipped, which is what makes
    /// incremental sync idempotent.
    #[test]
    fn absorb_skips_keys_already_in_store() {
        let records = vec![rec(Source::Claude, 1_700_000_000, 100, None)];
        let mut existing: HashSet<String> = ["cl_1700000000_100".to_string()].into_iter().collect();
        let mut new_records = Vec::new();
        let (imported, skipped) = absorb(&records, &mut existing, &mut new_records, claude_key);

        assert_eq!((imported, skipped), (0, 1));
        assert!(new_records.is_empty());
    }

    // --- Fleet aggregation ---

    fn temp_dir(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("tokenbuddy-store-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir should be creatable");
        dir
    }

    fn write_host_parquet(base: &Path, host: &str, records: &[(String, TokenRecord)]) {
        let dir = base.join(host);
        std::fs::create_dir_all(&dir).expect("host dir should be creatable");
        write_parquet(&dir.join("data.parquet"), &records_to_batch(records))
            .expect("golden host parquet should write");
    }

    /// Two fake hosts: alpha sends two plain claude requests, beta one
    /// request with a cache read and Qoder-style credits.
    fn golden_fleet(dir: &Path) -> Vec<(String, TokenRecord)> {
        let alpha = vec![
            (
                "a1".to_string(),
                rec(Source::Claude, 1_788_874_500, 100, None),
            ),
            (
                "a2".to_string(),
                rec(Source::Claude, 1_788_874_560, 200, None),
            ),
        ];
        let mut b1 = rec(Source::Claude, 1_788_874_520, 300, None);
        b1.cache_read_tokens = 150;
        b1.credits = 3.0;
        let beta = vec![("b1".to_string(), b1)];
        write_host_parquet(dir, "alpha", &alpha);
        write_host_parquet(dir, "beta", &beta);
        let mut all = alpha;
        all.extend(beta);
        all
    }

    /// Fleet totals must equal what a single-machine summary reports over
    /// the very same rows — the fleet path adds a dimension, not a formula.
    #[test]
    fn fleet_summary_totals_cross_check_against_single_machine_summary() {
        let dir = temp_dir("fleetsum");
        let all = golden_fleet(&dir);

        let local = Store {
            parquet_path: dir.join("all.parquet"),
        };
        write_parquet(&local.parquet_path, &records_to_batch(&all)).unwrap();
        let local_summary = local.query_summary(None, None, None, None).unwrap();

        let fleet = query_fleet_summary_in(&dir, None, None, None, None, None).unwrap();
        assert_eq!(fleet.totals.hosts, 2);
        assert_eq!(fleet.totals.requests, local_summary.total_requests);
        assert_eq!(fleet.totals.total_tokens, local_summary.total_tokens);
        assert_eq!(fleet.totals.input_tokens, local_summary.total_input_tokens);
        assert_eq!(
            fleet.totals.cache_read_tokens,
            local_summary.total_cache_read_tokens
        );
        assert_eq!(fleet.totals.credits, 3.0);
        // Input side: 600 fresh + 150 cached → hit rate exactly 0.2.
        assert_eq!(fleet.totals.cache_hit_rate, Some(0.2));
        // alpha = 100+1 + 200+1; beta = 300+150+1.
        assert_eq!(fleet.totals.total_tokens, 753);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fleet_summary_host_filter_and_matrix_cells() {
        let dir = temp_dir("fleethost");
        golden_fleet(&dir);

        let only_alpha =
            query_fleet_summary_in(&dir, Some("alpha"), None, None, None, None).unwrap();
        assert_eq!(only_alpha.totals.hosts, 1);
        assert_eq!(only_alpha.totals.requests, 2);
        assert_eq!(only_alpha.by_host.len(), 1);
        assert_eq!(only_alpha.by_host[0].host, "alpha");
        assert_eq!(only_alpha.by_host[0].total_tokens, 302);
        assert!(only_alpha.host_source.iter().all(|c| c.host == "alpha"));

        let full = query_fleet_summary_in(&dir, None, None, None, None, None).unwrap();
        assert_eq!(full.by_host.len(), 2);
        assert_eq!(full.by_host[0].host, "alpha");
        assert_eq!(full.by_host[1].host, "beta");
        assert_eq!(full.by_host[1].credits, 3.0);
        assert_eq!(full.by_host[1].sources, vec!["claude"]);
        let cells: Vec<(String, String, u64)> = full
            .host_source
            .iter()
            .map(|c| (c.host.clone(), c.source.clone(), c.requests))
            .collect();
        assert!(cells.contains(&("alpha".to_string(), "claude".to_string(), 2)));
        assert!(cells.contains(&("beta".to_string(), "claude".to_string(), 1)));
        // An unknown host filter matches nothing rather than everything.
        let none = query_fleet_summary_in(&dir, Some("ghost"), None, None, None, None).unwrap();
        assert_eq!(none.totals.requests, 0);
        assert!(none.by_host.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fleet_paths_require_a_landed_parquet_and_sort_by_host() {
        let dir = temp_dir("fleetpaths");
        let one = vec![(
            "x".to_string(),
            rec(Source::Claude, 1_788_874_500, 10, None),
        )];
        write_host_parquet(&dir, "beta", &one);
        // Directory created but parquet still mid-download (tmp name): ignored.
        std::fs::create_dir_all(dir.join("alpha")).unwrap();
        std::fs::write(dir.join("alpha").join("data.parquet.tmp"), b"junk").unwrap();

        let hosts: Vec<String> = fleet_paths_in(&dir).into_iter().map(|(h, _)| h).collect();
        assert_eq!(hosts, vec!["beta"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fleet_metrics_and_models_carry_host_dimension() {
        let dir = temp_dir("fleetmm");
        let all = golden_fleet(&dir);

        let metrics = query_fleet_metrics_in(&dir, None, None, None, None, None).unwrap();
        assert_eq!(metrics.by_host.len(), 2);
        assert_eq!(metrics.by_host[0].host, "alpha");
        assert_eq!(metrics.by_host[0].requests, 2);
        assert_eq!(metrics.by_host[1].host, "beta");
        assert_eq!(metrics.totals.requests, 3);

        let combined: Vec<RecordBatch> = read_fleet_batches(&dir, None)
            .unwrap()
            .into_iter()
            .flat_map(|(_, b)| b)
            .collect();
        let fleet_models = rust_compute_models(&combined, None, None, None, None).unwrap();
        assert_eq!(
            fleet_models.models.len(),
            1,
            "all golden rows share one model"
        );
        assert_eq!(fleet_models.models[0].hosts, vec!["alpha", "beta"]);
        assert_eq!(fleet_models.models[0].requests, 3);

        // The local path (no host column) must keep hosts empty, so the
        // /api/models JSON stays identical to pre-Fleet.
        let local_models =
            rust_compute_models(&[records_to_batch(&all)], None, None, None, None).unwrap();
        assert!(local_models.models[0].hosts.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
