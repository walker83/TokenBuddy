use crate::{claude, mimo, minimax, opencode, qoder, workbuddy, zcode, TokenRecord};
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
}

/// Response shape for `/api/models`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelComparison {
    /// Sorted by total_tokens descending.
    pub models: Vec<ModelStat>,
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
        Field::new("message_id", DataType::Utf8, false),
        Field::new("duration_ms", DataType::Int64, true),
        Field::new("ttft_ms", DataType::Int64, true),
    ]))
}

fn records_to_batch(records: &[(String, TokenRecord)]) -> RecordBatch {
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

fn write_parquet(path: &Path, batch: &RecordBatch) -> Result<()> {
    let tmp = path.with_extension("parquet.tmp");
    let file = std::fs::File::create(&tmp)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

fn sync_to_parquet(path: &Path, clear_existing: bool) -> Result<SyncResult> {
    let claude_records = claude::collect_records()?;
    let opencode_records = opencode::collect_records()?;
    let mimo_records = mimo::collect_records()?;
    let zcode_records = zcode::collect_records()?;
    let pi_records = crate::pi::collect_records()?;
    let qoder_records = qoder::collect_records()?;
    let workbuddy_records = workbuddy::collect_records()?;
    let minimax_records = minimax::collect_records()?;
    let mut existing: HashSet<String> = if clear_existing {
        HashSet::new()
    } else {
        read_existing_ids(path)?
    };

    let mut new_records: Vec<(String, TokenRecord)> = Vec::new();

    // Key formulas are frozen per source: rows already in `data.parquet` were
    // written with them, so changing one would re-import history as duplicates.
    // A collector that ships its own stable id (`record_id`) always wins.
    let (claude_imported, claude_skipped) =
        absorb(&claude_records, &mut existing, &mut new_records, |r| {
            format!("cl_{}_{}", r.timestamp, r.input_tokens)
        });
    let (opencode_imported, opencode_skipped) =
        absorb(&opencode_records, &mut existing, &mut new_records, |r| {
            format!("oc_{}_{}", r.timestamp, r.input_tokens)
        });
    let (mimo_imported, mimo_skipped) =
        absorb(&mimo_records, &mut existing, &mut new_records, |r| {
            format!("mi_{}_{}", r.timestamp, r.input_tokens)
        });
    let (zcode_imported, zcode_skipped) =
        absorb(&zcode_records, &mut existing, &mut new_records, |r| {
            format!(
                "zc_{}_{}_{}",
                r.session_id.as_deref().unwrap_or(""),
                r.timestamp,
                r.input_tokens
            )
        });
    let (pi_imported, pi_skipped) = absorb(&pi_records, &mut existing, &mut new_records, |r| {
        format!(
            "pi_{}_{}_{}",
            r.session_id.as_deref().unwrap_or(""),
            r.timestamp,
            r.input_tokens
        )
    });
    // Qoder masks every token count, so the timestamp+tokens fallback key would
    // collapse whole sessions into one row; its request id is the key instead.
    let (qoder_imported, qoder_skipped) =
        absorb(&qoder_records, &mut existing, &mut new_records, |r| {
            format!(
                "qo_{}_{}",
                r.session_id.as_deref().unwrap_or(""),
                r.timestamp
            )
        });
    let (workbuddy_imported, workbuddy_skipped) =
        absorb(&workbuddy_records, &mut existing, &mut new_records, |r| {
            format!(
                "wb_{}_{}",
                r.session_id.as_deref().unwrap_or(""),
                r.timestamp
            )
        });
    // MiniMax ships a provider responseId on every assistant line, so
    // `absorb` uses it via `record_id`; the session key below only covers
    // lines where the id is missing.
    let (minimax_imported, minimax_skipped) =
        absorb(&minimax_records, &mut existing, &mut new_records, |r| {
            format!(
                "mx_{}_{}",
                r.session_id.as_deref().unwrap_or(""),
                r.timestamp
            )
        });

    if !new_records.is_empty() {
        let new_batch = records_to_batch(&new_records);
        let s = parquet_schema();
        // A full rebuild replaces the file, so it must not merge the stale rows
        // still on disk — `existing` was emptied, so every record is "new" here.
        let merged = if path.exists() && !clear_existing {
            let file = std::fs::File::open(path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
            let batches: Vec<RecordBatch> = reader
                .into_iter()
                .map(|b| align_batch(&b?, &s))
                .collect::<Result<_>>()?;
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

pub struct Store {
    parquet_path: PathBuf,
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
        migrate_parquet_schema(&parquet_path)?;

        Ok(Self { parquet_path })
    }

    pub fn record_count(&self) -> Result<u64> {
        if !self.parquet_path.exists() {
            return Ok(0);
        }
        let file = std::fs::File::open(&self.parquet_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        Ok(builder.metadata().file_metadata().num_rows().max(0) as u64)
    }

    pub fn sync(&self) -> Result<SyncResult> {
        sync_to_parquet(&self.parquet_path, false)
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
        match sync_to_parquet(&self.parquet_path, true) {
            Ok(result) => {
                let kept = self.prune_snapshots(SNAPSHOT_KEEP)?;
                if kept > 0 {
                    eprintln!("[TokenBuddy] sync_full: pruned {kept} old snapshot(s)");
                }
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
    pub fn query_models(
        &self,
        source: Option<&str>,
        model: Option<&str>,
        date_start: Option<i64>,
        date_end: Option<i64>,
    ) -> Result<ModelComparison> {
        let batches = rust_read_agg_columns(&self.parquet_path)?;

        struct ModelAcc {
            sources: std::collections::BTreeSet<String>,
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
        for batch in &batches {
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

                if let Some(s) = source {
                    if src != s {
                        continue;
                    }
                }
                if let Some(m) = model {
                    if !mdl.contains(m) {
                        continue;
                    }
                }
                if let Some(d) = date_start {
                    if ts < d {
                        continue;
                    }
                }
                if let Some(d) = date_end {
                    if ts >= d {
                        continue;
                    }
                }

                let acc = map.entry(mdl.to_string()).or_insert_with(|| ModelAcc {
                    sources: Default::default(),
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
const TIMELINE_SOURCES: [&str; 8] = [
    "claude",
    "opencode",
    "mimo",
    "zcode",
    "pi",
    "qoder",
    "workbuddy",
    "minimax",
];

#[derive(Default)]
struct TimelineAcc {
    requests: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
    src: [u64; 8],
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
            _ => 8,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Source;

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
            duration_ms: None,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: record_id.map(|s| s.to_string()),
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
}
