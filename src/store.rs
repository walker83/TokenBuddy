use crate::{claude, mimo, opencode, qoder, workbuddy, zcode, TokenRecord};
use anyhow::Result;
use arrow::array::{
    Array, Float64Array, Float64Builder, Int64Array, Int64Builder, RecordBatch, StringBuilder,
    StringArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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

    RecordBatch::try_new(s, vec![
        Arc::new(source_b.finish()) as Arc<dyn Array>,
        Arc::new(model_b.finish()),
        Arc::new(input_b.finish()),
        Arc::new(output_b.finish()),
        Arc::new(cache_read_b.finish()),
        Arc::new(cache_write_b.finish()),
        Arc::new(credits_b.finish()),
        Arc::new(ts_b.finish()),
        Arc::new(sid_b.finish()),
        Arc::new(mid_b.finish()),
        Arc::new(dur_b.finish()),
        Arc::new(ttft_b.finish()),
    ]).expect("record batch construction")
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
    let mut existing: HashSet<String> = if clear_existing {
        HashSet::new()
    } else {
        read_existing_ids(path)?
    };

    let mut new_records: Vec<(String, TokenRecord)> = Vec::new();

    // Key formulas are frozen per source: rows already in `data.parquet` were
    // written with them, so changing one would re-import history as duplicates.
    // A collector that ships its own stable id (`record_id`) always wins.
    let (claude_imported, claude_skipped) = absorb(
        &claude_records,
        &mut existing,
        &mut new_records,
        |r| format!("cl_{}_{}", r.timestamp, r.input_tokens),
    );
    let (opencode_imported, opencode_skipped) = absorb(
        &opencode_records,
        &mut existing,
        &mut new_records,
        |r| format!("oc_{}_{}", r.timestamp, r.input_tokens),
    );
    let (mimo_imported, mimo_skipped) = absorb(
        &mimo_records,
        &mut existing,
        &mut new_records,
        |r| format!("mi_{}_{}", r.timestamp, r.input_tokens),
    );
    let (zcode_imported, zcode_skipped) = absorb(
        &zcode_records,
        &mut existing,
        &mut new_records,
        |r| {
            format!(
                "zc_{}_{}_{}",
                r.session_id.as_deref().unwrap_or(""),
                r.timestamp,
                r.input_tokens
            )
        },
    );
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
    let (qoder_imported, qoder_skipped) = absorb(&qoder_records, &mut existing, &mut new_records, |r| {
        format!("qo_{}_{}", r.session_id.as_deref().unwrap_or(""), r.timestamp)
    });
    let (workbuddy_imported, workbuddy_skipped) = absorb(
        &workbuddy_records,
        &mut existing,
        &mut new_records,
        |r| format!("wb_{}_{}", r.session_id.as_deref().unwrap_or(""), r.timestamp),
    );

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
    duck_conn: Mutex<duckdb::Connection>,
    _db_path: PathBuf,
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

        let db_path = base.join("tokenbuddy.duckdb");
        let parquet_path = base.join("data.parquet");

        // Migrate old DataFusion parquet if needed
        let old_df_path = base.join("df").join("token_records.parquet");
        if old_df_path.exists() && !parquet_path.exists() {
            eprintln!("[TokenBuddy] Migrating old DataFusion parquet to unified path");
            let _ = std::fs::rename(&old_df_path, &parquet_path);
        }

        let conn = duckdb::Connection::open(&db_path)?;
        eprintln!("[TokenBuddy] Using DuckDB store");
        migrate_parquet_schema(&parquet_path)?;

        Ok(Self {
            duck_conn: Mutex::new(conn),
            _db_path: db_path,
            parquet_path,
        })
    }

    fn pq(&self) -> String {
        self.parquet_path.to_string_lossy().to_string()
    }

    pub fn record_count(&self) -> Result<u64> {
        if !self.parquet_path.exists() {
            return Ok(0);
        }
        let conn = self.duck_conn.lock().unwrap_or_else(|e| e.into_inner());
        let count: u64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM read_parquet('{}')", self.pq()),
            [],
            |row| row.get(0),
        )?;
        Ok(count)
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
                    eprintln!("[TokenBuddy] sync_full failed ({e}); restoring {}", snap.display());
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
        eprintln!("[TokenBuddy] sync_full: snapshot kept at {}", dest.display());
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

    pub fn query_summary(&self, source: Option<&str>, model: Option<&str>, date_start: Option<i64>, date_end: Option<i64>) -> Result<Summary> {
        let summary = if !self.parquet_path.exists() {
            empty_summary()
        } else {
            let conn = self.duck_conn.lock().unwrap_or_else(|e| e.into_inner());
            let from = format!("read_parquet('{}')", self.pq());
            let wc = build_where(source, model, date_start, date_end);

            let (total_requests, ti, to, tcr, tcc, credits): (u64, u64, u64, u64, u64, f64) = conn.query_row(
                &format!(
                    "SELECT COUNT(*), COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0),
                            COALESCE(SUM(cache_read_tokens),0), COALESCE(SUM(cache_creation_tokens),0),
                            COALESCE(SUM(credits),0.0)
                     FROM {from} {}", wc.sql
                ), duckdb::params_from_iter(&wc.params),
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
            )?;

            let mut by_source = Vec::new();
            {
                let mut stmt = conn.prepare(&format!(
                    "SELECT source, COUNT(*), COALESCE(SUM(input_tokens),0),
                            COALESCE(SUM(output_tokens),0), COALESCE(SUM(cache_read_tokens),0),
                            COALESCE(SUM(cache_creation_tokens),0), COALESCE(SUM(credits),0.0)
                     FROM {from} {} GROUP BY source ORDER BY source", wc.sql
                ))?;
                let rows = stmt.query_map(duckdb::params_from_iter(&wc.params), |row| {
                    Ok(SourceRow {
                        source: row.get(0)?,
                        requests: row.get::<_, i64>(1)? as u64,
                        input_tokens: row.get::<_, i64>(2)? as u64,
                        output_tokens: row.get::<_, i64>(3)? as u64,
                        cache_read_tokens: row.get::<_, i64>(4)? as u64,
                        cache_creation_tokens: row.get::<_, i64>(5)? as u64,
                        credits: row.get::<_, f64>(6)?,
                    })
                })?;
                for r in rows { by_source.push(r?); }
            }

            let mut by_model = Vec::new();
            {
                let mut stmt = conn.prepare(&format!(
                    "SELECT source, model, COUNT(*),
                            COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0),
                            COALESCE(SUM(cache_read_tokens),0), COALESCE(SUM(cache_creation_tokens),0),
                            COALESCE(SUM(input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens),0),
                            COALESCE(SUM(credits),0.0)
                     FROM {from} {} GROUP BY source, model ORDER BY 8 DESC", wc.sql
                ))?;
                let rows = stmt.query_map(duckdb::params_from_iter(&wc.params), |row| {
                    Ok(ModelRow {
                        source: row.get(0)?,
                        model: row.get(1)?,
                        requests: row.get::<_, i64>(2)? as u64,
                        input_tokens: row.get::<_, i64>(3)? as u64,
                        output_tokens: row.get::<_, i64>(4)? as u64,
                        cache_read_tokens: row.get::<_, i64>(5)? as u64,
                        cache_creation_tokens: row.get::<_, i64>(6)? as u64,
                        total_tokens: row.get::<_, i64>(7)? as u64,
                        credits: row.get::<_, f64>(8)?,
                    })
                })?;
                for r in rows { by_model.push(r?); }
            }

            Summary {
                total_requests,
                total_input_tokens: ti,
                total_output_tokens: to,
                total_cache_read_tokens: tcr,
                total_cache_creation_tokens: tcc,
                total_tokens: ti + to + tcr + tcc,
                total_credits: credits,
                by_source,
                by_model,
            }
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
            let conn = self.duck_conn.lock().unwrap_or_else(|e| e.into_inner());
            let from = format!("read_parquet('{}')", self.pq());
            let wc = build_where(source, model, date_start, date_end);
            duck_timeline_query(&conn, &from, &wc, mode)?
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

                if let Some(s) = source { if src != s { continue; } }
                if let Some(m) = model { if !mdl.contains(m) { continue; } }
                if let Some(d) = date_start { if ts < d { continue; } }
                if let Some(d) = date_end { if ts >= d { continue; } }

                let acc = map.entry(mdl.to_string()).or_insert_with(|| ModelAcc {
                    sources: Default::default(),
                    requests: 0, inp: 0, out: 0, cr: 0, cw: 0,
                    credits: 0.0,
                    durs: Vec::new(), ttfts: Vec::new(),
                    out_with_dur: 0, dur_ms_with_out: 0,
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
                    if v.is_empty() { None } else { Some(v.iter().sum::<u64>() as f64 / v.len() as f64) }
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
                    avg_tokens_per_req: if a.requests > 0 { total_tokens as f64 / a.requests as f64 } else { 0.0 },
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

        models.sort_by(|a, b| b.total_tokens.cmp(&a.total_tokens));

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

/// Filter conditions plus the bound values for parameterised queries.
struct WhereClause {
    sql: String,
    params: Vec<duckdb::types::Value>,
}

fn build_where(source: Option<&str>, model: Option<&str>, date_start: Option<i64>, date_end: Option<i64>) -> WhereClause {
    let mut conds = Vec::new();
    let mut params = Vec::new();
    if let Some(s) = source {
        conds.push("source = ?");
        params.push(duckdb::types::Value::Text(s.to_string()));
    }
    if let Some(m) = model {
        conds.push("model LIKE ?");
        params.push(duckdb::types::Value::Text(format!("%{m}%")));
    }
    if let Some(ds) = date_start {
        conds.push("timestamp >= ?");
        params.push(duckdb::types::Value::BigInt(ds));
    }
    if let Some(de) = date_end {
        conds.push("timestamp < ?");
        params.push(duckdb::types::Value::BigInt(de));
    }
    let sql = if conds.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conds.join(" AND "))
    };
    WhereClause { sql, params }
}

/// Rewrite `data.parquet` when it was written by an older schema so the SQL
/// queries, which name every column, work before the next sync.
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

// Bucket-label expressions shared by the aggregate timeline query so every
// granularity produces consistent labels.

/// Bucket label expression for the given granularity. The epoch is shifted by
/// the China offset *before* formatting so buckets break at 00:00 CST; the
/// stored `timestamp` column stays true UTC.
fn duck_trunc(mode: TimelineMode) -> String {
    let shift = format!("to_timestamp(timestamp + {})", crate::CN_OFFSET_SECS);
    let fmt = match mode {
        TimelineMode::Hourly => "'%Y-%m-%d %H:00'",
        TimelineMode::Daily => "'%Y-%m-%d'",
        TimelineMode::Weekly => "'%Y-W%W'",
        TimelineMode::Monthly => "'%Y-%m'",
    };
    format!("strftime(CAST({shift} AS TIMESTAMP), {fmt})")
}

fn duck_timeline_query(
    conn: &duckdb::Connection,
    from: &str,
    wc: &WhereClause,
    mode: TimelineMode,
) -> Result<Vec<TimelineBucket>> {
    let trunc = duck_trunc(mode);
    let sql = format!(
        "SELECT {trunc} as label,
                COUNT(*) as requests,
                COALESCE(SUM(input_tokens),0),
                COALESCE(SUM(output_tokens),0),
                COALESCE(SUM(cache_read_tokens),0),
                COALESCE(SUM(cache_creation_tokens),0),
                COALESCE(SUM(input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens),0),
                COALESCE(SUM(CASE WHEN source='claude' THEN input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN source='opencode' THEN input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN source='mimo' THEN input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN source='zcode' THEN input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN source='pi' THEN input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN source='qoder' THEN input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens ELSE 0 END),0),
                COALESCE(SUM(CASE WHEN source='workbuddy' THEN input_tokens+output_tokens+cache_read_tokens+cache_creation_tokens ELSE 0 END),0)
         FROM {from} {}
         GROUP BY label ORDER BY label",
        wc.sql
    );

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(duckdb::params_from_iter(&wc.params), |row| {
        Ok(TimelineBucket {
            label: row.get::<_, String>(0)?,
            requests: row.get::<_, i64>(1)? as u64,
            input_tokens: row.get::<_, i64>(2)? as u64,
            output_tokens: row.get::<_, i64>(3)? as u64,
            cache_read_tokens: row.get::<_, i64>(4)? as u64,
            cache_creation_tokens: row.get::<_, i64>(5)? as u64,
            total_tokens: row.get::<_, i64>(6)? as u64,
            claude_tokens: row.get::<_, i64>(7)? as u64,
            opencode_tokens: row.get::<_, i64>(8)? as u64,
            mimo_tokens: row.get::<_, i64>(9)? as u64,
            zcode_tokens: row.get::<_, i64>(10)? as u64,
            pi_tokens: row.get::<_, i64>(11)? as u64,
            qoder_tokens: row.get::<_, i64>(12)? as u64,
            workbuddy_tokens: row.get::<_, i64>(13)? as u64,
        })
    })?;

    let mut buckets = Vec::new();
    for r in rows {
        buckets.push(r?);
    }
    Ok(buckets)
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
        }
    }
    fn avg_duration(&self) -> Option<f64> {
        if self.dur_count == 0 { None } else { Some(self.dur_sum as f64 / self.dur_count as f64) }
    }
    fn avg_ttft(&self) -> Option<f64> {
        if self.ttft_count == 0 { None } else { Some(self.ttft_sum as f64 / self.ttft_count as f64) }
    }
    fn cache_hit_rate(&self) -> Option<f64> {
        if self.cache_in_total == 0 { None }
        else { Some(self.cache_read_sum as f64 / self.cache_in_total as f64) }
    }
    fn out_in_ratio(&self) -> Option<f64> {
        if self.inp_sum == 0 { None }
        else { Some(self.out_sum as f64 / self.inp_sum as f64) }
    }
    fn avg_input(&self) -> f64 {
        if self.requests == 0 { 0.0 } else { self.inp_sum as f64 / self.requests as f64 }
    }
    fn avg_output(&self) -> f64 {
        if self.requests == 0 { 0.0 } else { self.out_sum as f64 / self.requests as f64 }
    }
    fn avg_cache_read(&self) -> f64 {
        if self.requests == 0 { 0.0 } else { self.cache_read_sum as f64 / self.requests as f64 }
    }
}

fn acc_record(acc: &mut SourceAcc, src: &str, mdl: &str, ts: i64, inp: u64, out: u64, cr: u64, cw: u64, dur: Option<i64>, ttft: Option<i64>, source_filter: Option<&str>, model_filter: Option<&str>, ds: Option<i64>, de: Option<i64>) -> bool {
    if let Some(s) = source_filter { if src != s { return false; } }
    if let Some(m) = model_filter { if !mdl.contains(m) { return false; } }
    if let Some(d) = ds { if ts < d { return false; } }
    if let Some(d) = de { if ts >= d { return false; } }
    acc.requests += 1;
    acc.inp_sum += inp as u128;
    acc.out_sum += out as u128;
    acc.cache_read_sum += cr as u128;
    acc.cache_in_total += (cr + inp) as u128;
    if let Some(d) = dur { if d >= 0 { acc.dur_sum += d as u128; acc.dur_count += 1; } }
    if let Some(t) = ttft { if t >= 0 { acc.ttft_sum += t as u128; acc.ttft_count += 1; } }
    let _ = cw; // not used in current metrics
    true
}

fn finalize_metrics(map: HashMap<String, SourceAcc>) -> Vec<SourceMetrics> {
    let mut rows: Vec<SourceMetrics> = map.into_iter().map(|(src, a)| SourceMetrics {
        source: src,
        requests: a.requests,
        avg_duration_ms: a.avg_duration(),
        avg_ttft_ms: a.avg_ttft(),
        cache_hit_rate: a.cache_hit_rate(),
        output_input_ratio: a.out_in_ratio(),
        avg_input_per_req: a.avg_input(),
        avg_output_per_req: a.avg_output(),
        avg_cache_read_per_req: a.avg_cache_read(),
    }).collect();
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

fn rust_compute_metrics(batches: &[RecordBatch], source_filter: Option<&str>, model_filter: Option<&str>, ds: Option<i64>, de: Option<i64>) -> Result<Metrics> {
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
            let acc = map.entry(src.to_string()).or_insert_with(SourceAcc::new);
            acc_record(acc, src, mdl, ts, inp, out, cr, cw, dur, ttft, source_filter, model_filter, ds, de);
        }
    }
    let totals = combine_totals(&map);
    Ok(Metrics { by_source: finalize_metrics(map), totals })
}

// --- Heatmap (model × source or model × day) ---

fn rust_compute_heatmap(batches: &[RecordBatch], mode: &str, metric: &str, source_filter: Option<&str>, model_filter: Option<&str>, ds: Option<i64>, de: Option<i64>) -> Result<Heatmap> {
    // Use a HashMap keyed on (model, source) or (model, day).
    struct Acc { count: u64, dur_sum: f64, dur_count: u64, total_tokens: u64 }

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
            if let Some(s) = source_filter { if src != s { continue; } }
            if let Some(m) = model_filter { if !mdl.contains(m) { continue; } }
            if let Some(d) = ds { if ts < d { continue; } }
            if let Some(d) = de { if ts >= d { continue; } }

            let dur = col_i64_opt(batch, "duration_ms", i);
            let ttft = col_i64_opt(batch, "ttft_ms", i);
            let total = inp + out + cr + cw;

            let day = crate::cn_day_label(ts);

            let (col_key, row_key) = match mode {
                "model_x_day" => (day.clone(), mdl.to_string()),
                _ => (src.to_string(), mdl.to_string()), // model_x_source
            };

            let acc = map.entry((row_key.clone(), col_key.clone())).or_insert(Acc {
                count: 0, dur_sum: 0.0, dur_count: 0, total_tokens: 0,
            });
            acc.count += 1;
            acc.total_tokens += total;
            if let Some(d) = dur { if d >= 0 { acc.dur_sum += d as f64; acc.dur_count += 1; } }
            let _ = ttft;

            models.insert(row_key);
            cols.insert(col_key.clone());
            if mode == "model_x_day" { days.insert(col_key); }
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
            "claude" => 0, "opencode" => 1, "mimo" => 2, "zcode" => 3, "pi" => 4,
            "qoder" => 5, "workbuddy" => 6,
            _ => 7,
        };
        col_labels.sort_by_key(|c| (order(c), c.clone()));
    }

    let mut values: Vec<Vec<Option<f64>>> = Vec::new();
    let mut max_value: f64 = 0.0;
    for row in &row_labels {
        let mut row_vals = Vec::new();
        for col in &col_labels {
            let v = map.get(&(row.clone(), col.clone())).map(|a| {
                match metric {
                    "requests" => Some(a.count as f64),
                    "avg_duration_ms" => if a.dur_count > 0 { Some(a.dur_sum / a.dur_count as f64) } else { None },
                    "total_tokens" => Some(a.total_tokens as f64),
                    _ => Some(a.total_tokens as f64),
                }
            }).flatten();
            if let Some(x) = v { if x > max_value { max_value = x; } }
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
        } else { vec![] },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Source;

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
            record_id: record_id.map(|s| s.to_string()),
        }
    }

    fn claude_key(r: &TokenRecord) -> String {
        format!("cl_{}_{}", r.timestamp, r.input_tokens)
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
