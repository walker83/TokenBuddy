//! `cargo r --example dedupe_audit` — measure, on the real store, how often
//! the frozen fallback dedupe key (timestamp + input_tokens) collides for
//! distinct rows of sources that ship no stable record_id. A high collision
//! rate means the store is *under-counting* (distinct billable events folded
//! together); zero means the frozen key is empirically safe.
//!
//! Read-only: opens data.parquet and aggregates in memory.

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;

fn main() {
    let home = dirs::home_dir().expect("home dir");
    let path = home.join(".tokenbuddy/data.parquet");
    if !path.exists() {
        eprintln!("no store at {}", path.display());
        std::process::exit(1);
    }
    let file = std::fs::File::open(&path).expect("opens");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("parquet")
        .build()
        .expect("reader");

    // (source, timestamp, input_tokens) -> row count, plus per-source totals.
    let mut keys: HashMap<(String, i64, i64), usize> = HashMap::new();
    let mut total: HashMap<String, usize> = HashMap::new();

    for batch in reader {
        let batch = batch.expect("batch");
        let n = batch.num_rows();
        let col = |name: &str| {
            batch
                .schema()
                .index_of(name)
                .map(|i| batch.column(i).clone())
                .expect("column")
        };
        let source = col("source");
        let source = source
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("source is string");
        let ts = col("timestamp");
        let ts = ts
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("ts is i64");
        let input = col("input_tokens");
        let input = input
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("input is i64");

        for i in 0..n {
            let s = source.value(i).to_string();
            *total.entry(s.clone()).or_insert(0) += 1;
            *keys.entry((s, ts.value(i), input.value(i))).or_insert(0) += 1;
        }
    }

    // Collisions: keys carrying 2+ rows. (Rows that legitimately share the
    // key are under-counted by exactly rows-1.)
    let mut by_source: HashMap<String, (usize, usize)> = HashMap::new();
    for ((s, _, _), rows) in &keys {
        if *rows > 1 {
            let e = by_source.entry(s.clone()).or_insert((0, 0));
            e.0 += 1;
            e.1 += rows - 1;
        }
    }

    println!(
        "{:<10} {:>8} {:>10} {:>10} {:>8}",
        "source", "rows", "coll_keys", "lost_rows", "lost%"
    );
    let mut sources: Vec<_> = total.keys().cloned().collect();
    sources.sort();
    for s in &sources {
        let rows = total[s];
        let (ck, lost) = by_source.get(s).copied().unwrap_or((0, 0));
        println!(
            "{:<10} {:>8} {:>10} {:>10} {:>7.2}%",
            s,
            rows,
            ck,
            lost,
            lost as f64 * 100.0 / rows.max(1) as f64
        );
    }
}
