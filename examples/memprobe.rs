//! One-off memory probe: builds the context index from the real parquet,
//! then opens the token store, pausing at each phase so the shell can sample
//! the process footprint. `cargo run --release --example memprobe` while
//! `while true; do footprint <pid>; sleep 1; done` runs alongside.

use std::time::Duration;

fn phase(name: &str) {
    eprintln!("=== PHASE {name} ===");
    std::thread::sleep(Duration::from_secs(4));
}

fn main() -> anyhow::Result<()> {
    let path = dirs::home_dir()
        .expect("home dir")
        .join(".tokenbuddy/context.parquet");

    phase("1-startup");
    let index = tokenbuddy::context::ContextIndex::build(&path)?;
    let stats = index.stats();
    eprintln!(
        "docs={} approx_bytes={}",
        stats.docs, stats.approx_bytes
    );
    phase("3-index-steady");
    drop(index);
    phase("4-index-dropped");

    let store = tokenbuddy::store::Store::open()?;
    let _ = store.query_summary(None, None, None, None)?;
    phase("5-store-open-plus-one-query");
    drop(store);
    phase("6-store-dropped");
    Ok(())
}
