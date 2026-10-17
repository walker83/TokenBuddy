//! Memory budget gate: `cargo run --release --example memgate`.
//!
//! The service's <100 MB footprint is the hardest invariant in the project
//! (CLAUDE.md), and the 16 GB target/ incident showed how quietly budgets
//! die. This gate builds the context index from a deterministic synthetic
//! corpus and fails if the process's peak RSS or the index's per-doc cost
//! blows the budget. Peak RSS comes from getrusage(RU_MAXRSS): a
//! high-water mark that needs no platform samplers.
//!
//! Ceilings are deliberately loose — this is a tripwire for regression
//! classes (an accidental full-corpus Vec, an uncompressed arena), not a
//! benchmark. See docs/iteration-plan-2026-09-28.md Phase E.

use std::time::Duration;

const DOCS: usize = 20_000;
/// Absolute peak-RSS ceiling for the whole run.
const PEAK_RSS_BUDGET_MB: u64 = 250;
/// Index arena bytes per doc (approx_bytes / docs) ceiling: real corpus ran
/// ~1.3 KB/doc; 3 KB/doc leaves 2x headroom before this trips.
const BYTES_PER_DOC_BUDGET: usize = 3 * 1024;

fn peak_rss_bytes() -> u64 {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        assert_eq!(
            libc::getrusage(libc::RUSAGE_SELF, &mut usage),
            0,
            "getrusage should not fail"
        );
        // macOS reports bytes; Linux reports KiB.
        if cfg!(target_os = "macos") {
            usage.ru_maxrss as u64
        } else {
            usage.ru_maxrss as u64 * 1024
        }
    }
}

fn mb(bytes: u64) -> u64 {
    bytes / 1024 / 1024
}

const WORDS: [&str; 32] = [
    "the",
    "quick",
    "brown",
    "fox",
    "jumps",
    "over",
    "lazy",
    "dog",
    "token",
    "ledger",
    "session",
    "index",
    "arena",
    "budget",
    "gate",
    "corpus",
    "cache",
    "window",
    "collector",
    "parquet",
    "search",
    "rank",
    "query",
    "terms",
    "posting",
    "delta",
    "varint",
    "digest",
    "merge",
    "resume",
    "compact",
    "agent",
];

fn main() -> anyhow::Result<()> {
    let corpus = dirs::home_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join(format!(".tokenbuddy-memgate-{}", std::process::id()));
    let parquet = corpus.join("context.parquet");
    let _ = std::fs::create_dir_all(&corpus);

    let base = peak_rss_bytes();
    eprintln!("baseline peak RSS: {} MB", mb(base));

    let mut messages = Vec::with_capacity(DOCS);
    for i in 0..DOCS {
        // Vary the vocabulary with the doc index: a purely repetitive corpus
        // compresses to an unrealistically small arena and the per-doc budget
        // would never trip.
        let word = |n: usize| WORDS[(i * 7 + n * 13) % WORDS.len()];
        let ascii: String = (0..24).map(|n| format!("{}{} ", word(n), i % 97)).collect();
        // ~600 chars of mixed ASCII + CJK per doc, mirroring real turns.
        let cjk: String = "本地优先的令牌账本与对话检索索引预算门禁".repeat(3);
        let text = format!("{ascii}{cjk} turn {i}");
        messages.push(tokenbuddy::context::ContextMessage {
            source: tokenbuddy::Source::Claude,
            session_id: format!("sess-{}", i % 500),
            role: if i % 2 == 0 { "user" } else { "assistant" },
            timestamp: 1_788_874_500 + i as i64,
            text,
            project: format!("/code/project-{}", i % 20),
            title: String::new(),
        });
    }

    let t0 = std::time::Instant::now();
    tokenbuddy::context::sync_messages(&parquet, true, messages)?;
    eprintln!(
        "corpus written: {} docs in {:.1}s",
        DOCS,
        t0.elapsed().as_secs_f64()
    );

    let index = tokenbuddy::context::ContextIndex::build(&parquet)?;
    let stats = index.stats();
    let per_doc = stats.approx_bytes / stats.docs.max(1);
    eprintln!(
        "index: {} docs, {} terms, {} grams, approx {} KB/doc, built in {:.1}s",
        stats.docs,
        stats.ascii_terms,
        stats.cjk_grams,
        per_doc / 1024,
        stats.build_ms as f64 / 1000.0
    );
    let peak = peak_rss_bytes();
    eprintln!("peak RSS: {} MB (budget {PEAK_RSS_BUDGET_MB} MB)", mb(peak));

    let mut failed = false;
    if mb(peak) > PEAK_RSS_BUDGET_MB {
        eprintln!(
            "FAIL: peak RSS {} MB over the {PEAK_RSS_BUDGET_MB} MB budget",
            mb(peak)
        );
        failed = true;
    }
    if per_doc > BYTES_PER_DOC_BUDGET {
        eprintln!(
            "FAIL: index costs {per_doc} bytes/doc, over the {BYTES_PER_DOC_BUDGET} B budget"
        );
        failed = true;
    }

    drop(index);
    // Keep the page alive briefly so a human watching RSS sees the dropped
    // state too; the asserts above are what CI reads.
    std::thread::sleep(Duration::from_millis(200));
    let _ = std::fs::remove_dir_all(&corpus);

    if failed {
        std::process::exit(1);
    }
    println!("memgate passed: peak {} MB, {} B/doc", mb(peak), per_doc);
    Ok(())
}
