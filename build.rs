// Build-time asset precompression: the embedded dashboard is compressed once
// here (zstd, high level) and shipped in the binary as compressed bytes, so
// zstd-capable clients get the compact form with zero per-request work and
// everyone else gets an in-memory decompression of the same bytes.

use std::env;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=src/dashboard.html");
    let src = Path::new("src/dashboard.html");
    let html = std::fs::read(src).expect("dashboard.html is readable");
    let out = env::var("OUT_DIR").expect("OUT_DIR set by cargo");
    let dest = Path::new(&out).join("dashboard.html.zstd");
    let compressed = zstd::stream::encode_all(&html[..], 19).expect("zstd compression");
    let before = html.len();
    let after = compressed.len();
    std::fs::write(dest, compressed).expect("write compressed asset");
    println!(
        "cargo:warning=dashboard asset: {} -> {} bytes ({}% of original)",
        before,
        after,
        (after as f64 / before as f64 * 100.0) as u64
    );
}
