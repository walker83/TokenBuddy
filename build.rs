// Build-time asset precompression: the embedded dashboard is compressed once
// here (zstd, high level) and shipped in the binary as compressed bytes, so
// zstd-capable clients get the compact form with zero per-request work and
// everyone else gets an in-memory decompression of the same bytes.

use std::env;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=src/dashboard.html");
    // 构建指纹（docs/build-fingerprint-proposal-2026-09-29.md / issue #17）：
    // version 只在发版时递增，一天内十几个 fix 提交不改变它；探查方需要
    // build_commit 才能知道自己在对哪个构建说话。git 不可用时降级 unknown，
    // 构建不因拿不到 commit 失败。
    let commit = std::process::Command::new("git")
        .args(["rev-parse", "--short=8", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=TOKENBUDDY_BUILD_COMMIT={commit}");
    println!(
        "cargo:rustc-env=TOKENBUDDY_BUILD_TIME={}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    );
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
