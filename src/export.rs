//! `tokenbuddy export` / `tokenbuddy import` — carry the whole local store
//! between machines with zero new dependencies.
//!
//! An export is a folder holding `data.parquet`, `context.parquet`,
//! `state.json` and a `manifest.json` (row count, sizes, sha256 of every
//! payload file). Import verifies the hashes before anything is touched and
//! refuses to overwrite a non-empty store without `--force`. Users who want
//! a single file can `tar czf` the folder — universal tooling, no container
//! format of our own to get wrong.

use anyhow::{bail, Context as _, Result};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub const MANIFEST: &str = "manifest.json";

/// The payload files an export carries, in fixed order.
/// Payload files copied verbatim into an export. `quota.json` carries the
/// collector config + the drain-curve history lives in `quota.jsonl` — both
/// are user data, so data sovereignty says they travel with the export.
/// Absent files (quota never configured) are skipped by the loop below.
const PAYLOAD: [&str; 5] = [
    "data.parquet",
    "context.parquet",
    "state.json",
    "quota.json",
    "quota.jsonl",
];

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path)?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

/// Export the store rooted at `source_dir` into `dest` (created; must be
/// empty or absent). Returns the folder and the exported row count.
pub fn export_to(source_dir: &Path, dest: &Path) -> Result<(PathBuf, u64)> {
    if dest.exists() {
        bail!("导出目录 {} 已存在——换一个目录或先删除", dest.display());
    }
    std::fs::create_dir_all(dest).with_context(|| format!("创建导出目录 {}", dest.display()))?;

    let data_parquet = source_dir.join("data.parquet");
    if !data_parquet.exists() {
        bail!(
            "还没有账本({} 不存在)——先跑一次同步再导出",
            data_parquet.display()
        );
    }

    let mut entries = serde_json::Map::new();
    let mut rows = 0u64;
    for name in PAYLOAD {
        let from = source_dir.join(name);
        if !from.exists() {
            continue;
        }
        let to = dest.join(name);
        std::fs::copy(&from, &to).with_context(|| format!("复制 {}", from.display()))?;
        if name == "data.parquet" {
            rows = tokenbuddy_row_count(&to);
        }
        entries.insert(
            name.to_string(),
            serde_json::json!({
                "bytes": std::fs::metadata(&to)?.len(),
                "sha256": sha256_file(&to)?,
            }),
        );
    }

    let manifest = serde_json::json!({
        "format": 1,
        "exported_at": crate::now_ts(),
        "rows": rows,
        "files": entries,
    });
    let manifest_path = dest.join(MANIFEST);
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)
        .with_context(|| format!("写 {}", manifest_path.display()))?;
    Ok((dest.to_path_buf(), rows))
}

/// Row count without reaching into the private store helpers: the parquet
/// footer knows. Failure reads as 0 — the manifest's sha256 is the real
/// integrity check; this number is informational.
fn tokenbuddy_row_count(path: &Path) -> u64 {
    std::fs::File::open(path)
        .ok()
        .and_then(|f| {
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f).ok()
        })
        .map(|b| b.metadata().file_metadata().num_rows().max(0) as u64)
        .unwrap_or(0)
}

/// Import a previously exported folder over the live store. `force` allows
/// replacing a store that already holds rows; without it, a non-empty store
/// is protected.
pub fn import_from(src: &Path, force: bool, dest_dir: &Path) -> Result<(u64, u64)> {
    let manifest_bytes = std::fs::read(src.join(MANIFEST))
        .with_context(|| format!("读取 {}", src.join(MANIFEST).display()))?;
    let manifest: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).context("manifest.json 不是合法 JSON")?;
    let files = manifest
        .get("files")
        .and_then(|f| f.as_object())
        .context("manifest 缺少 files 清单")?;

    // Verify every payload file against the manifest before touching the
    // live store — a corrupt import must never half-land.
    for (name, meta) in files {
        let expected = meta.get("sha256").and_then(|s| s.as_str()).unwrap_or("");
        let actual = sha256_file(&src.join(name))?;
        if actual != expected {
            bail!("{name} 校验和不符:损坏或不完整的导出");
        }
    }

    let existing = dest_dir.join("data.parquet");
    if !force && tokenbuddy_row_count(&existing) > 0 {
        bail!("当前账本已有数据——覆盖导入需要 --force(或先 tokenbuddy export 备份现有数据)");
    }

    let mut rows = 0u64;
    for name in PAYLOAD {
        let from = src.join(name);
        if !from.exists() {
            continue;
        }
        let to = dest_dir.join(name);
        let tmp = dest_dir.join(format!("{name}.import-tmp"));
        std::fs::copy(&from, &tmp).with_context(|| format!("暂存 {name}"))?;
        std::fs::rename(&tmp, &to).with_context(|| format!("落地 {}", to.display()))?;
        if name == "data.parquet" {
            rows = tokenbuddy_row_count(&to);
        }
    }
    let exported_rows = manifest.get("rows").and_then(|r| r.as_u64()).unwrap_or(0);
    Ok((exported_rows, rows))
}

#[cfg(test)]
mod tests {
    use super::{export_to, import_from, tokenbuddy_row_count, PAYLOAD};
    use std::path::Path;

    fn write_store(dir: &Path) {
        let records: Vec<(String, crate::TokenRecord)> = (0..3)
            .map(|i| {
                let r = crate::TokenRecord {
                    source: crate::Source::Claude,
                    model: "m".into(),
                    input_tokens: 10,
                    output_tokens: 1,
                    cache_read_tokens: 0,
                    cache_creation_tokens: 0,
                    timestamp: 1_788_874_500 + i,
                    session_id: None,
                    project: String::new(),
                    duration_ms: None,
                    ttft_ms: None,
                    credits: 0.0,
                    context_ratio: 0.0,
                    record_id: Some(format!("r{i}")),
                    sidechain: false,
                    merge_key: None,
                };
                (format!("k{i}"), r)
            })
            .collect();
        let batch = crate::store::records_to_batch(&records);
        crate::store::write_parquet(&dir.join("data.parquet"), &batch).unwrap();
        // quota 配置 + 消耗历史:用户数据,导出/导入都要带走。
        std::fs::write(dir.join("quota.json"), r#"{"collectors":[]}"#).unwrap();
        std::fs::write(dir.join("quota.jsonl"), "{\"kind\":\"snap\"}\n").unwrap();
    }

    #[test]
    fn export_import_roundtrip_preserves_rows_and_verifies_hashes() {
        let base = std::env::temp_dir().join(format!("tb-exp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let live = base.join("live");
        std::fs::create_dir_all(&live).unwrap();
        write_store(&live);

        let exported = base.join("exported");
        let (_, rows) = export_to(&live, &exported).expect("exports");
        assert_eq!(rows, 3);
        assert!(exported.join(PAYLOAD[0]).exists());
        assert!(exported.join(super::MANIFEST).exists());

        // A fresh empty target imports cleanly.
        let target = base.join("target");
        std::fs::create_dir_all(&target).unwrap();
        let (manifest_rows, imported_rows) =
            import_from(&exported, false, &target).expect("imports");
        assert_eq!(manifest_rows, 3);
        assert_eq!(imported_rows, 3);
        assert_eq!(tokenbuddy_row_count(&target.join("data.parquet")), 3);
        // quota 配置与消耗历史是用户数据,随导出走、随导入回。
        assert!(target.join("quota.json").exists(), "quota.json exported");
        assert!(
            target.join("quota.jsonl").exists(),
            "quota history exported"
        );

        // A corrupt export is refused before anything is touched.
        let corrupt = base.join("corrupt");
        export_to(&live, &corrupt).expect("second export");
        std::fs::write(corrupt.join("data.parquet"), b"garbage").unwrap();
        assert!(
            import_from(&corrupt, false, &target).is_err(),
            "hash mismatch refuses"
        );

        let _ = std::fs::remove_dir_all(&base);
    }
}
