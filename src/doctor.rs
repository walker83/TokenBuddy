//! `tokenbuddy doctor` — per-source diagnosis for the question behind most
//! support issues in this tool category: "why is my tool not counted?".
//!
//! Doctor never writes anything: it reports which log locations each
//! collector would scan, whether they exist and are readable, what the
//! newest file on disk is, and — for the SQLite-backed sources — which
//! expected tables the database actually contains (a schema change upstream
//! shows up as a missing table instead of a silent zero). Rows the sync
//! skips on purpose (the 1 MB blob bound) are counted, not guessed.

use crate::{context, Source};
use anyhow::Result;
use std::path::{Path, PathBuf};

#[derive(serde::Serialize)]
/// One scanned location: does it exist, can this user read it, how big, and
/// when did its newest entry change?
pub struct PathCheck {
    pub path: PathBuf,
    pub exists: bool,
    pub readable: bool,
    pub bytes: u64,
    pub newest_mtime: Option<i64>,
}

#[derive(serde::Serialize)]
/// Diagnosis for one source.
pub struct SourceDiagnosis {
    pub source: String,
    /// "jsonl" (file walkers) or "sqlite" (single database file).
    pub kind: &'static str,
    /// True when at least one candidate location exists.
    pub present: bool,
    pub paths: Vec<PathCheck>,
    /// SQLite sources only: tables found among the ones this collector
    /// SELECTs from. An empty list with an existing db file means the
    /// upstream schema moved — the collector will quietly import nothing.
    pub tables_found: Vec<String>,
    /// Rows over `SQLITE_BLOB_LIMIT` the sync skips by design.
    pub oversize_rows: Option<u64>,
    /// Rows this source has in the ledger and the newest record's epoch
    /// second (0 = none). The bridge between "logs readable" and "counted".
    pub ledger_rows: u64,
    pub ledger_latest_ts: i64,
    /// Issue #20: rows inside the settle window — the upstream may still
    /// backfill these (token totals, timestamps), so they can still change
    /// or, on fallback-keyed append-only sources, double-count. 0 well after
    /// the window means the tail is settled.
    #[serde(default)]
    pub unsettled_rows: u64,
    /// User switched this collector off (R102): sync never reads it.
    pub disabled: bool,
    pub hint: String,
}

impl SourceDiagnosis {
    /// One-line human verdict, the part people actually read.
    pub fn verdict(&self) -> String {
        if self.disabled {
            return "⏸ 已停用(⚙ 面板可重新开启;已入账的历史保留)".into();
        }
        if !self.present {
            return "未安装或未产生日志(没有找到任何目录)".into();
        }
        if self.kind == "sqlite"
            && self.paths.iter().any(|p| p.exists && p.readable)
            && self.tables_found.is_empty()
        {
            return "⚠ 数据库可读但预期的表都不在——上游 schema 可能已变更,采集将得 0 条".into();
        }
        if self.paths.iter().any(|p| p.exists && !p.readable) {
            return "⚠ 目录存在但不可读(权限?)".into();
        }
        if self.ledger_rows == 0 {
            return "⚠ 日志可读,但账本里这个源 0 行——可能全部被去重跳过,或格式判据未命中".into();
        }
        "✓ 日志在,采集器可以读到".into()
    }
}

/// SQLite sources: which tables the collector SELECTs from. Keep in sync with
/// the collectors' SQL (opencode has two schema generations, v1 `message` and
/// v2 `session_v2` — either counts).
fn expected_tables(source: Source) -> &'static [&'static str] {
    match source {
        Source::Zcode => &["model_usage", "messages"],
        Source::Hermes => &["session_model_usage", "sessions"],
        Source::OpenCode => &["session_v2", "message"],
        Source::Mimo => &["message", "session_v"],
        _ => &[],
    }
}

fn source_kind(source: Source) -> &'static str {
    match source {
        Source::Zcode | Source::Hermes | Source::OpenCode | Source::Mimo => "sqlite",
        _ => "jsonl",
    }
}

fn check_path(path: &Path) -> PathCheck {
    let meta = std::fs::metadata(path);
    let readable = meta.is_ok() && std::fs::read_dir(path).is_ok() || path.is_file();
    let newest_mtime = if path.is_dir() {
        newest_mtime_under(path, 0)
    } else {
        crate::file_mtime(path)
    };
    PathCheck {
        path: path.to_path_buf(),
        exists: path.exists(),
        readable,
        bytes: meta.map(|m| m.len()).unwrap_or(0),
        newest_mtime,
    }
}

/// Newest mtime under a directory, one level of nesting deep enough for the
/// layouts we scan (projects/<slug>/*.jsonl, sessions/<id>/messages.jsonl).
/// A hard cap keeps a huge tree from making doctor slow; 4000 files covers
/// any real corpus.
fn newest_mtime_under(dir: &Path, depth: u8) -> Option<i64> {
    if depth > 3 {
        return None;
    }
    let mut best = crate::file_mtime(dir);
    let mut seen = 0usize;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return best;
    };
    for entry in entries.flatten() {
        seen += 1;
        if seen > 4000 {
            break;
        }
        let p = entry.path();
        let m = if p.is_dir() {
            newest_mtime_under(&p, depth + 1)
        } else {
            crate::file_mtime(&p)
        };
        if m.map_or(true, |v| best.map_or(true, |b| v > b)) {
            best = m;
        }
    }
    best
}

fn log_paths_for(source: Source) -> Vec<PathBuf> {
    match source {
        Source::Claude => crate::claude::log_paths(),
        Source::Codex => crate::codex::log_paths(),
        Source::Gemini => crate::gemini::log_paths(),
        Source::Qwen => crate::qwen::log_paths(),
        Source::Zcode => crate::zcode::log_paths(),
        Source::Qoder => crate::qoder::log_paths(),
        Source::WorkBuddy => crate::workbuddy::log_paths(),
        Source::OpenCode => crate::opencode::log_paths(),
        Source::Mimo => crate::mimo::log_paths(),
        Source::Pi => crate::pi::log_paths(),
        Source::MiniMax => crate::minimax::log_paths(),
        Source::Hermes => crate::hermes::log_paths(),
        Source::Cline => crate::cline::log_paths_for(Source::Cline),
        Source::RooCode => crate::cline::log_paths_for(Source::RooCode),
        Source::Kilo => crate::cline::log_paths_for(Source::Kilo),
        Source::Kimi => crate::kimi::log_paths(),
        Source::Amp => crate::amp::log_paths(),
        Source::Mirasim => crate::mirasim::log_paths(),
        // 网关账在 ~/.tokenbuddy 自己家里(而非工具的日志目录)。
        #[cfg(feature = "gateway")]
        Source::Gateway => crate::gateway::log_paths(),
        #[cfg(not(feature = "gateway"))]
        Source::Gateway => Vec::new(),
    }
}

fn diagnose_source(source: Source, ledger: Option<&(u64, i64)>, unsettled: u64) -> SourceDiagnosis {
    let paths: Vec<PathBuf> = log_paths_for(source);
    let checks: Vec<PathCheck> = paths.iter().map(|p| check_path(p)).collect();
    let present = checks.iter().any(|p| p.exists);
    let kind = source_kind(source);

    let mut tables_found = Vec::new();
    let mut oversize = None;
    let mut hint = String::new();
    if kind == "sqlite" {
        // The collector databases are single files; probe each existing one.
        for check in checks
            .iter()
            .filter(|c| c.exists && c.readable && c.path.is_file())
        {
            if let Ok(conn) = rusqlite::Connection::open_with_flags(
                &check.path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            ) {
                for t in expected_tables(source) {
                    if context::sqlite_table_exists(&conn, t)
                        && !tables_found.contains(&t.to_string())
                    {
                        tables_found.push(t.to_string());
                    }
                }
                if oversize.is_none() {
                    if let Some(first) = expected_tables(source).first() {
                        oversize = crate::sqlite_oversized_rows(&conn, first).ok();
                    }
                }
            }
        }
        if !present {
            hint = "该工具可能未安装;路径支持环境变量覆盖(如 ZCODE_CONFIG_DIR)".into();
        }
    } else if !present {
        hint = "该工具可能未安装,或日志目录为空".into();
    }

    // Issue #39: "present 但账本 0 行" 是最需要排查的分支,hint 不能是空串,
    // 且要区分「上游真没数据(无需处理)」和「有数据但判据未命中(提 issue)」。
    if hint.is_empty() && ledger.map(|l| l.0 == 0).unwrap_or(true) {
        if kind == "sqlite" {
            let has_rows = checks.iter().filter(|c| c.exists && c.readable).any(|c| {
                rusqlite::Connection::open_with_flags(
                    &c.path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .map(|conn| {
                    expected_tables(source).iter().any(|t| {
                        context::sqlite_table_exists(&conn, t)
                            && conn
                                .query_row(&format!("SELECT COUNT(*) FROM {t}"), [], |r| {
                                    r.get::<_, i64>(0)
                                })
                                .unwrap_or(0)
                                > 0
                    })
                })
                .unwrap_or(false)
            });
            hint = if has_rows {
                "源库里有数据但账本 0 行——采集判据未命中(上游可能改了 schema)或全部被去重跳过,请附一条样本记录提 issue".into()
            } else {
                "源库的表存在但确实 0 行——上游还没产生记录,最近用过该工具后再跑一次 sync".into()
            };
        } else {
            hint = "日志可读但账本 0 行——若近期确实没用过该工具属正常;否则采集判据可能未命中,请附样本提 issue".into()
                ;
        }
    }

    SourceDiagnosis {
        source: source.as_str().to_string(),
        kind,
        present,
        paths: checks,
        tables_found,
        oversize_rows: oversize,
        ledger_rows: ledger.map(|l| l.0).unwrap_or(0),
        ledger_latest_ts: ledger.map(|l| l.1).unwrap_or(0),
        unsettled_rows: unsettled,
        disabled: !crate::quota::source_enabled(source.as_str()),
        hint,
    }
}

/// Store-side figures: the two parquet files and the sync bookkeeping.
#[derive(serde::Serialize)]
pub struct StoreDiagnosis {
    pub data_parquet_bytes: u64,
    pub context_parquet_bytes: u64,
    pub record_count: u64,
    pub last_sync_ts: Option<i64>,
}

/// Quota-side figures: is the feature even configured, what did the file
/// readers see live, and what shape are the command collectors in. Doctor
/// never runs collector commands — read-only paths only.
#[derive(serde::Serialize)]
pub struct QuotaCollectorDiagnosis {
    pub name: String,
    pub last_ok_at: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(serde::Serialize)]
pub struct QuotaDiagnosis {
    pub config_present: bool,
    /// Live codex rollout readings, human strings ("5h 已用 72.3%").
    pub file_windows: Vec<String>,
    pub collectors: Vec<QuotaCollectorDiagnosis>,
}

#[derive(serde::Serialize)]
pub struct DoctorReport {
    pub store: StoreDiagnosis,
    pub sources: Vec<SourceDiagnosis>,
    pub quota: QuotaDiagnosis,
}

/// Run every check. Read-only against the live store; collectors are not
/// invoked, so doctor costs a couple of SQLite opens, never a sync.
/// Issue #20: rows younger than this are "not yet settled" — the upstream
/// may still rewrite them (resume/compact backfills). Mirrors the 1h settle
/// window CreditDaddy's usageSync learned to wait out.
const UNSETTLED_WINDOW_SECS: i64 = 3_600;

pub fn diagnose() -> Result<DoctorReport> {
    let dir = crate::data_dir();
    let data = dir.join("data.parquet");
    let ctx = dir.join("context.parquet");
    let store = crate::store::Store::open()?;
    let state = store.state();
    let ledger: std::collections::HashMap<String, (u64, i64)> = store
        .query_source_ledger_stats()?
        .into_iter()
        .map(|s| (s.source, (s.rows, s.latest_ts)))
        .collect();
    let unsettled: std::collections::HashMap<String, u64> = store
        .unsettled_rows_by_source(UNSETTLED_WINDOW_SECS)?
        .into_iter()
        .collect();
    let report = DoctorReport {
        store: StoreDiagnosis {
            data_parquet_bytes: std::fs::metadata(&data).map(|m| m.len()).unwrap_or(0),
            context_parquet_bytes: std::fs::metadata(&ctx).map(|m| m.len()).unwrap_or(0),
            record_count: store.record_count()?,
            last_sync_ts: state.last_sync_at,
        },
        sources: [
            Source::Claude,
            Source::Codex,
            Source::Gemini,
            Source::Qwen,
            Source::Zcode,
            Source::Qoder,
            Source::WorkBuddy,
            Source::OpenCode,
            Source::Mimo,
            Source::Pi,
            Source::MiniMax,
            Source::Hermes,
            Source::Cline,
            Source::RooCode,
            Source::Kilo,
            Source::Kimi,
            Source::Amp,
            Source::Gateway,
            Source::Mirasim,
        ]
        .iter()
        .map(|s| {
            let stat = ledger.get(s.as_str());
            diagnose_source(*s, stat, unsettled.get(s.as_str()).copied().unwrap_or(0))
        })
        .collect(),
        quota: {
            let view = crate::quota::collect_view(false);
            QuotaDiagnosis {
                config_present: crate::quota::config_path().exists(),
                file_windows: view
                    .snapshots
                    .iter()
                    .filter(|s| s.origin == "file")
                    .map(|s| format!("{} 窗 已用 {:.1}%", s.window, s.used_percent))
                    .collect(),
                collectors: view
                    .collectors
                    .iter()
                    .map(|c| QuotaCollectorDiagnosis {
                        name: c.name.clone(),
                        last_ok_at: c.last_ok_at,
                        last_error: c.last_error.clone(),
                    })
                    .collect(),
            }
        },
    };
    Ok(report)
}

impl DoctorReport {
    /// Human-readable report, zh, one block per source.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "数据文件  data.parquet {:.1} MB · {} 条记录\n         context.parquet {:.1} MB(不上传,仅本机)\n",
            self.store.data_parquet_bytes as f64 / 1_048_576.0,
            self.store.record_count,
            self.store.context_parquet_bytes as f64 / 1_048_576.0,
        ));
        match self.store.last_sync_ts {
            Some(s) => out.push_str(&format!(
                "上次同步  {}\n",
                chrono::DateTime::from_timestamp(s, 0)
                    .map(|d| d
                        .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string())
                    .unwrap_or_else(|| "未知".into())
            )),
            None => out.push_str("上次同步  从未(先跑一次同步或打开仪表盘点「同步数据」)\n"),
        }
        out.push_str("\n逐源诊断:\n");
        for s in &self.sources {
            out.push_str(&format!("\n■ {} ({})\n", s.source, s.kind));
            for p in &s.paths {
                if !p.exists {
                    out.push_str(&format!("  ✗ {}  不存在\n", p.path.display()));
                } else if !p.readable {
                    out.push_str(&format!("  ⚠ {}  不可读\n", p.path.display()));
                } else {
                    let when = p
                        .newest_mtime
                        .map(|t| {
                            chrono::DateTime::from_timestamp(t, 0)
                                .map(|d| {
                                    d.with_timezone(
                                        &chrono::FixedOffset::east_opt(8 * 3600).unwrap(),
                                    )
                                    .format("%Y-%m-%d %H:%M")
                                    .to_string()
                                })
                                .unwrap_or_else(|| "未知".into())
                        })
                        .unwrap_or_else(|| "—".into());
                    out.push_str(&format!("  ✓ {}  最新条目 {when}\n", p.path.display()));
                }
            }
            if !s.tables_found.is_empty() {
                out.push_str(&format!("  表: {}\n", s.tables_found.join(", ")));
            }
            if let Some(n) = s.oversize_rows {
                if n > 0 {
                    out.push_str(&format!(
                        "  {n} 行超过 1 MB 读取上限被跳过(非计费内容,设计行为)\n"
                    ));
                }
            }
            if s.present && s.ledger_rows > 0 {
                let when = chrono::DateTime::from_timestamp(s.ledger_latest_ts, 0)
                    .map(|d| {
                        d.with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
                            .format("%Y-%m-%d %H:%M")
                            .to_string()
                    })
                    .unwrap_or_else(|| "未知".into());
                out.push_str(&format!(
                    "  账本     {} 行 · 最新一条 {when}\n",
                    s.ledger_rows
                ));
                if s.unsettled_rows > 0 {
                    out.push_str(&format!(
                        "  ⏳ 未定型行(近 {} min):{} —— 上游仍可能回填这些行;另一来源的退订/去重判定请以此窗为界\n",
                        UNSETTLED_WINDOW_SECS / 60,
                        s.unsettled_rows
                    ));
                }
            }
            out.push_str(&format!("  → {}\n", s.verdict()));
            if !s.hint.is_empty() {
                out.push_str(&format!("    ({})\n", s.hint));
            }
        }
        out.push_str("\n套餐余量:\n");
        if !self.quota.config_present {
            out.push_str("  未配置 quota.json(可选功能;Codex rollout 里的限流数据仍会自动读取)\n");
        }
        if self.quota.file_windows.is_empty() {
            out.push_str("  Codex rollout:没有读到限流数据(未订阅/未装/无会话,均属正常)\n");
        } else {
            out.push_str(&format!(
                "  Codex rollout: {}\n",
                self.quota.file_windows.join(" · ")
            ));
        }
        for c in &self.quota.collectors {
            match (&c.last_error, c.last_ok_at) {
                (Some(err), _) => out.push_str(&format!("  ⚠ {err}\n")),
                (None, Some(at)) => {
                    let when = chrono::DateTime::from_timestamp(at, 0)
                        .map(|d| {
                            d.with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
                                .format("%Y-%m-%d %H:%M")
                                .to_string()
                        })
                        .unwrap_or_else(|| "未知".into());
                    out.push_str(&format!("  ✓ 采集器 {} 最近成功 {when}\n", c.name));
                }
                (None, None) => {
                    out.push_str(&format!(
                        "  · 采集器 {} 尚未运行过(tokenbuddy quota --refresh 触发)\n",
                        c.name
                    ));
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::{check_path, PathCheck, SourceDiagnosis};
    use std::path::Path;

    #[test]
    fn path_check_reports_absent_and_present() {
        let missing = check_path(Path::new("/nonexistent-tokenbuddy-doctor/xyz"));
        assert!(!missing.exists);
        assert!(!missing.readable);

        let dir = std::env::temp_dir().join(format!("tb-doctor-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.jsonl"), "{}\n").unwrap();
        let present = check_path(&dir);
        assert!(present.exists);
        assert!(present.readable);
        assert!(present.newest_mtime.is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn verdict_names_the_zero_row_gap() {
        // Logs exist and are readable, but nothing reached the ledger — the
        // silent failure mode doctor exists to expose.
        let mut d = SourceDiagnosis {
            source: "zcode".into(),
            kind: "jsonl",
            present: true,
            paths: vec![PathCheck {
                path: Path::new("/tmp/whatever").to_path_buf(),
                exists: true,
                readable: true,
                bytes: 10,
                newest_mtime: Some(1),
            }],
            tables_found: vec![],
            oversize_rows: None,
            ledger_rows: 0,
            ledger_latest_ts: 0,
            unsettled_rows: 0,
            disabled: false,
            hint: String::new(),
        };
        assert!(
            d.verdict().contains("0 行"),
            "zero-row gap named: {}",
            d.verdict()
        );

        d.ledger_rows = 42;
        d.ledger_latest_ts = 1_788_874_500;
        assert!(d.verdict().starts_with("✓"), "rows present reads healthy");
        let report = format!("账本     {} 行", d.ledger_rows);
        assert!(report.contains("42"));
    }
}
