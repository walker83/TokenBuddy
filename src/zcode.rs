use crate::{Source, TokenRecord};
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

static SQLITE_CACHE: Mutex<Option<(SystemTime, Vec<TokenRecord>)>> = Mutex::new(None);

/// Drop the resident parse cache. The cache only exists to make a *second*
/// sync cheaper than the first; left in place it pins every record of every
/// session log in the heap for the life of the process, growing with total
/// history and eating the resident-memory budget the dashboard is measured
/// against. `store::sync` calls this once the parquet has been written, so
/// the saving is paid back only by whoever asks for the next sync.
pub fn release_caches() {
    let mut cache = SQLITE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    *cache = None;
}

/// Where this collector reads from, when that place exists on this machine.
/// Powers the dashboard's source-health panel and the first-run prompt, so a
/// user with a missing or unmoved tool directory is told which one instead of
/// just seeing zeros.
pub fn log_path() -> Option<std::path::PathBuf> {
    log_paths().into_iter().find(|p| p.exists())
}

pub fn collect_records() -> Result<Vec<TokenRecord>> {
    let db_path = get_zcode_db_path();
    if !db_path.exists() {
        return Ok(vec![]);
    }

    let mtime = std::fs::metadata(&db_path)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);

    let mut cache = SQLITE_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((cached_mtime, ref records)) = *cache {
        if mtime <= cached_mtime {
            return Ok(records.clone());
        }
    }

    // Cache miss or stale — re-read
    let records = read_all_records(&db_path)?;
    *cache = Some((mtime, records.clone()));
    Ok(records)
}

fn read_all_records(db_path: &Path) -> Result<Vec<TokenRecord>> {
    // Default read-write flags. WAL mode allows concurrent readers; opening
    // read-only fails (SQLITE_CANTOPEN / code 14) when the WAL hasn't been
    // checkpointed. We only issue SELECTs so this is safe.
    let conn = rusqlite::Connection::open(db_path)?;

    // A tool that is mid-write holds an exclusive lock on its own database.
    // Without a busy timeout the SELECT fails instantly with SQLITE_BUSY —
    // and before sync became per-collector fault-tolerant, that single
    // locked database voided every other source's import as well.
    conn.busy_timeout(std::time::Duration::from_secs(5))?;

    // model_usage row id is unique — keep only completed rows so we don't
    // double-count in-flight or retried requests.
    let mut stmt = conn.prepare(
        "SELECT m.id, m.session_id, m.model_id, m.started_at, m.completed_at,
                m.input_tokens, m.output_tokens, m.reasoning_tokens,
                m.cache_creation_input_tokens, m.cache_read_input_tokens,
                m.duration_ms, m.time_to_first_token_ms, s.directory,
                m.agent
         FROM model_usage m
         JOIN session s ON s.id = m.session_id
         WHERE m.status='completed'
         ORDER BY COALESCE(m.completed_at, m.started_at) ASC",
    )?;

    let mut records = Vec::new();
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let session_id: String = row.get(1)?;
        let model_id: String = row.get(2)?;
        let started_at: i64 = row.get(3)?;
        let completed_at: Option<i64> = row.get(4)?;
        let input_tokens: i64 = row.get(5)?;
        let output_tokens: i64 = row.get(6)?;
        let _reasoning_tokens: i64 = row.get(7)?;
        let cache_creation_input_tokens: i64 = row.get(8)?;
        let cache_read_input_tokens: i64 = row.get(9)?;
        let duration_ms_raw: Option<i64> = row.get(10).ok();
        let ttft_ms_raw: Option<i64> = row.get(11).ok();
        let directory: Option<String> = row.get(12).ok();
        // R64:主线 agent 固定为 "zcode-agent";其余(general-purpose、
        // skill 派生的 visual-judge 等)都是子代理。NULL(老行)视为主线。
        let agent: Option<String> = row.get(13).ok();
        let sidechain = agent
            .as_deref()
            .map(|a| !a.is_empty() && a != "zcode-agent")
            .unwrap_or(false);

        if input_tokens == 0
            && output_tokens == 0
            && cache_read_input_tokens == 0
            && cache_creation_input_tokens == 0
        {
            continue;
        }

        let ts_ms = completed_at.unwrap_or(started_at);
        let timestamp_secs = ts_ms / 1000;

        // zcode reports duration_ms directly; fall back to completed_at - started_at.
        let duration_ms = duration_ms_raw
            .filter(|d| *d >= 0)
            .map(|d| d as u64)
            .or_else(|| {
                completed_at
                    .zip(Some(started_at))
                    .filter(|(e, s)| e >= s)
                    .map(|(e, s)| (e - s) as u64)
            });
        let ttft_ms = ttft_ms_raw.filter(|d| *d >= 0).map(|d| d as u64);

        // `input_tokens` from zcode already contains the cached prefix, so the
        // cached share is split out here to keep the same meaning as the other
        // sources: input + cache_read + cache_creation is the full prompt.
        let uncached_input = input_tokens
            .saturating_sub(cache_read_input_tokens.max(0))
            .saturating_sub(cache_creation_input_tokens.max(0))
            .max(0) as u64;

        records.push(TokenRecord {
            source: Source::Zcode,
            model: model_id,
            sidechain,
            input_tokens: uncached_input,
            output_tokens: output_tokens.max(0) as u64,
            cache_read_tokens: cache_read_input_tokens.max(0) as u64,
            cache_creation_tokens: cache_creation_input_tokens.max(0) as u64,
            timestamp: timestamp_secs,
            session_id: Some(session_id),
            project: directory.unwrap_or_default(),
            duration_ms,
            ttft_ms,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: Some(id),
            merge_key: None,
        });
    }

    Ok(records)
}

// Storage location for context search; see `context.rs`.
pub(crate) fn db_path() -> PathBuf {
    get_zcode_db_path()
}

/// Candidate log locations, for `tokenbuddy doctor`: presence is optional,
/// the doctor reports what exists and what does not.
pub fn log_paths() -> Vec<PathBuf> {
    vec![get_zcode_db_path()]
}

fn get_zcode_db_path() -> PathBuf {
    if let Ok(custom) = std::env::var("ZCODE_CONFIG_DIR") {
        let trimmed = custom.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed)
                .join("cli")
                .join("db")
                .join("db.sqlite");
        }
    }
    dirs::home_dir()
        .map(|h| h.join(".zcode/cli/db/db.sqlite"))
        .unwrap_or_else(|| PathBuf::from(".zcode/cli/db/db.sqlite"))
}

/// R56 — work receipts (工作收据): what a ZCode session actually *did*,
/// derived from the `part` table's tool entries — files touched (Edit/Write/
/// MultiEdit `file_path`), shell commands run, test-ish commands among them.
/// Derived live from the agent's own database (the same store the usage
/// collector reads), zero new state; a missing database is simply no
/// receipts. Like every receipts feature, it reports facts, not judgments.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkReceipt {
    pub session_id: String,
    /// (path, touch count), count-descending, capped at 50 paths.
    pub files: Vec<(String, u64)>,
    pub bash_count: u64,
    /// Bash commands that look like test runs (contain a test marker).
    pub test_count: u64,
    /// Total tool parts in the session.
    pub tool_calls: u64,
    /// R68 — raw tool-name → call count (per session). Category rollups
    /// happen at the outlet, not here.
    pub tools: std::collections::BTreeMap<String, u64>,
    /// Newest tool part for the session, epoch seconds (0 when unknown).
    pub last_ts: i64,
}

/// Bash substrings that count as "ran tests". Deliberately narrow: the word
/// `test` inside a path or a flag would trip false positives otherwise.
const TEST_MARKERS: [&str; 6] = [
    "cargo test",
    "npm test",
    "npm run test",
    "pnpm test",
    "pytest",
    "go test",
];

pub(crate) fn looks_like_test(command: &str) -> bool {
    TEST_MARKERS.iter().any(|m| command.contains(m))
}

/// Receipts for tool parts newer than `since` (epoch seconds). Sessions are
/// newest-first (capped 200); malformed part rows are skipped, never fatal.
pub fn work_receipts(since: i64) -> Vec<WorkReceipt> {
    let db = get_zcode_db_path();
    if !db.is_file() {
        return Vec::new();
    }
    let conn = match rusqlite::Connection::open_with_flags(
        &db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    ) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    let mut stmt = match conn.prepare(
        "SELECT session_id, time_created, data FROM part
         WHERE json_extract(data,'$.type') = 'tool'
           AND time_created >= ?1",
    ) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    struct Acc {
        files: std::collections::BTreeMap<String, u64>,
        bash: u64,
        tests: u64,
        calls: u64,
        tools: std::collections::BTreeMap<String, u64>,
        last_ms: i64,
    }
    let mut acc: std::collections::HashMap<String, Acc> = std::collections::HashMap::new();
    let rows = stmt.query_map([since * 1000], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
        ))
    });
    let rows = match rows {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    for row in rows.flatten() {
        let (session_id, time_ms, data) = row;
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&data) else {
            continue;
        };
        let tool = v.get("tool").and_then(|t| t.as_str()).unwrap_or("");
        if tool.is_empty() {
            continue;
        }
        let entry = acc.entry(session_id).or_insert(Acc {
            files: std::collections::BTreeMap::new(),
            bash: 0,
            tests: 0,
            calls: 0,
            tools: std::collections::BTreeMap::new(),
            last_ms: 0,
        });
        entry.calls += 1;
        entry.last_ms = entry.last_ms.max(time_ms);
        *entry.tools.entry(tool.to_string()).or_insert(0) += 1;
        let input = v.pointer("/state/input");
        match tool {
            "Edit" | "Write" | "MultiEdit" => {
                if let Some(path) = input
                    .and_then(|i| i.get("file_path"))
                    .and_then(|p| p.as_str())
                {
                    if !path.is_empty() {
                        *entry.files.entry(path.to_string()).or_insert(0) += 1;
                    }
                }
            }
            "Bash" => {
                entry.bash += 1;
                if let Some(cmd) = input
                    .and_then(|i| i.get("command"))
                    .and_then(|c| c.as_str())
                {
                    if looks_like_test(cmd) {
                        entry.tests += 1;
                    }
                }
            }
            _ => {}
        }
    }

    let mut out: Vec<WorkReceipt> = acc
        .into_iter()
        .map(|(session_id, a)| {
            let mut files: Vec<(String, u64)> = a.files.into_iter().collect();
            files.sort_by_key(|(path, n)| std::cmp::Reverse((*n, path.clone())));
            files.truncate(50);
            WorkReceipt {
                session_id,
                files,
                bash_count: a.bash,
                test_count: a.tests,
                tool_calls: a.calls,
                tools: a.tools,
                last_ts: a.last_ms / 1000,
            }
        })
        .collect();
    out.sort_by_key(|r| std::cmp::Reverse(r.last_ts));
    out.truncate(200);
    out
}

/// Three-source receipt merge — ZCode(part 表)+ Claude(transcript)+
/// OpenCode(session_message)——每个出口都走这里,合并规则只写一遍。
/// Newest-first;不截断:报表合计要全会话,展示出口自行 take(200)。
pub fn merged_receipts(since: i64) -> Vec<WorkReceipt> {
    let mut receipts = work_receipts(since);
    receipts.extend(crate::claude::work_receipts(since));
    receipts.extend(crate::opencode::work_receipts(since));
    receipts.sort_by_key(|r| std::cmp::Reverse(r.last_ts));
    receipts
}

/// R81 — 跨会话返工热点:窗口内被 ≥2 个不同会话编辑过的文件。
/// 会话内的反复编辑是迭代不是返工,分界线画在会话边界上,恰好是
/// 收据能证明的东西(文件路径是确定性证据)。跨源 session_id 若撞车
/// 只会算作一个会话——少报不虚报,与整个收据面同一口径。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReworkHotspot {
    pub path: String,
    /// 窗口内碰过这个文件的不同会话数。
    pub sessions: u64,
    /// 跨会话累计编辑次数。
    pub edits: u64,
    /// 最近一次触碰,epoch 秒(0 = 未知)。
    pub last_ts: i64,
}

pub fn rework_hotspots(receipts: &[WorkReceipt], top: usize) -> Vec<ReworkHotspot> {
    struct Acc {
        sessions: std::collections::BTreeSet<String>,
        edits: u64,
        last_ts: i64,
    }
    let mut acc: std::collections::BTreeMap<String, Acc> = Default::default();
    for r in receipts {
        for (path, n) in &r.files {
            let e = acc.entry(path.clone()).or_insert_with(|| Acc {
                sessions: Default::default(),
                edits: 0,
                last_ts: 0,
            });
            e.sessions.insert(r.session_id.clone());
            e.edits += n;
            e.last_ts = e.last_ts.max(r.last_ts);
        }
    }
    let mut out: Vec<ReworkHotspot> = acc
        .into_iter()
        .filter(|(_, a)| a.sessions.len() >= 2)
        .map(|(path, a)| ReworkHotspot {
            path,
            sessions: a.sessions.len() as u64,
            edits: a.edits,
            last_ts: a.last_ts,
        })
        .collect();
    // 会话数优先(返工强度),其次编辑次数,path 兜底保证顺序确定。
    out.sort_by(|a, b| {
        b.sessions
            .cmp(&a.sessions)
            .then(b.edits.cmp(&a.edits))
            .then(a.path.cmp(&b.path))
    });
    out.truncate(top);
    out
}

#[cfg(test)]
mod tests {
    use super::{looks_like_test, work_receipts};

    /// 测试标记刻意收窄:路径里带 test 不算跑测试。
    #[test]
    fn test_markers_are_narrow() {
        assert!(looks_like_test("cargo test --lib"));
        assert!(looks_like_test("npm run test && npm run lint"));
        assert!(looks_like_test("python -m pytest -q"));
        assert!(!looks_like_test("ls tests/"));
        assert!(!looks_like_test("cat src/lib.rs"));
    }

    /// 端到端:临时 SQLite 建 part 表,种 tool 部件(Bash/Edit/毒行),
    /// work_receipts 应按会话聚合文件与计数,毒行跳过,旧行被 since 过滤。
    #[test]
    fn work_receipts_aggregates_files_commands_and_tests() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-zcode-wr-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("ZCODE_CONFIG_DIR", &dir);

        // get_zcode_db_path 期望 <dir>/cli/db/db.sqlite。
        let dbdir = dir.join("cli/db");
        std::fs::create_dir_all(&dbdir).unwrap();
        let db = dbdir.join("db.sqlite");
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE part (
                id text primary key,
                message_id text not null,
                session_id text not null,
                time_created integer not null,
                time_updated integer not null,
                data text not null,
                sequence integer
            );",
        )
        .unwrap();

        let now: i64 = 1_800_000_000;
        let ins = |id: &str, sid: &str, ts_ms: i64, data: String| {
            conn.execute(
                "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
                 VALUES (?1, ?1, ?2, ?3, ?3, ?4)",
                rusqlite::params![id, sid, ts_ms, data],
            )
            .unwrap();
        };
        // 会话 A:一次 Edit + 一次 cargo test + 一次普通命令。
        ins(
            "p1",
            "sessA",
            (now - 100) * 1000,
            r#"{"type":"tool","tool":"Edit","state":{"status":"completed","input":{"file_path":"/tmp/a.rs","old_string":"x","new_string":"y"}}}"#.into(),
        );
        ins(
            "p2",
            "sessA",
            (now - 50) * 1000,
            r#"{"type":"tool","tool":"Bash","state":{"status":"completed","input":{"command":"cargo test --lib"}}}"#.into(),
        );
        ins(
            "p3",
            "sessA",
            (now - 40) * 1000,
            r#"{"type":"tool","tool":"Bash","state":{"status":"completed","input":{"command":"ls -la"}}}"#.into(),
        );
        // 会话 B:同一文件两次 Edit + 一条毒行(非法 JSON)。
        ins(
            "p4",
            "sessB",
            (now - 30) * 1000,
            r#"{"type":"tool","tool":"Edit","state":{"input":{"file_path":"/tmp/a.rs"}}}"#.into(),
        );
        ins(
            "p5",
            "sessB",
            (now - 20) * 1000,
            r#"{"type":"tool","tool":"Write","state":{"input":{"file_path":"/tmp/a.rs","content":"hi"}}}"#.into(),
        );
        ins("p6", "sessB", (now - 10) * 1000, "{broken".into());
        // 早于 since 的行:被过滤。
        ins(
            "p7",
            "sessC",
            (now - 100_000) * 1000,
            r#"{"type":"tool","tool":"Bash","state":{"input":{"command":"cargo test"}}}"#.into(),
        );

        let receipts = work_receipts(now - 10_000);
        std::env::remove_var("ZCODE_CONFIG_DIR");
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(receipts.len(), 2, "sessC filtered by since; others kept");
        let a = receipts.iter().find(|r| r.session_id == "sessA").unwrap();
        assert_eq!(a.files, vec![("/tmp/a.rs".to_string(), 1)]);
        assert_eq!(a.bash_count, 2);
        assert_eq!(a.test_count, 1);
        assert_eq!(a.tool_calls, 3);
        let b = receipts.iter().find(|r| r.session_id == "sessB").unwrap();
        assert_eq!(
            b.files,
            vec![("/tmp/a.rs".to_string(), 2)],
            "同一文件合并计数"
        );
        assert_eq!(b.tool_calls, 2, "毒行跳过,不计调用");
        assert!(receipts[0].last_ts >= receipts[1].last_ts, "newest first");
    }

    /// 无库:空收据,不是错误。
    #[test]
    fn work_receipts_missing_db_is_empty() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-zcode-wr-none-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("ZCODE_CONFIG_DIR", &dir);
        let receipts = work_receipts(0);
        std::env::remove_var("ZCODE_CONFIG_DIR");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(receipts.is_empty());
    }
}

#[cfg(test)]
mod sidechain_tests {
    use super::work_receipts;

    /// model_usage 的 agent 列:主线固定 zcode-agent,其余全是子代理;
    /// NULL(老行)视为主线。判定逻辑内联在 read_all_records,这里用
    /// work_receipts 的存在性守护该文件编译健康(判定本身在 R64 已有
    /// summary 拆分测试覆盖)。
    #[test]
    fn receipts_still_work_after_agent_column() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-zcode-sc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("ZCODE_CONFIG_DIR", &dir);
        let receipts = work_receipts(0);
        std::env::remove_var("ZCODE_CONFIG_DIR");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(receipts.is_empty());
    }

    /// agent 判定的真值表(zcode-agent=主线,其他=子代理,NULL=主线)。
    #[test]
    fn agent_to_sidechain_truth_table() {
        let is_sc = |a: Option<&str>| {
            a.map(|a| !a.is_empty() && a != "zcode-agent")
                .unwrap_or(false)
        };
        assert!(!is_sc(Some("zcode-agent")));
        assert!(is_sc(Some("zcode-general-purpose")));
        assert!(is_sc(Some("zcode-documents:visual-judge")));
        assert!(!is_sc(None));
        assert!(!is_sc(Some("")));
    }

    fn receipt(session: &str, files: &[(&str, u64)], last_ts: i64) -> super::WorkReceipt {
        super::WorkReceipt {
            session_id: session.to_string(),
            files: files.iter().map(|(p, n)| (p.to_string(), *n)).collect(),
            bash_count: 0,
            test_count: 0,
            tool_calls: 0,
            tools: Default::default(),
            last_ts,
        }
    }

    /// 返工热点只认跨会话:单会话反复编辑不算,跨会话合计编辑数,
    /// 排序 = 会话数 desc → 编辑数 desc → path 兜底,last_ts 取最新。
    #[test]
    fn rework_hotspots_need_two_sessions_and_order_deterministically() {
        let rs = vec![
            receipt("s1", &[("a.rs", 3), ("solo.rs", 1), ("b.rs", 2)], 100),
            receipt("s2", &[("a.rs", 2), ("b.rs", 2)], 200),
            receipt("s3", &[("b.rs", 5)], 150),
        ];
        let out = super::rework_hotspots(&rs, 10);
        // a.rs: 2 会话 5 次;b.rs: 3 会话 9 次;solo.rs 单会话被排除。
        assert_eq!(out.len(), 2, "单会话文件不应进入热点: {out:?}");
        assert_eq!(out[0].path, "b.rs");
        assert_eq!(out[0].sessions, 3);
        assert_eq!(out[0].edits, 9);
        assert_eq!(out[0].last_ts, 200);
        assert_eq!(out[1].path, "a.rs");
        assert_eq!(out[1].edits, 5);
        // top 截断:只要 1 个 → 留排序头。
        let out1 = super::rework_hotspots(&rs, 1);
        assert_eq!(out1.len(), 1);
        assert_eq!(out1[0].path, "b.rs");
    }

    /// 三源合并冒烟:三个来源根全空时静默退化,合并结果为空不 panic。
    #[test]
    fn merged_receipts_empty_sources_degrade_to_empty() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-merged-empty-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("ZCODE_CONFIG_DIR", dir.join("zcode"));
        std::env::set_var("CLAUDE_CONFIG_DIR", dir.join("claude"));
        std::env::set_var("XDG_DATA_HOME", dir.join("xdg"));
        let out = super::merged_receipts(0);
        assert!(out.is_empty());
        for v in ["ZCODE_CONFIG_DIR", "CLAUDE_CONFIG_DIR", "XDG_DATA_HOME"] {
            std::env::remove_var(v);
        }
    }
}
