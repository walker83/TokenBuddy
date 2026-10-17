use crate::{Source, TokenRecord, SQLITE_BLOB_FILTER};
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
    let db_path = get_opencode_db_path();
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
    // Open with default read-write flags. SQLite WAL mode allows many
    // concurrent readers without blocking the writer; opening read-only
    // fails (SQLITE_CANTOPEN / extended code 14) when the WAL hasn't been
    // checkpointed recently. We only issue SELECTs so the RW handle is safe.
    let conn = rusqlite::Connection::open(db_path)?;

    // A tool that is mid-write holds an exclusive lock on its own database.
    // Without a busy timeout the SELECT fails instantly with SQLITE_BUSY —
    // and before sync became per-collector fault-tolerant, that single
    // locked database voided every other source's import as well.
    conn.busy_timeout(std::time::Duration::from_secs(5))?;

    // OpenCode shipped two SQLite shapes:
    //   - v1 (<= ~2025): table `message` with role + token counts inside JSON.
    //   - v2 (>= 2026): table `session_message` per-message rows, each
    //     assistant message carrying its own `tokens` + `time` JSON — the
    //     session_v2 rollup exists alongside but is session-shaped, not
    //     request-shaped, so the collector reads the messages. See PR
    //     feat/opencode-v2-on-gitea and issue #13.
    // Detect at runtime so both stay supported without forcing users onto
    // a specific OpenCode version.
    let (sql, is_v2): (String, bool) =
        if crate::context::sqlite_table_exists(&conn, "session_message")
            && crate::context::sqlite_table_exists(&conn, "session_v2")
        {
            // v2 — per-assistant-message rows. The tokens live in the
            // message `data` JSON (input/output/cache + time.created/
            // streamed/completed); the session_v2 rollup is a SESSION per
            // row, which made the dashboard count sessions as requests and
            // hide every latency column (issue #13). The join only fetches
            // the project directory; the blob cap keeps an inline screenshot
            // from ever being assembled into the heap.
            (
                format!(
                    "SELECT m.id, m.session_id, m.time_created, m.data, s.directory \
                 FROM session_message m \
                 JOIN session_v2 s ON s.id = m.session_id \
                 WHERE {SQLITE_BLOB_FILTER} \
                   AND m.type = 'assistant' \
                 ORDER BY m.time_created ASC"
                ),
                true,
            )
        } else if crate::context::sqlite_table_exists(&conn, "message") {
            // v1 — per-message rows, role + tokens in `data` JSON. The blob bound
            // keeps an inline screenshot from ever being assembled into the heap;
            // see `SQLITE_BLOB_LIMIT` for the measurement behind it, and note the
            // role filter alone would not have helped.
            (
                format!(
                    "SELECT id, session_id, time_created, data \
                 FROM message \
                 WHERE {SQLITE_BLOB_FILTER} \
                   AND json_extract(data, '$.role') = 'assistant' \
                 ORDER BY time_created ASC"
                ),
                false,
            )
        } else {
            // Neither table present — nothing to read; surface as empty so
            // the collector quietly moves on instead of blowing up.
            return Ok(Vec::new());
        };

    let mut stmt = conn.prepare(&sql)?;

    let mut records = Vec::new();
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        if is_v2 {
            // v2 path: one record per assistant message — the same shape the
            // other sources emit, so request counts, Tok/s and latency
            // percentiles mean the same thing across the panel (issue #13).
            let msg_id: String = row.get(0)?;
            let session_id: String = row.get(1)?;
            let time_created_ms: i64 = row.get(2)?;
            let data: String = row.get(3)?;
            let directory: Option<String> = row.get(4).ok();

            let value: serde_json::Value = match serde_json::from_str(&data) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let tokens = match value.get("tokens") {
                Some(t) => t,
                None => continue,
            };
            let input_tokens = tokens.get("input").and_then(|v| v.as_u64()).unwrap_or(0);
            let output_tokens = tokens.get("output").and_then(|v| v.as_u64()).unwrap_or(0);
            let cache_read = tokens
                .get("cache")
                .and_then(|c| c.get("read"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let cache_write = tokens
                .get("cache")
                .and_then(|c| c.get("write"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if input_tokens == 0 && output_tokens == 0 && cache_read == 0 && cache_write == 0 {
                continue;
            }

            // v2 message `model` is the same JSON `{"id":"...","providerID":...}`
            // shape the session rollup carried.
            let model_raw = value
                .get("model")
                .map(|m| {
                    if m.is_string() {
                        m.as_str().unwrap_or_default().to_string()
                    } else {
                        m.to_string()
                    }
                })
                .unwrap_or_default();
            let model_id = serde_json::from_str::<serde_json::Value>(&model_raw)
                .ok()
                .and_then(|v| v.get("id").and_then(|x| x.as_str()).map(String::from))
                .unwrap_or_else(|| model_raw.clone());
            let agent = value
                .get("agent")
                .and_then(|a| a.as_str())
                .unwrap_or_default()
                .to_string();
            let model_label = if agent.is_empty() {
                model_id
            } else {
                format!("{}/{}", agent, model_id)
            };

            let time = value.get("time");
            let created = time
                .and_then(|t| t.get("created"))
                .and_then(|v| v.as_i64())
                .unwrap_or(time_created_ms);
            let completed = time
                .and_then(|t| t.get("completed"))
                .and_then(|v| v.as_i64());
            let streamed = time
                .and_then(|t| t.get("streamed"))
                .and_then(|v| v.as_i64());
            // Session wall-clock is per-message here: completed - created.
            let duration_ms = completed
                .zip(Some(created))
                .and_then(|(end, start)| (end >= start).then_some((end - start) as u64));
            // TTFT: created → streamed (the first token leaves the model).
            let ttft_ms = streamed
                .zip(Some(created))
                .and_then(|(first, start)| (first >= start).then_some((first - start) as u64));

            records.push(TokenRecord {
                source: Source::OpenCode,
                model: model_label,
                sidechain: agent_is_sidechain(&agent),
                input_tokens,
                output_tokens,
                cache_read_tokens: cache_read,
                cache_creation_tokens: cache_write,
                timestamp: created / 1000,
                session_id: Some(session_id),
                project: directory.unwrap_or_default(),
                duration_ms,
                ttft_ms,
                credits: 0.0,
                context_ratio: 0.0,
                // Stable per-message id: the absorb key survives message
                // edits and gates the legacy-row shed below.
                record_id: Some(format!("msg_{msg_id}")),
                merge_key: None,
                request_count: 1,
            });
            continue;
        }

        let _msg_id: String = row.get(0)?;
        let session_id: String = row.get(1)?;
        let time_created: i64 = row.get(2)?;
        let data: String = row.get(3)?;

        let value: serde_json::Value = match serde_json::from_str(&data) {
            Ok(v) => v,
            Err(_) => continue,
        };

        // The query already filtered on role; this stays as the guard for the
        // shape of the row itself.
        if value.get("role").and_then(|v| v.as_str()) != Some("assistant") {
            continue;
        }

        let tokens = match value.get("tokens") {
            Some(t) => t,
            None => continue,
        };

        let input_tokens = tokens.get("input").and_then(|v| v.as_u64()).unwrap_or(0);
        let output_tokens = tokens.get("output").and_then(|v| v.as_u64()).unwrap_or(0);
        let cache_read = tokens
            .get("cache")
            .and_then(|c| c.get("read"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let cache_write = tokens
            .get("cache")
            .and_then(|c| c.get("write"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        if input_tokens == 0 && output_tokens == 0 && cache_read == 0 && cache_write == 0 {
            continue;
        }

        let model = value
            .get("modelID")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let timestamp_ms = value
            .get("time")
            .and_then(|t| t.get("completed"))
            .and_then(|v| v.as_i64())
            .unwrap_or(time_created);

        let timestamp_secs = timestamp_ms / 1000;
        let agent = value
            .get("agent")
            .and_then(|a| a.as_str())
            .unwrap_or_default()
            .to_string();

        // Wall-clock duration: time.completed - time.created (both ms).
        let duration_ms = value
            .get("time")
            .and_then(|t| t.get("completed"))
            .and_then(|c| c.as_i64())
            .zip(
                value
                    .get("time")
                    .and_then(|t| t.get("created"))
                    .and_then(|c| c.as_i64()),
            )
            .and_then(|(end, start)| {
                if end >= start {
                    Some((end - start) as u64)
                } else {
                    None
                }
            });

        records.push(TokenRecord {
            source: Source::OpenCode,
            model,
            sidechain: agent_is_sidechain(&agent),
            input_tokens,
            output_tokens,
            cache_read_tokens: cache_read,
            cache_creation_tokens: cache_write,
            timestamp: timestamp_secs,
            session_id: Some(session_id),
            project: String::new(),
            duration_ms,
            ttft_ms: None,
            credits: 0.0,
            context_ratio: 0.0,
            record_id: None,
            merge_key: None,
            request_count: 1,
        });
    }

    Ok(records)
}

// Storage location for context search; see `context.rs`.
pub(crate) fn db_path() -> PathBuf {
    get_opencode_db_path()
}

/// Candidate log locations, for `tokenbuddy doctor`: presence is optional,
/// the doctor reports what exists and what does not.
pub fn log_paths() -> Vec<PathBuf> {
    vec![get_opencode_db_path()]
}

fn get_opencode_db_path() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        if !xdg.is_empty() {
            return PathBuf::from(xdg).join("opencode").join("opencode.db");
        }
    }
    dirs::home_dir()
        .map(|h| h.join(".local/share/opencode/opencode.db"))
        .unwrap_or_else(|| PathBuf::from(".local/share/opencode/opencode.db"))
}

/// R58 — work receipts for OpenCode, third source of the same shape
/// (ZCode reads `part`, Claude reads transcripts, OpenCode reads
/// `session_message` assistant rows whose `content[]` carries `tool`
/// blocks). Tool names are lowercase here and the file-edit input key is
/// `path` (not `file_path`); writes without a path are skipped — unknown is
/// not fabricated. Shell-family tools: `shell`/`bash`/`execute`.
/// R66 — OpenCode subagent attribution. `build`/`plan` are the built-in
/// main-thread modes; any other (custom) agent is a subagent. Empty/NULL is
/// mainline — unknown is not guilty. Mirrors R64's Claude/R65's ZCode
/// semantics: provenance only, totals untouched.
pub(crate) fn agent_is_sidechain(agent: &str) -> bool {
    !agent.is_empty() && agent != "build" && agent != "plan"
}

/// Receipts for assistant rows newer than `since` (epoch seconds).
/// Sessions newest-first (capped 200); malformed rows are skipped.
/// Shape reused from [`crate::zcode::WorkReceipt`] — one receipt shape,
/// many sources.
pub fn work_receipts(since: i64) -> Vec<crate::zcode::WorkReceipt> {
    let db = get_opencode_db_path();
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
        "SELECT session_id, time_created, data FROM session_message
         WHERE type = 'assistant' AND time_created >= ?1",
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
    let rows = match stmt.query_map([since * 1000], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
        ))
    }) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    for row in rows.flatten() {
        let (session_id, time_ms, data) = row;
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&data) else {
            continue;
        };
        let Some(content) = v.get("content").and_then(|c| c.as_array()) else {
            continue;
        };
        let entry = acc.entry(session_id).or_insert(Acc {
            files: std::collections::BTreeMap::new(),
            bash: 0,
            tests: 0,
            calls: 0,
            tools: std::collections::BTreeMap::new(),
            last_ms: 0,
        });
        entry.last_ms = entry.last_ms.max(time_ms);
        for block in content {
            if block.get("type").and_then(|t| t.as_str()) != Some("tool") {
                continue;
            }
            let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("");
            if name.is_empty() {
                continue;
            }
            entry.calls += 1;
            *entry.tools.entry(name.to_string()).or_insert(0) += 1;
            let input = block.pointer("/state/input");
            match name {
                "edit" | "write" | "patch" => {
                    let path = input
                        .and_then(|i| i.get("path").and_then(|p| p.as_str()))
                        .or_else(|| {
                            input
                                .and_then(|i| i.get("file_path"))
                                .and_then(|p| p.as_str())
                        });
                    if let Some(p) = path {
                        if !p.is_empty() {
                            *entry.files.entry(p.to_string()).or_insert(0) += 1;
                        }
                    }
                }
                "shell" | "bash" | "execute" => {
                    entry.bash += 1;
                    if let Some(cmd) = input
                        .and_then(|i| i.get("command"))
                        .and_then(|c| c.as_str())
                    {
                        if crate::zcode::looks_like_test(cmd) {
                            entry.tests += 1;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    let mut out: Vec<crate::zcode::WorkReceipt> = acc
        .into_iter()
        .filter(|(_, a)| a.calls > 0)
        .map(|(session_id, a)| {
            let mut files: Vec<(String, u64)> = a.files.into_iter().collect();
            files.sort_by_key(|(path, n)| std::cmp::Reverse((*n, path.clone())));
            files.truncate(50);
            crate::zcode::WorkReceipt {
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

#[cfg(test)]
mod work_receipt_tests {
    use super::work_receipts;

    /// 端到端:临时库种 assistant 行(edit 带 path / write 无 path /
    /// shell 带测试命令 / 毒行),断言聚合语义。
    #[test]
    fn opencode_receipts_parse_tool_blocks() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-oc-wr-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("XDG_DATA_HOME", &dir);

        let conn = rusqlite::Connection::open(dir.join("opencode/opencode.db").clone())
            .or_else(|_| {
                std::fs::create_dir_all(dir.join("opencode")).unwrap();
                rusqlite::Connection::open(dir.join("opencode/opencode.db"))
            })
            .unwrap();
        conn.execute_batch(
            "CREATE TABLE session_message (
                id text PRIMARY KEY,
                session_id text NOT NULL,
                type text NOT NULL,
                seq integer NOT NULL,
                time_created integer NOT NULL,
                time_updated integer NOT NULL,
                data text NOT NULL
            );",
        )
        .unwrap();

        let now = 1_800_000_000;
        let ins = |id: &str, sid: &str, ts: i64, data: String| {
            conn.execute(
                "INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
                 VALUES (?1, ?2, 'assistant', 1, ?3, ?3, ?4)",
                rusqlite::params![id, sid, ts * 1000, data],
            )
            .unwrap();
        };
        ins(
            "m1",
            "sess-oc",
            now - 100,
            r#"{"content":[{"type":"tool","name":"edit","state":{"input":{"path":"/tmp/a.rs"}}}]}"#
                .into(),
        );
        ins(
            "m2",
            "sess-oc",
            now - 90,
            r#"{"content":[{"type":"tool","name":"write","state":{"input":{"content":"no path here"}}}]}"#.into(),
        );
        ins(
            "m3",
            "sess-oc",
            now - 80,
            r#"{"content":[{"type":"tool","name":"shell","state":{"input":{"command":"cargo test --lib"}}}]}"#.into(),
        );
        ins("m4", "sess-oc", now - 70, "{broken".into());
        ins(
            "m5",
            "sess-old",
            now - 1_000_000,
            r#"{"content":[{"type":"tool","name":"edit","state":{"input":{"path":"/tmp/old.rs"}}}]}"#.into(),
        );

        let receipts = work_receipts(now - 10_000);
        std::env::remove_var("XDG_DATA_HOME");
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(receipts.len(), 1, "sess-old filtered; others merged");
        let r = &receipts[0];
        assert_eq!(r.session_id, "sess-oc");
        assert_eq!(
            r.files,
            vec![("/tmp/a.rs".to_string(), 1)],
            "无 path 的 write 跳过"
        );
        assert_eq!(r.bash_count, 1);
        assert_eq!(r.test_count, 1);
        assert_eq!(r.tool_calls, 3, "毒行不计调用");
    }

    #[test]
    fn opencode_receipts_missing_db_is_empty() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-oc-wr-none-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("XDG_DATA_HOME", &dir);
        let receipts = work_receipts(0);
        std::env::remove_var("XDG_DATA_HOME");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(receipts.is_empty());
    }
}

#[cfg(test)]
mod sidechain_tests {
    use super::agent_is_sidechain;

    /// build/plan 是内建主线程模式;自定义 agent 是子代理;空=NULL=主线。
    #[test]
    fn opencode_agent_truth_table() {
        assert!(!agent_is_sidechain("build"));
        assert!(!agent_is_sidechain("plan"));
        assert!(!agent_is_sidechain(""));
        assert!(agent_is_sidechain("general"));
        assert!(agent_is_sidechain("my-custom-agent"));
    }
}

#[cfg(test)]
mod v2_message_tests {
    use super::collect_records;

    /// Issue #13: v2 must produce per-message records with real latency —
    /// duration = completed - created, TTFT = streamed - created, and a
    /// stable `msg_*` record id. Broken JSON and token-less rows are skipped.
    #[test]
    fn opencode_v2_reads_message_level_tokens_and_latency() {
        let _guard = crate::TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let dir = std::env::temp_dir().join(format!("tb-oc-v2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("opencode")).unwrap();
        std::env::set_var("XDG_DATA_HOME", &dir);

        let conn = rusqlite::Connection::open(dir.join("opencode/opencode.db")).unwrap();
        conn.execute_batch(
            "CREATE TABLE session_v2 (
                id text PRIMARY KEY, directory text, tokens_input integer,
                tokens_output integer, tokens_cache_read integer, tokens_cache_write integer
            );
             CREATE TABLE session_message (
                id text PRIMARY KEY, session_id text NOT NULL, type text NOT NULL,
                seq integer NOT NULL, time_created integer NOT NULL,
                time_updated integer NOT NULL, data text NOT NULL
            );
             INSERT INTO session_v2 (id, directory) VALUES ('sess-1', '/code/demo');",
        )
        .unwrap();

        let msg = |tokens: &str, time: &str, extra: &str| {
            format!(
                r#"{{"model":{{"id":"glm-5.3","providerID":"opencode"}},"agent":"build",
                    "tokens":{tokens},"time":{time}{extra}}}"#
            )
        };
        let ins = |id: &str, data: String| {
            conn.execute(
                "INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
                 VALUES (?1, 'sess-1', 'assistant', 1, 1790420689000, 1790420699000, ?2)",
                rusqlite::params![id, data],
            )
            .unwrap();
        };
        // 完整行:tokens + created/streamed/completed → duration 8s,ttft 2s。
        ins(
            "m1",
            msg(
                r#"{"input":6984,"output":126,"cache":{"read":25000,"write":10}}"#,
                r#"{"created":1790420689000,"streamed":1790420691000,"completed":1790420697000}"#,
                "",
            ),
        );
        // 无 completed → duration None;streamed 在 → ttft 有。
        ins(
            "m2",
            msg(
                r#"{"input":100,"output":5,"cache":{"read":0,"write":0}}"#,
                r#"{"created":1790420700000,"streamed":1790420701500}"#,
                "",
            ),
        );
        // 全零 tokens → 跳过;坏 JSON → 跳过。
        ins(
            "m3",
            msg(
                r#"{"input":0,"output":0,"cache":{"read":0,"write":0}}"#,
                r#"{"created":1}"#,
                "",
            ),
        );
        ins("m4", "{broken".into());

        let records = collect_records().unwrap();
        std::env::remove_var("XDG_DATA_HOME");
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(records.len(), 2, "token-less and broken rows skipped");
        let r0 = &records[0];
        assert_eq!(r0.model, "build/glm-5.3");
        assert_eq!(r0.session_id.as_deref(), Some("sess-1"));
        assert_eq!(r0.project, "/code/demo");
        assert_eq!(r0.duration_ms, Some(8_000));
        assert_eq!(r0.ttft_ms, Some(2_000));
        assert_eq!(
            r0.record_id.as_deref(),
            Some("msg_m1"),
            "stable per-message id"
        );
        let r1 = &records[1];
        assert_eq!(r1.duration_ms, None, "no completed → no duration");
        assert_eq!(r1.ttft_ms, Some(1_500));
    }
}
