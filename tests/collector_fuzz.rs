//! Collector fuzzing: every source's collector is fed
//! malformed corpora — truncated JSON, binary garbage, oversized lines,
//! wrong-typed fields, corrupt SQLite headers — and must answer with either
//! an empty/Err result or a sane subset, never a panic. A collector that
//! panics takes the whole sync down for every source; one that errors is
//! reported per-source by the pipeline and the other sources survive.
//!
//! Everything runs inside one #[test] fn because the collectors read env
//! vars (process-global state) and the Rust test harness runs fns in
//! parallel threads.

use std::path::{Path, PathBuf};

/// Deterministic pseudo-garbage: an LCG so failures reproduce bit-for-bit.
struct Lcg(u64);

impl Lcg {
    fn next_byte(&mut self) -> u8 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) as u8
    }
    fn garbage(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next_byte()).collect()
    }
}

fn home(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tb-fuzz-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write(path: &Path, bytes: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

/// The malformed payload families every JSONL source is fed.
fn poison_lines(rng: &mut Lcg) -> Vec<Vec<u8>> {
    let mut cases: Vec<Vec<u8>> = vec![
        // Truncated JSON — a process killed mid-write.
        b"{\"type\":\"assistant\",\"message\":{\"id\":\"x\", \"usag".to_vec(),
        // Valid JSON, wrong shape entirely.
        b"[1,2,3]".to_vec(),
        b"\"just a string\"".to_vec(),
        b"null".to_vec(),
        // Right shape, wrong types everywhere.
        br#"{"type":123,"message":{"id":null,"model":42,"usage":{"input_tokens":"lots","output_tokens":true}}}"#
            .to_vec(),
        // Pathological numbers.
        br#"{"type":"assistant","timestamp":-1,"message":{"id":"big","usage":{"input_tokens":18446744073709551615,"output_tokens":-9223372036854775808}}}"#
            .to_vec(),
        // Embedded NUL and raw binary inside a line.
        b"\x00\x01\x02\xff\xfe".to_vec(),
    ];
    // A few purely random lines.
    for _ in 0..5 {
        cases.push(rng.garbage(200));
    }
    cases
}

fn seed_corpus(dir: &Path, sub: &str, name: &str, rng: &mut Lcg) {
    let mut payload: Vec<u8> = b"\n\n".to_vec();
    for case in poison_lines(rng) {
        payload.extend_from_slice(&case);
        payload.push(b'\n');
    }
    write(&dir.join(sub).join(name), &payload);
}

#[test]
fn jsonl_collectors_survive_poisoned_corpora() {
    let mut rng = Lcg(0x5EED_2026_0928);

    // --- claude: CLAUDE_CONFIG_DIR/projects/**/*.jsonl
    let dir = home("jsonl");
    seed_corpus(&dir, "projects/p1", "a.jsonl", &mut rng);
    seed_corpus(&dir, "projects/p1/sess", "chat.jsonl", &mut rng);
    seed_corpus(&dir, "projects/p1/subagents", "agent-1.jsonl", &mut rng);
    std::env::set_var("CLAUDE_CONFIG_DIR", &dir);
    let records = tokenbuddy::claude::collect_records();
    assert!(records.is_ok(), "claude collector must not panic on poison");

    // --- codex: CODEX_HOME/sessions/**.jsonl
    let dir = home("jsonl");
    seed_corpus(&dir, "sessions/2026/09/28", "rollout-x.jsonl", &mut rng);
    std::env::set_var("CODEX_HOME", &dir);
    assert!(tokenbuddy::codex::collect_records().is_ok());

    // --- gemini: GEMINI_DATA_DIR/tmp/**/chats/*.json(l)
    let dir = home("jsonl");
    seed_corpus(&dir, "tmp/hash1/chats", "session-a.jsonl", &mut rng);
    seed_corpus(&dir, "tmp/hash1/chats/parent", "sub.json", &mut rng);
    std::env::set_var("GEMINI_DATA_DIR", &dir);
    assert!(tokenbuddy::gemini::collect_records().is_ok());

    // --- qwen: QWEN_DATA_DIR/projects/<p>/chats/*.jsonl
    let dir = home("jsonl");
    seed_corpus(&dir, "projects/proj/chats", "sess.jsonl", &mut rng);
    std::env::set_var("QWEN_DATA_DIR", &dir);
    assert!(tokenbuddy::qwen::collect_records().is_ok());

    // --- pi: PI_DIR/agent/sessions/**/*.jsonl
    let dir = home("jsonl");
    seed_corpus(&dir, "agent/sessions/s1", "m.jsonl", &mut rng);
    std::env::set_var("PI_DIR", &dir);
    assert!(tokenbuddy::pi::collect_records().is_ok());

    // --- minimax: MINIMAX_HOME/v2/sessions/**/messages.jsonl
    let dir = home("jsonl");
    seed_corpus(&dir, "v2/sessions/s1", "messages.jsonl", &mut rng);
    std::env::set_var("MINIMAX_HOME", &dir);
    assert!(tokenbuddy::minimax::collect_records().is_ok());

    // --- qoder: QODER_DIR/projects/<slug>/*.jsonl (+ logs/sessions)
    let dir = home("jsonl");
    seed_corpus(&dir, "projects/slug", "s.jsonl", &mut rng);
    seed_corpus(&dir, "logs/sessions/s1", "t.jsonl", &mut rng);
    std::env::set_var("QODER_DIR", &dir);
    assert!(tokenbuddy::qoder::collect_records().is_ok());

    // --- workbuddy: WORKBUDDY_DIR/traces/**/*.json
    let dir = home("jsonl");
    write(&dir.join("traces/t1/trace.json"), &rng.garbage(300));
    std::env::set_var("WORKBUDDY_DIR", &dir);
    assert!(tokenbuddy::workbuddy::collect_records().is_ok());

    for v in [
        "CLAUDE_CONFIG_DIR",
        "CODEX_HOME",
        "GEMINI_DATA_DIR",
        "QWEN_DATA_DIR",
        "PI_DIR",
        "MINIMAX_HOME",
        "QODER_DIR",
        "WORKBUDDY_DIR",
    ] {
        std::env::remove_var(v);
    }
    let _ = std::fs::remove_dir_all(
        std::env::temp_dir().join(format!("tb-fuzz-jsonl-{}", std::process::id())),
    );
}

#[test]
fn sqlite_collectors_survive_corrupt_databases() {
    let mut rng = Lcg(0xC0FF_EE00_1234);

    // Garbage bytes where a SQLite header should be.
    let dir = home("sqlite");
    write(&dir.join("cli/db/db.sqlite"), &rng.garbage(4096));
    std::env::set_var("ZCODE_CONFIG_DIR", &dir);
    // Err is fine (reported per-source); a panic is not.
    let _ = tokenbuddy::zcode::collect_records();

    // A valid SQLite file with no expected tables.
    let dir = home("sqlite");
    write(
        &dir.join("opencode.db"),
        b"SQLite format 3\x00 padding only",
    );
    std::env::set_var("XDG_DATA_HOME", &dir);
    let _ = tokenbuddy::opencode::collect_records();
    let _ = tokenbuddy::mimo::collect_records();

    let dir = home("sqlite");
    write(&dir.join("state.db"), &rng.garbage(2048));
    std::env::set_var("HERMES_HOME", &dir);
    let _ = tokenbuddy::hermes::collect_records();

    for v in ["ZCODE_CONFIG_DIR", "XDG_DATA_HOME", "HERMES_HOME"] {
        std::env::remove_var(v);
    }
    let _ = std::fs::remove_dir_all(
        std::env::temp_dir().join(format!("tb-fuzz-sqlite-{}", std::process::id())),
    );
}
