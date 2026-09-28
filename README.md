<div align="center">

# TokenBuddy

**WakaTime for AI coding agents — token billing plus full-text conversation
search, running entirely on your own machine.**

*You switch between three AI coding tools. Can you answer where this week's
tokens actually went?*

**4.7 MB single binary · zero runtime deps · 18 MB idle · 43K turns indexed on demand in 2s**

[简体中文说明](README.zh-CN.md) · [Download a Release](../../releases) · [Issues](../../issues) · [MIT](LICENSE)

**Rust** · **macOS / Linux** · **12 agents supported**

</div>

---

TokenBuddy watches the local session logs your AI coding tools already write
and turns them into **one queryable Parquet file** plus a web dashboard that
opens instantly: which tools and models you actually use, how many tokens
(and cache hits, and credits) they burn, when you code — and a full-text
search engine across **every conversation you have ever had with every
agent**.

No cloud. No telemetry. No accounts. Nothing to install. **A single static
binary bound to `127.0.0.1` — privacy here is physics, not a setting.**

![TokenBuddy dashboard](docs/screenshot-dashboard.png)

## The numbers

| | |
|---|---|
| Single binary | **4.6 MB**, stripped, no runtime dependencies |
| Full clean build | **48 s** (`cargo build --release`, no C++ toolchain) |
| Server memory, statistics only | **~18 MB resident** — the search index is not built until you open the search view, and is released after 15 idle minutes |
| Server memory, one sync | **~104 MB peak**, flat across repeated syncs (older versions: 294 MB and still climbing; 379 MB before that) |
| Server memory, index loaded | ~165 MB while the search index is in use |
| First index build | 43K conversation turns in ≈ **2 s**, incremental after that |
| Disk footprint | two Parquet files you can read, copy, `rm` |

Every one of those is reproducible on your machine with
`cargo run --release --example memprobe`. No embedded query engine, no
third-party database — aggregation and search are pure Rust.

**Why the idle number is the honest one.** The statistics come out of a 3 MB
`data.parquet`; the conversation index is a second, much larger structure that
a reader of totals never touches. So it is not built at startup — it is built
when you open the search view, and dropped again after 15 minutes without a
query. A sync does not build it either. macOS does not return freed pages to
the OS, so whatever a phase peaks at is what the process holds afterwards;
that is why the per-phase numbers above matter more than an average.

Four things were worth fixing to get here, all measured on a real corpus of
31K requests and 43K turns:

- the collectors no longer keep every parsed record resident between syncs;
- a sync absorbs and releases one source at a time instead of holding all
  eight at once;
- the SQLite-backed collectors ask SQLite for the eight fields they need
  instead of copying a whole `data` blob into the heap. One mimo message on a
  real machine carried a **19.5 MB body** — an inline `data:image` screenshot
  on a *user* turn, which the collector discards anyway. User turns hold 99%
  of the blob mass in that file; assistant turns top out at 1.7 KB. Filtering
  on `length()` before any JSON function is worth 24 MB on its own, because
  SQLite assembles a value before it can walk it;
- and the two bounds are ordered on purpose. `length()` first (cheap, and it
  rejects the huge rows before anything looks inside), then `json_valid`
  before every `json_extract`, because SQLite's JSON functions *error* on
  malformed input — one corrupt row would otherwise fail the whole query and
  take the source dark, where the previous blob walk just skipped it. Put the
  other way round, `json_valid` has to parse the 19.5 MB body to answer, and
  the saving disappears.

Rows above the 1 MB read bound are counted and reported rather than dropped
in silence.

## Why

Every AI coding tool keeps its own local logs — in its own format, in its own
directory, read by nobody after it is written. Once you run more than one
agent, these questions become unanswerable:

- How many tokens did I burn this week, on which model — and which
  subscription is collecting dust?
- What was my cache-hit rate? Is that context strategy I'm so proud of
  actually working?
- Which tool earns its keep — Claude Code, ZCode, Qoder, OpenCode…?
- *Where did I discuss that threading bug three weeks ago?*

TokenBuddy answers all of it, locally, with one binary.

## Features

### 📊 One bill: every token accounted for

Per-request input / output / cache-read / cache-creation tokens, duration and
TTFT (where the source log reports it), plus host-reported credits for sources
that mask token counts. Rollups by day/hour/week/month, by source, by model,
by model family (`claude-sonnet-4-5-…` → `sonnet`); period-over-period digest,
source comparison, model heatmaps (model × source, model × date). Time buckets
use a fixed UTC+8 offset — no DST surprises.

### 🧠 Deep analysis: how you actually work

One extra tab over the same parquet: an hour-of-day work-rhythm histogram,
daily cache-efficiency trend (and the raw token volume the cache absorbed),
a session leaderboard with click-through to the full conversation, and a
context-fill trend from sources that report it (Qoder's
`context_usage_ratio` — the one token-scale signal that survives its masking).
All facts, zero extrapolation.

### 🔍 A time machine: search everything you ever told an agent

The feature you cannot go back from — and the only reason the idle footprint
is 18 MB instead of 165. The index is built when you open the search view
and released after 15 minutes of not searching, so looking at totals never
pays for it. A pure-Rust inverted index over
user/assistant turns only — tool output and re-sent system context never enter
it, so cached boilerplate doesn't drown real conversations. Documents are
content-hash deduplicated. Tokenizing is two-layer: ASCII tokens with prefix
matching, plus CJK character bigrams that catch matches across any word
boundary; substring verification at query time carries precision — **Chinese
search without a dictionary** (jieba's 55 MB of resident dictionary was tried
and removed; recall stayed, memory dropped). Candidates cascade from strict
term-AND to IDF-weighted ranking, with click feedback and a quality panel.

Compact by construction: text lives in a zstd-compressed arena decompressed
per candidate, posting lists are delta-varint-encoded into one flat arena,
metadata is fully interned. 43K turns index in two seconds under 100 MB —
a **"remember everything" retrieval system that runs on your laptop.**

### 📦 One Parquet, every agent

All sources append into a single `~/.tokenbuddy/data.parquet` (Arrow schema,
zstd). Every aggregation runs a pure-Rust path projecting only the columns it
needs (~62% less read volume). Sync is incremental — already-seen records are
skipped; full rebuilds keep rotated snapshots. Each collector is one small
file in `src/`: adding an agent is a community-friendly pull request, not a
fork of the pipeline.

### 🤖 A skill so your agent can analyze itself

The corpus TokenBuddy builds is machine-readable. Two agent-facing channels,
same numbers as the dashboard by construction:

- **MCP server** — `tokenbuddy mcp` speaks the Model Context Protocol over
  stdio (zero new dependencies). One config entry and your agent can query
  usage totals, window facts, anomalies, the project × model pivot, run a
  full-text search over every conversation, or check per-source health:

  ```json
  { "mcpServers": { "tokenbuddy": { "command": "tokenbuddy", "args": ["mcp"] } } }
  ```

- **Skill** — drop in
  [`skills/tokenbuddy-analyze/SKILL.md`](skills/tokenbuddy-analyze/SKILL.md)
  and the agent talks to the local HTTP API: daily retrospectives, which
  flows keep repeating, what should become a skill or a repo rule.

Combined with tiered summarization this costs 90% fewer tokens than feeding
raw logs back to a model (see Agent self-evolution below).

### 🔒 Zero-friction privacy

The server binds to `127.0.0.1:8080`; the codebase makes **no outbound
network calls unless you explicitly enable Fleet sync** — no telemetry, no
config option that could export anything, no account. The entire database is
two files. Delete `~/.tokenbuddy/` and TokenBuddy knows nothing about you
again.

## Supported agents

| Tool | `source` id | Notes |
|---|---|---|
| Claude Code | `claude` | reads local JSONL session logs |
| ZCode | `zcode` | |
| Qoder | `qoder` | token counts masked by host; usage tracked via host-reported credits + reported context-window fill |
| WorkBuddy | `workbuddy` | |
| MiniMax Code | `minimax` | reads `~/.minimax/v2/sessions/**/messages.jsonl` |
| Hermes Agent | `hermes` | reads `~/.hermes/state.db` (`session_model_usage`, incl. side tasks); rows are aggregates, replaced on every sync |
| Codex CLI | `codex` | reads `~/.codex/sessions/**/rollout-*.jsonl`; bills only events whose cumulative total advanced (dedupes UI resends). Format verified against ccusage's official fixtures |
| Gemini CLI | `gemini` | reads `~/.gemini/tmp/**/chats/*.{json,jsonl}`; splits cached out of the prompt, thoughts join output. Format from Gemini CLI's official recording types (ccusage-cross-checked) |
| Qwen Code | `qwen` | reads `~/.qwen/projects/<project>/chats/*.jsonl`; bills assistant turns' `usageMetadata` with cached split out; `cwd` becomes project attribution |
| OpenCode | `opencode` | |
| Mimo | `mimo` | |
| Pi | `pi` | |

## Quick start

**Apple Silicon / Linux x64** — install script (recommended on macOS; see the
Gatekeeper note below):

```bash
curl -fsSL https://raw.githubusercontent.com/walker83/TokenBuddy/main/scripts/install.sh | bash
tokenbuddy
```

Add `--service` to also register a background service (launchd user agent on
macOS, systemd `--user` unit on Linux) so the dashboard is up at login without
a terminal to keep open:

```bash
curl -fsSL .../install.sh | bash -s -- --service
```

Or grab `tokenbuddy-v0.5.1-aarch64-apple-darwin.tar.gz` from
[Releases](../../releases), unpack, run.

### Command line

```
tokenbuddy [serve]        start the dashboard (default 127.0.0.1:8080)
tokenbuddy status         print record count, last sync and detected agents
tokenbuddy doctor [--json] per-source check: log paths, readability, latest entry
tokenbuddy push           sync locally, then push to Fleet
tokenbuddy fleet-sync    pull every host's Fleet data
  --port <n>              listen on another port (or TOKENBUDDY_PORT)
  --no-open               do not open a browser (or TOKENBUDDY_NO_OPEN=1)
  -h, --help  -V, --version
```

`tokenbuddy` opens your browser once the listener is up — except when stdout
is not a terminal (a service manager), where it stays quiet.

### Built-in web hardening

The server refuses requests whose `Host` is not a loopback name on the
listener's port (DNS-rebinding defense) and rejects cross-origin POSTs
(`Origin` / `Sec-Fetch-Site` check) — a web page in your browser can point
requests at `127.0.0.1`, so the server itself refuses writes it did not ask
for. curl, scripts and the dashboard itself are unaffected. Serving the
dashboard behind a reverse proxy that rewrites `Host`? Add that hostname via
`TOKENBUDDY_ALLOWED_HOSTS=proxy.example.lan` (comma separated).

### Statusline / prompt in one line

`GET /api/brief` answers with today's and the trailing week's consumption in
one small JSON — and `tokenbuddy today` prints the same line without a
server. Both read two columns off the local parquet: the full round trip is
single-digit milliseconds, unlike the monitor-tool category's statusline
incidents (runaway processes, OOM, 300% CPU). Claude Code statusline example:

```bash
tokenbuddy today
# 今日 21.3M tokens（入 358.4K · 出 126.4K · 239 请求） · 7日 124.8M
```

Opening the dashboard to other machines is explicit and locked:
`TOKENBUDDY_TOKEN=<secret> tokenbuddy --addr 0.0.0.0` — a non-loopback bind
**refuses to start** without a token. Every request then needs the token
(`Authorization: Bearer <secret>` for scripts, or browse to
`http://<ip>:8080/?token=<secret>`; the page forwards it to every API call
automatically). Loopback-only usage stays exactly as before.

### macOS says "cannot be opened / not secure"?

The release binary is ad-hoc signed — without a paid Apple Developer ID it
cannot be notarized, so a tarball **downloaded through a browser** carries a
quarantine attribute and Gatekeeper blocks it. Two fixes:

```bash
# a) remove the quarantine flag from the unpacked binary
xattr -d com.apple.quarantine ./tokenbuddy

# b) or avoid it entirely: the install script downloads via curl,
#    which never applies the flag
curl -fsSL https://raw.githubusercontent.com/walker83/TokenBuddy/main/scripts/install.sh | bash
```

Proper Developer-ID signing + notarization is on the roadmap once an Apple
Developer account is in play.

**Any other platform, from source** (Rust 1.75+, ~48 s to build):

```bash
git clone https://github.com/walker83/TokenBuddy.git
cd TokenBuddy
cargo b                     # build --release (alias defined in .cargo/config.toml)
./target/release/tokenbuddy
# open http://127.0.0.1:8080
```

- The dashboard loads immediately; the first index build runs in the
  background (search reports its progress instead of pretending to be empty).
- On a fresh install the first screen offers to import: it lists the agent log
  directories it found and nothing else happens until you press the button.
  Afterwards **同步数据** (or `POST /api/sync`) pulls the latest logs and the
  context index refreshes alongside automatically.
- **数据源** in the header shows which collectors found their logs, which did
  not, and which failed last time. A collector that fails costs you its own
  rows only — the other sources still import.
- **全量重建** rebuilds from surviving logs and can therefore *lose* rows whose
  logs have been rotated away, so it asks first and reports the net change
  (`净减 N`) instead of the number of rows re-collected.
- Every table exports to CSV (⤓ CSV) with a UTF-8 BOM, so Excel opens Chinese
  correctly.
- In the dashboard, <kbd>/</kbd> focuses the search box from anywhere.

## How it works

```
Claude Code ─┐                            ┌─► ~/.tokenbuddy/data.parquet  ─► pure-Rust agg ─┐
ZCode        │  local session logs        │                                                   ├─► dashboard
Qoder        ├─►  (each tool's own   ─►   │                                                   │   127.0.0.1:8080
WorkBuddy    │     format on disk)        └─► ~/.tokenbuddy/context.parquet ─► in-memory      │
OpenCode     │                                (deduplicated turns)      inverted idx ┘
Mimo / Pi   ──┘
```

Everything right of the arrow is one process: collectors run on sync, Parquet
is both the storage and the exchange format, aggregation and indexing are
pure Rust, and the UI is a single HTML file compiled into the binary.

## Reconciling the numbers (read this before comparing with ccusage)

- **Day boundary**: fixed UTC+8, no DST. ccusage defaults to the machine's
  local timezone — on a +8 machine the two agree day by day; anywhere else
  they differ by a day. That is a definition difference, not a bug on either
  side.
- **Money**: TokenBuddy estimates no cost. Sources whose host reports credits
  (Qoder et al.) pass them through verbatim, never converted.
- **Cache**: cache-read and cache-creation are recorded in their own columns;
  no rollup ever mixes them into input/output.
- **Dedup keys** (what counts as one record per source): the criteria follow
  ccusage's public implementation.

| Source | What bills as one record |
|---|---|
| `claude` | (message.id, requestId) compound identity; falls back to (message.id, sessionId, timestamp) when requestId is missing; non-sidechain only; on key collision the highest token total wins |
| `codex` | skipped unless `total_token_usage` advanced (UI resends don't bill); only the `last_token_usage` increment is recorded |
| `gemini` | later lines overwrite earlier ones for the same message id; cached is split out of the prompt, thoughts join output |
| `qwen` | (session, ts, model, in, out, cached) compound key; assistant turns' `usageMetadata` only |
| others | native primary key or `record_id` — see the comment at the top of `src/store.rs` |

The key formulas are frozen per source: changing one means rewriting the
history it produced, and requires a full re-import. `tokenbuddy doctor`
answers "why isn't this source showing up".

## Agent self-evolution

What makes self-review affordable is tiered summarization: ~80% of sessions
are two-message throwaways that get a free rule-based digest (the first user
message *is* the intent), and only substantive sessions go to an LLM. On a
real 3,391-session corpus that turned 4.2 MB of raw conversation into ~0.4 MB
of digests — **90% fewer tokens** than feeding raw logs back to a model, with
every session still individually summarized (no sampling).

A practical loop: a daily digest for "what did I do today", a weekly pass
over substantive sessions, a monthly full report — and any flow that shows
up three or more times is a skill candidate.

## Fleet sync (optional)

TokenBuddy is local-first by default; Fleet is the opt-in exception that
rolls up several machines into one ledger. Create `~/.tokenbuddy/fleet.toml`
on every machine:

```toml
enabled = true
endpoint = "http://rustfs.lan:9000"   # any S3-compatible endpoint (RustFS, MinIO, …)
bucket = "tokenbuddy"
access_key = "…"
secret_key = "…"
# region = "us-east-1"     # default
# path_style = true        # default
# host_id = "mini"         # default: machine hostname
# auto_push = true         # default: background push every push_interval_secs = 3600
```

- **Push** — `tokenbuddy push` (or the dashboard's ⤒ Push now) syncs locally,
  then PutObjects the whole `data.parquet` to `hosts/{host_id}/data.parquet`.
  One fixed key overwritten in place: idempotent, no small files.
- **Pull** — `tokenbuddy fleet-sync` (or ⤓ Sync now) lists `hosts/` and
  downloads every machine's parquet into `~/.tokenbuddy/fleet/{host}/`,
  skipping unchanged objects via an etag manifest.
- **View** — the dashboard's 数据 → Fleet 多机 switch shows the fleet ledger:
  totals, a per-host table and a host × source matrix, filterable by
  timeRange / source / model / host.
- **Privacy boundary** — only the token bill (`data.parquet`) participates.
  `context.parquet` — your conversation text — never leaves the machine.
- Without `fleet.toml` nothing changes at all: no network calls, no
  background threads, no extra files. The S3 client is a hand-rolled SigV4
  signer over plain HTTP, sized for a LAN RustFS; no S3 SDK is involved.

## Data & privacy

| File | Contents |
|---|---|
| `~/.tokenbuddy/data.parquet` | one row per token-bearing request: source, project, model, tokens, duration, credits… |
| `~/.tokenbuddy/context.parquet` | deduplicated user/assistant turns for search |
| `~/.tokenbuddy/data.parquet.snapshots/` | rotated snapshots kept by full rebuilds (5 retained) |
| `~/.tokenbuddy/fleet.toml` | optional Fleet config; its absence keeps the whole feature off |
| `~/.tokenbuddy/fleet/{host}/data.parquet` | other machines' token bills pulled by fleet-sync |
| `~/.tokenbuddy/fleet/manifest.json` | etag/size bookkeeping so unchanged hosts are not re-downloaded |

The HTTP API is local-only by construction: it binds to `127.0.0.1` and
there is no account, no export target and no outbound network call —
**unless you explicitly enable Fleet sync**, which uploads only
`data.parquet` (never `context.parquet`) to the S3-compatible bucket you
configured.

Fleet uploads can be encrypted at rest: set `encrypt = true` in
`fleet.toml` and the object leaves your machine as XChaCha20-Poly1305
ciphertext (RustCrypto, pure Rust) under a key derived from your existing
`secret_key` — the bucket then stores nothing readable about which projects
ran which models. Pulling hosts with the same `secret_key` decrypt
transparently; objects without the `TBENCRV1` header (old pushes, other
tools) keep working as plain parquet. Tampering or a wrong key fails the
authentication tag instead of yielding garbage.

## HTTP API

<details>
<summary>All endpoints (click to expand)</summary>

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | dashboard (single embedded HTML file) |
| POST | `/api/sync?mode=incremental\|full` | import new log records, refresh context index |
| GET | `/api/summary?timeRange=&source=&model=` | totals + per-source/per-model rollup |
| GET | `/api/timeline?mode=hourly\|daily\|weekly\|monthly` | bucketed usage |
| GET | `/api/metrics` | per-request metric aggregates |
| GET | `/api/heatmap?mode=model_x_source\|model_x_date&metric=` | heatmap matrix |
| GET | `/api/models` | model comparison table |
| GET | `/api/digest?days=7` | current vs previous window, top models, per-source split |
| GET | `/api/insights?limit=20` | deep analysis: hour-of-day rhythm, cache trend, session leaderboard, context fill |
| GET | `/api/brief` | statusline feed: today + trailing 7 days as one small JSON |
| GET | `/api/windows?days=28` | 5-hour window segments + a self-referenced 28-day P90 |
| GET | `/api/anomalies` | daily-usage anomalies (weekday-stratified median/MAD robust z) |
| GET | `/api/pivot?days=30` | project × model pivot |
| GET | `/api/context/search?q=&limit=` | full-text search; `q` accepts `source: project: role: days: -excluded "phrase"` syntax (overrides URL params) |
| GET | `/api/context/session?source=&session_id=&doc_id=&around=` | conversation around a hit |
| GET | `/api/context/stats` | index build status + corpus figures |
| POST | `/api/context/click?doc_id=` | record a search-result click (ranking feedback) |
| GET | `/api/context/quality` | search-quality report |
| GET | `/api/status` | row count, last sync, per-collector presence, index size |
| POST | `/api/context/rebuild` | force a full index rebuild |
| POST | `/api/fleet/push` | sync locally, then PutObject to the fleet bucket |
| POST | `/api/fleet/pull` | pull every host's parquet (etag manifest skips unchanged) |
| GET | `/api/fleet/hosts` | hosts available to the Fleet view |
| GET | `/api/fleet/summary?timeRange=&source=&model=&host=` | fleet totals + per-host rows + host × source matrix |
| GET | `/api/fleet/metrics?…` | latency/cache panel keyed by host |
| GET | `/api/fleet/models?…` | model comparison across hosts |

</details>

## Development

```bash
cargo b        # build --release
cargo c        # check --release
cargo test --release
```

This repo builds release-only (debug artifacts once grew `target/` to 16 GB;
the convention is enforced in `.cargo/config.toml` and [CLAUDE.md](CLAUDE.md)).
Code layout: one `src/<tool>.rs` per collector, `store.rs` for Parquet I/O and
aggregation, `context.rs` for the search index, `main.rs` for the HTTP layer,
and `src/dashboard.html` — the whole UI, embedded at compile time.

## Roadmap

- [ ] `cargo install` / Homebrew packaging
- [ ] Developer ID signing + notarization (needs an Apple Developer account)
- [x] CI-published prebuilt binaries for Apple Silicon / Linux x64 (`v*` tag, downloaded directly by install.sh)
- [x] English UI toggle (header 中/EN button; chrome, filters, fixed messages and chart tooltips translate — conversation content and a few interpolated sentences stay in their original language)
- [ ] Homebrew cask
- [ ] More agents: Cursor, Copilot CLI, Windsurf, …

PRs welcome — especially new collectors.

## License

[MIT](LICENSE) © 2026
