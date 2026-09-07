<div align="center">

# TokenBuddy

**WakaTime for AI coding agents — local-first token usage, session analytics
and full-text context search.**

One Parquet file · every agent · 100% local

[简体中文说明](README.zh-CN.md) · [Issues](../../issues) · [License](LICENSE)

**Rust** · **MIT** · **macOS / Linux** · **7 agents supported**

</div>

---

TokenBuddy watches the local session logs your AI coding tools already write,
and turns them into **one queryable Parquet file** plus a fast little web
dashboard: which tools and models you actually use, how many tokens (and cache
hits, and credits) they burn, when you code — and a full-text search engine
across **every conversation you have ever had with every agent**.

No cloud. No telemetry. No database server. One static Rust binary that binds
to `127.0.0.1` only.

![TokenBuddy dashboard](docs/screenshot-dashboard.png)

## Why

Every AI coding tool keeps its own local logs — in its own format, in its own
directory, useful to nobody. Once you run more than one agent, questions like
these become unanswerable:

- How many tokens did I burn this week, and on which model?
- Which tool earns its keep — Claude Code, ZCode, Qoder, OpenCode…?
- What was my cache-hit rate? Is my context strategy actually working?
- *Where did I discuss that threading bug three weeks ago?* — in which tool,
  which project, which session?

TokenBuddy answers all of them, locally, with a single binary.

## Features

**Usage analytics**
Per-request input / output / cache-read / cache-creation tokens, duration and
TTFT (where the source log reports it), plus host-reported credits for sources
that mask token counts. Rollups by day/hour/week/month, by source, by model and
by model family (`claude-sonnet-4-5-…` → `sonnet`). Period-over-period digest,
source comparison, model heatmaps (model × source, model × date). Time buckets
use a fixed UTC+8 offset — no DST surprises.

**Full-text context search**
A pure-Rust, in-memory inverted index over user/assistant turns only — tool
output and re-sent system context never enter the index, so cached boilerplate
doesn't drown real conversations. Documents are content-hash deduplicated.
Tokenizing is three-layer: ASCII tokens with prefix matching, jieba word
segmentation for Chinese, and CJK character bigrams as a recall layer that
catches matches crossing word boundaries. Candidates cascade from strict
token-AND to bigram-AND to shared-token ranking, scored by IDF, with click
feedback and a search-quality panel. An index of ~10⁵ turns builds in seconds
and persists to Parquet.

**One local Parquet, every agent**
All sources append into a single `~/.tokenbuddy/data.parquet` (Arrow schema, zstd).
Summary/timeline queries go through DuckDB SQL over `read_parquet`; metrics,
heatmaps and model tables use a pure-Rust aggregation path that projects only
the columns it needs (~62% less read volume). Sync is incremental —
already-seen records are skipped; full rebuilds keep rotated snapshots.

**Zero-friction privacy**
The server binds to `127.0.0.1:8080`, never phones home, and the entire
database is two files you can read, copy or `rm`. Your conversations never
leave the machine.

## Supported agents

| Tool | `source` id | Notes |
|---|---|---|
| Claude Code | `claude` | reads local JSONL session logs |
| ZCode | `zcode` | |
| Qoder | `qoder` | token counts masked by host; usage tracked via host-reported credits |
| WorkBuddy | `workbuddy` | |
| OpenCode | `opencode` | |
| Mimo | `mimo` | |
| Pi | `pi` | |

Each collector is one small file in `src/` implementing "extract conversations
+ token events from this tool's local logs". Adding an agent is a
community-friendly pull request, not a fork of the pipeline.

## Quick start

Requires Rust 1.75+.

```bash
git clone https://github.com/walker83/TokenBuddy.git
cd TokenBuddy
cargo b                     # build --release (alias defined in .cargo/config.toml)
./target/release/tokenbuddy
# open http://127.0.0.1:8080
```

- The dashboard loads immediately; the first index build runs in the
  background (search reports its progress instead of pretending to be empty).
- Hit the **Sync** button (or `POST /api/sync`) to pull the latest logs; the
  context index refreshes alongside automatically.
- In the dashboard, <kbd>/</kbd> focuses the search box from anywhere.

## How it works

```
Claude Code ─┐                            ┌─► ~/.tokenbuddy/data.parquet     ─► DuckDB SQL ─┐
ZCode        │  local session logs        │                                          ├─► dashboard
Qoder        ├─►  (each tool's own   ─►   │                                          │   127.0.0.1:8080
WorkBuddy    │     format on disk)        └─► ~/.tokenbuddy/context.parquet ─► in-memory    │
OpenCode     │                                (deduplicated turns)      inverted idx ┘
Mimo / Pi  ──┘
```

Everything on the right of the arrow is one process: collectors run on sync,
Parquet is both the storage and the exchange format, DuckDB is embedded, and
the UI is a single HTML file compiled into the binary.

## Data & privacy

| File | Contents |
|---|---|
| `~/.tokenbuddy/data.parquet` | one row per token-bearing request: source, project, model, tokens, duration, credits… |
| `~/.tokenbuddy/context.parquet` | deduplicated user/assistant turns for search |
| `~/.tokenbuddy/data.parquet.snapshots/` | rotated snapshots kept by full rebuilds (5 retained) |

The HTTP API is local-only by construction. There is no config to leak, no
account, no export target. Delete `~/.tokenbuddy/` and TokenBuddy knows nothing about
you again.

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
| GET | `/api/context/search?q=&source=&role=&project=&days=&limit=` | full-text search |
| GET | `/api/context/session?source=&session_id=&doc_id=&around=` | conversation around a hit |
| GET | `/api/context/stats` | index build status + corpus figures |
| POST | `/api/context/click?doc_id=` | record a search-result click (ranking feedback) |
| GET | `/api/context/quality` | search-quality report |
| POST | `/api/context/rebuild` | force a full index rebuild |

</details>

## Development

```bash
cargo b        # build --release
cargo c        # check --release
cargo test --release
```

This repo builds release-only (debug artifacts once grew `target/` to 16 GB;
the convention is enforced in `.cargo/config.toml` and [CLAUDE.md](CLAUDE.md)).
Code layout: one `src/<tool>.rs` per collector, `store.rs` for Parquet +
DuckDB + Rust aggregation, `context.rs` for the search index, `main.rs` for
the HTTP layer, and `src/dashboard.html` — the whole UI, embedded at compile
time.

## Roadmap

- [ ] `cargo install` / Homebrew packaging
- [ ] English UI toggle (dashboard is currently Chinese-first)
- [ ] More agents: Cursor, Copilot CLI, Windsurf, Gemini CLI, …
- [ ] Cost tables with configurable per-model pricing

PRs welcome — especially new collectors.

## License

[MIT](LICENSE) © 2026
