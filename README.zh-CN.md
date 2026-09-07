<div align="center">

# TokenBuddy

**AI 编码工具的 WakaTime —— 本地优先的 Token 用量分析、会话统计与全文上下文搜索。**

一份 Parquet · 全部工具 · 100% 本地

[English](README.md) · [问题反馈](../../issues) · [许可证](LICENSE)

**Rust** · **MIT** · **macOS / Linux** · **已支持 7 个工具**

</div>

---

TokenBuddy 安静地读你机器上 AI 编码工具本来就写好的本地会话日志，把它们汇成
**一份可查询的 Parquet 文件**，外加一个小而快的网页仪表盘：哪些工具、哪些模型
在真正被你使用，Token（以及缓存命中、点数）烧了多少，什么时段在写代码——还有
一台全文搜索引擎，覆盖**你和所有 AI 工具发生过的每一段对话**。

不联网、不上报、不装数据库服务。一个静态 Rust 单二进制，只绑定 `127.0.0.1`。

![TokenBuddy 仪表盘](docs/screenshot-dashboard.png)

## 为什么做这个

每个 AI 编码工具都在本地记自己的日志——格式各异、目录各异，记完就没人再看。
一旦你同时用超过一个工具，这些问题就变得没法回答：

- 这周烧了多少 Token？都烧在哪个模型上？
- 哪个工具真的好用——Claude Code、ZCode、Qoder、OpenCode……？
- 缓存命中率多少？我的上下文策略到底有没有生效？
- *三周前那个线程锁的 bug，我们在哪儿聊的来着？*——哪个工具、哪个工程、
  哪次会话？

TokenBuddy 在本地回答以上全部问题，只需要一个二进制。

## 功能

**用量分析**
逐请求统计输入 / 输出 / 缓存读 / 缓存写 Token、耗时与 TTFT（以来源日志上报
为准），并为遮蔽 Token 数的来源记录宿主上报的点数（credits）。支持按天/小时/
周/月、按来源、按模型、按模型族（`claude-sonnet-4-5-…` → `sonnet`）多维汇总；
本期 vs 上期速览、来源对比、模型热力图（模型 × 来源、模型 × 日期）。所有时间
分桶使用固定 UTC+8——没有夏令时的坑。

**全文上下文搜索**
纯 Rust 内存倒排索引，只索引 user/assistant 对话轮——工具输出、system 重发
上下文一律不进索引，重复缓存不会淹没搜索结果。文档按内容哈希去重。分词三层：
ASCII 词元支持前缀模糊、jieba 负责中文词粒度、CJK 字符 bigram 兜底跨词边界的
片段查询。候选级联：词元 AND → bigram AND → 共享词元排序，IDF 加权，带点击
反馈与搜索质量面板。十万轮量级的索引几秒建完，持久化到 Parquet。

**一份 Parquet，装下所有工具**
所有来源写入同一个 `~/.ltc/data.parquet`（Arrow schema，zstd 压缩）。
summary / timeline 走 DuckDB SQL 查 `read_parquet`；metrics / 热力图 / 模型表
走纯 Rust 聚合路径，只投影需要的列（读取量省约 62%）。同步是增量的——
见过的记录自动跳过；全量重建保留轮转快照。

**零负担的隐私**
服务只绑定 `127.0.0.1:8080`，从不外呼；整个"数据库"就是两个文件，可读、
可拷、可删。你的对话永远不会离开这台机器。

## 支持的工具

| 工具 | `source` 标识 | 备注 |
|---|---|---|
| Claude Code | `claude` | 读取本地 JSONL 会话日志 |
| ZCode | `zcode` | |
| Qoder | `qoder` | 宿主遮蔽 Token 数，按上报点数（credits）计量 |
| WorkBuddy | `workbuddy` | |
| OpenCode | `opencode` | |
| Mimo | `mimo` | |
| Pi | `pi` | |

每个采集器就是 `src/` 下一个小文件，职责单一："从这个工具的本地日志里提取
对话 + Token 事件"。新增一个工具 = 提一个采集器 PR，不用动管线。

## 快速开始

需要 Rust 1.75+。

```bash
git clone https://github.com/walker83/TokenBuddy.git
cd TokenBuddy
cargo b                     # 即 build --release（别名见 .cargo/config.toml）
./target/release/ltc
# 打开 http://127.0.0.1:8080
```

- 仪表盘立即可用；首次索引构建在后台进行（构建期间搜索会如实报告进度，
  而不是假装搜到空结果）。
- 点仪表盘的 **同步数据**（或 `POST /api/sync`）拉取最新日志；上下文索引
  随之自动刷新。
- 仪表盘任意位置按 <kbd>/</kbd> 聚焦搜索框。
- 二进制与数据路径沿用项目原名：CLI 叫 `ltc`，数据在 `~/.ltc/` 下。

## 工作原理

```
Claude Code ─┐                            ┌─► ~/.ltc/data.parquet     ─► DuckDB SQL ─┐
ZCode        │  本地会话日志               │                                          ├─► 仪表盘
Qoder        ├─►  （各工具自己的      ─►  │                                          │   127.0.0.1:8080
WorkBuddy    │     格式与目录）           └─► ~/.ltc/context.parquet ─► 内存倒排索引  ┘
OpenCode     │                                （去重后的对话轮）
Mimo / Pi  ──┘
```

箭头右边是一个进程：同步时运行采集器，Parquet 既是存储也是交换格式，DuckDB
以内嵌方式运行，UI 是编译期打进二进制的单个 HTML 文件。

## 数据与隐私

| 文件 | 内容 |
|---|---|
| `~/.ltc/data.parquet` | 每行一条带 Token 的请求：来源、工程、模型、Token 数、耗时、点数…… |
| `~/.ltc/context.parquet` | 去重后的 user/assistant 对话轮（供搜索） |
| `~/.ltc/data.parquet.snapshots/` | 全量重建保留的轮转快照（保留 5 份） |

HTTP API 天然只监听本地；没有可泄密的配置、没有账号、没有导出目标。删掉
`~/.ltc/`，TokenBuddy 就对你一无所知。

## HTTP API

<details>
<summary>全部端点（点击展开）</summary>

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/` | 仪表盘（单个内嵌 HTML 文件） |
| POST | `/api/sync?mode=incremental\|full` | 导入新日志记录，刷新上下文索引 |
| GET | `/api/summary?timeRange=&source=&model=` | 总量 + 按来源/模型汇总 |
| GET | `/api/timeline?mode=hourly\|daily\|weekly\|monthly` | 分桶用量 |
| GET | `/api/metrics` | 逐请求指标聚合 |
| GET | `/api/heatmap?mode=model_x_source\|model_x_date&metric=` | 热力图矩阵 |
| GET | `/api/models` | 模型对比表 |
| GET | `/api/digest?days=7` | 本期 vs 上期、Top 模型、来源拆分 |
| GET | `/api/context/search?q=&source=&role=&project=&days=&limit=` | 全文搜索 |
| GET | `/api/context/session?source=&session_id=&doc_id=&around=` | 命中处的上下文会话 |
| GET | `/api/context/stats` | 索引构建状态 + 语料规模 |
| POST | `/api/context/click?doc_id=` | 记录搜索结果点击（排序反馈） |
| GET | `/api/context/quality` | 搜索质量报告 |
| POST | `/api/context/rebuild` | 强制全量重建索引 |

</details>

## 开发

```bash
cargo b        # build --release
cargo c        # check --release
cargo test --release
```

本仓库只做 release 构建（debug 中间产物曾把 `target/` 撑到 16 GB；约定固化在
`.cargo/config.toml` 与 [CLAUDE.md](CLAUDE.md)）。代码结构：每个采集器一个
`src/<tool>.rs`，`store.rs` 负责 Parquet + DuckDB + Rust 聚合，`context.rs`
是搜索索引，`main.rs` 是 HTTP 层，`src/dashboard.html` 是整个 UI、编译期内嵌。

## Roadmap

- [ ] `cargo install` / Homebrew 打包
- [ ] 界面英文切换（当前中文优先）
- [ ] 更多工具：Cursor、Copilot CLI、Windsurf、Gemini CLI……
- [ ] 可配置单价的成本表

欢迎 PR——尤其是新的采集器。

## 许可证

[MIT](LICENSE) © 2026
