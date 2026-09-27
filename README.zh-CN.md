<div align="center">

# TokenBuddy

**AI 编码工具的 WakaTime——Token 账单 + 会话全文搜索，跑在你自己机器上。**

*你在三个 AI 编程工具之间来回切换，答得上来"这周的 Token 烧在哪"吗？*

**4.1 MB 单二进制 · 零运行时依赖 · <100 MB 常驻 · 4.3 万轮对话 2 秒建索引**

[English](README.md) · [下载 Release](../../releases) · [问题反馈](../../issues) · [MIT](LICENSE)

**Rust** · **macOS / Linux** · **已支持 7 个工具**

</div>

---

TokenBuddy 安静地读你机器上 AI 编码工具本来就写好的本地会话日志，把它们汇成
**一份可查询的 Parquet**，外加一个秒开的网页仪表盘：哪些工具、哪些模型在真正
被你使用，Token（以及缓存命中、点数）烧了多少，什么时段在写代码——还有一台
全文搜索引擎，覆盖**你和所有 AI 工具发生过的每一段对话**。

不联网、不上报、没有账号、没有任何需要安装的数据库。**单个静态二进制，
只绑定 `127.0.0.1`——隐私不是功能，是它的物理形态。**

![TokenBuddy 仪表盘](docs/screenshot-dashboard.png)

## 数字说话

| | |
|---|---|
| 单二进制体积 | **4.1 MB**（strip 后静态链接，无运行时依赖） |
| 全量编译 | **48 秒**（`cargo build --release`，无需 C++ 工具链） |
| 服务常驻内存 | **< 100 MB**——实测 4.3 万轮对话全量索引（旧版是 379 MB） |
| 首次建索引 | 4.3 万轮对话 ≈ **2 秒**，增删改查全自动 |
| 磁盘足迹 | 全部数据就两个 Parquet，可读可拷可 `rm` |

以上每一条都可以用 `cargo run --release --example memprobe` 在你的机器上复现。
没有内嵌查询引擎、没有第三方数据库——聚合和搜索全部纯 Rust。

## 为什么需要它

每个 AI 编码工具都在本地记自己的日志——格式各异、目录各异，记完就没人再看。
一旦你同时用超过一个工具，这些问题就变得没法回答：

- 这周烧了多少 Token？都烧在哪个模型上？哪个订阅在吃灰？
- 缓存命中率多少？我引以为傲的上下文策略到底有没有生效？
- 哪个工具真的好用——Claude Code、ZCode、Qoder、OpenCode……？
- *三周前那个线程锁的 bug，我们到底在哪儿聊的来着？*

TokenBuddy 在本地回答以上全部问题，只需要一个二进制。

## 功能

### 📊 一张账单：每个 Token 去向透明

逐请求统计输入 / 输出 / 缓存读 / 缓存写 Token、耗时与 TTFT（以来源日志上报
为准），为遮蔽 Token 数的来源记录宿主点数（credits）。按天/小时/周/月、按
来源、按模型、按模型族（`claude-sonnet-4-5-…` → `sonnet`）多维汇总；本期 vs
上期速览、来源对比、模型热力图（模型 × 来源、模型 × 日期）。所有时间分桶固定
UTC+8——没有夏令时的坑。

### 🧠 深度分析：看清你的真实工作节奏

同一份 parquet 之上的一个附加标签页：按小时的工作节奏直方图、逐日缓存效率
趋势（附缓存承接的原始 token 量）、会话 Top 榜（点击直达完整对话）、以及
上下文水位趋势（来自会上报该指标的来源——Qoder 的 `context_usage_ratio`，
是它遮蔽 Token 后唯一存活的 token 尺度信号）。全部是原始事实，零推算。

### 🔍 一台对话时光机：搜到你说过的每一句话

这是用过就回不去的功能。纯 Rust 倒排索引，只索引 user/assistant 对话轮——
工具输出、system 重发上下文一律不进索引，重复缓存淹不掉真对话。文档按内容
哈希去重。分词两层：ASCII 词元支持前缀模糊，CJK 字符 bigram 兜底跨词边界
的片段查询，查询期子串复验负责精度——**中文搜索不需要词典**（曾试过 jieba，
词典 alone 常驻 55 MB，删了之后召回没掉，内存掉了一大截）。候选级联：内容词
AND → IDF 加权排序，带点击反馈与搜索质量面板。

索引天生紧凑：正文压缩进 zstd arena 按候选解压、postings 以 delta-varint
编码进单一平坦 arena、元数据全部驻留内化。4.3 万轮对话 2 秒建完、常驻不到
100 MB——**一台跑在你笔记本上的"全部 AI 记忆"检索系统**。

### 📦 一份 Parquet，装下所有工具

所有来源写入同一个 `~/.tokenbuddy/data.parquet`（Arrow schema，zstd 压缩），
聚合全部走纯 Rust 路径，只投影需要的列（读取量省约 62%）。同步是增量的——
见过的记录自动跳过；全量重建保留轮转快照。每个采集器就是 `src/` 下一个小
文件，新增一个工具 = 提一个采集器 PR，不用动管线。

### 🤖 一个 skill：让你的 Agent 学会分析自己

TokenBuddy 攒下的语料是机器可读的，装好
[`skills/tokenbuddy-analyze/SKILL.md`](skills/tokenbuddy-analyze/SKILL.md)，
你的编码 Agent 就能直连本地 API 做复盘：今天做了什么、哪些流程在反复出现、
什么该沉淀成 skill 或仓库规则。配合分层摘要，把原始日志喂给 LLM 的成本砍掉
90%（详见下文"Agent 自我进化"）。

### 🔒 零负担的隐私

服务只绑定 `127.0.0.1:8080`，代码里没有一行网络出站、没有一个配置项可以
把数据导出去。整个"数据库"就是两个文件。删掉 `~/.tokenbuddy/`，TokenBuddy
就对你一无所知。

## 支持的工具

| 工具 | `source` 标识 | 备注 |
|---|---|---|
| Claude Code | `claude` | 读取本地 JSONL 会话日志 |
| ZCode | `zcode` | |
| Qoder | `qoder` | 宿主遮蔽 Token 数，按上报点数（credits）+ 上报的上下文窗口水位计量 |
| WorkBuddy | `workbuddy` | |
| MiniMax Code | `minimax` | 读取 `~/.minimax/v2/sessions/**/messages.jsonl` |
| OpenCode | `opencode` | |
| Mimo | `mimo` | |
| Pi | `pi` | |

## 快速开始

**Apple Silicon / Linux x64** —— 一键安装脚本（macOS 推荐，原因见下方
Gatekeeper 说明）：

```bash
curl -fsSL https://raw.githubusercontent.com/walker83/TokenBuddy/main/scripts/install.sh | bash
tokenbuddy
```

或到 [Releases](../../releases) 取
`tokenbuddy-v0.4.1-aarch64-apple-darwin.tar.gz`，解压即跑。

### macOS 提示"无法打开 / 不安全"？

发布二进制是 ad-hoc 签名——没有付费的 Apple 开发者账号就无法公证，
而**通过浏览器下载**的压缩包会带 quarantine 隔离属性，Gatekeeper 因此拦截。
两种解决：

```bash
# a) 解压后移除隔离属性
xattr -d com.apple.quarantine ./tokenbuddy

# b) 或直接用安装脚本：curl 下载不会附加隔离属性，天然绕开该问题
curl -fsSL https://raw.githubusercontent.com/walker83/TokenBuddy/main/scripts/install.sh | bash
```

正式的 Developer ID 签名 + 公证已列入 Roadmap（需要 Apple 开发者账号）。

**其他平台从源码构建**（需要 Rust 1.75+，全程约 48 秒）：

```bash
git clone https://github.com/walker83/TokenBuddy.git
cd TokenBuddy
cargo b                     # 即 build --release（别名见 .cargo/config.toml）
./target/release/tokenbuddy
# 打开 http://127.0.0.1:8080
```

- 仪表盘立即可用；首次索引构建在后台进行（构建期间搜索会如实报告进度，
  而不是假装搜到空结果）。
- 点仪表盘的 **同步数据**（或 `POST /api/sync`）拉取最新日志；上下文索引
  随之自动刷新。
- 仪表盘任意位置按 <kbd>/</kbd> 聚焦搜索框。

## 工作原理

```
Claude Code ─┐                            ┌─► ~/.tokenbuddy/data.parquet  ─► 纯 Rust 聚合    ─┐
ZCode        │  本地会话日志               │                                                   ├─► 仪表盘
Qoder        ├─►  （各工具自己的      ─►  │                                                   │   127.0.0.1:8080
WorkBuddy    │     格式与目录）           └─► ~/.tokenbuddy/context.parquet ─► 内存倒排索引     ┘
OpenCode     │                                （去重后的对话轮）
Mimo / Pi  ──┘
```

箭头右边是一个进程：同步时运行采集器，Parquet 既是存储也是交换格式，聚合与
索引全在纯 Rust 里完成，UI 是编译期打进二进制的单个 HTML 文件。

## Agent 自我进化

让这件事负担得起的关键是分层摘要：约 80% 的会话是两条消息的即用即弃，走零
成本的规则摘要（首条用户消息即意图）；只有实质性会话才交给 LLM。在真实的
3,391 个会话语料上，这把 4.2 MB 原始对话压缩成约 0.4 MB 摘要——比把原始日志
直接喂回模型**省 90% token**，且每个会话仍有独立摘要（不抽样）。

实用节奏：每日摘要回答"今天做了什么"，每周增量过一遍实质会话，每月出全量
报告——任何出现三次以上的流程都是 skill 候选。

## 数据与隐私

| 文件 | 内容 |
|---|---|
| `~/.tokenbuddy/data.parquet` | 每行一条带 Token 的请求：来源、工程、模型、Token 数、耗时、点数…… |
| `~/.tokenbuddy/context.parquet` | 去重后的 user/assistant 对话轮（供搜索） |
| `~/.tokenbuddy/data.parquet.snapshots/` | 全量重建保留的轮转快照（保留 5 份） |

HTTP API 天然只监听本地；没有可泄密的配置、没有账号、没有导出目标。

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
| GET | `/api/insights?limit=20` | 深度分析：分时节奏、缓存趋势、会话 Top、上下文水位 |
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
`src/<tool>.rs`，`store.rs` 负责 Parquet 读写与聚合，`context.rs`
是搜索索引，`main.rs` 是 HTTP 层，`src/dashboard.html` 是整个 UI、编译期内嵌。

## Roadmap

- [ ] `cargo install` / Homebrew 打包
- [ ] Developer ID 签名 + 公证（需 Apple 开发者账号）
- [ ] CI 发布：Linux / Intel macOS 预编译二进制
- [ ] 界面英文切换（当前中文优先）
- [ ] 更多工具：Cursor、Copilot CLI、Windsurf、Gemini CLI……
- [ ] 可配置单价的成本表

欢迎 PR——尤其是新的采集器。

## 许可证

[MIT](LICENSE) © 2026
