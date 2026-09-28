<div align="center">

# TokenBuddy

**AI 编码工具的 WakaTime——Token 账单 + 会话全文搜索，跑在你自己机器上。**

*你在三个 AI 编程工具之间来回切换，答得上来"这周的 Token 烧在哪"吗？*

**4.7 MB 单二进制 · 零运行时依赖 · 空闲 18 MB · 4.3 万轮对话按需 2 秒建索引**

[English](README.md) · [下载 Release](../../releases) · [问题反馈](../../issues) · [MIT](LICENSE)

**Rust** · **macOS / Linux** · **已支持 12 个工具**

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
| 单二进制体积 | **4.6 MB**（strip 后静态链接，无运行时依赖） |
| 全量编译 | **48 秒**（`cargo build --release`，无需 C++ 工具链） |
| 常驻内存·只看统计 | **~18 MB**——搜索索引不进搜索页不构建，空闲 15 分钟后自动释放 |
| 常驻内存·同步一次 | **峰值 ~104 MB**，连刷多次也不涨（旧版同步几次就到 294 MB 且仍在爬，再早是 379 MB） |
| 常驻内存·索引已加载 | 搜索期间约 165 MB |
| 首次建索引 | 4.3 万轮对话 ≈ **2 秒**，增删改查全自动 |
| 磁盘足迹 | 全部数据就两个 Parquet，可读可拷可 `rm` |

以上每一条都可以用 `cargo run --release --example memprobe` 在你的机器上复现。
没有内嵌查询引擎、没有第三方数据库——聚合和搜索全部纯 Rust。

**为什么"空闲"那个数字才是诚实的。** 统计来自 3 MB 的 `data.parquet`；对话
索引是另一套大得多的结构，只看汇总数字的人根本不会碰它。所以它不在启动时构建
——进搜索页时才构建，15 分钟没人搜就再释放掉。同步也不会触发构建。macOS 不
把释放掉的小块堆页还给 OS，所以某个阶段冲多高，进程之后就一直占多高；这也是
上面按阶段列数字、而不是给一个平均值的原因。

为此改了四处，都是在 3.1 万条请求 / 4.3 万轮对话的真实语料上量的：

- 采集器不再把解析出的每条记录常驻到下次同步；
- 一次同步改成逐个来源"采集→吸收→释放"，不再八个来源同时在手；
- SQLite 系采集器改成让 SQLite 自己去抽那八个需要的字段，不再把整块 `data`
  拷进堆里。真实机器上 mimo 有一条消息的 `data` 达 **19.5 MB**——是一条
  *用户* 消息里内联的 `data:image` 截图，而采集器本来就会丢弃所有非
  assistant 行。那个文件里 99% 的 blob 质量都在用户消息上，assistant 消息
  最大只有 1.7 KB。在任何 JSON 函数之前先用 `length()` 挡掉，单这一项就值
  24 MB——因为 SQLite 必须先把值组装出来才能走进去；
- 两个过滤条件的**顺序是刻意的**：`length()` 在前（便宜，且能在任何人去看
  内部之前挡掉那些巨大的行），`json_valid` 紧挨在每个 `json_extract` 前面，
  因为 SQLite 的 JSON 函数遇到畸形输入是**报错**而不是返回 NULL——一行坏
  数据会让整条查询失败、整个来源瞎掉，而它替代的旧 blob 路径只是跳过那一行。
  顺序反过来，`json_valid` 就必须解析那个 19.5 MB 的 body 才能作答，省下的
  内存也就没了。

超过 1 MB 读取上限的行会被**计数并报告**，而不是静默丢弃。

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

TokenBuddy 攒下的语料是机器可读的，两条 Agent 通道，数字与仪表盘同源同口径：

- **MCP server**——`tokenbuddy mcp` 以 Model Context Protocol 走 stdio
  （零新增依赖）。加一条配置，Agent 就能查用量总账、窗口事实、异常、
  项目 × 模型透视，全文搜索所有历史会话，或做逐源体检：

  ```json
  { "mcpServers": { "tokenbuddy": { "command": "tokenbuddy", "args": ["mcp"] } } }
  ```

- **Skill**——装好
  [`skills/tokenbuddy-analyze/SKILL.md`](skills/tokenbuddy-analyze/SKILL.md)，
  Agent 走本地 HTTP API 做复盘：今天做了什么、哪些流程在反复出现、
  什么该沉淀成 skill 或仓库规则。

配合分层摘要，把原始日志喂给 LLM 的成本砍掉
90%（详见下文"Agent 自我进化"）。

### 🔒 零负担的隐私

服务只绑定 `127.0.0.1:8080`，**除非你显式启用 Fleet 同步，代码里没有一行
网络出站**——没有遥测、没有一个配置项可以把数据导出去、没有账号。整个
"数据库"就是两个文件。删掉 `~/.tokenbuddy/`，TokenBuddy 就对你一无所知。

## 支持的工具

| 工具 | `source` 标识 | 备注 |
|---|---|---|
| Claude Code | `claude` | 读取本地 JSONL 会话日志 |
| ZCode | `zcode` | |
| Qoder | `qoder` | 宿主遮蔽 Token 数，按上报点数（credits）+ 上报的上下文窗口水位计量 |
| WorkBuddy | `workbuddy` | |
| MiniMax Code | `minimax` | 读取 `~/.minimax/v2/sessions/**/messages.jsonl` |
| Hermes Agent | `hermes` | 读取 `~/.hermes/state.db`（`session_model_usage`，含辅助任务调用）；聚合行按次全量替换 |
| Codex CLI | `codex` | 读取 `~/.codex/sessions/**/rollout-*.jsonl`；只计累计值前进的 token_count 事件（自动去重 UI 重发），格式对照 ccusage 官方夹具实现 |
| Gemini CLI | `gemini` | 读取 `~/.gemini/tmp/**/chats/*.{json,jsonl}`；cached 从 prompt 中拆出、thoughts 并入 output，格式对照官方 chatRecording 类型（ccusage 交叉验证） |
| Qwen Code | `qwen` | 读取 `~/.qwen/projects/<项目>/chats/*.jsonl`；按 assistant 轮的 `usageMetadata` 计费并拆出 cached，行内 `cwd` 直接用作项目归因 |
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

加 `--service` 会一并注册开机常驻（macOS 是 launchd user agent，Linux 是
systemd `--user` unit），以后开机就有服务，不用一直挂着一个终端窗口：

```bash
curl -fsSL .../install.sh | bash -s -- --service
```

或到 [Releases](../../releases) 取
`tokenbuddy-v0.5.0-aarch64-apple-darwin.tar.gz`，解压即跑。

### 命令行

```
tokenbuddy [serve]        启动仪表盘（默认 127.0.0.1:8080）
tokenbuddy status         打印记录数、上次同步时间、检测到的工具
tokenbuddy doctor [--json] 逐源体检:日志在哪/可读吗/最近一条记录
tokenbuddy push           同步本地数据并推送到 Fleet
tokenbuddy fleet-sync    拉取全部主机的 Fleet 数据
  --port <端口>           换端口（也可用 TOKENBUDDY_PORT）
  --no-open               不自动打开浏览器（也可用 TOKENBUDDY_NO_OPEN=1）
  -h, --help  -V, --version
```

监听就绪后会自动打开浏览器；但 stdout 不是终端时（也就是被服务管理器拉起
时）不会弹窗。

### 内置 Web 加固

服务端拒绝 `Host` 不是环回地址（或与本监听端口不符）的请求——DNS rebinding
防护；同时拒绝跨源的 POST（校验 `Origin` / `Sec-Fetch-Site`）——浏览器里
任意网页都可以向 `127.0.0.1` 发请求，所以服务端必须自己拒绝不是自己页面发
起的写操作。curl、脚本与仪表盘本身完全不受影响。若仪表盘被反向代理转发且
代理改写了 `Host`，用 `TOKENBUDDY_ALLOWED_HOSTS=proxy.example.lan`（逗号
分隔）把代理主机名加入白名单。

### 一行接进 statusline / 提示符

`GET /api/brief` 用一个小 JSON 返回今日与近 7 天用量;`tokenbuddy today`
不启服务直接打印同一行。两者只扫本地 parquet 的两列,全程个位数毫秒——
对比同类监控工具的 statusline 事故(进程分裂、OOM、300% CPU)。Claude
Code 的 statusline 直接可用:

```bash
tokenbuddy today
# 今日 21.3M tokens（入 358.4K · 出 126.4K · 239 请求） · 7日 124.8M
```

把仪表盘开放给其他机器是显式且上锁的操作：`TOKENBUDDY_TOKEN=<密钥>
tokenbuddy --addr 0.0.0.0` —— 非环回绑定**没有密钥会直接拒绝启动**。此后
每个请求都需要令牌（脚本用 `Authorization: Bearer <密钥>` 头；浏览器打开
`http://<ip>:8080/?token=<密钥>`，页面会自动把令牌带进所有 API 调用）。
只在本机使用的场景行为完全不变。

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
- 全新安装时首屏会出现导入引导，列出检测到的工具日志目录——不点按钮就什么
  都不会发生。导入之后用 **同步数据**（或 `POST /api/sync`）拉最新日志，
  上下文索引会同步刷新。
- 顶栏 **数据源** 显示每个采集器找没找到日志、上次有没有采集失败。某个来源
  失败只影响它自己那一路，其余来源照常导入。
- **全量重建** 从现存日志重采，因此**可能丢掉**日志已被轮转掉的记录，所以会
  先确认，并如实报告净变化（`净减 N`）而不是"重新采集到多少条"。
- 每张表都能导出 CSV（⤓ CSV），带 UTF-8 BOM，Excel 打开中文不会乱码。
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

## 对账口径（想和 ccusage 核数字时读这节）

- **日界**：固定 UTC+8，无夏令时。ccusage 默认用机器本地时区——在 +8 时区的
  机器上两者逐日可比，其他时区会差一天（这是口径差异，不是谁算错）。
- **货币**：TokenBuddy 不估算任何成本。host 上报 credits 的源（Qoder 等）原样
  透传，绝不折算。
- **缓存**：cache-read 与 cache-creation 单列入账，任何汇总都不把它们混进
  input/output。
- **去重键**（每个源"什么算一条"）：判定依据全部对齐 ccusage 的公开实现。

| 源 | 记一条的判据 |
|---|---|
| `claude` | (message.id, requestId) 复合身份；requestId 缺失时降级 (message.id, sessionId, timestamp)；非 sidechain；同键保留 token 总和最大者 |
| `codex` | `total_token_usage` 不前进即跳过（UI 重发不计），只计 `last_token_usage` 增量 |
| `gemini` | 同消息 id 后行覆盖前行；cached 从 prompt 拆出，thoughts 并入 output |
| `qwen` | (session, ts, model, in, out, cached) 复合键；只记 assistant 轮的 `usageMetadata` |
| 其余 | 源生主键或 `record_id`，详见 `src/store.rs` 顶部注释 |

去重键公式按源冻结：改动公式等于重写历史口径，需要全量重导。doctor
（`tokenbuddy doctor`）能回答"这个源为什么没统计到"。

## Agent 自我进化

让这件事负担得起的关键是分层摘要：约 80% 的会话是两条消息的即用即弃，走零
成本的规则摘要（首条用户消息即意图）；只有实质性会话才交给 LLM。在真实的
3,391 个会话语料上，这把 4.2 MB 原始对话压缩成约 0.4 MB 摘要——比把原始日志
直接喂回模型**省 90% token**，且每个会话仍有独立摘要（不抽样）。

实用节奏：每日摘要回答"今天做了什么"，每周增量过一遍实质会话，每月出全量
报告——任何出现三次以上的流程都是 skill 候选。

## Fleet 多机汇聚（可选）

TokenBuddy 默认 local-first；Fleet 是唯一的可选例外，把多台机器的 token
账单汇成一本总账。在每台机器上创建 `~/.tokenbuddy/fleet.toml`：

```toml
enabled = true
endpoint = "http://rustfs.lan:9000"   # 任意 S3 兼容端点（RustFS、MinIO……）
bucket = "tokenbuddy"
access_key = "…"
secret_key = "…"
# region = "us-east-1"     # 默认值
# path_style = true        # 默认值
# host_id = "mini"         # 默认取主机名
# auto_push = true         # 默认开启：每 push_interval_secs = 3600 秒后台推送
```

- **推送**——`tokenbuddy push`（或仪表盘 ⤒ Push now）先本地同步，再把整个
  `data.parquet` PutObject 到 `hosts/{host_id}/data.parquet`。固定 key 覆盖
  写：幂等、无小文件。
- **拉取**——`tokenbuddy fleet-sync`（或 ⤓ Sync now）列出 `hosts/` 前缀，
  把每台机器的 parquet 下载到 `~/.tokenbuddy/fleet/{host}/`，manifest 记录
  etag 未变化的直接跳过。
- **查看**——仪表盘「数据 → Fleet 多机」切换出总账：汇总卡、各机明细表、
  主机 × 来源矩阵，支持 timeRange / 来源 / 模型 / 主机过滤。
- **隐私边界**——参与的只有 token 账单（`data.parquet`）；对话全文
  `context.parquet` 永远不出机器。
- 不写 `fleet.toml` 就一切照旧：没有网络调用、没有后台线程、没有额外文件。
  S3 客户端是手写 SigV4 + 内网明文 HTTP，为局域网 RustFS 而生，无任何
  S3 SDK。

## 数据与隐私

| 文件 | 内容 |
|---|---|
| `~/.tokenbuddy/data.parquet` | 每行一条带 Token 的请求：来源、工程、模型、Token 数、耗时、点数…… |
| `~/.tokenbuddy/context.parquet` | 去重后的 user/assistant 对话轮（供搜索） |
| `~/.tokenbuddy/data.parquet.snapshots/` | 全量重建保留的轮转快照（保留 5 份） |
| `~/.tokenbuddy/fleet.toml` | 可选的 Fleet 配置；文件不存在则整个功能静默关闭 |
| `~/.tokenbuddy/fleet/{host}/data.parquet` | fleet-sync 拉回的其他机器 token 账单 |
| `~/.tokenbuddy/fleet/manifest.json` | etag/size 台账，未变化的主机不重复下载 |

HTTP API 天然只监听本地：绑定 `127.0.0.1`，没有账号、没有导出目标、没有
网络出站——**除非你显式启用 Fleet 同步**，且即便那时上传的也只有
`data.parquet`（绝无 `context.parquet`），去往的是你自己配置的 S3 兼容
存储。

Fleet 上传可开静态加密：在 `fleet.toml` 里写 `encrypt = true`，出机器的
对象就是 XChaCha20-Poly1305 密文（RustCrypto，纯 Rust 实现），内容密钥
从既有 `secret_key` 派生——桶里不再有任何可读的"哪个项目/哪个模型"。
同密钥的拉取端透明解密；没有 `TBENCRV1` 头的对象（旧推送、其他工具）
按明文 parquet 继续，互不干扰。篡改或密钥错误会在认证标签处失败，
绝不会解出垃圾数据。

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
| GET | `/api/brief` | statusline 用：今日 + 近 7 天一行小 JSON |
| GET | `/api/windows?days=28` | 5 小时窗口分段事实 + 28 天 P90 自参考 |
| GET | `/api/anomalies?days=56` | 日用量异常（工作日分层 median/MAD 稳健 z） |
| GET | `/api/pivot?days=30` | 项目 × 模型透视 |
| GET | `/api/context/search?q=&limit=` | 全文搜索；`q` 支持语法 `source: project: role: days: -排除词 "短语"`（语法优先于 URL 参数） |
| GET | `/api/context/session?source=&session_id=&doc_id=&around=` | 命中处的上下文会话 |
| GET | `/api/context/stats` | 索引构建状态 + 语料规模 |
| POST | `/api/context/click?doc_id=` | 记录搜索结果点击（排序反馈） |
| GET | `/api/context/quality` | 搜索质量报告 |
| POST | `/api/context/rebuild` | 强制全量重建索引 |
| POST | `/api/fleet/push` | 本地同步后整文件推送到 Fleet bucket |
| POST | `/api/fleet/pull` | 拉取全部主机 parquet（etag manifest 跳过未变化） |
| GET | `/api/fleet/hosts` | Fleet 视图可用的主机列表 |
| GET | `/api/fleet/summary?timeRange=&source=&model=&host=` | Fleet 总账 + 各机明细 + 主机 × 来源矩阵 |
| GET | `/api/fleet/metrics?…` | 按主机分维的耗时/缓存面板 |
| GET | `/api/fleet/models?…` | 跨主机的模型对比 |

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
- [x] CI 发布：Apple Silicon / Linux x64 预编译二进制（`v*` 标签触发，install.sh 直接下载）
- [x] 界面英文切换（右上角「中/EN」按钮；导航、筛选、固定文案与图表 tooltip 译为英文，对话内容与少数嵌数字句子保留原文）
- [ ] 更多工具：Cursor、Copilot CLI、Windsurf……
- [ ] 可配置单价的成本表

欢迎 PR——尤其是新的采集器。

## 许可证

[MIT](LICENSE) © 2026
