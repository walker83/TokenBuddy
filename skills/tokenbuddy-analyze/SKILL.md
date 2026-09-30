---
name: tokenbuddy-analyze
description: 查询 TokenBuddy 本地账本与全部 AI 会话历史——"烧了多少 token / 哪个模型 / 缓存命中率""当时 XX 怎么聊的""每日复盘与经验沉淀"。触发词:TokenBuddy、token 用量、会话搜索、复盘、自我进化。
---

# TokenBuddy Analyze — AI 编程助手的会话数据分析技能

> 把本 skill 装进你的编码 Agent（Claude Code / ZCode / OpenCode 等），它就能
> 直接查询 TokenBuddy 的本地 API，回答"我的 Token 烧在哪""那个 bug 当时怎么
> 聊的"这类问题，并做每日/每周复盘。

## 什么时候用

用户提到以下任意一种时触发本 skill：

- "这周/这个月烧了多少 Token""哪个模型用得最多""缓存命中率"
- "我们当时在哪聊过 XX""找回那次讨论""搜索所有会话里的 XX"
- "复盘一下今天的 AI 使用""总结我的编程习惯""自我进化/沉淀经验"
- TokenBuddy / tokenbuddy / token 用量 / 会话搜索

## 前置条件

TokenBuddy 服务在本机运行（默认 `http://127.0.0.1:8080`，只绑定本机回环）。
没起就先启动：在 TokenBuddy 仓库目录 `cargo b && ./target/release/tokenbuddy`。
首次启动后台建索引，搜索就绪前 `/api/context/stats` 会如实报告构建进度。

两条等价通道，数字同源同口径：

- **HTTP API**（本 skill 主体）；
- **MCP server**：`tokenbuddy mcp`（stdio），一次配置后 16 个工具直接可调，适合长驻 Agent。
  全部工具名：usage_summary / usage_timeline / daily_report / window_facts / quota /
  work_receipts / anomalies / project_pivot / search_context / sessions / source_health /
  models / metrics / active_time / digest / heatmap（与 HTTP 端点一一对应，数字同源同口径）。

可选过滤器只有白名单内的值合法（`claude|codex|gemini|qwen|opencode|mimo|zcode|pi|qoder|workbuddy|minimax|hermes|cline|roocode|kilo|kimi|amp`，另 `all`）；
空值 = 不过滤；未知源名返回 400 并列出合法 id（`/api/sources` 可机器查询）。`project:` 过滤接受目录名尾段
（`project:local-token-compute` 命中 `code/local-token-compute`），命中键回在 `project_matched`。

## API 速查（全部 GET，除注明 POST；返回 JSON）

**用量分析**

| 端点 | 用途 |
|---|---|
| `/api/summary?timeRange=&source=&model=` | 总量 + 按来源/模型汇总 |
| `/api/timeline?mode=hourly\|daily\|weekly\|monthly` | 分桶用量 |
| `/api/metrics` | 逐请求指标聚合（耗时/TTFT） |
| `/api/heatmap?mode=model_x_source\|model_x_date&metric=` | 模型热力图 |
| `/api/models` | 模型对比表 |
| `/api/digest?days=7` | 本期 vs 上期速览 |
| `/api/brief` | 今日 + 近 7 天一行小 JSON（statusline 用） |
| `/api/windows` | 5h 窗口分段 + 28 天 P90 自参考（本地口径，非官方限额） |
| `/api/anomalies` | 日用量异常（工作日分层稳健 z，审计窗内建 56 天） |
| `/api/pivot?start=&end=` | 项目 × 模型透视（epoch 秒，可省略） |
| `/api/active-time?days=7` | 投入时长：每日活跃小时+按来源/项目拆分（15 分钟间隔会话化） |
| `/api/quota` | 套餐余量：Codex/Claude 文件源实时读 + 命令采集器最近结果 + 消耗曲线（只读，不触发采集命令） |
| `POST /api/quota/refresh` | 显式运行 quota.json 里的命令采集器并落盘 |
| `/api/fleet/quota` | Fleet 各主机套餐余量（fleet-sync 拉回的副本） |
| `POST /api/sync?mode=incremental\|full` | 拉取最新日志（自动刷新索引；顺带采样套餐快照） |

**上下文搜索（覆盖所有工具的全部历史对话）**

| 端点 | 用途 |
|---|---|
| `/api/context/search?q=&limit=&source=&project=&days=` | 全文搜索 |
| `/api/context/session?source=&session_id=&doc_id=&around=` | 命中处的上下文会话 |
| `/api/context/stats` | 索引状态 + 语料规模 |
| `POST /api/context/rebuild` | 强制全量重建索引 |

**元信息**

| 端点 | 用途 |
|---|---|
| `/api/health` | 存活探针（不触数据层） |
| `/api/docs` | 全部端点的机器可读索引——本表的权威来源，拿不准先查它 |
| `/api/status` | 行数/上次同步/各采集器状态 |

`source` 取值（17 个）：`claude` / `codex` / `gemini` / `qwen` / `zcode` /
`qoder` / `workbuddy` / `opencode` / `mimo` / `pi` / `minimax` / `hermes` /
`cline` / `roocode` / `kilo` / `kimi` / `amp`。不用的源可在 ⚙ 面板停用。

**参数校验**：传错参数返回 **400** + 具体原因（如 `days=abc` → "参数 days
必须是整数"）；`days` 合法域 1–365、`limit` 1–100，`timeRange`/`mode`/
`metric` 只收白名单枚举值，timeRange 系端点传 `days` 也返 400——没有静默
忽略。

**搜索查询语法**（写在 `q` 里，优先于 URL 参数）：

- `source:zcode` `project:目录名` `role:user` `days:7` — 字段过滤
- `-排除词` — 排除；`"精确短语"` — 短语匹配
- 中/英/混排均可，无需词典；响应里 `session_headers` 是会话级摘要，
  `results[].doc_id` 可直接传给 session 端点回看原文

## 分析套路

**Token 复盘（回答"钱烧哪了"）**
1. `/api/digest?days=7` 拿本期 vs 上期总量；
2. `/api/summary` + `/api/models` 看来源与模型分布，指出最大头；
3. `/api/pivot` 看"哪个项目在烧"，`/api/heatmap?mode=model_x_source` 找
   高频低价值组合；
4. 结论按"砍掉什么 / 换什么模型 / 保持什么"三段输出。

**找回历史讨论（回答"当时怎么聊的"）**
1. 用 2-4 个关键中/英文词 `/api/context/search`（可叠加 `project:` `days:`）；
2. 挑最相关的 1-3 条 `doc_id` 调 `/api/context/session` 展开前后文；
3. 汇总：问题是什么、当时的结论、在哪个工程哪个会话。

**套餐余量（回答"还能用多久"）**
1. `/api/quota` 拿全部来源的窗口用量与重置倒计时（只读，不会触发
   任何采集命令；`history` 字段是消耗曲线）；
2. 邻近重置（<45 分钟）且用量低时提示"现在用掉不浪费"；≥90% 提示
   切换来源或等待重置；
3. 只有读数、没有来源时，提示用户在 `~/.tokenbuddy/quota.json` 配置
   命令采集器（MiniMax 等），Codex/Claude 文件源自动生效。

**每日自我进化（喂给 Agent 自己做复盘）**
1. `tokenbuddy report --days 1`（含投入时长与套餐余量两节；或
   `/api/brief` + `/api/active-time`）看当天量级与投入，
   `/api/context/search` 按"当天 + 当前项目名"过滤近几天的用户消息；
2. 归纳：今天做了什么、投入多少小时、哪些流程重复出现（3 次以上 =
   skill/规则候选）、哪些报错反复（= 工程改进候选）；
3. 产出一份"经验教训 + skill 候选 + 工程改进"三节报告。

## 注意

- 一切数据都在本机；不要把会话内容发给外部服务。
- 时间分桶固定 UTC+8；货币成本不估算，credits 原样透传。
- 搜索索引在每次 sync 后自动增量刷新，无需手动重建。
- 端点/参数以 `/api/docs` 实时返回为准（本表可能滞后于服务版本）。
- statusline 场景：`tokenbuddy statusline` 读 Claude Code stdin JSON 出
  一行 enriched 状态；`tokenbuddy today` 出今日/7 日一行。
