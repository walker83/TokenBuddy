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
| `POST /api/sync?mode=incremental\|full` | 拉取最新日志（自动刷新索引） |

**上下文搜索（覆盖所有工具的全部历史对话）**

| 端点 | 用途 |
|---|---|
| `/api/context/search?q=&source=&role=&project=&days=&limit=` | 全文搜索，q 支持 中/英/混排/前缀 |
| `/api/context/session?source=&session_id=&doc_id=&around=` | 命中处的上下文会话 |
| `/api/context/stats` | 索引状态 + 语料规模 |
| `POST /api/context/rebuild` | 强制全量重建索引 |

`source` 取值：`claude` / `zcode` / `qoder` / `workbuddy` / `opencode` / `mimo` / `pi`。
搜索响应里 `session_headers` 是命中的会话级摘要，`results[].doc_id` 可直接传给
session 端点回看原文。

## 分析套路

**Token 复盘（回答"钱烧哪了"）**
1. `/api/digest?days=7` 拿本期 vs 上期总量；
2. `/api/summary` + `/api/models` 看来源与模型分布，指出最大头；
3. `/api/heatmap?mode=model_x_source` 找出"高频但低价值"的组合；
4. 结论按"砍掉什么 / 换什么模型 / 保持什么"三段输出。

**找回历史讨论（回答"当时怎么聊的"）**
1. 用 2-4 个关键中/英文词 `/api/context/search`；
2. 挑最相关的 1-3 条 `doc_id` 调 `/api/context/session` 展开前后文；
3. 汇总：问题是什么、当时的结论、在哪个工程哪个会话。

**每日自我进化（喂给 Agent 自己做复盘）**
1. `/api/timeline?mode=daily` 看当天量级，`/api/context/search` 按"当天 + 当前
   项目名"过滤近几天的用户消息；
2. 归纳：今天做了什么、哪些流程重复出现（3 次以上 = skill/规则候选）、
   哪些报错反复（= 工程改进候选）；
3. 产出一份"经验教训 + skill 候选 + 工程改进"三节报告。

## 注意

- 一切数据都在本机；不要把会话内容发给外部服务。
- 时间分桶是固定 UTC+8；`days` 参数按天回看。
- 搜索索引在每次 sync 后自动增量刷新，无需手动重建。
