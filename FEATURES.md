# TokenBuddy 特性清单 / Feature Inventory

> 30 轮自主迭代的全部能力清单(v0.4.1 → v0.5.1)。每条 = 是什么 + 为什么重要。
> English speakers: the headline version lives in [README.md](README.md#whats-new);
> this file is the complete inventory.

**总账**:单二进制 4.8 MB · 165+ 测试全绿 · 空闲常驻 ~18 MB · 全量构建 48 s ·
12 个采集源 · 零运行时依赖 · 除 Fleet 外零外呼 · **多机汇聚支持 RustFS / MinIO / 任意 S3 兼容存储**。

---

## 一、安全 / Security

### 1. HTTP 攻击面收敛
请求体 1 MiB 上限(超限 413)、Host 白名单(废 DNS rebinding)、
Origin/Sec-Fetch-Site 校验(废 drive-by CSRF,含"改 fleet 配置指向攻击者桶"
这条真实数据外泄链)、安全响应头。环回默认行为零变化。

### 2. 访问令牌 + 远程绑定守卫
`TOKENBUDDY_TOKEN` 开启后全端点要求 Bearer / `?token=` 双通道鉴权;
绑定非环回地址而未设令牌时**拒绝启动**——"裸奔上 LAN"在启动时就拦住。

### 3. Fleet 静态加密(opt-in)
`fleet.toml` 写 `encrypt = true`,出机器的账本即 XChaCha20-Poly1305 密文
(RustCrypto 纯 Rust,零 C 工具链),内容密钥经 HKDF-SHA256 从既有
secret_key 域分隔派生——**不新增需要保管的密钥**。同密钥拉取端透明解密;
篡改/错钥在 Poly1305 认证标签处失败,绝不解出垃圾。二进制仅 +17 KB。

### 4. parquet 损坏自愈
账本打开/读失败时自动隔离损坏快照、回退最近可读快照——个人版没有 DBA,
数据文件要能自救,服务不因账本损坏拒绝启动。

### 5. doctor 逐源体检(账本侧 + 日志侧)
`tokenbuddy doctor [--json]`:每个源日志在哪、可读吗、SQLite 源表齐不齐、
**账本里有几行、最新一条何时**。"日志可读但账本 0 行"会被显式命名——
去重全跳、格式判据未命中这类静默失败从此有名字。MCP 的 source_health
工具同构获得。

### 5.5 export 带套餐数据(R48)
export/import 载荷扩为 5 文件:+quota.json(配置)+quota.jsonl
(消耗历史),换机不断供;sha256 校验、缺文件跳过语义不变。
纪律:每新增落盘文件,重审一次 export 清单。

### 6. 数据可携带 + 先验导入
`tokenbuddy export`(文件夹 + sha256 清单)/ `tokenbuddy import DIR
[--force]`(先验哈希再原子落地)。个人版的数据主权 = 随时带走、随时验证。

---

## 二、账本与采集 / Ledger & Collectors

### 7.1 Codex 归档会话覆盖(R42)
采集器与套餐读取器覆盖 `~/.codex/archived_sessions/`(归档即失明
的补丁)与 Xcode 托管 Codex store;同会话双目录由冻结的 record_id
去重吸收,零双计。

### 7. 12 个采集源
Claude Code、Codex CLI、Gemini CLI、Qwen Code、ZCode、Qoder、WorkBuddy、
OpenCode、Mimo、Pi、MiniMax、Hermes。本轮新增 4 源:codex(累计 total
前进性哨兵,UI 重发不计)、gemini(cached 拆出/thoughts 并入 output)、
qwen(usageMetadata,cwd 归项目)、hermes(网关聚合表 replace-per-sync)。

### 7.85 work_receipts 会话下钻(R92)
MCP 与 API 同参 session(前缀):sessions → receipts(session) →
search(session:)三件套构成 agent 自助查账闭环。

### 7.8 MCP sessions 工具(R90)
第 12 个工具:最近会话清单(分型/请求/token),agent 以 session_id
交叉引用 work_receipts/search_context;源 id 清单同步 17 探针。

### 7.7 Amp(R87)
`~/.local/share/amp/threads/*.json` 的 usageLedger.events:Amp 自己
的计费账本,tokens 四项+credits 原始单位;messages[].usage 重复不收。
分诊三问:真数据?(Kiro bytes/4 估算出局)本地数据?(Cursor 依赖
服务端缓存出局)权威账本?(Amp 只收 ledger)。

### 7.6 Kimi CLI(R85)
`~/.kimi/sessions/<GROUP>/<UUID>/wire.jsonl`:StatusUpdate 的
token_usage,同 message_id 滚动累计折叠末值——快照不是增量,直接
求和会爆炸。config.json 提供模型名。kimi-code 布局刻意不碰。

### 7.5 Cline/Roo Code/Kilo Code 家族(R83)
Roo/Kilo 是 Cline 的 fork,共用 VS Code globalStorage 的
`tasks/<taskId>/ui_messages.json`:每条 api_req_started = 一次请求,
tokensIn/tokensOut/cacheReads/cacheWrites 入账,cost 刻意不收。
modelInfo 优先、legacy `<model>`/`<cwd>` 标签流式兜底(2MiB 硬顶);
冻结键 task_ts毫秒_入_出;Source +3(cline/roocode/kilo)。
格式经 tokscale 开源解析器逐字段确认。对话侧:api_conversation
_history.json 只收 user/assistant 文本块(环境包裹剥除/纯
tool_result 轮不算发言,16MiB 硬顶,R84)。

### 8. 去重正确性对齐 ccusage
claude 升级为 (message.id, requestId) 复合身份跨文件折叠,requestId 缺失
降级三元组;判定依据全部对齐 ccusage 公开实现与官方夹具。README 有
「对账口径」章节,和 ccusage 核数字不再靠猜。

### 9.1 新解析器毒化补齐(R53)
R31 以来的四个新解析面(minimax/claude 缓存/rate_limits 行/statusline
stdin)纳入确定性 LCG 变异语料(截断/字节翻转/垃圾注入,600+300 轮),
断言只拒绝不崩溃。承诺名对名:防线要能被找到才算存在。

### 9. 模糊测试防线
LCG 确定性毒化语料喂全部采集器:截断行、非法 UTF-8、超大字段、负数
usage、类型错配——**崩溃上限被证明存在**:最坏是少统计一行,不是服务挂掉。
进 CI 常态运行。

### 10. 键冻结承诺
去重键公式按源冻结,改动 = 重写历史口径;新列一律可空迁移(如 project 列),
老数据零重导。

---

## 三、分析 / Analysis

### 11. 5 小时窗口事实卡 + P90 自参考
`/api/windows` + 仪表盘卡:窗口分段、燃速、到 P90 的剩余——**本地口径,
零外呼、不折钱**,和你自己的历史节奏比,不和服务端拉百分比。

### 12. 日用量异常检测
`/api/anomalies` + 横幅:工作日分层的 median/MAD 稳健 z(|z|>3.5),
周末安静不误报,≥3 周同星期历史才开金口。零依赖。

### 13. 项目 × 模型透视
账本加 project 列(cwd 等确定性字段归因,不猜),`/api/pivot` +
仪表盘透视面板 + CSV 导出——"哪个项目在烧 token"有事实答案。

### 13.3 工作收据扩展 Claude(R57)
同张收据第二来源:transcript JSONL 的 tool_use 块(name+input),
projects 目录最新优先 cap 200 文件;`/api/work-receipts` 合并
ZCode+Claude 两源统一排序。形状与来源分离,第二来源只花一个
遍历函数。

### 13.32 工具构成统计(R68)
收据带原始工具名分布;tool_category 七类目方言归一(Bash/shell/
execute→命令,Edit/write→编辑…未知进"其他"=新工具信号);
/api/work-receipts 增 tool_mix,面板七彩构成条。

### 13.35 工作收据第三来源(R58)
OpenCode session_message 的 tool 块进同一张收据(工具名小写,路径键
为 path,无路径跳过);三源同形合并统一排序。

### 13.0 OpenCode 子代理归属(R66)
build/plan=内建主线模式记主线,自定义 agent 记子代理(空/NULL=主线)
——与 ZCode/Claude 三家方言各自适配,同收敛于 sidechain 列。

### 13.1 ZCode 子代理归属(R65)
model_usage 自带 agent 列(主线 zcode-agent,派生 agent 记子代理)
——sidechain 拆分对 ZCode 生效;历史回填走全量重建(快照兜底)。

### 13.2 子代理消耗拆分(R64)
账本加 sidechain 列(Claude isSidechain;其余来源恒 false),summary
增 subagent_tokens/requests——主线 vs 子代理分开看,总量不变。
可空迁移先例的第二次使用(本次 non-null+默认值)。

### 13.4 工作收据(R56)
ZCode part 表 tool 部件 → 每会话工作收据:改动文件(Edit/Write
计数)/命令数/测试数(窄标记:宁少报不虚报)/工具调用;实时只读
零新落盘。`/api/work-receipts?days=` + 面板(近期文件 feed+每会话
表)。证据分级:文件路径和命令是确定性证据。

### 13.45 跨会话返工热点(R81)
窗口内被 ≥2 个会话编辑的文件(rework_top):会话内反复编辑是迭代
不是返工,分界画在能证明的会话边界上;API/MCP/面板三出口,三源
合并收敛为单一 merged_receipts。

### 13.5 会话画像(R55)
insights 会话分型:单发(≤1 请求)/快问 <5m/标准 5–30m/深度
30m–2h/马拉松 >2h——会话自身事实(跨度+请求数)推导,零新采集;
分布计数进 `/api/insights`,会话榜加形态徽章与五色分布条。

### 13.95 报告外推节(R70)
报告(与 MCP daily_report 同源)加「周终外推」三行小节,数字与
/api/forecast 同源。

### 14.0 周终外推(R69)
本自然周(周一起 UTC+8)至今用量,按前 28 天中位日节奏与本期实际
日均双口径预计周末总量;分叉即洞察(口径一致=节奏如常)。纯算术
不建模型,`/api/forecast` + digest 行。

### 14.15 报告返工热点(R96)
收据节加热点 top3(≥2 会话编辑同一文件);同轮修 R69 括号错位——
有异常日的报告曾丢失时长/收据/套餐/外推四节。

### 14.1 报告收据节(R62)
日报/周报新增「工作收据」节:合计行 + 跨会话合并的改动文件 top5;
三源由调用方聚合,渲染保持只读。

### 14.05 statusline 上下文段(R99)
stdin 的 context_window.used_percentage 直出(官方 schema),null
回退 current_usage 入侧三项÷窗口;≥75% ⚠。只报上游说的,不猜。

### 14. 日报/周报 + statusline
`tokenbuddy report`(markdown,数字与仪表盘同源同口径,--json 给
脚本 R89)、`/api/brief` +
`tokenbuddy today`(一行今日/7 日,statusline 直用,尾含机群最紧
窗口 R93)、
`tokenbuddy sessions`(终端最近会话清单,R86,--json 给脚本)。

---

## 四、检索 / Retrieval

### 15. 查询语法
`source:zcode project:目录名 session:id前缀 role:user days:7 -排除词 "精确短语"`——
语法优先于 URL 参数;对索引中不存在的 source 返回空而非忽略过滤
(fail-closed);搜索框下方有语法提示。

### 16. 无词典中文搜索(既有,持续在线)
CJK bigram + 前缀 token 两层分词,zstd 压缩 arena,43K 轮 2 秒建索引、
100 MB 内;软 AND 兜底 + 点击反馈。

### 16.5 对话语料覆盖 Codex/Gemini/Qwen(R79–R80)
三家 CLI 的 drain_messages 同款接入(与用量采集同一批文件,零新
采集):Codex rollout 的 response_item/message、Gemini 的 JSONL 行
+ 遗留 "messages" 文档、Qwen 的 message.parts(thought 跳过)——
R30 自声明的「未实现」清零,工具输出/系统重发依旧不进索引。

---

## 五、多机汇聚与性能 / Fleet & Performance

### 16.3 report 完整化(R40)
日报/周报(MCP daily_report 同源)新增「投入时长」(合计/逐日/来源
top3)与「套餐余量」(来源×套餐×窗口×已用×重置倒计时)两节;
纯渲染,数字与仪表盘同源同口径,渲染永不运行采集命令。

### 16.34 上下文健康条(R52)
水位面板下方每来源当前水位横条+阈值判定文案(<50 绿/50–75 橙/
≥75 红该收尾);非阻塞拉取,无数据零噪音。

### 16.35 上下文健康(R47)
`/api/context-health`:各来源最近一次上下文水位;today/statusline
在最高水位 ≥75% 时追加 ⚠ 告警(>85% 面临截断)。均值看趋势,
最新值看行动——同一列数据的两种时间聚合。

### 16.4 投入时长(R39)
从账本请求时间戳推导的「投入时长」:≤15 分钟间隔同一工作段
(WakaTime heartbeat 惯例),段跨度求和;日合计为真实墙钟时间,
来源/项目拆分并行各自计并标注会重叠。`/api/active-time?days=` +
Overview 面板。零新采集,账本里只有事实。

### 16.5 Fleet auto_pull(R41)
fleet.toml 增 `auto_pull`(默认开):与 auto-push 同间隔的后台拉取,
Fleet 视图/各主机套餐表免手动 fleet-sync;只读 bucket、只写本机
fleet 目录(tmp+rename),⚙ 配置页可关。

### 16.6 Fleet 套餐汇总(R38)
各主机随 Push 上报 `hosts/{id}/quota.json`(encrypt=true 时同密钥
XChaCha 加密),fleet-sync 按 etag 增量拉回,`GET /api/fleet/quota`
聚合本地副本,Fleet 视图「套餐余量(各主机)」表分色展示——跨机器
回答「哪台机器的哪个套餐快烧完」。manifest 加 quota 簿记表,
serde default 兼容旧文件。

### 17. Fleet 多机汇聚（RustFS / MinIO / 任意 S3 兼容存储）
`tokenbuddy push` 把本机 `data.parquet` 覆盖写为 `hosts/{host}/data.parquet`
（固定 key,幂等无小文件）;`fleet-sync` 按 etag 增量拉取,未变化主机零下载;
每小时后台自动推送（可关）。仪表盘 Fleet 视图:总账卡、各机明细、主机 ×
来源矩阵,支持 timeRange / 来源 / 模型 / 主机过滤;「⚙ 配置」页在面板里直接
读写 fleet.toml 并测试连接（secret_key 只写不读,页面永远拿不到明文）。

工程上的取舍:S3 客户端是**手写 SigV4**（签名向量与 AWS 文档逐字节对齐）,
面向局域网 RustFS / MinIO 明文 HTTP——**零 SDK 依赖**,二进制不背 AWS 全家桶。
已在真实 RustFS + 三台混合架构主机（Termux × 2 + macOS）完成端到端验收,
Fleet 总账与各机本机视图交叉对账全对。出机器的只有 token 账本,对话全文
`context.parquet` 永不上传;再叠加 opt-in 静态加密（见 #3）。

### 18. 性能优化专场
- **空闲常驻 165 MB → 18 MB**:上下文索引按需构建、15 分钟不用即卸载;
  采集缓存逐源释放。看总量的人永远不为搜索索引付内存。
- 聚合走**列投影纯 Rust 路径**（无查询引擎）,读量比全列 -62%;
  43K 轮对话 **2 秒**建索引,检索 p50 个位数毫秒。
- 仪表盘资产构建期 zstd 预压缩:传输体积 **-75%**,zstd 客户端零逐请求开销。
- 全量构建 48 s（aarch64 release）;`--profile fast` 编辑-测试循环 25 s。
- 多机增量同步按 etag 跳过未变化主机,15 分钟 timer 常态拉取近乎零流量。

---

## 六、Agent 通道 / Agent surface

### 18.3 配置面板(R37)
`GET/POST /api/quota/config` + 仪表盘「⚙ 配置」:表格编辑采集器
(name/command/parser/timeout),校验后原子写 quota.json(0600),
「↻ 测试运行」真跑一次并逐个报告成败;坏配置文件在面板报错,绝不
静默读空。空态带入口——功能能被找到才算存在。

### 18.4 套餐消耗历史(R36)
sync 成功即采样文件读取器快照进 quota.jsonl(每键最新 + 近 300 条
尾巴,2MiB 硬顶),`/api/quota` 带 history 序列(每键封顶 200 点),
仪表盘卡片画迷你 SVG 消耗曲线——「这个套餐一天烧多少」首次有答案:
供应商口径的排水率,账本侧永远算不出。采样挂在已有节奏上(sync/
显式刷新),没有发明任何新定时器。

### 18.5 上下文水位扩展(R35)
「上下文水位」面板从仅 Qoder 扩展到 Codex:读 Codex 自缓存的模型
目录 `models_cache.json`(context_window ×
effective_context_window_percent = 产品侧真实生效窗口,常远小于
模型规格),除以最近一次请求的 prompt 大小(rollout
last_token_usage.input_tokens——会话累计 total 不是水位)。未知
模型留 0.0 不猜;零 schema 变更,聚合层 ratio>0 过滤既有。

### 19. MCP server
`tokenbuddy mcp`:stdio 上的 Model Context Protocol,**零新增依赖**。
10 个工具(R59 起,新增 work_receipts):usage_summary / usage_timeline / daily_report / window_facts /
anomalies / project_pivot / search_context / source_health。一条配置接入
coding agent;搜索按需建索引用完即卸;响应截断 64 KB 防 agent 上下文打爆。

### 20. tokenbuddy-analyze skill
`skills/tokenbuddy-analyze/SKILL.md`:frontmatter 齐全、12 源、查询语法、
双通道(HTTP + MCP)说明;配合分层摘要,自我复盘成本比喂原始日志省 90%。

---

## 七、API 与界面 / API & UI

### 21. 参数校验 400 化
`days=abc` / `limit=999` / `start=xx` 返回 400 + 具体原因,绝不静默忽略;
未知参数忽略向前兼容。对 watchdog / bot 的调用方可预测。

### 22. /api/health + /api/docs
health 是不触数据层的存活探针(数据层卡死时仍应答,区分"服务死"与
"数据层死");docs 是 36 条端点的机器可读索引——API 自文档,不再靠读源码。

### 23. 英文界面切换
仪表盘右上角「中/EN」:三层 DOM 字典(精确节点/锚定正则/前缀)+ 微任务
观察器覆盖动态渲染;中文是源真值,破碎英文宁可不翻。首访按
navigator.language 定默认。

---

## 八、工程与发布 / Engineering & release

### 24. 门禁三件套(CI 强制)
**体积守卫**(≤20 MB)× **内存门禁 memgate**(建索引峰值 RSS ≤250 MB、
≤3 KB/doc)× **模糊测试**。写进 README 的承诺如果没有 CI,它会在第三个月
悄悄失效——所以全部门禁化。

### 25. 构建期 zstd 预压缩
build.rs 以 zstd-19 预压 dashboard.html,服务端按 Accept-Encoding 直吐:
传输 -75%,二进制 -75 KB。

### 26. 预编译发布流
`v*` 标签触发:Apple Silicon / Linux x64 musl 双目标,workflow 内复跑体积
门禁;install.sh 下载后 sha256 按名校验(损坏/篡改拒绝安装)。

### 27. fast profile
`cargo build/test --profile fast`(opt-level 1、无 LTO、增量):Termux 实测
release 全量 ~4 分钟的编辑-测试循环有了快通道;发布口径不变。

### 27.5 statusline 提供方(R43)
`tokenbuddy statusline`:读 Claude Code statusline stdin JSON,一行
输出 模型|目录|今日 tokens|各源最紧窗口配额(claude 限流取自 stdin,
其余为本地套餐快照);只读,永不 spawn 采集命令;紧凑倒计时
(format_countdown_short)。ccusage statusline 的同类位。

### 28. 运维细节
`pkill -x tokenbuddy` 正确姿势、serve 端口占用人话报错、TOKENBUDDY_HOME
重定位数据目录(多实例/测试)。

---

## 九、套餐余量 / Plan quota(R31 起)

### 28.2 每日复盘推送(R76)
`alert.daily_digest = true`:过午夜后首次 sync 自动推昨日完整报告
(四节 markdown),每天至多一次,失败重试;fire_once 原语统一推送
通道(冷却/发送/标记单点)。

### 28.3 滚动提醒接 webhook(R73)
`alert.rollover = true`:窗口 45 分钟内重置且用量 ≤80% 的机会型
通知也推 webhook(与阈值告警分开冷却);>80% 走告急语义,机会型
与告急型的分界线就是阈值线。

### 28.4 配额阈值告警(R50)
quota.json 可选 `alert`:越线(默认 80%)POST 用户自己的 webhook
(generic/飞书格式),每窗口冷却默认 12h;检查点=显式刷新+sync 采样
(detached 线程,锁内收集锁外发送)。本构建无 TLS:仅 HTTP webhook,
https 配置不动作并双处声明。软上限通知,企业成本治理的最低配。

### 28.55 告警一键测试(R95)
`POST /api/quota/alert-test` + 面板按钮:向配置的 webhook 发测试
消息,未配置/https/连接错误当场人话报出;五条推送通道共用管道,
管道本身可验证。

### 28.5 会话静默提醒(R88)
`alert.session_idle`(opt-in):今日活跃会话(≥3 请求)静默超
`idle_minutes`(默认 15)即 knock,fire_once 每会话每天一封。
「agent 需要你」的本地答案——时间戳只证明「安静了」,不猜意图。

### 28.45 异常用量推送——runaway agent 警报(R82)
`alert.anomaly = true`(opt-in):今天的滚动总量已达同星期整天中位数
(3.5 稳健 z)即推送——半天超过整天基线=大概率空转,健康的上午够不到
这条线;fire_once 每日期至多一封,与其它通道共用 webhook/格式/冷却。
配置面板补齐滚动/日报/异常三开关,修复保存抹掉后两者的 bug。

### 28.5 ledger 口径配额源(R44)
quota.json 增 `"type":"ledger"` 采集器:今日某来源请求数 ÷ 已知日
额度(Gemini CLI OAuth 1000/天、免费 Key 250/天等公开口径)= 配额
百分比,窗口止于下一个 UTC+8 午夜。第三种配额来源:自测 vs 公开
已知上限,零外呼。配置面板「类型」列同步支持。

### 29. 套餐余量采集(零外呼版;R33 起含 Claude)
供应商口径的「还剩多少」,TokenBuddy 自身依旧零外呼:两条通道。
**文件读取器**(内建常开):Codex CLI 把 rate_limits(5h/7d 窗,
used_percent + resets_at)挂在 rollout 的每个 token_count 事件上,
实时读、大文件流式、封顶 24 个——读 agent 自己写下的缓存,一分钱
配额不花。R33 加入 Claude:`~/.claude.json` 里 Claude Code 自己
缓存的限流响应(utilization.five_hour/seven_day),窗口键/ISO
resets_at/过期丢弃语义对齐 cc-switch 的读取实现,plan 取 OAuth
账户 tier。文件快照的 collected_at = 来源文件 mtime,新鲜度按
数据年龄说话,老缓存不冒充实时的。**命令采集器**(opt-in,`~/.tokenbuddy/quota.json`):用户
配置本地命令(如 MiniMax 的 `mmx quota show --output json`,预置
解析器),只在显式刷新(`tokenbuddy quota --refresh` /
`POST /api/quota/refresh`)时运行——外呼与否由用户的命令决定,
且永不定时偷跑。输出:`/api/quota`、`tokenbuddy quota`、doctor
「套餐余量」段、仪表盘卡片(用量 <70 绿 / 70–89 橙 / ≥90 红,
重置倒计时)。R34 接进所有旧出口:MCP `quota` 工具(agent 可查,
绝不运行采集命令)、`tokenbuddy today` 尾段与 `/api/brief` 的
`quota` 数组——每 (source,plan) 只带最紧窗口,statusline 即取即用。落盘 `quota.jsonl`(追加式,>2MiB 每键留最新);
采集失败也进 doctor/卡片,不静默。窗口标签来自供应商自报时长,
不写死语义;缺字段=未知=跳过,绝不冒充 0%。

### 30. 窗口滚动提醒(R32)
cc-switch 的招牌通知,本地化重写:供应商窗口(quota 快照 resets_at)
45 分钟内滚动且用量 ≤80%、或自参考 5h 窗 45 分钟内滚动且用量不到
自己 P90 一半时——「现在用掉不浪费」。判定按本机时钟 30 秒一次,
页面不可见不打扰;每窗口只提醒一次(localStorage FIFO 封顶 64,
键含滚动时间戳,过窗永不诈尸)。横幅 + 系统通知双通道,🔔 开关
opt-in。`/api/windows` 补 `window_end` 确定性滚动点。

---

## 十、用户回访轮(R101-R106,v0.6.1 后)

### 30. 采集源开关(R102)
⚙ 面板逐个停用 17 个采集源:停用后账本同步/全文索引/quota 采样
三路同门完全不读,已入账历史保留。全停 sync 13ms——小机器不再为
不用的工具白扫盘。

### 31. Skill 面板 + 一键复制(R103)
导航新增 Skill 视图:内嵌 tokenbuddy-analyze SKILL.md 全文展示
(二进制 include_str,内容永远跟服务版本走),「⧉ 复制全文」一键
拷贝(execCommand 保底,LAN http 可用)。`GET /api/skill`。

### 32. 检索索引自动构建(R104)
启动即后台建索引,sync 后标 stale 自动重收集重预热——打开搜索不再
等构建。修了 stale 永无置位导致索引从不增量刷新的暗病;15 分钟空闲
卸载保留,内存有界。

### 33. 会话展开 iMessage 化(R105)
用户消息蓝渐变气泡靠右(此前全左独白),AI 消息浅灰左置;命中琥珀
柔光晕;会话头吸顶毛玻璃。

### 34. 筛选选项记忆(R106)
统计页 9 项 + 搜索页 4 项筛选 localStorage 持久化,回填带选项存在性
校验;刷新回来还是上次调好的视图。

### 35. API 参数校验全端点对齐(R101 + issue #1-#12 收口)
days/timeRange/mode/metric 非法值一律 400 + 人话;context/session
缺参 400 化、doc_id 单参可取会话;404 JSON 化。12 个 cron 回访 issue
全部关闭。

---

## 延期与理由(诚实清单)

- **BM25/BM25F、RM3、点击排位折扣**:黄金语料(43K 轮)不在本机,无法按
  Recall@10 ≥ 92.6%、MRR ≥ 0.80 的门槛验收——宁缺毋滥,语料可用即做。
- **Fleet export 加密**:导出走本机文件夹,威胁模型不同,有真实需求再论证。
- **pidfile/--pidfile**:常驻生命周期由 launchd/systemd 管,自管状态多一个
  漂移面。
