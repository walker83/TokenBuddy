# TokenBuddy 开发约定

## 构建规则（必须遵守）

**禁止编译 debug 构建。** 本项目 debug 中间产物曾把 `target/` 撑到 16G，已清理。
今后所有编译、测试、运行一律走 release：

```bash
cargo b            # build --release
cargo r            # run --release
cargo c            # check --release
cargo test --release
```

- 短别名 `b` / `r` / `c` 已在 `.cargo/config.toml` 定义，等价于 `--release`。
- 不要执行 `cargo build` / `cargo test` / `cargo run`（无 `--release` 的形式），
  也不要使用 `--profile dev`。
- 服务以 `./target/release/tokenbuddy` 常驻 127.0.0.1:8080，重启前先
  `pkill -f target/release/tokenbuddy`。
- 保持 `target/` 只保留 release 产物；如需深度清理，保留
  `target/release/tokenbuddy` 二进制即可。

## 质量门禁（CI/CD）

单一入口是 `scripts/check.sh`：`cargo fmt --check` → `clippy --release
--all-targets -D warnings` → `cargo test --release` → release 构建后检查二进制
体积（默认上限 8 MB，`TOKENBUDDY_SIZE_LIMIT_MB` 可调；README 主打 4.1 MB，
上限放宽到 2 倍只为拦住"拖进重依赖"级别的事故，如当年 DuckDB +19 MB）。

三道闸共用这一个脚本，改检查只改这里：

1. **本地 pre-push 钩子**（已生效）：`git config core.hooksPath .githooks`
   指向 `.githooks/pre-push`，push 前全量跑一遍。应急绕过用
   `git push --no-verify`（CI 会再拦一次），不要把绕过当习惯。
2. **内网 Gitea Actions（flod3 host runner）**：`.gitea/workflows/ci-flod3.yml`，
   push main / 手动触发时在 flod3（Galaxy Z Fold3 上的 Termux，无 Docker）
   跑同一份 gate，全量约 8 分钟。runner 常驻：flod3 的
   `~/act_runner/gitea-runner`（v4），由 `sv` 监管为服务 `act_runner`，
   label `termux-host`（host 执行模式），只在 flod3 在线时接活，离线任务
   排队。CI 里 `CARGO_TARGET_DIR` 指向 `~/act_runner/target-cache` 持久化，
   后续 push 增量编译。**不用 actions/checkout**：runner 是 Go 二进制，
   Android 没有 /etc/resolv.conf，解析不了 github.com（git/cargo 走系统
   解析没这问题），改用 git 直接克隆。该 Gitea 实测只索引
   `.gitea/workflows/`（ci.yml 未被读取，无 pending 噪音）。
3. **GitHub 公开仓**：ci.yml 随发布流推上去即生效（公开仓 Actions 免费）。

**失败闭环（watchdog）**：box 上 `/usr/local/bin/tokenbuddy-ci-watchdog.py`
（cron 每 2 分钟，配置 `/etc/tokenbuddy-watchdog.conf`，root 600）轮询 Gitea
API：CI 失败 → 飞书群机器人 ❌（标题+链接+日志尾部），只报一次；修复 push
变绿后补 ✅ 收尾。flod3 runner 掉线 ⚠️（30 分钟冷却重报，防手机没电后任务
静默排队）/恢复在线 ✅。首次启动只标记历史失败不追溯。`FEISHU_WEBHOOK` 留空
时一切照常运行只是不发消息——填入群自定义机器人地址后，在 box 跑
`python3 /usr/local/bin/tokenbuddy-ci-watchdog.py --test` 验证链路。

新代码必须保持 fmt/clippy 干净。个别 lint 确有理由保留时，用带注释的
`#[allow]`（仓库现有先例：`acc_record` / `push_doc` 的参数个数），不要全仓
降级 lint 等级。

## 双远程发布纪律

本仓库有两个远程：`origin`（内网，完整私有历史）和 `github`（公开，
刻意整理过的干净历史，**不含 `analysis/`、`examples/test_sync.rs`、
`docs/` 下的内部过程文档**）。**严禁直接 `git push github main`**——那会把
私有历史连同 `analysis/` 里的会话数据一起推上去。发布到 GitHub 的正确流程：

```bash
git checkout -B public-release github/main
git checkout main -- .cargo .github CLAUDE.md Cargo.lock Cargo.toml LICENSE README.md README.zh-CN.md docs/screenshot-dashboard.png src skills examples/memprobe.rs
git rm src/tools.rs  # 若已删除
git diff --cached main --stat -- . ':(exclude)analysis' ':(exclude)examples/test_sync.rs' ':(exclude)docs'  # 必须为空
git commit ... && git push github public-release:main <tag>
```

发布前对暂存树跑一遍隐私扫描：
`git grep -l -E "192\.168\.|walker@|ssh://git|密码[是为：]" -- .`

## 存储

无查询引擎：所有来源写入同一份 `~/.tokenbuddy/data.parquet`，全部聚合
（summary / timeline / metrics / heatmap / models）走 `store.rs` 里的纯 Rust
路径（`rust_read_agg_columns` + `rust_compute_*`），只投影聚合需要的列
（`session_id` / `message_id` 不读，省约 62% 读取量），不要为了"简化"把它
换成全列读取，也不要加回 DuckDB / DataFusion 等查询引擎——bundled DuckDB
曾占全量编译的 2/3、二进制的 19 MB 和进程 15-25 MB 内存，换成纯 Rust 聚合后
输出与 SQL 路径逐桶一致（见 `timeline_labels_match_the_sql_formats` 等
parity 测试）。timeline 的周标签复刻 strftime `%W` 语义（周 1 从当年首个
周一开始，之前是 W00），改动分桶前先跑那两个测试。

## 上下文搜索

对话全文搜索在 `context.rs`：各采集器的 `drain_messages(sink)` 流式提取
user/assistant 对话文本（工具输出、tool_result、system 重发上下文一律不进
索引——重复缓存淹没搜索结果正是要解决的问题），归一化哈希去重后流式写
`~/.tokenbuddy/context.parquet`，查询走纯 Rust 倒排索引。分词两层：ASCII
token（BTreeMap，≥3 字符支持前缀模糊）、CJK 字符 bigram（兜住跨词边界的
片段查询如"下文搜"）；查询期子串复验负责精度，候选级联是内容词 AND →
IDF 加权排序，改任何一层都要带上跨边界用例。

**内存预算是硬约束：整个服务 footprint < 100 MB**（macOS 不回收已释放的小块
堆页，footprint ≈ 历史峰值分配，`cargo run --release --example memprobe` 实测）。
为此索引做了四件事，改结构前先想清楚别破坏：正文按文档 zstd 压缩进单一
arena、查询时逐候选解压（不要把 `texts`/`lows` 全量驻内存加回来）；
postings 以 delta-varint 编码进单一平坦 arena；DocMeta 的 source/session/
project 全部驻留内化成整数 id；构建是两遍流式（第一遍统计各词 postings
长度，第二遍填一个预分配的大数组——大块分配才会在释放时还给 OS）。构建期间
也要先释放旧索引再建新（`sync_and_build` 已这样做）。曾因 jieba 词典常驻
55 MB 和 8.2 万条工具 synopsis 文档把索引撑到 379 MB，两者已删除，不要加回
来。

搜索框固定在 dashboard 页面最前（`/` 键聚焦），不要挪进 Tab。本机系统
SQLite 没有 FTS5（实测 `no such module: fts5`），不要再尝试 SQLite FTS 或
往回加第二种存储引擎；`doc_id` 是 FNV-1a，换成 `DefaultHasher` 会让已入库
的 parquet 在版本升级后全部误判为重复/新增。

## 计量口径

- **不做成本**：本项目**不计算、不展示任何货币成本**。`pricing.rs` / `budget.rs`
  / `/api/budget` / `~/.tokenbuddy/pricing.json` / `~/.tokenbuddy/budget.json` 已整体删除，
  各聚合结构体里的 `cost*` / `currency` / `free` / `estimated_price` 字段也一并
  移除，不要再加回来——混元等模型没有可核对的单价，估出来的数没有意义。
  排行、份额、热力图、堆叠图一律以 **total_tokens** 排序和度量。
- **credits 是原始事实，不是钱**：parquet 保留 `credits` 列并在表格里原样展示。
  它是工具自己上报的消耗量（Qoder 的 token 全被掩码成 0，只有 credits 精确），
  只做记录，不折算货币。
- **context_ratio 是 Qoder 唯一存活的 token 尺度信号**：Qoder 服务端把所有
  token 字段掩码成 0（会话 JSONL、runtime 日志、SQLite 云缓存全部如此，模型
  目录是加密的拿不到上下文窗口大小），但 `usage.context_usage_ratio`（0..=1
  的上下文窗口占比）是真实值。入库为 `context_ratio` f64（0.0 = 该来源不上报），
  聚合暴露为各处的 `avg_context_ratio`。**不要**拿假定的窗口大小去乘它换算
  绝对 token——那是编造数字。掩码来源在排行里以 credits 参与（见 digest 与
  insights 会话榜的排序），保证 Qoder 不是永远垫底的 0。
- **Qoder 模型名同理被掩码**（`qmodel`/`qfmodel`/`gmodel`…）。CN 应用的
  globalStorage（`~/Library/Application Support/QoderCN/User/globalStorage/
  state.vscdb` 的 `aicoding.modelConfigs.cache.*` 键）以明文 KV 存了官方目录，
  `qoder.rs` 的 `demask_model()` 内置该映射（qmodel→Qwen3.7-Plus、qfmodel→
  Qwen3.8-Flash、gmodel→GLM-5.3 等）。目录键以精确匹配走最长键优先，未知键
  原样透传（新模型、`byok:<uuid>`）。应用侧 `com.qodercn.app.stable/
  main.sqlite` 有会话标题/真实 cwd/掩码模型名，token 一样全 0，别指望它。
- **时区**：所有日/周/月分桶与 `timeRange` 过滤一律按 **中国时区（UTC+8）** 切分，
  日界是 00:00 CST，不是 00:00 UTC。parquet 里存的 `timestamp` 仍是真正的 UTC
  epoch 秒，偏移只在读取时施加。唯一入口是 `lib.rs` 的 `CN_OFFSET_SECS` /
  `cn_midnight()` / `cn_month_start()` / `cn_day_label()`，timeline 分桶靠
  `bucket_label()` 的 `timestamp + CN_OFFSET_SECS` 对齐——不要在别处手写 `and_utc()`
  或 `to_timestamp(timestamp)`，那会把凌晨 00:00–08:00 的记录算进前一天。
- **zcode**：`model_usage.input_tokens` **已包含** `cache_read_input_tokens` 与
  `cache_creation_tokens`（每条 completed 行都满足
  `computed_total_tokens = input + output`），入库前必须减掉这两项，否则 token
  会双算（历史上曾把 ~4 千万输入报成 ~13.6 亿）。
  dedupe key 用稳定的 `zcode_<model_usage.id>`（collector 的 `record_id`）；
  历史上用过 `zc_<session>_<ts>_<input_tokens>`，token 语义一变就会把历史重复导入。
