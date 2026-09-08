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
