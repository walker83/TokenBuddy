# dsh-plugin-tokenbuddy

把 TokenBuddy（本地 token 账本 + 全部 AI 会话全文搜索）作为 **DeepSeek Harness
(`dsh`) 原生工具** 提供。数据走本机 `127.0.0.1:8080` HTTP API，与 Dashboard
同源同口径；不联网、不采集，隐私边界与 TokenBuddy 一致。

## 注册的工具

| 工具 | 用途 |
|---|---|
| `tokenbuddy_usage` | 用量/花费/缓存命中：summary、digest、timeline、heatmap、models、active-time、pivot、anomalies、windows、quota、metrics 十一合一 |
| `tokenbuddy_search` | 全文搜索所有 agent 会话历史（"我们当时在哪聊过 XX"） |
| `tokenbuddy_sessions` | 最近会话列表（按项目/来源分组） |
| `tokenbuddy_sync` | 触发增量/全量同步 |

## 前置条件

TokenBuddy 服务在本机运行（`tokenbuddy` 二进制直接启动即可，默认绑定
`127.0.0.1:8080`）。远程/换端口用环境变量：

```sh
export TOKENBUDDY_URL=http://127.0.0.1:9000
export TOKENBUDDY_TOKEN=<远程绑定时的 Bearer token>
```

## 本地试用

```sh
npx @deepseek-ai/dsh web --patch ./dsh-plugin/cordis.patch.yml
# patch 里 name 需指向本包；开发期可用绝对路径指向 index.js
```

## 安装到 profile

```sh
# 从本仓库目录或 npm 发布后
dsh plugin --profile default add dsh-plugin-tokenbuddy
```

## 发布

```sh
cd dsh-plugin && npm publish
```

发布后给仓库加 [dsh-plugin](https://github.com/topics/dsh-plugin) topic 收录，
并提交 PR 到 `deepseek-ai/awesome-deepseek-agent`。

## 设计说明

- 走 HTTP API 而不是重新实现统计：TokenBuddy 已有 16 个 MCP 工具与 20+ HTTP
  端点，dsh 侧 4 个粗粒度工具 + 枚举参数即可覆盖主要问法，插件体积小、不随
  上游端点漂移。
- `defineTool` 的 `enum` 参数在 harness 侧做校验，模型传错值拿不到工具调用。
- 一切经 `ctx.tools.register` 注册，卸载插件即自动清理。
