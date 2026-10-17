// TokenBuddy plugin for DeepSeek Harness — everything-is-a-plugin bundle.
// Registers local-ledger tools that proxy TokenBuddy's loopback HTTP API,
// so numbers come from the same Parquet as the dashboard (同源同口径)。
export const name = 'tokenbuddy'
export const inject = ['tools']

import { defineTool } from '@deepseek-ai/dsh-tools'
import { readFile } from 'node:fs/promises'
import { homedir } from 'node:os'
import { join } from 'node:path'

const ENV_URL = process.env.TOKENBUDDY_URL
const TOKEN = process.env.TOKENBUDDY_TOKEN || ''
// Mirrors data_dir() in src/lib.rs: TOKENBUDDY_HOME replaces the whole store
// rather than nesting under it, so a relocated instance still finds server.json.
const DATA_HOME = process.env.TOKENBUDDY_HOME || join(homedir(), '.tokenbuddy')

function headers() {
  return TOKEN ? { Authorization: `Bearer ${TOKEN}` } : {}
}

// The server may run on any --port, so discovery is mandatory: env override,
// then the server.json the binary writes at startup, then scanning the process
// table for `tokenbuddy ... --port N` (covers binaries older than server.json).
// Every candidate is confirmed with a real /api/brief probe, so a stale
// server.json from a crashed instance can never win.
async function probe(base) {
  const ctrl = new AbortController()
  const timer = setTimeout(() => ctrl.abort(), 2000)
  try {
    const res = await fetch(new URL('/api/brief', base), {
      signal: ctrl.signal,
      headers: headers(),
    })
    return res.ok ? base : undefined
  } catch {
    return undefined
  } finally {
    clearTimeout(timer)
  }
}

async function candidates() {
  const found = []
  if (ENV_URL) found.push(ENV_URL.replace(/\/$/, ''))
  try {
    const raw = JSON.parse(await readFile(join(DATA_HOME, 'server.json'), 'utf8'))
    if (raw.port) found.push(`http://127.0.0.1:${raw.port}`)
  } catch {}
  found.push('http://127.0.0.1:33940')
  found.push('http://127.0.0.1:8080')
  try {
    const { execFile } = await import('node:child_process')
    const { promisify } = await import('node:util')
    const ps = await promisify(execFile)('ps', ['ax', '-o', 'command'])
    for (const m of ps.stdout.matchAll(/tokenbuddy\S* [^\n]*?--port (\d+)/g)) {
      const url = `http://127.0.0.1:${m[1]}`
      if (!found.includes(url)) found.push(url)
    }
  } catch {}
  return found
}

let cachedBase

async function resolveBase() {
  if (cachedBase) return cachedBase
  for (const base of await candidates()) {
    if (await probe(base)) {
      cachedBase = base
      return base
    }
  }
  throw new Error(
    '找不到 TokenBuddy 服务：请先在后台启动（tokenbuddy serve，默认 127.0.0.1:8080），' +
      '或用 TOKENBUDDY_URL 指定地址。',
  )
}

async function request(path, params, method = 'GET', derivedDays) {
  const send = async (base, p) => {
    const url = new URL(path, base)
    for (const [k, v] of Object.entries(p || {})) {
      if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, String(v))
    }
    return fetch(url, { method, headers: headers() })
  }
  const base = await resolveBase()
  let res
  try {
    res = await send(base, params)
  } catch {
    // The instance may have restarted on another port — drop the cache and
    // re-run discovery once before surfacing the failure.
    cachedBase = undefined
    res = await send(await resolveBase(), params)
  }
  let text = await res.text()
  // Builds before the generic-Nd window (v0.7.0) only accept the literal set
  // all|today|7d|30d|90d. A day count we derived ourselves is our own doing, so
  // rather than surfacing a 400 we retry on the nearest bucket the old build
  // understands — and say so, so nobody reports a 90-day window as 365.
  if (!res.ok && res.status === 400 && derivedDays && text.includes('timeRange')) {
    const snap = snapLegacyRange(derivedDays)
    if (snap) {
      const retry = { ...params, timeRange: snap }
      res = await send(base, retry)
      const body = await res.text()
      if (res.ok) {
        text = `${body}\n\n⚠️ 该服务是旧构建，只接受 all|today|7d|30d|90d；` +
          `${derivedDays} 天窗口已降级为 ${snap}，数字按此口径读。`
      } else {
        text = body
      }
    }
  }
  if (!res.ok) throw new Error(`TokenBuddy ${res.status}: ${text.slice(0, 500)}`)
  // 带上面板入口；超长结果截断，引导去网页看全量而不是把 JSON 一股脑倒给模型
  if (text.length > 6000) {
    text = text.slice(0, 6000) + '\n…(结果过长已截断，完整数据打开面板查看)'
  }
  return `${text}\n\n—\n📊 网页面板：${base}`
}

// Per-endpoint parameter whitelist, transcribed from `GET /api/docs`
// (api_endpoints() in src/main.rs). The server is strict on purpose — an
// unknown parameter is a 400, not a silently ignored extra — so the plugin has
// to send each endpoint only what it documents instead of forwarding whatever
// the model happened to fill in. Eight of the eleven GET endpoints reject
// `days` outright; the split below is the contract, not a guess.
const ENDPOINTS = {
  summary: { path: '/api/summary', params: ['timeRange', 'source', 'model'] },
  digest: { path: '/api/digest', params: ['timeRange', 'source', 'model', 'days'] },
  timeline: { path: '/api/timeline', params: ['timeRange', 'mode', 'source', 'model'] },
  // heatmap's HTTP param is `mode`, but the tool-facing one is heatmapMode —
  // `mode` is deliberately absent from its whitelist so a stray timeline `mode`
  // can never leak through and collide with the matrix vocabulary.
  heatmap: { path: '/api/heatmap', params: ['metric', 'timeRange', 'source', 'model'] },
  models: { path: '/api/models', params: ['timeRange', 'source', 'model'] },
  'active-time': { path: '/api/active-time', params: ['days', 'source', 'project'] },
  pivot: { path: '/api/pivot', params: ['start', 'end', 'source', 'model'] },
  anomalies: { path: '/api/anomalies', params: [] },
  windows: { path: '/api/windows', params: ['timeRange', 'source', 'model'] },
  quota: { path: '/api/quota', params: ['timeRange', 'source'] },
  metrics: { path: '/api/metrics', params: ['timeRange', 'source', 'model'] },
  insights: { path: '/api/insights', params: ['timeRange', 'source', 'model', 'limit'] },
}

const TIMELINE_MODES = ['hourly', 'daily', 'weekly', 'monthly']
const HEATMAP_MODES = ['model_x_source', 'model_x_day']

// `mode` and `heatmapMode` are deliberately separate parameters rather than one
// union enum: the harness validates arguments against the declared schema before
// execute() ever runs, so a single merged enum would let the model legally pick
// "daily" for a heatmap and only discover the mismatch from a 400. Split, each
// enum is exactly the vocabulary its endpoint accepts.
//
// Because the split already makes a wrong *value* impossible, an irrelevant
// parameter arriving alongside the right one is ignored rather than rejected —
// models routinely fill in both, and failing a request that has an unambiguous
// answer is worse than dropping a value the endpoint never reads. The one hard
// error left is a missing heatmapMode, where the server would otherwise 400.
function checkMode(query, args) {
  if (query === 'heatmap' && !args.heatmapMode) {
    throw new Error(
      `heatmap 必须给 heatmapMode：${HEATMAP_MODES.join('|')}` +
        `（model_x_source=按来源，model_x_day=按天）。`,
    )
  }
}

// limit is 1–100 server-side; clamp instead of letting a loose number 400.
function boundedLimit(v, dflt, max) {
  const n = Number(v)
  if (!Number.isFinite(n) || n < 1) return undefined
  return Math.min(Math.round(n), max) ?? dflt
}

// The day buckets pre-Nd builds understand. Nearest-not-larger is deliberate:
// over-reporting a window is worse than under-reporting it.
const LEGACY_RANGES = [7, 30, 90]
function snapLegacyRange(days) {
  let best = null
  for (const d of LEGACY_RANGES) if (d <= days && (best === null || d > best)) best = d
  return best === null ? '7d' : `${best}d`
}

// Returns the query plus the day count we derived a timeRange from, so the
// caller can downgrade the window if it turns out to be talking to an old build.
function buildQuery(spec, args) {
  const out = {}
  for (const k of spec.params) {
    const v = args[k]
    if (v === undefined || v === null || v === '') continue
    out[k] = typeof v === 'number' ? v : String(v)
  }
  let derivedDays
  // heatmapMode is the tool-facing name; the HTTP param is plain `mode`.
  if (spec.path === '/api/heatmap' && args.heatmapMode) {
    out.mode = String(args.heatmapMode)
  }
  // `days` is the natural thing for a model to reach for, but the time-filtered
  // endpoints only speak timeRange (all|today|Nd). Translate instead of dropping
  // it, so "这周用了多少" survives even when the model spelled the window wrong.
  if (!spec.params.includes('days') && out.timeRange === undefined) {
    const d = Number(args.days)
    if (Number.isFinite(d) && d >= 1 && d <= 365) {
      out.timeRange = `${Math.round(d)}d`
      derivedDays = Math.round(d)
    }
  }
  if (spec.params.includes('limit')) {
    const lim = boundedLimit(args.limit, undefined, 100)
    if (lim) out.limit = lim
    else delete out.limit
  }
  return { params: out, derivedDays }
}

const sourceParam = {
  type: 'string',
  description:
    '只看某个 agent 来源，如 claude/codex/zcode/gemini…（/api/sources 可查全部；留空=全部）',
}

export function apply(ctx) {
  console.log('[tokenbuddy] plugin loaded — registering local ledger tools')
  ctx.tools.register(
    defineTool({
      name: 'tokenbuddy_usage',
      description:
        '查本地 TokenBuddy 账本：token 用量与缓存命中，按模型、来源、时间分桶。回答“这周烧了多少 token”“哪个模型用得最多”。所有金额以 total_tokens 计（本工具不产出货币成本）。需先在后台运行 TokenBuddy 服务。',
      parameters: {
        query: {
          type: 'string',
          required: true,
          enum: [
            'summary',
            'digest',
            'timeline',
            'heatmap',
            'models',
            'active-time',
            'pivot',
            'anomalies',
            'windows',
            'quota',
            'metrics',
            'insights',
          ],
          description:
            'summary=总量+按来源/模型汇总; digest=本期vs上期速览; timeline=分桶(需mode); heatmap=模型热力图(需heatmapMode); models=模型对比; active-time=投入时长(需days); pivot=项目×模型透视(需start/end); anomalies=日用量异常; windows=5h窗口+P90; quota=套餐余量; metrics=逐请求耗时/TTFT; insights=深度分析(时段节律/会话榜)',
        },
        mode: {
          type: 'string',
          enum: ['hourly', 'daily', 'weekly', 'monthly'],
          description: 'timeline 的分桶粒度（只有 timeline 用这个）',
        },
        heatmapMode: {
          type: 'string',
          enum: ['model_x_source', 'model_x_day'],
          description: 'heatmap 的矩阵方向（只有 heatmap 用这个）：按来源或按天',
        },
        timeRange: {
          type: 'string',
          description: '时间范围 all|today|7d|30d（除 digest/active-time 外统一用这个）',
        },
        source: sourceParam,
        model: { type: 'string', description: '只看某个模型名' },
        days: {
          type: 'number',
          description: '天数窗口。active-time/digest 用它；其他端点会自动换算成 timeRange',
        },
        start: { type: 'number', description: 'pivot 起始（epoch 秒）' },
        end: { type: 'number', description: 'pivot 结束（epoch 秒）' },
        limit: { type: 'number', description: 'insights 会话榜条数，1–100' },
      },
      output: {
        schema: { type: 'string' },
        render: (_args, value) => [{ type: 'text', text: value }],
      },
      async execute(args) {
        const spec = ENDPOINTS[args.query]
        if (!spec) throw new Error(`未知 query “${args.query}”`)
        checkMode(args.query, args)
        const { params, derivedDays } = buildQuery(spec, args)
        return request(spec.path, params, 'GET', derivedDays)
      },
    }),
  )

  ctx.tools.register(
    defineTool({
      name: 'tokenbuddy_search',
      description:
        '全文搜索本机所有 AI 编码会话历史（claude/codex/zcode 等 17 个来源）。回答“我们当时在哪聊过 XX”“找回那次讨论”。',
      parameters: {
        q: { type: 'string', required: true, description: '2-4 个关键中/英文词效果最好' },
        source: sourceParam,
        project: {
          type: 'string',
          description: '目录名尾段，如 local-token-compute 命中 code/local-token-compute',
        },
        days: { type: 'number', description: '只搜最近 N 天' },
        limit: { type: 'number', description: '返回条数，1–100' },
      },
      output: {
        schema: { type: 'string' },
        render: (_args, value) => [{ type: 'text', text: value }],
      },
      async execute(args) {
        const params = { q: args.q, source: args.source, project: args.project, days: args.days }
        const lim = boundedLimit(args.limit, undefined, 100)
        if (lim) params.limit = lim
        return request('/api/context/search', params)
      },
    }),
  )

  ctx.tools.register(
    defineTool({
      name: 'tokenbuddy_sessions',
      description:
        '列出最近的 AI 会话排行（每条含来源、会话 id、token 数、起止时间、会话节奏类型）。回答“最近哪几个会话最费 token”“上周主要在哪些项目上干活”。',
      parameters: {
        days: { type: 'number', description: '回看天数，默认 7' },
        limit: { type: 'number', description: '返回条数，1–100，默认 15' },
      },
      output: {
        schema: { type: 'string' },
        render: (_args, value) => [{ type: 'text', text: value }],
      },
      async execute(args) {
        // There is no GET /api/sessions — the session leaderboard rides inside
        // /api/insights as its `sessions` array (`tokenbuddy sessions` is a CLI
        // subcommand, not a route). It is a flat list keyed by session_id, so
        // this tool must not promise project grouping.
        const d = Number(args.days)
        const days = Number.isFinite(d) && d >= 1 && d <= 365 ? Math.round(d) : 7
        const params = { timeRange: `${days}d` }
        const lim = boundedLimit(args.limit, 15, 100)
        if (lim) params.limit = lim
        return request('/api/insights', params, 'GET', days)
      },
    }),
  )

  ctx.tools.register(
    defineTool({
      name: 'tokenbuddy_sync',
      description:
        '让 TokenBuddy 立刻扫描各 agent 的本地日志并更新账本（查询前数据不新鲜时用）。',
      parameters: {
        mode: {
          type: 'string',
          enum: ['incremental', 'full'],
          description: 'incremental=增量（默认）；full=全量重建',
        },
      },
      output: {
        schema: { type: 'string' },
        render: (_args, value) => [{ type: 'text', text: value }],
      },
      async execute(args) {
        return request('/api/sync', { mode: args.mode || 'incremental' }, 'POST')
      },
    }),
  )
}
