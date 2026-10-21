// TokenBuddy client face for DeepSeek Harness — sidebar entry + main panel.
// The host serves this bundle at /plugins/dsh-plugin-tokenbuddy/client.js and the
// browser boots it through window.__ModuleLoader__ (the vendored cordis loader's
// seam). apply(ctx) registers the same slot pair the schedule bundle uses for
// its "自动化任务" page: "sidebar.panellist" (the sidebar icon row) and "main"
// (the center panel keyed by that row's id).
window.__ModuleLoader__.load({
	id: 'dsh-plugin-tokenbuddy',
	factory: (require) => {
		var module = { exports: {} }
		var exports = module.exports
		Object.defineProperty(exports, Symbol.toStringTag, { value: 'Module' })
		let react_jsx_runtime = require('react/jsx-runtime')
		let react = require('react')

		const PANEL_ID = 'tokenbuddy'
		const NS = 'tokenbuddy.panel'

		const zh = {
			panel: 'TokenBuddy',
			loading: '正在寻找 TokenBuddy 服务…',
			'missing.title': '没有找到 TokenBuddy 服务',
			'missing.hint':
				'先在终端启动服务（tokenbuddy serve，默认端口 33940），再回来点重试。插件依次探测 33940 和 8080 两个端口。',
			'missing.retry': '重试',
		}
		const en = {
			panel: 'TokenBuddy',
			loading: 'Looking for the TokenBuddy service…',
			'missing.title': 'TokenBuddy service not found',
			'missing.hint':
				'Start it in a terminal first (tokenbuddy serve, default port 33940), then retry. The panel probes ports 33940 and 8080.',
			'missing.retry': 'Retry',
		}

		// Mirrors the node-side discovery in index.js minus what a browser cannot
		// reach (env, server.json, the process table): probe the default loopback
		// ports with no-cors fetches. An opaque response means something answered
		// on /api/brief; a rejection means the port is dead.
		async function probe(port) {
			const ctrl = new AbortController()
			const timer = setTimeout(() => ctrl.abort(), 2000)
			try {
				await fetch(`http://127.0.0.1:${port}/api/brief`, {
					mode: 'no-cors',
					signal: ctrl.signal,
				})
				return `http://127.0.0.1:${port}`
			} catch {
				return undefined
			} finally {
				clearTimeout(timer)
			}
		}

		async function discoverBase() {
			for (const port of [33940, 8080]) {
				const base = await probe(port)
				if (base) return base
			}
			return null
		}

		// Hand-rolled bar-chart glyph (no primitives icon dependency): three bars
		// over a baseline, stroked with currentColor so it follows the sidebar
		// theme in both states.
		function PanelIcon({ size }) {
			return react_jsx_runtime.jsxs(
				'svg',
				{
					width: size,
					height: size,
					viewBox: '0 0 24 24',
					fill: 'none',
					stroke: 'currentColor',
					strokeWidth: 2,
					strokeLinecap: 'round',
					strokeLinejoin: 'round',
					'aria-hidden': 'true',
					children: [
						react_jsx_runtime.jsx('path', { d: 'M3 21h18' }),
						react_jsx_runtime.jsx('path', { d: 'M7 17v-6' }),
						react_jsx_runtime.jsx('path', { d: 'M12 17V5' }),
						react_jsx_runtime.jsx('path', { d: 'M17 17v-9' }),
					],
				},
			)
		}

		const pageStyle = {
			height: '100%',
			display: 'flex',
			flexDirection: 'column',
			alignItems: 'center',
			justifyContent: 'center',
			gap: 12,
			padding: 24,
			textAlign: 'center',
			color: 'var(--dsw-alias-fg-secondary, #888)',
		}
		const retryStyle = {
			padding: '6px 18px',
			borderRadius: 8,
			border: '1px solid var(--dsw-alias-border-strong, #bbb)',
			background: 'transparent',
			color: 'inherit',
			cursor: 'pointer',
			fontSize: 13,
		}

		function MissingNotice({ t, onRetry }) {
			return react_jsx_runtime.jsxs('div', {
				style: pageStyle,
				children: [
					react_jsx_runtime.jsx('div', {
						style: { fontSize: 15, fontWeight: 600, color: 'inherit' },
						children: t('missing.title'),
					}),
					react_jsx_runtime.jsx('div', {
						style: { maxWidth: 460, lineHeight: 1.6, fontSize: 13 },
						children: t('missing.hint'),
					}),
					react_jsx_runtime.jsx('button', { style: retryStyle, onClick: onRetry, children: t('missing.retry') }),
				],
			})
		}

		// The panel is the real TokenBuddy dashboard, iframed: one implementation,
		// same numbers as the tools quote (同源同口径). undefined = still probing,
		// null = probe came up empty, string = base URL to embed.
		function TokenBuddyPage({ t }) {
			const [base, setBase] = react.useState(undefined)
			const [attempt, setAttempt] = react.useState(0)
			react.useEffect(() => {
				let alive = true
				discoverBase().then((found) => {
					if (alive) setBase(found)
				})
				return () => {
					alive = false
				}
			}, [attempt])
			if (base === undefined) {
				return react_jsx_runtime.jsx('div', { style: pageStyle, children: react_jsx_runtime.jsx('div', { children: t('loading') }) })
			}
			if (base === null) {
				return react_jsx_runtime.jsx(MissingNotice, {
					t,
					onRetry: () => {
						setBase(undefined)
						setAttempt((n) => n + 1)
					},
				})
			}
			return react_jsx_runtime.jsx('iframe', {
				src: base,
				title: 'TokenBuddy',
				style: { width: '100%', height: '100%', border: 'none', display: 'block' },
			})
		}

		const inject = ['slots', 'locale']

		function apply(ctx) {
			ctx.effect(() => ctx.locale.register(NS, { zh, en }), 'tokenbuddy: dictionaries')
			const t = ctx.locale.bind(NS)
			ctx.slots.inject('sidebar.panellist', () =>
				ctx.slots.register(
					{
						name: 'sidebar.panellist',
						id: PANEL_ID,
						order: 20,
						locale: NS,
						label: () => t('panel'),
					},
					PanelIcon,
				),
			)
			ctx.slots.inject('main', () =>
				ctx.slots.register(
					{
						name: 'main',
						key: PANEL_ID,
						locale: NS,
						inject: () => ({ t }),
					},
					TokenBuddyPage,
				),
			)
		}

		exports.apply = apply
		exports.inject = inject
		return module.exports
	},
})
