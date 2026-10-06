#!/usr/bin/env bash
# 网关真机 smoke(R108):MiniMax 实链路。隔离 TOKENBUDDY_HOME,不碰真实数据。
# 前置:~/.mmx/config.json 里有 MiniMax key(或 MMX_KEY 环境变量)。
# 用法: scripts/gw-smoke.sh [端口,默认 18790]
set -euo pipefail

PORT="${1:-18790}"
SMOKE_HOME="$(mktemp -d /tmp/tb-gw-smoke.XXXXXX)"
BIN="${BIN:-./target/debug/tokenbuddy}"

echo "→ smoke 数据目录: $SMOKE_HOME"

# MiniMax key:mmx CLI 配置或环境变量
KEY="${MMX_KEY:-}"
if [ -z "$KEY" ] && [ -f "$HOME/.mmx/config.json" ]; then
  KEY="$(python3 -c "import json;print(json.load(open('$HOME/.mmx/config.json'))['api_key'])")"
fi
[ -n "$KEY" ] || { echo "✗ 找不到 MiniMax key(~/.mmx/config.json 或 MMX_KEY)"; exit 1; }

mkdir -p "$SMOKE_HOME/keys"
printf '%s' "$KEY" > "$SMOKE_HOME/keys/minimax.key"
chmod 600 "$SMOKE_HOME/keys/minimax.key"

cat > "$SMOKE_HOME/gateway.json" <<EOF
{
  "enabled": true,
  "listen": "127.0.0.1:$PORT",
  "providers": [{
    "id": "minimax",
    "protocol": "openai",
    "base_url": "https://api.minimaxi.com/v1",
    "key_file": "keys/minimax.key",
    "models": ["MiniMax-M3.1-Flash-Preview"],
    "dialects": ["include_usage", "cached_tokens_in_details"]
  }],
  "combos": []
}
EOF

export TOKENBUDDY_HOME="$SMOKE_HOME"
CLIENT_KEY="$("$BIN" gateway key add smoke | sed -n 's/^.*\(tb-local-[0-9a-f]\{32\}\).*/\1/p' | head -1)"
[ -n "$CLIENT_KEY" ] || { echo "✗ 签发客户端密钥失败"; "$BIN" gateway key add smoke || true; exit 1; }
echo "→ 客户端密钥: ${CLIENT_KEY:0:12}…"

SERVE_PORT=$((RANDOM % 20000 + 30000))
"$BIN" serve --port "$SERVE_PORT" --no-open > "$SMOKE_HOME/serve.log" 2>&1 &
SERVE_PID=$!
trap 'kill $SERVE_PID 2>/dev/null || true' EXIT
sleep 2

echo "→ 探活: $BIN gateway probe"
"$BIN" gateway probe || true

echo "→ 流式请求(经网关 → MiniMax):"
curl -sN --max-time 60 "http://127.0.0.1:$PORT/v1/chat/completions" \
  -H "Authorization: Bearer $CLIENT_KEY" \
  -H 'content-type: application/json' \
  -d '{"model":"MiniMax-M3.1-Flash-Preview","stream":true,"messages":[{"role":"user","content":"只回复两个字:收到"}]}' \
  | tail -3

echo "→ 非流式请求:"
curl -s --max-time 60 "http://127.0.0.1:$PORT/v1/chat/completions" \
  -H "Authorization: Bearer $CLIENT_KEY" \
  -H 'content-type: application/json' \
  -d '{"model":"MiniMax-M3.1-Flash-Preview","stream":false,"messages":[{"role":"user","content":"只回复两个字:收到"}]}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print('  回答:', d['choices'][0]['message']['content'], '| usage:', d.get('usage'))"

echo "→ 模型列表:"
curl -s --max-time 15 "http://127.0.0.1:$PORT/v1/models" -H "Authorization: Bearer $CLIENT_KEY" | head -c 200; echo

echo "→ 入账断言:"
sleep 1
ROWS="$(python3 - "$SMOKE_HOME/gateway/usage.jsonl" <<'EOF'
import json,sys
rows=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]
assert len(rows)>=2, f"应有 ≥2 行,得到 {len(rows)}"
for r in rows:
    assert r["usage_source"]=="upstream", r
    assert r["input"]>0 and r["output"]>0, r
    assert r["client"]=="smoke", r
    assert r["model"]=="MiniMax-M3.1-Flash-Preview", r
print(f"  {len(rows)} 行全部 upstream 直报,client=smoke,token>0 ✓")
for r in rows:
    print(f"  in={r['input']} out={r['output']} cache_read={r['cache_read']} stream={r['stream']} ttft={r['ttft_ms']}ms dur={r['duration_ms']}ms")
EOF
)"
echo "$ROWS"

echo "→ sync 后账本统计:"
curl -s --max-time 30 -X POST "http://127.0.0.1:$SERVE_PORT/api/sync" > /dev/null
"$BIN" gateway status | grep 账本 || true

kill $SERVE_PID 2>/dev/null || true
echo "✓ smoke 通过(数据在 $SMOKE_HOME,可随时删除)"
