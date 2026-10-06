#!/usr/bin/env bash
# 重启常驻 TokenBuddy 并核验构建指纹（issue #17 / docs/build-fingerprint-proposal-2026-09-29.md）。
# 为什么存在：macOS 下 rename 换 inode，重新 cargo build 后已运行的进程仍持有旧镜像，
# version 只在发版时递增，HTTP 行为对不上时无法区分「没修」和「在对陈旧进程说话」。
# 本脚本在重启后轮询 /api/status，直到 build_commit 与目标提交一致才算成功。
#
# 用法：scripts/restart.sh [目标提交] [端口]   （默认 HEAD 与 8080）

set -euo pipefail

TARGET_COMMIT="${1:-$(git rev-parse --short=8 HEAD)}"
PORT="${2:-8080}"
BASE="http://127.0.0.1:${PORT}"
BIN="${TOKENBUDDY_BIN:-./target/release/tokenbuddy}"

status_field() { # $1=字段名
    curl -fsS "${BASE}/api/status" |
        python3 -c "import json,sys; print(json.load(sys.stdin).get('$1',''))" 2>/dev/null || true
}

# 1. 停：只停本仓二进制起的 serve（--addr 与 serve 两种启动形态都覆盖）
pkill -f "target/release/tokenbuddy" 2>/dev/null || true

# 2. 等端口空（最多 10s）
for _ in $(seq 1 50); do
    if ! lsof -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1; then break; fi
    sleep 0.2
done

# 3. 起：脱离本脚本生命周期，日志落 /tmp
nohup "${BIN}" --addr "127.0.0.1:${PORT}" >/tmp/tokenbuddy-serve.log 2>&1 &
disown

# 4. 轮询 /api/status 直到 build_commit 与目标一致（最多 15s）
for _ in $(seq 1 75); do
    COMMIT="$(status_field build_commit)"
    if [ -n "${COMMIT}" ]; then
        if [ "${COMMIT}" = "${TARGET_COMMIT}" ]; then
            echo "OK: build_commit=${COMMIT}（= 目标 ${TARGET_COMMIT}）· 端口 ${PORT}"
            exit 0
        fi
        echo "FATAL: 服务 build_commit=${COMMIT} ≠ 目标 ${TARGET_COMMIT}——二进制不是这次构建的产物" >&2
        exit 1
    fi
    sleep 0.2
done

echo "FATAL: ${BASE}/api/status 15s 内不可达" >&2
exit 1
