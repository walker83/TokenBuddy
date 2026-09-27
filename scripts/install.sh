#!/usr/bin/env bash
# TokenBuddy installer — downloads the latest release from GitHub and
# installs it without tripping Gatekeeper.
#
# Why this exists: the release binary is ad-hoc signed (no paid Apple
# Developer ID). A tarball downloaded through a BROWSER gets the
# com.apple.quarantine xattr, and macOS Gatekeeper then blocks execution
# ("无法打开，因为无法验证开发者"). Downloads made by curl — like this
# script — never receive the quarantine attribute, so the binary runs as-is.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/walker83/TokenBuddy/main/scripts/install.sh | bash
#
# Env overrides:
#   TOKENBUDDY_VERSION  specific release tag (default: latest)
#   TOKENBUDDY_PREFIX   install dir (default: ~/.local/bin)

set -euo pipefail

REPO="walker83/TokenBuddy"
VERSION="${TOKENBUDDY_VERSION:-}"
PREFIX="${TOKENBUDDY_PREFIX:-$HOME/.local/bin}"
ARCH="$(uname -m)"
OS="$(uname -s)"

case "$OS/$ARCH" in
  Darwin/arm64) TARGET="aarch64-apple-darwin" ;;
  Darwin/x86_64) TARGET="x86_64-apple-darwin" ;;
  Linux/x86_64) TARGET="x86_64-unknown-linux-gnu" ;;
  Linux/aarch64) TARGET="aarch64-unknown-linux-gnu" ;;
  *) echo "不支持的平台: $OS/$ARCH —— 请从源码构建（见 README）" >&2; exit 1 ;;
esac

if [ -z "$VERSION" ]; then
  echo "==> 查询最新版本…"
  VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)"
  [ -n "$VERSION" ] || { echo "无法获取最新版本号（GitHub API 限流？）" >&2; exit 1; }
fi

URL="https://github.com/${REPO}/releases/download/${VERSION}/tokenbuddy-${VERSION#v}-${TARGET}.tar.gz"
echo "==> 下载 ${URL}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
curl -fSL "$URL" -o "$TMP/tb.tar.gz"

echo "==> 校验 sha256…"
SHA_URL="${URL}.sha256"
if curl -fsSL "$SHA_URL" -o "$TMP/tb.sha256" 2>/dev/null; then
  (cd "$TMP" && shasum -a 256 -c --status tb.sha256 2>/dev/null) \
    || { echo "sha256 校验失败" >&2; exit 1; }
else
  echo "    （发布未附 sha256，跳过校验）"
fi

tar -xzf "$TMP/tb.tar.gz" -C "$TMP"
BIN="$(find "$TMP" -type f -name tokenbuddy | head -1)"
[ -n "$BIN" ] || { echo "压缩包里没有找到 tokenbuddy 二进制" >&2; exit 1; }

mkdir -p "$PREFIX"
install -m 0755 "$BIN" "$PREFIX/tokenbuddy"

# 保险起见仍剥离 quarantine（curl 不会加，但用户可能通过管道外的方式重跑）
xattr -d com.apple.quarantine "$PREFIX/tokenbuddy" 2>/dev/null || true

case ":$PATH:" in
  *":$PREFIX:"*) ;;
  *) echo "==> 提示：把 $PREFIX 加进 PATH 才能直接运行 tokenbuddy" ;;
esac

echo "==> 安装完成：$PREFIX/tokenbuddy"
echo "    启动：tokenbuddy   然后打开 http://127.0.0.1:8080"
