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
#   ... | bash -s -- --service     # also register a background service
#
# Options:
#   --service     start the dashboard at login (macOS: launchd user agent,
#                 Linux: systemd --user unit) so it is always up without a
#                 terminal to keep open
#
# Env overrides:
#   TOKENBUDDY_VERSION  specific release tag (default: latest)
#   TOKENBUDDY_PREFIX   install dir (default: ~/.local/bin)

set -euo pipefail

INSTALL_SERVICE=0
for arg in "$@"; do
  case "$arg" in
    --service) INSTALL_SERVICE=1 ;;
    *) echo "未知参数：$arg（可用：--service）" >&2; exit 2 ;;
  esac
done

REPO="walker83/TokenBuddy"
VERSION="${TOKENBUDDY_VERSION:-}"
PREFIX="${TOKENBUDDY_PREFIX:-$HOME/.local/bin}"
ARCH="$(uname -m)"
OS="$(uname -s)"

case "$OS/$ARCH" in
  Darwin/arm64) TARGET="aarch64-apple-darwin" ;;
  Darwin/x86_64) TARGET="x86_64-apple-darwin" ;;
  # x64 Linux ships a musl static build — glibc-free, runs anywhere.
  Linux/x86_64) TARGET="x86_64-unknown-linux-musl" ;;
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
# Keep the release's own filename: the .sha256 manifest embeds it, and
# `sha256sum -c` matches by name, not by argument.
TGZ_NAME="${URL##*/}"
curl -fSL "$URL" -o "$TMP/$TGZ_NAME"

echo "==> 校验 sha256…"
SHA_URL="${URL}.sha256"
if curl -fsSL "$SHA_URL" -o "$TMP/checksums.sha256" 2>/dev/null; then
  # Linux ships sha256sum (GNU, has --status); macOS ships the perl shasum
  # (no --status — silence it with a redirect instead).
  if (cd "$TMP" && sha256sum -c --status checksums.sha256 2>/dev/null) \
     || (cd "$TMP" && shasum -a 256 -c checksums.sha256 >/dev/null 2>&1); then
    echo "    ✓ 校验通过"
  else
    echo "sha256 校验失败——下载已损坏或被篡改,拒绝安装" >&2
    exit 1
  fi
else
  echo "    ⚠ 该发布未附 sha256 文件，跳过校验（建议核对 Releases 页面的校验和）" >&2
fi

tar -xzf "$TMP/$TGZ_NAME" -C "$TMP"
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

# Register a background service so the dashboard is there after a reboot
# without anyone remembering to start it. Opt-in: a service manager writing
# into the user's home is not something an installer should do unasked.
install_service() {
  local bin="$PREFIX/tokenbuddy"
  local base="https://raw.githubusercontent.com/$REPO/main/scripts"
  local tmp
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN

  if [ "$OS" = "Darwin" ]; then
    local plist="$HOME/Library/LaunchAgents/com.tokenbuddy.server.plist"
    mkdir -p "$(dirname "$plist")" "$HOME/.tokenbuddy"
    curl -fsSL "$base/tokenbuddy.plist.in" -o "$tmp/plist.in"
    sed -e "s|__BIN__|$bin|" -e "s|__LOG__|$HOME/.tokenbuddy/tokenbuddy.log|" \
      "$tmp/plist.in" > "$plist"
    # bootout first: re-running the installer must not fail just because the
    # agent is already loaded.
    launchctl bootout "gui/$(id -u)/com.tokenbuddy.server" 2>/dev/null || true
    launchctl bootstrap "gui/$(id -u)" "$plist"
    echo "==> 已注册开机常驻（launchd）"
    echo "    日志：tail -f $HOME/.tokenbuddy/tokenbuddy.log"
    echo "    卸载：launchctl bootout gui/$(id -u)/com.tokenbuddy.server"
  else
    local unit="$HOME/.config/systemd/user/tokenbuddy.service"
    mkdir -p "$(dirname "$unit")"
    curl -fsSL "$base/tokenbuddy.service" -o "$unit"
    systemctl --user daemon-reload
    systemctl --user enable --now tokenbuddy.service
    echo "==> 已注册开机常驻（systemd --user）"
    echo "    状态：systemctl --user status tokenbuddy"
    echo "    登出后仍常驻：loginctl enable-linger \$(id -un)"
  fi
}

if [ "$INSTALL_SERVICE" -eq 1 ]; then
  install_service
fi

echo "==> 安装完成：$PREFIX/tokenbuddy"
echo "    启动：tokenbuddy   （会自动打开 http://127.0.0.1:8080）"
echo "    查看：tokenbuddy status"
echo "    换端口：tokenbuddy --port 9000"
[ "$INSTALL_SERVICE" -eq 1 ] || echo "    想开机自启：重跑本脚本并加 --service"
