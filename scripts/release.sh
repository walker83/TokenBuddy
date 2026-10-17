#!/usr/bin/env bash
# TokenBuddy release packaging.
#
# Builds the release binary, applies an ad-hoc code signature (macOS arm64
# refuses to execute completely unsigned binaries, and a stable identifier
# keeps the signature deterministic), then packages the tarball + sha256.
#
# Why not notarize: notarization requires a paid Apple Developer ID. Without
# one, a browser-downloaded tarball still carries the quarantine xattr and
# Gatekeeper will block the binary — that is what scripts/install.sh and the
# README troubleshooting section work around (curl downloads never get the
# quarantine attribute).

set -euo pipefail
cd "$(dirname "$0")/.."

VERSION="$(grep -m1 '^version' Cargo.toml | sed 's/version = "\(.*\)"/\1/')"
TARGET="$(rustc -vV | awk '/^host:/ {print $2}')"
BIN=target/release/tokenbuddy
OUT="dist/tokenbuddy-v${VERSION}-${TARGET}"

echo "==> tokenbuddy v${VERSION} (${TARGET})"

echo "==> cargo build --release"
cargo b

echo "==> ad-hoc codesign"
codesign --force --sign - --timestamp=none -i tokenbuddy "$BIN"
codesign --verify "$BIN"

rm -rf dist
mkdir -p dist
cp "$BIN" "$OUT"
tar -czf "${OUT}.tar.gz" -C dist "$(basename "$OUT")"
rm "$OUT"
shasum -a 256 "${OUT}.tar.gz" | tee "${OUT}.tar.gz.sha256"

cat <<EOF

==> 打包完成 dist/${OUT}.tar.gz

用户侧两种安装方式（README 已同步说明）：

  1) 安装脚本（推荐，无 Gatekeeper 拦截）：
     curl -fsSL https://raw.githubusercontent.com/walker83/TokenBuddy/main/scripts/install.sh | bash

  2) 手动下载解压后若提示"无法打开/不安全"：
     xattr -d com.apple.quarantine ./tokenbuddy

彻底消除该提示需要 Apple Developer ID 签名 + 公证（付费账号），见 README 路线图。
EOF
