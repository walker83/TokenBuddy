#!/usr/bin/env bash
# TokenBuddy quality gate — the single entry point that both CI
# (.github/workflows/ci.yml, picked up by Gitea Actions and GitHub alike) and
# the local pre-push hook (.githooks/) run. Adding a new check here is enough;
# do not fork the logic into CI YAML or the hook.
#
# All compilation is --release: debug builds are banned in this repo
# (see CLAUDE.md — dev artifacts once grew target/ to 16G).
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> cargo fmt --check"
cargo fmt --all --check

echo "==> cargo clippy --release --all-targets (deny warnings)"
cargo clippy --release --all-targets -- -D warnings

echo "==> cargo test --release"
cargo test --release

echo "==> memory budget gate (synthetic 20k-doc index)"
cargo run --release --example memgate --quiet

echo "==> cargo build --release"
cargo b

# 公开仓门禁：每次 push 都要能证明"按白名单摘出来的公开树是干净的"。
# 2026-09-30 的教训——旧流程从 github/main 起步，起点本身带着 analysis/ 的
# 9MB 私有会话（97 处明文密码），而当时的扫描只查源码标识符，0 命中。
# 这里改成从内网 main 按白名单重摘一棵干净树并跑三层隐私闸，几秒钟的成本。
echo "==> public release tree + privacy gate"
# mktemp -d 本身会创建目录，而 public-release.sh build 要求目标不存在（避免
# 往旧树上叠加），所以让 build 建 mktemp 目录下的 tree 子目录。
PUB_PARENT="$(mktemp -d "${TMPDIR:-/tmp}/tb-pubcheck.XXXXXX")"
PUB_TMP="$PUB_PARENT/tree"
trap 'rm -rf "$PUB_PARENT"' EXIT
if scripts/public-release.sh build "$PUB_TMP" >/dev/null; then
  echo "public tree ok: $PUB_TMP"
else
  echo "FAIL: 公开树构造或隐私扫描未通过——不要往 GitHub 推。" >&2
  exit 1
fi

# The README's headline claim is a single-binary tool; the guard exists to
# catch runaway dependency creep (DuckDB once added 19 MB), not to pin an
# exact size. Budget raised to 20 MB (2026-09-28) so new collectors and
# features have room; anything approaching the ceiling still warrants a
# bloat check before merge. Override: TOKENBUDDY_SIZE_LIMIT_MB.
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
BIN="$TARGET_DIR/release/tokenbuddy"
LIMIT_MB="${TOKENBUDDY_SIZE_LIMIT_MB:-20}"
LIMIT=$((LIMIT_MB * 1024 * 1024))
SIZE=$(wc -c < "$BIN" | tr -d ' ')
echo "==> binary size guard: $SIZE bytes (limit ${LIMIT_MB} MB)"
if [ "$SIZE" -gt "$LIMIT" ]; then
    echo "FAIL: $BIN is $SIZE bytes, over the ${LIMIT_MB} MB guard." >&2
    echo "A dependency likely dragged the binary size; check Cargo.toml before force-pushing." >&2
    exit 1
fi

echo "==> gate passed"
