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

echo "==> cargo build --release"
cargo b

# The README's headline claim is a ~4 MB single binary. The guard sits at 2x
# so it never trips on platform variance, but still catches adding a heavy
# dependency (DuckDB once added 19 MB). Override: TOKENBUDDY_SIZE_LIMIT_MB.
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
BIN="$TARGET_DIR/release/tokenbuddy"
LIMIT_MB="${TOKENBUDDY_SIZE_LIMIT_MB:-8}"
LIMIT=$((LIMIT_MB * 1024 * 1024))
SIZE=$(wc -c < "$BIN" | tr -d ' ')
echo "==> binary size guard: $SIZE bytes (limit ${LIMIT_MB} MB)"
if [ "$SIZE" -gt "$LIMIT" ]; then
    echo "FAIL: $BIN is $SIZE bytes, over the ${LIMIT_MB} MB guard." >&2
    echo "A dependency likely dragged the binary size; check Cargo.toml before force-pushing." >&2
    exit 1
fi

echo "==> gate passed"
