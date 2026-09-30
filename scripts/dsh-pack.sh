#!/usr/bin/env bash
# 一键把 TokenBuddy 的 dsh 插件打包并装进本机 DeepSeek Harness。
#
# 用法:
#   scripts/dsh-pack.sh                 # 同步到 ~/code/dsh-plugin-tokenbuddy 并安装到 profile
#   scripts/dsh-pack.sh <profile>       # 指定 profile（默认 desktop）
#   scripts/dsh-pack.sh --publish       # 同步 + npm publish（发布前手动确认版本）
#   scripts/dsh-pack.sh --remove        # 从 profile 卸载
#
# dsh plugin add 用 pnpm link 方式挂载 checkout，之后在仓库里改 index.js
# 重启 dsh 即生效，无需重复打包。
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$REPO_DIR/dsh-plugin"
DEST="$HOME/code/dsh-plugin-tokenbuddy"
PROFILE=desktop
PUBLISH=0

while [ $# -gt 0 ]; do
  case "$1" in
    --publish) PUBLISH=1 ;;
    --remove)  REMOTE=1 ;;
    *) PROFILE="$1" ;;
  esac
  shift
done

command -v dsh >/dev/null || { echo "未找到 dsh（DeepSeek Harness CLI）"; exit 1; }
[ -f "$SRC/package.json" ] || { echo "缺 $SRC/package.json"; exit 1; }

if [ "${REMOTE:-0}" = 1 ]; then
  dsh plugin --profile "$PROFILE" remove dsh-plugin-tokenbuddy
  echo "已从 profile $PROFILE 卸载"
  exit 0
fi

# 同步到独立发布树（保留其中的 .git，其余全量镜像）
mkdir -p "$DEST"
if [ ! -d "$DEST/.git" ]; then
  git init -q "$DEST"
  echo "已初始化独立仓库 $DEST"
fi
rsync -a --delete --exclude '.git/' --exclude 'node_modules/' "$SRC/" "$DEST/"

# 插件版本号跟随 TokenBuddy 主版本（Cargo.toml），两处保持一致
TB_VERSION=$(grep -m1 '^version' "$REPO_DIR/Cargo.toml" | sed 's/.*"\(.*\)".*/\1/')
node -e "
const fs = require('fs')
const p = '$DEST/package.json'
const pkg = JSON.parse(fs.readFileSync(p, 'utf8'))
if (pkg.version !== '$TB_VERSION') {
  pkg.version = '$TB_VERSION'
  fs.writeFileSync(p, JSON.stringify(pkg, null, 2) + '\n')
  console.log('插件版本号已对齐 TokenBuddy: $TB_VERSION')
}
"
(cd "$SRC" && sed -i '' "s/^  \"version\": \".*\"/  \"version\": \"$TB_VERSION\"/" package.json)

# peer 依赖必须真实装在发布树里，否则 harness 导入插件时解析不到 dsh-tools 会被静默跳过
if [ ! -d "$DEST/node_modules/@deepseek-ai/dsh-tools" ]; then
  (cd "$DEST" && pnpm add @deepseek-ai/dsh-tools@0.2.0-rc.2 >/dev/null 2>&1)
  echo "已补装 peer 依赖 @deepseek-ai/dsh-tools"
fi

cd "$DEST"
CHANGED=$(git status --porcelain || true)
if [ -n "$CHANGED" ]; then
  git add -A
  git commit -q -m "sync from local-token-compute@dsh-plugin $(date +%F)"
  echo "发布树有更新，已提交快照"
fi

if [ "$PUBLISH" = 1 ]; then
  npm publish --access public
  echo "已发布到 npm"
fi

# 装进 profile：pnpm link 本目录，并把 bundle 追加进 dsh.profile.bundles
dsh plugin --profile "$PROFILE" add "$DEST"

echo
echo "== 完成 =="
echo "profile: ${PROFILE}，bundle: dsh-plugin-tokenbuddy"
echo "重启 dsh（dsh web / 桌面端）后生效；验证：让模型调用 tokenbuddy_usage 查询 summary"
echo "卸载：scripts/dsh-pack.sh --remove"
