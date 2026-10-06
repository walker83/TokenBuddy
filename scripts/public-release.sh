#!/usr/bin/env bash
# 构造 TokenBuddy 的**公开仓**：从内网 main 里按白名单摘出该公开的文件，
# 落成一棵干净历史（单提交），并对成品跑隐私闸。
#
#   scripts/public-release.sh build [目标目录]   # 默认 /tmp/tokenbuddy-public
#   scripts/public-release.sh verify [目录]      # 只对已有公开树重跑隐私闸
#
# 为什么是脚本而不是 CLAUDE.md 里那串 git 命令：
#   1) 白名单只有一个出处。旧流程把路径列表抄在文档里、实际靠人肉 checkout，
#      改一个文件漏一个文件没有任何东西会报错。
#   2) 干净历史。2026-09-30 发现公开仓的 main 上带着 analysis/sessions.jsonl
#      （9MB、3391 个私有会话、97 处明文密码）—— 白名单流程是"从 github/main 起步"
#      再覆盖，起点本身就脏，于是脏东西被原样继承下来。这里改成从内网 main 起步、
#      只摘白名单，起点可证是干净的。
#   3) 成品必须过 scripts/privacy-scan.sh 才算可发布。
#
# 注意：本脚本**不推送**。推送是独立的、不可逆的一步，由人显式决定。
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# 公开白名单：内网仓里唯一允许出现在公开仓的内容。
# 私有的东西一律不在此列：analysis/（会话语料与评估集）、.gitea/（内网 CI）、
# docs/ 除截图外的内部过程文档、examples/test_sync.rs、site/、
# CLAUDE.md（内部工作文档：事故复盘、内网拓扑、口令形态；2026-10-01 曾摘除，
# 白名单回填后又随 v0.9.0/v0.10.0 带出去，2026-10-06 起永久禁入——别再回填，
# privacy-scan 结构层也会拦）。
PUBLIC_PATHS=(
  .cargo
  .github
  Cargo.lock
  Cargo.toml
  build.rs
  FEATURES.md
  LICENSE
  README.md
  README.zh-CN.md
  docs/screenshot-dashboard.png
  dsh-plugin
  examples/memgate.rs
  examples/memprobe.rs
  scripts/check.sh
  scripts/dsh-pack.sh
  scripts/install.sh
  scripts/privacy-scan.sh
  scripts/public-release.sh
  scripts/release.sh
  scripts/restart.sh
  skills
  src
  tests
)

cmd="${1:-build}"
DEST="${2:-/tmp/tokenbuddy-public}"

case "$cmd" in
build)
  [ -d "$DEST" ] && { echo "目标目录已存在：${DEST}（先自行清理或换目录）" >&2; exit 1; }
  mkdir -p "$DEST"

  echo "==> 从内网 main 按白名单摘取 ${#PUBLIC_PATHS[@]} 项（基准 HEAD：本地=main，CI=detached checkout 也成立）"
  # 容器 CI（rust:1 以 root 跑、工作区属主是 runner 用户）会撞 dubious ownership，
  # checkout 的 set-safe-directory 只写 runner 侧全局配置、容器里读不到——补一刀，
  # 否则下面每个 cat-file 都静默失败，摘出一棵空树。
  git -C "$REPO_DIR" rev-parse HEAD >/dev/null 2>&1 || \
    git config --global --add safe.directory "$REPO_DIR"
  git -C "$REPO_DIR" rev-parse HEAD >/dev/null 2>&1 \
    || { echo "git 无法读取仓库（dubious ownership？）：$REPO_DIR" >&2; exit 1; }
  for p in "${PUBLIC_PATHS[@]}"; do
    if git -C "$REPO_DIR" cat-file -e "HEAD:$p" 2>/dev/null; then
      git -C "$REPO_DIR" archive "HEAD" -- "$p" | tar -x -C "$DEST"
    else
      echo "  跳过（main 上不存在）：$p"
    fi
  done
  [ -n "$(ls -A "$DEST")" ] || { echo "白名单一项都没摘到——git 对仓库不可读或白名单全错" >&2; exit 1; }

  # 兜底一：根目录构建关键文件必须全部入白名单，否则 cargo 编译会挂且无人察觉
  echo "==> 核对构建关键文件"
  for critical in build.rs Cargo.toml Cargo.lock; do
    [ -f "$DEST/$critical" ] || { echo "公开树缺构建关键文件：$critical（白名单漏了？）" >&2; exit 1; }
  done

  # 兜底二：白名单写错路径时这里会静默少文件，所以显式核对禁路径
  echo "==> 核对禁路径"
  leaked=$(cd "$DEST" && find . -type f \
    | sed 's|^\./||' \
    | grep -E '^(analysis/|CLAUDE\.md$|examples/test_sync\.rs$|docs/iteration-|docs/.*-20[0-9]{2}-|site/|\.env)' || true)
  if [ -n "$leaked" ]; then
    echo "公开树里出现了禁路径：" >&2
    echo "$leaked" >&2
    exit 1
  fi

  VERSION=$(grep -m1 '^version' "$REPO_DIR/Cargo.toml" | sed 's/.*"\(.*\)".*/\1/')
  (cd "$DEST" && git init -q . && git add -A && \
    git -c user.name=walker -c user.email=walker@users.noreply.github.com commit -q \
    -m "v${VERSION}: TokenBuddy — local-first token ledger & AI conversation search

Single squashed public tree. The public repo previously inherited analysis/ from an
earlier branch-based flow, which exposed private session transcripts and plaintext
credentials; this tree is rebuilt from the internal main via an explicit allowlist
and gated by scripts/privacy-scan.sh.")
  # tag 打不上不致命（可后补），但 commit 失败绝不能被吞——否则 CI 会拿空树跑隐私闸
  (cd "$DEST" && git tag -f "v${VERSION}" >/dev/null 2>&1) || true

  echo "==> 公开树已生成：${DEST}（版本 v${VERSION}）"
  ;;

verify)
  [ -d "$DEST/.git" ] || { echo "不是 git 仓：$DEST" >&2; exit 1; }
  ;;

*)
  echo "用法: $0 [build|verify] [目录]" >&2
  exit 2
  ;;
esac

echo
echo "==> 对成品跑隐私闸"
(cd "$DEST" && "$REPO_DIR/scripts/privacy-scan.sh" HEAD)
echo
echo "公开树：$DEST"
echo "推送是独立一步，确认后再做（本脚本刻意不做）。"
