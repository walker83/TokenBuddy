#!/usr/bin/env bash
# 隐私扫描闸：在**公开发布前**对一棵 git 树做三层检查。
#
#   scripts/privacy-scan.sh <ref>      扫描某个已提交的树（分支/tag/commit）
#   scripts/privacy-scan.sh --index    扫描当前暂存区
#
# 存在的理由：旧规矩只有一条 `git grep -E "192\.168\.|walker@|ssh://git|密码[是为：]"，
# 它只查源码里的标识符，查不出数据文件正文里的话。2026-09-30 发现公开仓
# analysis/sessions.jsonl（9MB、3391 个私有会话、97 处明文密码）而该扫描 0 命中，
# 因为真实文本是 `密码DBStack@root`——"密码"后直接跟值，不带 为/是/：。
#
# 三层各管一类漏法：
#   1 结构  禁路径       —— analysis/ 等私有目录压根不该进公开树
#   2 形状  体量/后缀    —— 会话语料必然是大体量数据文件，与内容无关也拦得住
#   3 内容  凭据模式     —— 高置信度密钥形态；分隔符一律可选
#
# 设计取舍：误报比漏报更容易让门禁失效（改用 --no-verify 就绕过了）。所以第 3 层
# 只收高置信度形态：早期版本还匹配 `access_key|password|secret` 后跟 : =，结果把
# `access_key.to_string()`、`getElementById` 这类标识符用法全误报，而源码里这些词
# 永远会出现。内网网段只警告不拦截——源码注释里提到 LAN 属正常。
set -uo pipefail

# 不强制 cd：这套脚本要能对**另一个仓**跑（public-release.sh 会在临时公开树里调用它），
# 所以按当前工作目录定位 git 仓。内部仓里 analysis/ 天然存在、必然不通过——它只对
# 待发布的公开树有意义。
if ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo "不在 git 仓里，无法扫描" >&2
  exit 2
fi

REF="${1:-}"
[ "$REF" = "--index" ] && REF="--cached"
if [ -z "$REF" ]; then
  echo "用法: $0 <ref> | --index" >&2
  exit 2
fi

FAIL=0
note() { printf '  %s\n' "$*"; }
fail() { printf '  ✗ %s\n' "$*"; FAIL=1; }
warn() { printf '  ! %s\n' "$*"; }

# 1) 结构层：公开树里绝不该出现的路径
FORBIDDEN_RE='^(analysis/|examples/test_sync\.rs$|docs/iteration-|docs/.*-20[0-9]{2}-|site/|\.env$|\.env\.[^/]+$)'

# 2) 形状层：单文件体积上限（字节）。README 不可能到 5MB；会话语料动辄几 MB。
MAX_BYTES=$((5 * 1024 * 1024))
CORPUS_MIN=200000
CORPUS_RE='\.(jsonl|ndjson|json|parquet|sqlite|db|log)$'

# 3) 内容层：凭据。分隔符一律可选 —— 这是旧正则漏掉"密码DBStack@root"的原因。
Q="['\"]"
CRED_RE="密码[为数：:=[:space:]]{0,3}${Q}?[A-Za-z0-9@!#\$%^&*_.-]{6,}"
CRED_RE="${CRED_RE}|口令[为数：:=[:space:]]{0,3}${Q}?[A-Za-z0-9@!#\$%^&*_.-]{6,}"
CRED_RE="${CRED_RE}|(password|passwd|pwd)[[:space:]]*[:=][[:space:]]*${Q}[^\"]{8,}${Q}"
CRED_RE="${CRED_RE}|AKIA[0-9A-Z]{16}|gh[psoar]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}"
CRED_RE="${CRED_RE}|xox[baprs]-[A-Za-z0-9-]{10,}|sk-ant-[A-Za-z0-9-]{20,}|sk-[A-Za-z0-9]{32,}"
CRED_RE="${CRED_RE}|-----BEGIN [A-Z ]*PRIVATE KEY-----"
# AWS 官方文档示例密钥，测试夹具里合法存在
CRED_EXCLUDE='AKIAIOSFODNN7EXAMPLE'
# 内网网段。不加 \b —— git grep -E 走 POSIX ERE，\b 不生效，会静默漏掉整类匹配。
IP_RE='(192\.168\.[0-9]{1,3}\.[0-9]{1,3}|10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}|172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3})'

files=$(git ls-tree -r --name-only "$REF")
if [ -z "$files" ]; then
  echo "扫描目标为空或不存在：${REF}" >&2
  exit 2
fi

echo "==> 隐私扫描：${REF}（$(echo "$files" | wc -l | tr -d ' ') 个文件）"

echo "  [1/3] 禁路径"
n=0
while IFS= read -r f; do
  [ -n "$f" ] || continue
  fail "禁路径进入公开树：$f"
  n=$((n + 1))
done < <(echo "$files" | grep -E "$FORBIDDEN_RE" || true)
[ "$n" = 0 ] && note "无禁路径"

echo "  [2/3] 体量与语料后缀（上限 $((MAX_BYTES / 1024 / 1024))MB）"
n=0
while read -r meta path; do
  [ -n "${path:-}" ] || continue
  size=$(printf '%s' "$meta" | awk '{print $4}')
  [ -z "$size" ] && continue
  if [ "$size" -gt "$MAX_BYTES" ]; then
    fail "超大体量文件：${path}（$size 字节）"
    n=$((n + 1))
  elif [ "$size" -gt "$CORPUS_MIN" ] && echo "$path" | grep -qE "$CORPUS_RE"; then
    fail "疑似语料/数据文件：${path}（$size 字节）"
    n=$((n + 1))
  fi
done < <(git ls-tree -r -l "$REF")
[ "$n" = 0 ] && note "无超限文件"

echo "  [3/3] 凭据与内网地址"
n=0
while IFS= read -r f; do
  [ -n "$f" ] || continue
  f="${f#*:}"
  # 扫描器自身必然包含全部检测模式（"密码X"、"口令X" 就是它的字面量），
  # 不自排除就是永久误报。这里按路径排除，不做内容级豁免。
  if [ "$f" = "scripts/privacy-scan.sh" ]; then
    note "扫描器自身，跳过：$f"
    continue
  fi
  if git show "${REF}:${f}" 2>/dev/null | grep -qF "$CRED_EXCLUDE" && \
     [ "$(git show "${REF}:${f}" 2>/dev/null | grep -oE "$CRED_RE" | grep -vcF "$CRED_EXCLUDE")" = 0 ]; then
    note "仅含 AWS 示例密钥，跳过：$f"
    continue
  fi
  fail "疑似凭据：$f"
  n=$((n + 1))
done < <(git grep -I -l -E "$CRED_RE" "$REF" -- 2>/dev/null || true)
[ "$n" = 0 ] && note "无疑似凭据"

while IFS= read -r f; do
  [ -n "$f" ] && warn "含内网地址（需人工确认）：$f"
done < <(git grep -I -l -E "$IP_RE" "$REF" -- 2>/dev/null || true)

if [ "$FAIL" = 1 ]; then
  echo "==> 隐私扫描未通过 —— 禁止发布" >&2
  exit 1
fi
echo "==> 隐私扫描通过"
