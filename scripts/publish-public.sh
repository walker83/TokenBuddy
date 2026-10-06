#!/usr/bin/env bash
# TokenBuddy dual-remote publish — the 6 hand-copied lines from CLAUDE.md §双远程
# turned into one gated command.
#
# Why this exists: the public remote carries a deliberately clean history and
# MUST NOT contain analysis/, examples/test_sync.rs, or the internal docs/.
# `git push github main` would push the private history — including the session
# data under analysis/ — in one keystroke. That flow was re-run by hand across
# 6 sessions / 5 days ("双远程" 命中 6 会话/5 天, "public-release" 3 会话/3 天),
# three tags went out on 2026-09-29 alone, and scripts/release.sh does not cover
# it (that one only builds, signs and packages). The highest-frequency
# hand-copied procedure in the repo happened to be the irreversible one.
#
#   scripts/publish-public.sh              plan only — verify every gate, print
#                                          the tree that WOULD ship, no writes
#   scripts/publish-public.sh --publish    commit and push (asks for YES first)
#   scripts/publish-public.sh --ref v0.7.0 --tag v0.7.0
#   scripts/publish-public.sh --check-only  gates only: no tree printout, never
#                                          publishes; non-zero if a hard gate
#                                          failed (every gate still runs, so one
#                                          invocation reports every problem)
#
# Design rules baked in:
#   * works in a throwaway `git worktree`, never in your checkout — a failed
#     hand-run release used to leave the operator on public-release with a
#     partially applied index;
#   * never touches the network (no fetch): run `git fetch github` yourself so
#     the base you gate against is the base that exists;
#   * four independent gates; a green plan is a precondition for a push, not a
#     substitute for reading the printed file list.

set -euo pipefail
cd "$(dirname "$0")/.."

PUBLIC_REMOTE=github
SOURCE_REF=""
PUSH_TAG=""
DO_PUBLISH=0
AUTO_YES=0
CHECK_ONLY=0

# Mirrors CLAUDE.md §双远程 — and the two lists agree only because the gate
# said so: the documented checkout line omitted `build.rs` (the crate does not
# compile without it — it embeds dashboard.html), `scripts/` (the public CI
# workflow literally runs `scripts/check.sh`), `tests/` and `.gitignore`, so
# github/main carried stale copies of all four. If you change any of them,
# they must be listed here or the public repo keeps the old version silently.
# `docs/` stays deliberately partial: only the screenshot ships.
#
# CLAUDE.md is NOT allowlisted, deliberately and permanently (2026-10-01). It is
# a maintainer working agreement, not contributor documentation: it names
# internal hosts, records incident write-ups, quotes real ledger figures, and
# describes this very two-remote setup. `is_denied_path` below hard-fails if it
# is ever re-added here, so the rule is enforced rather than remembered.
# Keep that explanation free of specifics — this file ships, and a comment
# explaining *why* something is private leaks as effectively as the thing itself.
ALLOWLIST=(.cargo .github .gitignore Cargo.lock Cargo.toml FEATURES.md
    LICENSE README.md README.zh-CN.md build.rs docs/screenshot-dashboard.png
    scripts src tests skills examples/memprobe.rs)

# Hard: anything shaped like a credential, plus the maintainer-only docs —
# CLAUDE.md sits next to analysis/ for the same reason: it is a private working
# agreement (internal hosts, incident write-ups, real usage stats), not
# contributor documentation. Soft: LAN literals that this repo legitimately used
# as test placeholders; since 2026-10-01 the shipped tests use RFC 5737
# TEST-NET-1 (192.0.2.x) rather than real-looking 192.168.x, so this class
# should normally come back empty — a hit here is now worth reading, not
# skimming past as a known false positive.
SECRET_HARD='BEGIN [A-Z ]*PRIVATE KEY|ghp_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9]{30,}|xox[baprs]-[A-Za-z0-9-]{10,}|://[^/@[:space:]]+:[^/@[:space:]]+@|(api_?key|secret|passwd|password|access_token)[^A-Za-z0-9_]{0,3}[:=][^A-Za-z0-9]{0,2}[A-Za-z0-9+/_=-]{16,}'
SECRET_SOFT='192\.168\.[0-9]+\.[0-9]+|10\.0\.1\.[0-9]+|walker@|ssh://git@|密码[是为：]'

while [ $# -gt 0 ]; do
    case "$1" in
        --publish) DO_PUBLISH=1 ;;
        --check-only) CHECK_ONLY=1 ;;
        --yes|-y) AUTO_YES=1 ;;
        --ref) [ $# -ge 2 ] || { echo "--ref needs a value" >&2; exit 2; }; SOURCE_REF="$2"; shift ;;
        --ref=*) SOURCE_REF="${1#*=}" ;;
        --tag) [ $# -ge 2 ] || { echo "--tag needs a value" >&2; exit 2; }; PUSH_TAG="$2"; shift ;;
        --tag=*) PUSH_TAG="${1#*=}" ;;
        --remote) [ $# -ge 2 ] || { echo "--remote needs a value" >&2; exit 2; }; PUBLIC_REMOTE="$2"; shift ;;
        --remote=*) PUBLIC_REMOTE="${1#*=}" ;;
        -h|--help) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown argument: $1 (try --help)" >&2; exit 2 ;;
    esac
    shift
done

die() { echo "FAIL: $*" >&2; exit 1; }

# --check-only is the CI/hook form of the same gate run: it may never publish,
# and it must not look like it might.
if [ "$CHECK_ONLY" = 1 ] && [ "$DO_PUBLISH" = 1 ]; then
    die "--check-only and --publish are mutually exclusive"
fi

# --- is a path private-only? -------------------------------------------------
# CLAUDE.md is listed here as well as being absent from ALLOWLIST: the allowlist
# controls what gets copied in, this decides what is allowed to survive, so a
# re-add in either place still trips gate 2.
is_denied_path() {
    case "$1" in
        analysis/*|analysis) return 0 ;;
        CLAUDE.md) return 0 ;;
        examples/test_sync.rs) return 0 ;;
        docs/*) [ "$1" = "docs/screenshot-dashboard.png" ] && return 1; return 0 ;;
        .env|.env.*|*.env) return 0 ;;
        *.pem|*.key|*.p12|id_rsa|id_ed25519) return 0 ;;
        target/*|dist/*) return 0 ;;
        *) return 1 ;;
    esac
}

# --- preflight ---------------------------------------------------------------
git remote get-url "$PUBLIC_REMOTE" >/dev/null 2>&1 \
    || die "remote '$PUBLIC_REMOTE' is not configured — refusing to guess a public URL"

# Preflight: credentials embedded in a remote URL. CLAUDE.md's privacy grep only
# scans the working tree, so an origin that carries its password inline never
# trips it — the value lives in .git/config and is never pushed, but it does leak
# into any `git remote -v` output pasted into a doc, screenshot or session log.
# Report the shape only; never echo the URL.
#
# The pattern is assembled rather than written out: `scripts/` is allowlisted, so
# this file ships, and spelling an inline-credential URL out in full (the usual
# user-at-host illustration) matched this very script's own gate-3 credential
# regex — the tool would have failed its own check the moment it got committed.
# Splitting the separator out keeps the shipped tree clean without weakening the
# scan; gate 3 has no exception list, least of all for itself.
SCHEME_SEP='://'
while read -r name url; do
    case "$url" in *"${SCHEME_SEP}"*:*@*)
        echo "note: remote '$name' embeds credentials in its URL (values not shown)" >&2 ;;
    esac
done < <(git remote -v | awk '$1!~/\(/ {print $1, $2}' | sort -u)

BASE_REF="refs/remotes/$PUBLIC_REMOTE/main"
git rev-parse --verify "$BASE_REF" >/dev/null 2>&1 \
    || die "$BASE_REF is missing — run 'git fetch $PUBLIC_REMOTE' first (this script never touches the network)"
SOURCE_REF="${SOURCE_REF:-$(git rev-parse --abbrev-ref HEAD)}"
git rev-parse --verify "$SOURCE_REF^{commit}" >/dev/null 2>&1 \
    || die "source ref '$SOURCE_REF' does not resolve to a commit"

if [ "$DO_PUBLISH" = 1 ] && [ -n "$(git status --porcelain 2>/dev/null | head -1)" ]; then
    die "checkout is dirty — commit or stash first so the published tree is reproducible"
fi

WT="$(mktemp -d "${TMPDIR:-/tmp}/tb-publish.XXXXXX")"
git worktree add --detach "$WT" "$BASE_REF" >/dev/null 2>&1 \
    || die "could not create a temporary worktree"
cleanup() { git worktree remove --force "$WT" >/dev/null 2>&1 || rm -rf "$WT"; }
trap cleanup EXIT

echo "==> source ref  : $SOURCE_REF ($(git rev-parse --short "$SOURCE_REF"))"
echo "==> public base : $BASE_REF ($(git rev-parse --short "$BASE_REF"))"
echo "==> worktree    : $WT (throwaway; your checkout is never switched)"

git -C "$WT" checkout -B public-release "$BASE_REF" >/dev/null 2>&1
for path in "${ALLOWLIST[@]}"; do
    if git -C "$WT" rev-parse --verify --quiet "$SOURCE_REF:$path" >/dev/null 2>&1; then
        git -C "$WT" checkout "$SOURCE_REF" -- "$path"
    else
        echo "    (skip $path — absent at $SOURCE_REF)"
    fi
done
# Files that exist only in private history must be dropped even if absent from
# the allowlist (they survive in older public commits until explicitly removed).
#
# `-f`, not `--cached`: the worktree is checked out from the public base, so a
# doomed path that EXISTS there sits in the working directory. `rm --cached`
# only unstages it, and the `git add -A` two lines below re-stages it from disk —
# a net no-op. That made this list silently ineffective for exactly the paths
# that needed it (CLAUDE.md was still shipping and still tripping gate 2 until
# 2026-10-01), while the paths it did work for were only working because they
# happened to be absent from the public base. The worktree is throwaway, so
# deleting the file outright is the only version that actually drops it.
for doomed in analysis examples/test_sync.rs src/tools.rs docs CLAUDE.md; do
    if git -C "$WT" ls-files --error-unmatch "$doomed" >/dev/null 2>&1; then
        case "$doomed" in
            docs) git -C "$WT" rm -r -q -f -- "$doomed" 2>/dev/null || true
                  git -C "$WT" checkout "$SOURCE_REF" -- docs/screenshot-dashboard.png 2>/dev/null || true ;;
            *) git -C "$WT" rm -r -q -f -- "$doomed" 2>/dev/null || true ;;
        esac
    fi
done
git -C "$WT" add -A -- . >/dev/null 2>&1 || true

FAILED=0

# --- gate 1: allowlist coverage (a silent skip is a stale public file) --------
echo "==> gate 1/4  every allowlisted path present at $SOURCE_REF is staged"
MISSING=""
for path in "${ALLOWLIST[@]}"; do
    if git -C "$WT" rev-parse --verify --quiet "$SOURCE_REF:$path" >/dev/null 2>&1; then
        git -C "$WT" ls-files --error-unmatch "$path" >/dev/null 2>&1 || MISSING="$MISSING$path
"
    fi
done
if [ -n "$MISSING" ]; then
    printf '%s' "$MISSING" | sed '/^$/d; s/^/    - /'
    echo "    FAIL: allowlisted path missing from the shipped tree"; FAILED=1
else
    echo "    ok"
fi

# ...and the mirror question: what does NOT ship. `docs` is the trap here — one
# allowlisted screenshot makes the whole directory look covered while every
# internal process doc is actually skipped. So classify each top-level entry:
# full / partial (name the files that do ship) / absent (does not ship).
echo "    coverage per top-level path at $SOURCE_REF:"
git -C "$WT" ls-tree --name-only "$SOURCE_REF" | while read -r top; do
    exact=0
    partial=""
    for path in "${ALLOWLIST[@]}"; do
        if [ "$path" = "$top" ]; then exact=1; break; fi
        case "$path" in "$top"/*) partial="$partial $path" ;; esac
    done
    # An `if`, not `&&` — under `set -e` a test-false on the last iteration
    # aborts the whole script through the pipeline's exit status
    # (this exact shape broke gate 2 once already).
    if [ "$exact" = 1 ]; then
        :
    elif [ -n "$partial" ]; then
        echo "      ~ $top/ ships ONLY:$partial  (the rest stays private)"
    else
        # No verdict from here: analysis/ genuinely must not ship, while
        # CHANGELOG.md and .githooks/ are only *currently* not shipped — the
        # operator decides which of the two cases applies.
        echo "      - $top does NOT ship  (intended? if not, add it to ALLOWLIST)"
    fi
done

# --- gate 2: private-only paths absent --------------------------------------
echo "==> gate 2/4  no private-only path may exist in the shipped tree"
LEAKY=""
while read -r f; do
    if is_denied_path "$f"; then LEAKY="$LEAKY$f
"; fi
done < <(git -C "$WT" ls-files)
if [ -n "$LEAKY" ]; then
    printf '%s' "$LEAKY" | sed '/^$/d; s/^/    ! /'
    echo "    FAIL: private path(s) would ship"; FAILED=1
else
    echo "    ok  (analysis/, CLAUDE.md, internal docs/ and examples/test_sync.rs are all absent)"
fi

# --- gate 3: credentials in the shipped tree (hard) --------------------------
echo "==> gate 3/4  credential scan over the shipped tree"
HITS="$(git -C "$WT" grep -l -I -E "$SECRET_HARD" -- . 2>/dev/null | head -40 || true)"
if [ -n "$HITS" ]; then
    printf '%s\n' "$HITS" | sed 's/^/    ! /'
    echo "    FAIL: credential-shaped content — clean it at the source ref, not here"; FAILED=1
else
    echo "    ok  (no private keys, provider tokens, or user:pass@host URLs)"
fi
SOFT="$(git -C "$WT" grep -l -I -E "$SECRET_SOFT" -- . 2>/dev/null | head -40 || true)"
if [ -n "$SOFT" ]; then
    printf '%s\n' "$SOFT" | sed 's/^/    ~ /'
    echo "    advisory: LAN/email literals — this repo uses these as test placeholders,"
    echo "            and github/main already carries them. Read the lines, then proceed."
fi

# --- gate 4: LAN-only identifiers (informational) ----------------------------
echo "==> gate 4/4  internal host literals (advisory)"
HOSTS="$(git -C "$WT" grep -l -I -E 'flod3|flip6|box-ubuntu|walker-mini|my-linux-server' -- . 2>/dev/null | head -20 || true)"
if [ -n "$HOSTS" ]; then
    printf '%s\n' "$HOSTS" | sed 's/^/    ~ /'
    echo "    advisory only — README screenshots/links sometimes name a host; review, no gate"
else
    echo "    none"
fi

echo "==> shipped tree: $(git -C "$WT" ls-files | wc -l | tr -d ' ') files"
if [ "$CHECK_ONLY" != 1 ]; then
    # Incoming change = index vs the public branch we would fast-forward.
    # Skipped under --check-only: the gate verdict is the whole answer there,
    # and a 12-line stat tail is what gets skimmed (and missed) in a hook log.
    git -C "$WT" diff --cached --stat HEAD | tail -12 || true
fi

if [ "$FAILED" = 1 ]; then
    die "one or more hard gates failed — nothing was committed or pushed"
fi

if [ "$CHECK_ONLY" = 1 ]; then
    echo "==> CHECK ONLY — every hard gate green, no tree printout, nothing committed or pushed"
    exit 0
fi

if [ "$DO_PUBLISH" = 0 ]; then
    cat <<EOF

==> PLAN MODE — gates 1-3 green, nothing committed, nothing pushed.
    publish with:  scripts/publish-public.sh --publish${PUSH_TAG:+ --tag $PUSH_TAG}
EOF
    exit 0
fi

# --- the irreversible part ---------------------------------------------------
MSG="${PUSH_TAG:-$(git rev-parse --short "$SOURCE_REF")}"
if [ "$AUTO_YES" != 1 ]; then
    echo
    echo "About to push branch 'public-release' → $PUBLIC_REMOTE/main"
    echo "  source      : $SOURCE_REF"
    echo "  base        : $BASE_REF"
    echo "  files       : $(git -C "$WT" ls-files | wc -l | tr -d ' ')"
    [ -n "$PUSH_TAG" ] && echo "  tag         : $PUSH_TAG"
    printf "Type YES to continue (anything else aborts): "
    read -r reply
    [ "$reply" = "YES" ] || die "aborted by operator"
fi

git -C "$WT" commit -q -m "release: publish $MSG from $SOURCE_REF" || die "nothing to commit / commit failed"
[ -n "$PUSH_TAG" ] && git -C "$WT" tag -a "$PUSH_TAG" -m "v$MSG public tree"
git -C "$WT" push "$PUBLIC_REMOTE" public-release:main || die "push failed — the tree was verified, the remote was not changed"
[ -n "$PUSH_TAG" ] && git -C "$WT" push "$PUBLIC_REMOTE" "$PUSH_TAG"

echo "==> published: $PUBLIC_REMOTE/main now at $(git -C "$WT" rev-parse --short HEAD)"
