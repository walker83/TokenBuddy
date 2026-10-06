#!/usr/bin/env bash
# TokenBuddy dev restart — one command instead of six hand-run steps.
#
# Why this exists: the same "is the thing answering me the thing I just built?"
# question was re-derived by hand in 5+ sessions across 4 days (36 sessions
# matched "重启服务" over 24 days), and issue #17 / #15 / #16 turned out to be
# three reports against a stale process — a 0.6.0 binary answering while HEAD
# was already 0.7.0. `--check` now prints that verdict before anything is
# killed, so a probe can never again silently talk to an old build.
#
#   scripts/dev-restart.sh --check      report only: who owns the port, is the
#                                       binary stale, do versions agree; any of
#                                       the three being off is a failure (exit 1)
#   scripts/dev-restart.sh              restart (rebuild first if stale)
#   scripts/dev-restart.sh --rebuild     force `cargo b` even when fresh
#   scripts/dev-restart.sh --port 33940  Fleet instance instead of 8080
#   scripts/dev-restart.sh --no-verify   start without the /api/health round-trip
#
# Release builds only — debug artifacts are banned in this repo (see CLAUDE.md:
# dev intermediates once grew target/ to 16G).

set -euo pipefail
cd "$(dirname "$0")/.."

PORT=8080
MODE=restart
FORCE_REBUILD=0
VERIFY=1
BIN=target/release/tokenbuddy
LOG="${TMPDIR:-/tmp}/tokenbuddy-dev-restart.log"

while [ $# -gt 0 ]; do
    case "$1" in
        --check) MODE=check ;;
        --rebuild) FORCE_REBUILD=1 ;;
        --port) [ $# -ge 2 ] || { echo "--port needs a value" >&2; exit 2; }; PORT="$2"; shift ;;
        --port=*) PORT="${1#*=}" ;;
        --no-verify) VERIFY=0 ;;
        -h|--help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown argument: $1 (try --help)" >&2; exit 2 ;;
    esac
    shift
done

# Newest source vs the binary we would exec. `find -newer` is the whole check:
# an empty result means nothing to recompile.
is_stale() {
    [ ! -x "$BIN" ] && return 0
    [ -n "$(find src build.rs Cargo.toml -newer "$BIN" 2>/dev/null | head -1)" ]
}

running_pid() { lsof -ti ":$PORT" 2>/dev/null | head -1 || true; }

report() {
    local pid exec want have bad=0 reasons=""
    pid="$(running_pid)"
    if [ -n "$pid" ]; then
        # macOS `ps -o comm=` echoes argv[0] as invoked (`./target/release/tokenbuddy`),
        # so compare on the basename — not the whole string.
        exec="$(ps -p "$pid" -o comm= 2>/dev/null | tr -d ' ' || echo '?')"
        exec="${exec##*/}"
        echo "port $PORT  : pid $pid → ${exec:-unknown}"
        if [ "${exec:-}" != "tokenbuddy" ]; then
            # A foreign holder means /api/health on this port is not ours at all;
            # any probe result would belong to some other program.
            bad=1; reasons="${reasons}port held by '${exec:-unknown}' (not tokenbuddy); "
        fi
    else
        echo "port $PORT  : free"
    fi
    if [ -x "$BIN" ]; then
        echo "binary      : $BIN (mtime $(stat -f '%Sm' -t '%Y-%m-%d %H:%M' "$BIN" 2>/dev/null \
            || stat -c '%y' "$BIN" 2>/dev/null | cut -d. -f1))"
        # Stale source is a hard failure, not a note: this exact line printed
        # "rebuild required" while --check still exited 0, which is how a 0.7.0
        # process got to authoring a 0.7.1 claim into SKILL.md.
        if is_stale; then
            echo "source      : NEWER than binary — rebuild required"
            bad=1; reasons="${reasons}binary older than source; "
        else
            echo "source      : not newer than binary — build is current"
        fi
    else
        echo "binary      : $BIN missing — build required"
        bad=1; reasons="${reasons}no release binary built; "
    fi
    want="$(grep -m1 '^version' Cargo.toml | sed 's/version = "\(.*\)"/\1/')"
    echo "Cargo.toml  : $want"
    if [ -n "$pid" ]; then
        if curl -s --max-time 5 "http://127.0.0.1:$PORT/api/health" >/tmp/tb-health.$$.json 2>/dev/null; then
            have="$(sed -n 's/.*"version":"\([^"]*\)".*/\1/p' /tmp/tb-health.$$.json)"
            rm -f /tmp/tb-health.$$.json
            echo "serving     : ${have:-no /api/health}"
            if [ -z "$have" ]; then
                bad=1; reasons="${reasons}port held but no readable /api/health; "
            elif [ "$have" != "$want" ]; then
                bad=1; reasons="${reasons}serving $have but Cargo.toml is $want; "
            fi
        else
            echo "serving     : unreachable (not this tool, or still starting)"
            bad=1; reasons="${reasons}port held but /api/health unreachable; "
        fi
    fi
    if [ "$bad" = 1 ]; then
        echo "VERDICT     : MISMATCH — do not trust probes against this state (${reasons%; })"
        return 1
    fi
    echo "VERDICT     : OK — this checkout's build is the one answering"
    return 0
}

if [ "$MODE" = check ]; then
    report
    exit $?
fi

# --- restart path -----------------------------------------------------------
if [ "$FORCE_REBUILD" = 1 ] || is_stale; then
    echo "==> cargo build --release"
    cargo b
else
    echo "==> build is current, skipping compile (use --rebuild to force)"
fi

if pid="$(running_pid)"; [ -n "$pid" ]; then
    # Match on process NAME, not command line: `-f` + path patterns miss an
    # installed copy such as /usr/bin/tokenbuddy (CLAUDE.md gotcha).
    echo "==> stopping pid $pid on :$PORT"
    kill "$pid" 2>/dev/null || pkill -x tokenbuddy || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        sleep 0.5
        [ -z "$(running_pid)" ] && break
    done
    if [ -n "$(running_pid)" ]; then
        echo "FAIL: :$PORT still held after SIGTERM — investigate before killing -9" >&2
        exit 1
    fi
fi

echo "==> starting $BIN on 127.0.0.1:$PORT (log: $LOG)"
nohup "$BIN" --port "$PORT" --no-open >"$LOG" 2>&1 &
disown 2>/dev/null || true

if [ "$VERIFY" = 1 ]; then
    for _ in $(seq 1 40); do
        sleep 0.25
        curl -s --max-time 2 "http://127.0.0.1:$PORT/api/health" >/dev/null 2>&1 && break
    done
    if ! report; then
        echo "FAIL: started, but the self-check above did not pass" >&2
        exit 1
    fi
fi
echo "==> ready: http://127.0.0.1:$PORT"
