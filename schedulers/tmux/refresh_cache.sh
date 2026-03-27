#!/usr/bin/env bash
# refresh_cache.sh — Manage the cache-refresh tmux session
#
# Usage:
#   bash refresh_cache.sh start          # start a one-shot refresh (detached)
#   bash refresh_cache.sh loop [HOURS]   # start recurring refresh every HOURS (default: 2)
#   bash refresh_cache.sh attach         # attach to the session
#   bash refresh_cache.sh logs           # dump session output (without attaching)
#   bash refresh_cache.sh stop           # kill the session
#   bash refresh_cache.sh status         # show if session is running
set -euo pipefail

SESSION="cache-refresh"
BACKEND_DIR="$HOME/Documents/github/helioscta-pjm-da/backend"
CONDA_ENV="helioscta-pjm-da"
INTERVAL_HOURS="${2:-2}"

PREAMBLE="source ~/miniconda3/etc/profile.d/conda.sh && cd '$BACKEND_DIR' && conda activate $CONDA_ENV"
CMD_ONCE="$PREAMBLE && python -m src.scripts.refresh_cache --ttl 1"
CMD_LOOP="$PREAMBLE && while true; do echo '=== refresh started at \$(date) ==='; python -m src.scripts.refresh_cache --ttl 1; echo '=== done, sleeping ${INTERVAL_HOURS}h ==='; sleep \$((${INTERVAL_HOURS} * 3600)); done"

usage() {
    echo "Usage: bash $0 {start|loop [HOURS]|attach|logs|stop|status}"
    exit 1
}

is_running() {
    tmux has-session -t "$SESSION" 2>/dev/null
}

case "${1:-}" in
    start)
        if is_running; then
            echo "Session '$SESSION' already running. Use 'stop' first or 'attach' to view."
            exit 1
        fi
        tmux new-session -d -s "$SESSION" "$CMD_ONCE"
        echo "Started one-shot refresh in tmux session '$SESSION'"
        echo "  attach:  bash $0 attach"
        echo "  stop:    bash $0 stop"
        ;;

    loop)
        if is_running; then
            echo "Session '$SESSION' already running. Use 'stop' first or 'attach' to view."
            exit 1
        fi
        tmux new-session -d -s "$SESSION" "$CMD_LOOP"
        echo "Started recurring refresh (every ${INTERVAL_HOURS}h) in tmux session '$SESSION'"
        echo "  attach:  bash $0 attach"
        echo "  stop:    bash $0 stop"
        ;;

    attach)
        if ! is_running; then
            echo "No session '$SESSION' running."
            exit 1
        fi
        tmux attach -t "$SESSION"
        ;;

    logs)
        if ! is_running; then
            echo "No session '$SESSION' running."
            exit 1
        fi
        tmux capture-pane -t "$SESSION" -p -S -500
        ;;

    stop)
        if ! is_running; then
            echo "No session '$SESSION' running."
            exit 0
        fi
        tmux kill-session -t "$SESSION"
        echo "Stopped session '$SESSION'"
        ;;

    status)
        if is_running; then
            echo "Session '$SESSION' is RUNNING"
            tmux ls | grep "$SESSION"
        else
            echo "Session '$SESSION' is NOT running"
        fi
        ;;

    *)
        usage
        ;;
esac
