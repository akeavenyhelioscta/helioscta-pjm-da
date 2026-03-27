#!/usr/bin/env bash
# fastapi.sh — Manage the FastAPI backend tmux session
#
# Usage:
#   bash fastapi.sh start              # start the server (detached)
#   bash fastapi.sh start --reload     # start with auto-reload (detached)
#   bash fastapi.sh attach             # attach to the session
#   bash fastapi.sh logs               # dump session output (without attaching)
#   bash fastapi.sh stop               # kill the session
#   bash fastapi.sh status             # show if session is running
set -euo pipefail

SESSION="fastapi"
BACKEND_DIR="$HOME/Documents/github/helioscta-pjm-da/backend"
CONDA_ENV="helioscta-pjm-da"
HOST="${FASTAPI_HOST:-0.0.0.0}"
PORT="${FASTAPI_PORT:-8000}"
RELOAD_FLAG="${2:-}"

PREAMBLE="source ~/miniconda3/etc/profile.d/conda.sh && cd '$BACKEND_DIR' && conda activate $CONDA_ENV"

if [[ "$RELOAD_FLAG" == "--reload" ]]; then
    CMD_SERVER="$PREAMBLE && uvicorn src.api.main:app --host $HOST --port $PORT --reload"
else
    CMD_SERVER="$PREAMBLE && uvicorn src.api.main:app --host $HOST --port $PORT"
fi

usage() {
    echo "Usage: bash $0 {start [--reload]|attach|logs|stop|status}"
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
        tmux new-session -d -s "$SESSION" "$CMD_SERVER"
        echo "Started FastAPI server on $HOST:$PORT in tmux session '$SESSION'"
        [[ "$RELOAD_FLAG" == "--reload" ]] && echo "  (auto-reload enabled)"
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
