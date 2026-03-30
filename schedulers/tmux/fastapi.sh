#!/usr/bin/env bash
# fastapi.sh — Manage the FastAPI backend server
#
# Usage:
#   bash fastapi.sh start              # start the server (background)
#   bash fastapi.sh start --reload     # start with auto-reload (background)
#   bash fastapi.sh logs               # tail the log file
#   bash fastapi.sh stop               # kill the server
#   bash fastapi.sh status             # show if server is running
set -euo pipefail

BACKEND_DIR="$HOME/Documents/github/helioscta-pjm-da/backend"
CONDA_ENV="helioscta-pjm-da"
HOST="${FASTAPI_HOST:-0.0.0.0}"
PORT="${FASTAPI_PORT:-8000}"
RELOAD_FLAG="${2:-}"

PID_FILE="$BACKEND_DIR/.fastapi.pid"
LOG_FILE="$BACKEND_DIR/.fastapi.log"

usage() {
    echo "Usage: bash $0 {start [--reload]|logs|stop|status}"
    exit 1
}

is_running() {
    [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

case "${1:-}" in
    start)
        if is_running; then
            echo "Server already running (PID $(cat "$PID_FILE")). Use 'stop' first."
            exit 1
        fi

        RELOAD_ARG=""
        [[ "$RELOAD_FLAG" == "--reload" ]] && RELOAD_ARG="--reload"

        source ~/miniconda3/etc/profile.d/conda.sh
        conda activate "$CONDA_ENV"
        cd "$BACKEND_DIR"

        nohup uvicorn src.api.main:app --host "$HOST" --port "$PORT" $RELOAD_ARG \
            > "$LOG_FILE" 2>&1 &
        echo $! > "$PID_FILE"

        echo "Started FastAPI server on $HOST:$PORT (PID $(cat "$PID_FILE"))"
        [[ "$RELOAD_FLAG" == "--reload" ]] && echo "  (auto-reload enabled)"
        echo "  logs:    bash $0 logs"
        echo "  stop:    bash $0 stop"
        ;;

    logs)
        if [[ ! -f "$LOG_FILE" ]]; then
            echo "No log file found."
            exit 1
        fi
        tail -f "$LOG_FILE"
        ;;

    stop)
        if ! is_running; then
            echo "Server is not running."
            rm -f "$PID_FILE"
            exit 0
        fi
        kill "$(cat "$PID_FILE")"
        rm -f "$PID_FILE"
        echo "Server stopped."
        ;;

    status)
        if is_running; then
            echo "Server is RUNNING (PID $(cat "$PID_FILE"))"
        else
            echo "Server is NOT running"
            rm -f "$PID_FILE"
        fi
        ;;

    *)
        usage
        ;;
esac
