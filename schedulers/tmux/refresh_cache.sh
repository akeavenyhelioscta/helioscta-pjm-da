#!/usr/bin/env bash
# refresh_cache.sh — Manage the cache-refresh background process
#
# Usage:
#   bash refresh_cache.sh start          # start a one-shot refresh (background)
#   bash refresh_cache.sh loop [HOURS]   # start recurring refresh every HOURS (default: 2)
#   bash refresh_cache.sh logs           # tail the log file
#   bash refresh_cache.sh stop           # kill the process
#   bash refresh_cache.sh status         # show if process is running
set -euo pipefail

BACKEND_DIR="$HOME/Documents/github/helioscta-pjm-da/backend"
CONDA_ENV="helioscta-pjm-da"
INTERVAL_HOURS="${2:-1}"

PID_FILE="$BACKEND_DIR/.cache-refresh.pid"
LOG_FILE="$BACKEND_DIR/.cache-refresh.log"

usage() {
    echo "Usage: bash $0 {start|loop [HOURS]|logs|stop|status}"
    exit 1
}

is_running() {
    [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

activate_env() {
    source ~/miniconda3/etc/profile.d/conda.sh
    conda activate "$CONDA_ENV"
    cd "$BACKEND_DIR"
}

case "${1:-}" in
    start)
        if is_running; then
            echo "Already running (PID $(cat "$PID_FILE")). Use 'stop' first."
            exit 1
        fi
        activate_env

        nohup python -m src.scripts.refresh_cache --ttl 1 \
            > "$LOG_FILE" 2>&1 &
        echo $! > "$PID_FILE"

        echo "Started one-shot cache refresh (PID $(cat "$PID_FILE"))"
        echo "  logs:    bash $0 logs"
        echo "  stop:    bash $0 stop"
        ;;

    loop)
        if is_running; then
            echo "Already running (PID $(cat "$PID_FILE")). Use 'stop' first."
            exit 1
        fi
        activate_env

        nohup bash -c "
            while true; do
                echo '=== refresh started at \$(date) ==='
                python -m src.scripts.refresh_cache --ttl 1
                echo '=== done, sleeping ${INTERVAL_HOURS}h ==='
                sleep \$((${INTERVAL_HOURS} * 3600))
            done
        " > "$LOG_FILE" 2>&1 &
        echo $! > "$PID_FILE"

        echo "Started recurring refresh every ${INTERVAL_HOURS}h (PID $(cat "$PID_FILE"))"
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
            echo "Process is not running."
            rm -f "$PID_FILE"
            exit 0
        fi
        kill "$(cat "$PID_FILE")"
        rm -f "$PID_FILE"
        echo "Process stopped."
        ;;

    status)
        if is_running; then
            echo "Cache refresh is RUNNING (PID $(cat "$PID_FILE"))"
        else
            echo "Cache refresh is NOT running"
            rm -f "$PID_FILE"
        fi
        ;;

    *)
        usage
        ;;
esac
