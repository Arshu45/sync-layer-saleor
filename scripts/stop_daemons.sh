#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$BASE_DIR/logs"

stop_process() {
    NAME=$1
    PID_FILE="$LOG_DIR/$NAME.pid"
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "[INFO] Stopping $NAME (PID $PID)..."
            kill -TERM "$PID" 2>/dev/null || true
            for i in {1..5}; do
                if kill -0 "$PID" 2>/dev/null; then
                    sleep 1
                else
                    break
                fi
            done
            if kill -0 "$PID" 2>/dev/null; then
                echo "[WARNING] $NAME did not stop gracefully, forcing SIGKILL..."
                kill -9 "$PID" 2>/dev/null || true
            fi
            echo "[SUCCESS] $NAME stopped."
        else
            echo "[INFO] $NAME is not running."
        fi
        rm -f "$PID_FILE"
    else
        echo "[INFO] No PID file found for $NAME."
    fi
}

stop_process "relay"
stop_process "worker"

