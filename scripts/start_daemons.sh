#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_PYTHON="$BASE_DIR/venv/bin/python3"
LOG_DIR="$BASE_DIR/logs"

mkdir -p "$LOG_DIR"

# 1. Start Relay Daemon
if [ -f "$LOG_DIR/relay.pid" ] && kill -0 "$(cat "$LOG_DIR/relay.pid")" 2>/dev/null; then
    echo "[INFO] Relay is already running (PID $(cat "$LOG_DIR/relay.pid"))"
else
    echo "[INFO] Starting PRISM Relay Daemon..."
    PYTHONPATH="$BASE_DIR" nohup "$VENV_PYTHON" "$BASE_DIR/prism_sync/relay.py" > "$LOG_DIR/relay.log" 2>&1 &
    RELAY_PID=$!
    echo "$RELAY_PID" > "$LOG_DIR/relay.pid"
    sleep 1
    if kill -0 "$RELAY_PID" 2>/dev/null; then
        echo "[SUCCESS] Relay daemon started successfully (PID $RELAY_PID)"
    else
        echo "[ERROR] Relay daemon failed to start. Check $LOG_DIR/relay.log"
        exit 1
    fi
fi

# 2. Start Worker Daemon
if [ -f "$LOG_DIR/worker.pid" ] && kill -0 "$(cat "$LOG_DIR/worker.pid")" 2>/dev/null; then
    echo "[INFO] Worker is already running (PID $(cat "$LOG_DIR/worker.pid"))"
else
    echo "[INFO] Starting PRISM Sync Worker..."
    PYTHONPATH="$BASE_DIR" nohup "$VENV_PYTHON" "$BASE_DIR/prism_sync/worker.py" > "$LOG_DIR/worker.log" 2>&1 &
    WORKER_PID=$!
    echo "$WORKER_PID" > "$LOG_DIR/worker.pid"
    sleep 1
    if kill -0 "$WORKER_PID" 2>/dev/null; then
        echo "[SUCCESS] Worker daemon started successfully (PID $WORKER_PID)"
    else
        echo "[ERROR] Worker daemon failed to start. Check $LOG_DIR/worker.log"
        exit 1
    fi
fi

echo ""
echo "Both PRISM sync daemons are running."
echo "Logs available at:"
echo "  - $LOG_DIR/relay.log"
echo "  - $LOG_DIR/worker.log"

