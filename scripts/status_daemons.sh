#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$BASE_DIR/logs"

check_process() {
    NAME=$1
    PID_FILE="$LOG_DIR/$NAME.pid"
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "[STATUS] $NAME is RUNNING (PID: $PID)"
        else
            echo "[STATUS] $NAME is DEAD (Stale PID file: $PID)"
        fi
    else
        echo "[STATUS] $NAME is STOPPED"
    fi
}

echo "================ PRISM Sync Layer Status ================"
check_process "relay"
check_process "worker"
echo "=========================================================="

echo ""
echo "--- Remote Ingress Health Check (http://172.27.133.76:8095/healthz) ---"
curl -s --connect-timeout 2 http://172.27.133.76:8095/healthz || echo "(Remote Ingress is not reachable on 172.27.133.76:8095)"
echo ""

echo ""
echo "--- Last 5 lines of Relay log ---"
tail -n 5 "$LOG_DIR/relay.log" 2>/dev/null || echo "(no relay.log yet)"

echo ""
echo "--- Last 5 lines of Worker log ---"
tail -n 5 "$LOG_DIR/worker.log" 2>/dev/null || echo "(no worker.log yet)"
