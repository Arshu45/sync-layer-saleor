#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cleanup() {
    echo "[INFO] Shutting down daemons..."
    "$SCRIPT_DIR/stop_daemons.sh"
    exit 0
}

trap cleanup SIGINT SIGTERM

"$SCRIPT_DIR/start_daemons.sh"

echo "[INFO] PRISM Sync Daemons (relay & worker) are active. Monitoring loop started."

while true; do
    sleep 5
done
