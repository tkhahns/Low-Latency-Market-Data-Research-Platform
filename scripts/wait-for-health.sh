#!/usr/bin/env bash
# Usage: wait-for-health.sh <url> [timeout_seconds]
set -euo pipefail
URL=${1:?Usage: wait-for-health.sh <url> [timeout_seconds]}
TIMEOUT=${2:-120}
INTERVAL=2
elapsed=0
echo "Waiting for $URL (timeout=${TIMEOUT}s)..."
while true; do
  if curl -fsS "$URL" >/dev/null 2>&1; then
    echo "Health check passed after ${elapsed}s"
    exit 0
  fi
  if [ "$elapsed" -ge "$TIMEOUT" ]; then
    echo "Timed out after ${TIMEOUT}s waiting for $URL" >&2
    exit 1
  fi
  sleep "$INTERVAL"
  elapsed=$((elapsed + INTERVAL))
done
