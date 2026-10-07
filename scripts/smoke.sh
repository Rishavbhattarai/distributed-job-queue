#!/usr/bin/env bash
# Enqueue a job through the API and wait until it succeeds. Usage: scripts/smoke.sh [base_url]
set -euo pipefail
URL="${1:-http://localhost:8000}"

for _ in $(seq 1 30); do
  curl -fsS "$URL/readyz" >/dev/null 2>&1 && break
  sleep 1
done

resp=$(curl -fsS -X POST "$URL/jobs" -H 'Content-Type: application/json' \
  -d '{"type":"sleep","payload":{"seconds":0.2}}')
id=$(echo "$resp" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
echo "enqueued job $id"

for _ in $(seq 1 30); do
  status=$(curl -fsS "$URL/jobs/$id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
  echo "status=$status"
  case "$status" in
    succeeded) echo "OK"; exit 0 ;;
    failed|dead) curl -fsS "$URL/jobs/$id"; exit 1 ;;
  esac
  sleep 1
done
echo "timed out waiting for job $id" >&2
exit 1
