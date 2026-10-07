#!/usr/bin/env bash
# Load test at several worker counts against the docker compose stack.
#
#   loadtest/run.sh                 # workers 1 4 8, burst of 10,000 + steady 200/s for 30s
#   WORKERS="1 2" JOBS=2000 loadtest/run.sh
#
# Needs only docker. k6 runs in a container on the compose network (grafana/k6 image).
# Results: printed, and appended as markdown rows to loadtest/results.md.
set -euo pipefail
cd "$(dirname "$0")/.."

WORKERS="${WORKERS:-1 4 8}"
JOBS="${JOBS:-10000}"
RATE="${RATE:-200}"
DURATION="${DURATION:-30}"
JOB_TYPE="${JOB_TYPE:-echo}"
JOB_SECONDS="${JOB_SECONDS:-0}"
K6_IMAGE="${K6_IMAGE:-grafana/k6:1.3.0}"
NETWORK="jobq_default"
OUT="${OUT:-loadtest/results.md}"

psql() { docker compose exec -T postgres psql -U jobq -d jobq -qAt "$@"; }

k6() {
  docker run --rm -i --network "$NETWORK" \
    -e BASE=http://api:8000 -e JOB_TYPE="$JOB_TYPE" -e JOB_SECONDS="$JOB_SECONDS" "$@" \
    "$K6_IMAGE" run --quiet --summary-trend-stats "avg,p(50),p(99),max" - < loadtest/enqueue.js
}

wait_done() {  # wait until every job of a run is terminal
  local run=$1 total=$2
  for _ in $(seq 1 600); do
    done_count=$(psql -c "SELECT count(*) FROM jobs WHERE payload->>'run' = '$run' AND status IN ('succeeded','dead')")
    [ "$done_count" -ge "$total" ] && return 0
    sleep 1
  done
  echo "timed out: $done_count/$total done" >&2; return 1
}

measure() {
  docker compose exec -T postgres psql -U jobq -d jobq -qAt -F '|' -v run="$1" < loadtest/measure.sql
}

if [ ! -f "$OUT" ]; then
  {
    echo "| date | workers | test | job | jobs | succeeded | drain s | jobs/s | enqueue-to-start p50 ms | p99 ms |"
    echo "|---|---|---|---|---|---|---|---|---|---|"
  } > "$OUT"
fi

for n in $WORKERS; do
  echo "=== $n worker(s) ==="
  docker compose up -d --scale worker="$n" --wait >/dev/null 2>&1
  psql -c "TRUNCATE jobs, job_attempts CASCADE"
  sleep 2

  run="burst-w$n-$(date +%s)"
  echo "-- burst: $JOBS $JOB_TYPE jobs"
  k6 -e MODE=burst -e RUN="$run" -e JOBS="$JOBS" | grep -E "http_reqs|http_req_duration|checks" || true
  wait_done "$run" "$JOBS"
  IFS='|' read -r jobs ok drain tput p50 p99 <<< "$(measure "$run")"
  echo "burst: jobs=$jobs succeeded=$ok drain=${drain}s throughput=${tput}/s enqueue-to-start p50=${p50}ms p99=${p99}ms"
  echo "| $(date +%F) | $n | burst | $JOB_TYPE | $jobs | $ok | $drain | $tput | $p50 | $p99 |" >> "$OUT"

  run="steady-w$n-$(date +%s)"
  expected=$((RATE * DURATION))
  echo "-- steady: $RATE jobs/s for ${DURATION}s"
  k6 -e MODE=steady -e RUN="$run" -e RATE="$RATE" -e DURATION="$DURATION" | grep -E "http_reqs|http_req_duration|checks|dropped" || true
  sent=$(psql -c "SELECT count(*) FROM jobs WHERE payload->>'run' = '$run'")
  wait_done "$run" "$sent"
  IFS='|' read -r jobs ok drain tput p50 p99 <<< "$(measure "$run")"
  echo "steady: jobs=$jobs (target $expected) succeeded=$ok enqueue-to-start p50=${p50}ms p99=${p99}ms"
  echo "| $(date +%F) | $n | steady ${RATE}/s | $JOB_TYPE | $jobs | $ok | $drain | $tput | $p50 | $p99 |" >> "$OUT"
done
