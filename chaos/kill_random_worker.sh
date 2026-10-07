#!/usr/bin/env bash
# Chaos test: enqueue JOBS jobs, then `docker kill -s KILL` a random worker every
# KILL_EVERY seconds until the run finishes. Each killed worker is replaced (as an
# orchestrator would). Passes only if every job ends `succeeded`: zero lost, zero dead.
#
#   chaos/kill_random_worker.sh                       # 10,000 jobs, 4 workers, kill every 10 s
#   WORKERS=8 MAX_KILLS=2 KILL_EVERY=15 chaos/kill_random_worker.sh
#
# Needs only docker. Uses a 5 s lease so crashed workers' jobs come back quickly.
set -euo pipefail
cd "$(dirname "$0")/.."

JOBS="${JOBS:-10000}"
WORKERS="${WORKERS:-4}"
KILL_EVERY="${KILL_EVERY:-10}"
MAX_KILLS="${MAX_KILLS:-1000}"
JOB_SECONDS="${JOB_SECONDS:-0.05}"
TIMEOUT="${TIMEOUT:-900}"
K6_IMAGE="${K6_IMAGE:-grafana/k6:1.3.0}"
export JOBQ_LEASE_SECONDS="${JOBQ_LEASE_SECONDS:-5}"

psql() { docker compose exec -T postgres psql -U jobq -d jobq -qAt "$@"; }
count() { psql -c "SELECT count(*) FROM jobs WHERE payload->>'run' = '$RUN' $1"; }

echo "starting stack: $WORKERS workers, lease ${JOBQ_LEASE_SECONDS}s"
docker compose up -d --build --scale worker="$WORKERS" --wait >/dev/null 2>&1

RUN="chaos-$(date +%s)"
echo "run=$RUN: enqueueing $JOBS sleep(${JOB_SECONDS}s) jobs"
start=$(date +%s)
docker run --rm -i --network jobq_default -e BASE=http://api:8000 -e MODE=burst \
  -e RUN="$RUN" -e JOBS="$JOBS" -e JOB_TYPE=sleep -e JOB_SECONDS="$JOB_SECONDS" \
  "$K6_IMAGE" run --quiet - < loadtest/enqueue.js >/dev/null

created=$(count "")
echo "created $created jobs"
[ "$created" -eq "$JOBS" ] || { echo "FAIL: enqueue created $created, expected $JOBS"; exit 1; }

kills=0
next_kill=$(( $(date +%s) + KILL_EVERY ))
while true; do
  now=$(date +%s)
  ok=$(count "AND status = 'succeeded'")
  dead=$(count "AND status = 'dead'")
  running=$(count "AND status = 'running'")
  expired=$(psql -c "SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id
                     WHERE j.payload->>'run' = '$RUN' AND a.outcome = 'lease_expired'")
  printf '[%4ss] succeeded=%-6s running=%-3s dead=%-3s kills=%-3s leases_expired=%s\n' \
    "$((now - start))" "$ok" "$running" "$dead" "$kills" "$expired"
  [ $((ok + dead)) -ge "$JOBS" ] && break
  if [ $((now - start)) -gt "$TIMEOUT" ]; then echo "FAIL: timed out"; exit 1; fi

  if [ "$now" -ge "$next_kill" ] && [ "$kills" -lt "$MAX_KILLS" ]; then
    victims=($(docker compose ps -q worker))
    victim=${victims[$((RANDOM % ${#victims[@]}))]}
    echo "        kill -9 worker $(docker inspect -f '{{.Name}}' "$victim" | tr -d /)"
    docker kill -s KILL "$victim" >/dev/null
    kills=$((kills + 1))
    # Replace it, as an orchestrator would.
    docker compose up -d --scale worker="$WORKERS" --no-recreate >/dev/null 2>&1
    next_kill=$((now + KILL_EVERY))
  fi
  sleep 1
done

elapsed=$(( $(date +%s) - start ))
multi=$(count "AND attempts > 1")
dup_success=$(psql -c "SELECT count(*) FROM (SELECT a.job_id FROM job_attempts a JOIN jobs j ON j.id = a.job_id
                       WHERE j.payload->>'run' = '$RUN' AND a.outcome = 'succeeded'
                       GROUP BY a.job_id HAVING count(*) > 1) d")
echo
echo "result: $ok/$JOBS succeeded, $dead dead, lost=$((JOBS - ok - dead)), $kills workers killed,"
echo "        $expired leases reclaimed, $multi jobs needed >1 attempt,"
echo "        $dup_success jobs recorded more than one successful attempt, ${elapsed}s total"
if [ "$ok" -eq "$JOBS" ] && [ "$dead" -eq 0 ] && [ "$dup_success" -eq 0 ]; then
  echo "PASS: zero lost jobs"
else
  echo "FAIL"; exit 1
fi
