#!/usr/bin/env bash
# 60-second demo for a screen recording: 10,000 jobs on 8 workers, kill -9 two workers
# mid-run, watch leases expire and the count still reach 10,000.
#
# Open Grafana first (http://localhost:${JOBQ_GRAFANA_PORT:-3000}, dashboard "jobq"),
# put it next to this terminal, then run: chaos/demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."
echo "Grafana: http://localhost:${JOBQ_GRAFANA_PORT:-3000}/d/jobq"
WORKERS=8 MAX_KILLS=2 KILL_EVERY=15 JOB_SECONDS=0.04 JOBS=10000 exec chaos/kill_random_worker.sh
