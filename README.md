# jobq: Distributed Job Queue

[![CI](https://github.com/Rishavbhattarai/distributed-job-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Rishavbhattarai/distributed-job-queue/actions/workflows/ci.yml)

A job queue in Python where a job is never lost and never runs twice by mistake, even when a worker crashes mid-task. Redis dispatches job IDs and PostgreSQL holds the record of every job and attempt. Delivery is at-least-once, and idempotency keys make the effect happen once.

**Stack:** Python 3.12, FastAPI, Redis 7, PostgreSQL 16, SQLAlchemy (async), Alembic, Prometheus, Grafana, k6, Docker Compose, GitHub Actions, pytest, mypy (strict), Ruff

## Highlights

- Survives `kill -9`: in a 10,000-job run where a random worker was killed every 10 seconds (13 kills), all 10,000 jobs succeeded and none ran to success twice ([Results](#results)).
- Leases jobs with an atomic Lua script, so a job ID is always in a ready list or in the in-flight set, never in neither. Workers heartbeat while a job runs, and a reaper requeues jobs whose lease expired.
- Fences stale workers: a worker may only record attempt N while the job is still at attempt N. A worker whose lease expired cannot overwrite the result of the worker that took over.
- Retries failures with exponential backoff and full jitter, then moves the job to a dead-letter queue that can be inspected and replayed over the API.
- Rejects duplicate submissions with an `Idempotency-Key` header and a unique constraint in Postgres. In the test, 1,000 keys each sent twice at the same time created exactly 1,000 jobs.
- A reconciler re-pushes jobs that Postgres has but Redis lost (a crash between the two writes, or Redis data loss).
- Runs cron schedules once per tick, even with several schedulers, by deriving each job's idempotency key from the schedule name and tick time.
- Returns `429` with `Retry-After` when the ready queue passes a depth limit.
- Finishes or releases the in-flight job on `SIGTERM`, and logs structured JSON with the job ID on every line.
- Exports Prometheus metrics from the API, the workers and the reaper, with a Grafana dashboard provisioned from JSON in the repo.
- Ships a typed Python client (sync and async) that a second project, a [usage billing pipeline](https://github.com/Rishavbhattarai/usage-metering-billing), uses to run its invoice jobs.
- 63 tests (37 unit, 26 integration against real Postgres and Redis). CI runs lint, strict type checks and tests on every push, then starts the full stack with Docker Compose and runs a smoke test and a short chaos test.

## Architecture

```mermaid
flowchart LR
  C[Client] -->|POST /jobs + Idempotency-Key| API[FastAPI]
  API -->|1. insert job| PG[(PostgreSQL)]
  API -->|2. push job ID| R[(Redis)]
  W[Workers] -->|lease, heartbeat, ack| R
  W -->|claim, attempts, results| PG
  RP[Reaper] -->|expired leases, delayed jobs| R
  RP -->|retry or dead-letter, cron, reconcile| PG
  API & W & RP -->|/metrics| P[Prometheus] --> G[Grafana]
```

The API writes to Postgres first, then pushes the job ID to Redis. Every later state change follows the same order. Any crash between the two writes leaves a job that Postgres knows about and Redis does not; the reaper and reconciler find it and push it again, and the worker's conditional claim drops any duplicate.

| ADR | Decision |
|---|---|
| [0001](docs/adr/0001-redis-broker-postgres-truth-at-least-once.md) | Redis as broker, Postgres as source of truth, at-least-once plus idempotency |
| [0002](docs/adr/0002-leases-heartbeats-and-fencing.md) | Leases in a Redis sorted set, heartbeats, attempt-number fencing, lease length |
| [0003](docs/adr/0003-retry-backoff-full-jitter.md) | Exponential backoff with full jitter, the DLQ as a job status |
| [0004](docs/adr/0004-state-ownership-and-reconciler.md) | Where state lives, the windows where the stores disagree, the reconciler |
| [0005](docs/adr/0005-cron-schedules-and-backpressure.md) | Cron schedules keyed by tick, backpressure on queue depth |

More detail is in [docs/design.md](docs/design.md).

## Quickstart

```bash
docker compose up -d --build        # postgres, redis, migrations, api, worker, reaper, prometheus, grafana
./scripts/smoke.sh                  # enqueue a job and wait for status=succeeded

curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
     -d '{"type":"sleep","payload":{"seconds":0.5}}'
curl -s localhost:8000/jobs/<id>

docker compose up -d --scale worker=4   # more workers
docker compose down                     # add -v to delete the data
```

- API docs: http://localhost:8000/docs
- Grafana dashboard (no login): http://localhost:3000/d/jobq
- Prometheus: http://localhost:9090

Host ports are configurable, so the stack can run next to other projects: `JOBQ_API_PORT`, `JOBQ_POSTGRES_PORT`, `JOBQ_REDIS_PORT`, `JOBQ_PROMETHEUS_PORT`, `JOBQ_GRAFANA_PORT`. For example: `JOBQ_API_PORT=18000 docker compose up -d`, then `./scripts/smoke.sh http://localhost:18000`.

## API

| Method | Path | Notes |
|---|---|---|
| `POST` | `/jobs` | Body: `{type, payload?, priority?, run_at?, max_attempts?}`. Optional `Idempotency-Key` header. Returns `201`, or `200` with the original job if the key was used before. `409` if the key was used with a different type or payload. `429` with `Retry-After` when the queue is full. |
| `GET` | `/jobs/{id}` | Status, attempts, result and last error. `404` if unknown. |
| `POST` | `/jobs/{id}/retry` | Requeue a dead job now. Optional body `{extra_attempts}` (default 3). |
| `GET` | `/dlq` | Dead jobs, newest first. Query: `limit`, `offset`, `type`. Returns `{items, total}`. |
| `POST` | `/dlq/{id}/replay` | Same as retry. `409` if the job is not dead. |
| `PUT` | `/schedules/{name}` | Create or replace a cron schedule: `{cron, job_type, payload?, priority?, max_attempts?, enabled?}`. 5-field cron, UTC. |
| `GET` | `/schedules`, `/schedules/{name}` | List or read schedules, with `next_run_at`. |
| `DELETE` | `/schedules/{name}` | Delete a schedule. |
| `GET` | `/metrics` | Prometheus metrics. |
| `GET` | `/healthz` | Liveness. |
| `GET` | `/readyz` | Checks Postgres and Redis. `503` if either is down. |

Job status: `queued → running → succeeded`, or back to `queued` with a backoff delay after a failure, or `dead` (the dead-letter queue) after `max_attempts` attempts or a permanent error. Built-in demo handlers: `sleep`, `fail_randomly`, `echo`.

## Client library

```bash
pip install -e path/to/distributed-job-queue            # client only (depends on httpx)
pip install -e "path/to/distributed-job-queue[server]"  # plus API, worker and reaper
```

```python
from datetime import UTC, datetime, timedelta
import jobq

job = jobq.enqueue(
    "send_invoice",
    {"invoice_id": 42},
    idempotency_key="invoice-42-v1",
    priority="high",
    run_at=datetime.now(UTC) + timedelta(minutes=5),
)
job = jobq.wait(job.id, timeout=30)  # poll until the job finishes
```

```python
def enqueue(job_type: str, payload: dict | None = None, *, idempotency_key: str | None = None,
            priority: Literal["high", "normal", "low"] = "normal",
            run_at: datetime | None = None, max_attempts: int | None = None) -> Job
def get(job_id: UUID | str) -> Job
def wait(job_id: UUID | str, *, timeout: float = 30.0, poll_interval: float = 0.2) -> Job
```

- The module-level functions call `$JOBQ_URL` (default `http://localhost:8000`). Use `JobqClient(base_url)` or `AsyncJobqClient(base_url)` to set the URL or reuse connections.
- `Job` is a frozen dataclass with an `is_terminal` property (`succeeded`, `failed` or `dead`).
- Errors: `JobqError` (base), `JobNotFoundError` (404), `JobqHTTPError` (other 4xx/5xx, with `.status_code` and `.body`), and `QueueFullError` (429, a `JobqHTTPError` with `.retry_after`). A `run_at` without a timezone raises `ValueError`.
- Pass an `idempotency_key` for any job with side effects, so a retried `enqueue` returns the original job.
- If `max_attempts` is omitted, the server uses `JOBQ_MAX_ATTEMPTS_BY_TYPE` for that job type, then 3.

### Custom handlers

```python
# billing/jobs.py
from jobq.handlers import PermanentError, registry


@registry.handler("send_invoice")
async def send_invoice(payload: dict) -> dict:
    if "invoice_id" not in payload:
        raise PermanentError("missing invoice_id")  # no retries, straight to the DLQ
    ...  # must be idempotent: delivery is at-least-once
    return {"sent": True}
```

Run a worker with them: `JOBQ_HANDLER_MODULES=billing.jobs jobq-worker`. Any other exception is retried with backoff.

## Configuration

| Variable | Default |
|---|---|
| `JOBQ_DATABASE_URL` | `postgresql+asyncpg://jobq:jobq@localhost:5432/jobq` |
| `JOBQ_REDIS_URL` | `redis://localhost:6379/0` |
| `JOBQ_REDIS_PREFIX` | `jobq` |
| `JOBQ_WORKER_ID` | `<hostname>-<pid>` |
| `JOBQ_HANDLER_MODULES` | empty (comma-separated modules) |
| `JOBQ_LEASE_SECONDS` | `30` |
| `JOBQ_HEARTBEAT_INTERVAL` | lease / 3 |
| `JOBQ_DEFAULT_MAX_ATTEMPTS` | `3` |
| `JOBQ_MAX_ATTEMPTS_BY_TYPE` | empty, e.g. `send_invoice=10,sync=5` |
| `JOBQ_BACKOFF_BASE` / `JOBQ_BACKOFF_CAP` | `1` / `300` seconds |
| `JOBQ_SHUTDOWN_GRACE` | `25` seconds |
| `JOBQ_MAX_QUEUE_DEPTH` | `50000` (`0` turns backpressure off) |
| `JOBQ_RECONCILE_INTERVAL` / `JOBQ_RECONCILE_GRACE` | `30` / `30` seconds |
| `JOBQ_IDLE_SLEEP` | `0.02` seconds between empty lease attempts |
| `JOBQ_METRICS_PORT` | `0` (off); compose sets 9100 for workers and 9101 for the reaper |
| `JOBQ_LOG_LEVEL` | `INFO` |
| `JOBQ_URL` (client) | `http://localhost:8000` |

## Development

```bash
uv venv --python 3.12 && uv pip install -e ".[dev]"
.venv/bin/ruff check . && .venv/bin/ruff format --check . && .venv/bin/mypy
.venv/bin/pytest
```

Integration tests start Postgres and Redis with testcontainers when Docker is running. To use existing servers instead, set `JOBQ_TEST_DATABASE_URL` and `JOBQ_TEST_REDIS_URL`. Without either, the integration tests are skipped. Migrations: `alembic upgrade head`.

```
src/jobq/      api, worker, reaper, broker (Redis + Lua), lifecycle (Postgres transitions),
               backoff, cron, metrics, handlers, client, models, schemas
migrations/    Alembic (jobs, job_attempts, schedules)
tests/         unit/ and integration/
docs/          design.md, adr/
ops/           Prometheus config, Grafana provisioning and dashboard JSON
loadtest/      k6 script, run.sh, measure.sql, results.md
chaos/         kill_random_worker.sh, demo.sh
```

## Roadmap

| Stage | Scope | Status |
|---|---|---|
| 1 | API, worker, Postgres schema, idempotent enqueue, priorities, delayed jobs | Done |
| 2 | Retries with exponential backoff and full jitter, dead-letter queue with inspect and replay, leases with heartbeats, reaper, reconciler | Done |
| 3 | Duplicate-enqueue test, cron schedules, graceful shutdown, backpressure (`429`) | Done |
| 4 | Prometheus and Grafana dashboard, load test, chaos test | Done |
| Stretch | Job dependencies, per-tenant rate limits, Postgres `SKIP LOCKED` broker benchmark | Not started |

## Results

**Hardware:** Apple M4, 16 GB RAM. Docker Desktop VM with 10 CPUs and 7.75 GiB. Every container (API with 4 processes, workers, reaper, Postgres, Redis, k6) ran in that one VM. Other projects' containers were running on the same machine during the measurements, and the host load average ranged from 5 to 24, so the numbers vary between runs. Each cell is the median of three runs, with the range in brackets. Raw rows are in [loadtest/results.md](loadtest/results.md).

**Load test** ([loadtest/run.sh](loadtest/run.sh)). Job type `echo`, which does no work, so the numbers measure the queue's own overhead.
- **Burst:** k6 posts 10,000 jobs with 50 virtual users. Throughput is jobs divided by the time from the first attempt starting to the last one finishing.
- **Steady:** k6 posts 200 jobs/s for 30 s. Enqueue-to-start is the time from the job row being created to its first attempt starting, both read from Postgres clocks.

| Workers | Burst throughput (jobs/s) | Steady 200/s: enqueue-to-start p50 | p99 |
|---|---|---|---|
| 1 | 376 [351 to 450] | 18.5 ms [11.9 to 47.7] | 775 ms [25 to 1,174] |
| 4 | 659 [554 to 996] | 10.4 ms [8.4 to 12.3] | 628 ms [44 to 3,378] |
| 8 | 738 [702 to 1,047] | 6.7 ms [5.9 to 8.0] | 52 ms [39 to 149] |

What the numbers show:
- Throughput grows with workers, but not linearly. Each job costs 4 round trips (lease, claim, complete, ack) and 2 Postgres commits. At 8 workers, Postgres showed `WALWrite` and `WALSync` waits under load, so commit throughput is the next limit.
- The single-process API was the first bottleneck: one uvicorn process sat at 99% CPU at about 1,300 enqueues/s. Compose now runs 4 API processes, with Prometheus multiprocess mode aggregating their counters.
- I also dropped `pool_pre_ping` (an extra round trip on every connection checkout) and now run the single-statement claim, complete and insert in autocommit, without BEGIN/COMMIT round trips. The runs before and after this change were taken under different host load, so I don't claim a measured speedup from it.
- The p99 swings between runs track the host load from other workloads more than the worker count. In burst runs, enqueue-to-start mostly measures waiting in the backlog, so the table leaves it out. The raw rows include it.

**Chaos test** ([chaos/kill_random_worker.sh](chaos/kill_random_worker.sh)): 10,000 `sleep(50 ms)` jobs on 4 workers, `docker kill -s KILL` on a random worker every 10 s, killed workers replaced, 5 s lease.

| Jobs | Workers killed | Leases reclaimed | Succeeded | Dead | Lost | Jobs with two successful attempts | Run time |
|---|---|---|---|---|---|---|---|
| 10,000 | 13 | 13 | 10,000 | 0 | 0 | 0 | 151 s |

Each killed worker left one job leased. The reaper reclaimed it after the lease expired and another worker finished it.

**Demo:** `chaos/demo.sh` runs 10,000 jobs on 8 workers and kills 2 of them mid-run. It finished in 60 s with 10,000/10,000 succeeded and 2 leases reclaimed. Open the Grafana dashboard next to the terminal to watch workers drop out, leases expire and the succeeded count reach 10,000.
