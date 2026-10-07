# jobq: Distributed Job Processing System

![CI](https://github.com/OWNER/jobq/actions/workflows/ci.yml/badge.svg)
<!-- Replace OWNER/jobq with the real repo path once it is pushed. -->

A job queue where no job is lost or run twice by mistake, even when workers crash mid-task.
Redis dispatches job ids and Postgres is the source of truth. Delivery is at-least-once,
and idempotent effects make it "effectively once".

> **Status: Week 1.** The API, a single worker, the Postgres schema and the happy path
> work end to end. Retries, DLQ, leases, the reaper, metrics and load and chaos numbers
> come in later weeks. See [docs/design.md](docs/design.md) for where each one plugs in.

## Quickstart

```bash
docker compose up -d --build        # postgres, redis, migrate (one-shot), api, worker
./scripts/smoke.sh                  # enqueue a job and wait for status=succeeded

# or by hand:
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
     -d '{"type":"sleep","payload":{"seconds":0.5}}'
curl -s localhost:8000/jobs/<id>
docker compose down                 # add -v to wipe data
```

OpenAPI docs are at http://localhost:8000/docs.

## API

| Method | Path | Notes |
|---|---|---|
| `POST` | `/jobs` | Body `{type, payload?, priority?: high/normal/low, run_at?: ISO-8601 with tz, max_attempts?}`. Optional `Idempotency-Key` header. Returns `201`, or `200` with the original job when the key was already used. |
| `GET` | `/jobs/{id}` | Job status, attempts, result, last_error. `404` if unknown. |
| `GET` | `/healthz` | Liveness. |
| `GET` | `/readyz` | Checks Postgres and Redis. `503` if either is down. |

Job statuses: `queued → running → succeeded | failed` (`dead` is reserved for the DLQ).

Demo handlers: `sleep` (`{"seconds": 0.1}`), `fail_randomly` (`{"probability": 0.5}`),
`echo`.

## Client library

`jobq.client` is the **stable** interface other services use (Project 3 uses it to run
its invoice and sync jobs). The core install only depends on `httpx`:

```bash
pip install -e path/to/01-distributed-job-queue      # client only
pip install -e "path/to/01-distributed-job-queue[server]"   # + API/worker deps
```

```python
from datetime import UTC, datetime, timedelta
import jobq  # same as: from jobq.client import enqueue, get, wait

# Module-level helpers talk to $JOBQ_URL (default http://localhost:8000).
job = jobq.enqueue(
    "send_invoice",  # job_type: str
    {"invoice_id": 42},  # payload: dict | None = None
    idempotency_key="invoice-42-v1",  # keyword-only, str | None = None
    priority="high",  # "high" | "normal" | "low" = "normal"
    run_at=datetime.now(UTC) + timedelta(minutes=5),  # tz-aware datetime | None = None
)
job = jobq.get(job.id)  # job_id: UUID | str  -> Job
job = jobq.wait(job.id, timeout=30)  # poll until terminal -> Job, else TimeoutError
```

Signatures (kept stable):

```python
def enqueue(job_type: str, payload: dict | None = None, *, idempotency_key: str | None = None,
            priority: Literal["high", "normal", "low"] = "normal",
            run_at: datetime | None = None, max_attempts: int | None = None) -> Job
def get(job_id: UUID | str) -> Job
def wait(job_id: UUID | str, *, timeout: float = 30.0, poll_interval: float = 0.2) -> Job
```

- `Job` is a frozen dataclass with `id, type, payload, priority, status, attempts,
  max_attempts, run_at, idempotency_key, result, last_error, created_at, updated_at` and an
  `is_terminal` property.
- Errors: `JobqError` (base), `JobNotFoundError` (404), `JobqHTTPError` (other 4xx/5xx,
  with `.status_code` and `.body`). A naive `run_at` raises `ValueError`.
- Want an explicit base URL or connection reuse? Use
  `with JobqClient("http://jobq:8000") as c: c.enqueue(...)`. `AsyncJobqClient` has the
  same methods as coroutines.
- **Always pass an `idempotency_key` for jobs with side effects.** A retry of `enqueue`
  then returns the original job instead of creating a second one.

### Running your own handlers

The worker runs any handler registered on `jobq.handlers.registry`:

```python
# billing/jobs.py
from jobq.handlers import registry


@registry.handler("send_invoice")
async def send_invoice(payload: dict) -> dict:
    ...  # must be idempotent: delivery is at-least-once
    return {"sent": True}
```

Start a worker with `JOBQ_HANDLER_MODULES=billing.jobs jobq-worker`.

## Configuration

| Env var | Default |
|---|---|
| `JOBQ_DATABASE_URL` | `postgresql+asyncpg://jobq:jobq@localhost:5432/jobq` |
| `JOBQ_REDIS_URL` | `redis://localhost:6379/0` |
| `JOBQ_REDIS_PREFIX` | `jobq` |
| `JOBQ_WORKER_ID` | `<hostname>-<pid>` |
| `JOBQ_POLL_TIMEOUT` | `1.0` seconds |
| `JOBQ_HANDLER_MODULES` | empty (comma-separated module list) |
| `JOBQ_LOG_LEVEL` | `INFO` |
| `JOBQ_URL` (client) | `http://localhost:8000` |

## Development

```bash
uv venv --python 3.12 && uv pip install -e ".[dev]"
.venv/bin/ruff check . && .venv/bin/ruff format --check . && .venv/bin/mypy
.venv/bin/pytest                     # integration tests use testcontainers (Docker)...
JOBQ_TEST_DATABASE_URL=postgresql+asyncpg://jobq:jobq@localhost:5432/jobq \
JOBQ_TEST_REDIS_URL=redis://localhost:6379/0 .venv/bin/pytest   # ...or existing servers
```

If neither Docker nor the `JOBQ_TEST_*` vars are available, the integration tests are
skipped. Migrations live in `migrations/` (`alembic upgrade head`).

## Layout

```
src/jobq/      api.py, worker.py, broker.py, handlers.py, client.py, models.py, schemas.py
migrations/    Alembic (0001: jobs, job_attempts)
tests/         unit/ (no infra) and integration/ (real Postgres + Redis)
docs/          design.md, adr/
loadtest/      k6 scripts and results (Week 4)
chaos/         kill-a-worker scripts (Week 2/4)
```

## Results

*Load test and chaos results land here in Week 4: throughput and p50/p99
enqueue-to-start latency at 1, 4 and 8 workers, on stated hardware.*
