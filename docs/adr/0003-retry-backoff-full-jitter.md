# ADR 0003: Retries with exponential backoff and full jitter, then the dead-letter queue

- **Status:** Accepted
- **Date:** 2026-10-07

## Context

Handlers fail for transient reasons (timeouts, a dependency restarting) and for permanent
ones (bad input). Retrying immediately can make an outage worse. Never retrying turns every
blip into manual work.

## Options

- **Fixed delay.** Simple, but jobs that failed together retry together.
- **Exponential backoff.** Spreads retries over time, but jobs that failed together still
  retry in synchronized waves.
- **Full jitter:** `delay = uniform(0, min(cap, base * 2^(attempt-1)))`.
- **Decorrelated jitter:** `delay = min(cap, uniform(base, prev * 3))`. It has a similar
  spread, but each job has to store its previous delay.

## Decision

- **Full jitter**, with `base = 1 s` and `cap = 300 s` (both configurable). It needs no
  per-job state beyond the attempt count we already store, and it spreads a burst of
  failures evenly across the window.
- **Attempt budget.** Each job has `max_attempts`. If the request doesn't set it, it comes
  from `JOBQ_MAX_ATTEMPTS_BY_TYPE` (for example `send_invoice=10`), then the global
  default of 3. The API resolves it at enqueue, so the worker and the reaper read the same
  number from the row.
- **The DLQ is a status.** A job out of attempts becomes `dead`. There is no separate
  queue, so the dead job keeps its history, attempts and last error in one place.
  - `GET /dlq` lists dead jobs.
  - `POST /dlq/{id}/replay` (and `POST /jobs/{id}/retry`) requeues one with
    `extra_attempts` more tries.
  - `attempts` never goes down, because it numbers attempt rows and fences workers
    (ADR 0002). A replay raises `max_attempts` instead.
- **Permanent failures skip the remaining retries.** A handler raises `PermanentError`.
  A job type with no handler in the worker counts as permanent too, and a replay brings
  it back once the handler is deployed.
- **The delay lives in Redis.** A retry sets `status = queued` and `run_at = now + delay`
  in Postgres first, then adds the id to the `delayed` sorted set. The reaper moves it to
  its ready list when it is due.

## Consequences

- Retries after a shared outage spread out instead of arriving as one wave.
- `last_error` and one `job_attempts` row per attempt give a full failure history for
  each DLQ entry.
- Recovery after an outage is slower. With a 300 s cap, the late attempts of a job can
  wait up to five minutes.
