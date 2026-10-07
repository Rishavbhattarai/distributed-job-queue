# ADR 0005: Cron schedules keyed by tick, and backpressure on queue depth

- **Status:** Accepted
- **Date:** 2026-10-07

## Part 1: cron schedules

### Context

Project 3 needs recurring jobs (nightly invoicing). A schedule must fire once per tick.
That has to hold with more than one scheduler, and after a scheduler crashes mid-tick.

### Decision

- Schedules live in a Postgres `schedules` table (`name`, 5-field UTC `cron`, `job_type`,
  `payload`, `priority`, `max_attempts`, `enabled`, `next_run_at`). The API manages them
  with `PUT /schedules/{name}` (upsert), `GET` and `DELETE`.
- **The reaper process fires them.** Each second it locks due rows with
  `SELECT … FOR UPDATE SKIP LOCKED`. For each row it inserts a job and advances
  `next_run_at`, all in one transaction, then pushes the new ids after the commit.
- **Exactly-once per tick comes from the idempotency key.** Each job gets
  `idempotency_key = "schedule:{name}:{tick}"`. If a second scheduler, or a replay after a
  crash, processes the same tick, the insert hits the unique constraint and does nothing.
  `SKIP LOCKED` keeps concurrent schedulers from blocking each other.
- **Missed ticks collapse into one run.** If the reaper was down for hours, the schedule
  fires once and skips ahead to the next future tick. It does not replay every missed
  minute. For billing-style jobs, a burst of identical catch-up runs is worse than one.

### Consequences

- The scheduler reuses the same idempotency mechanism as client enqueues, so there is
  nothing new to reason about.
- Ticks resolve to `schedule_interval` (1 s) and run in UTC. Time zones and DST rules
  would need a tz column.

## Part 2: backpressure

### Context

If producers outrun workers, the ready lists grow without bound. Redis memory then goes
first, and latency for every job grows with it.

### Decision

- `POST /jobs` checks the total length of the ready lists, using three `LLEN` calls in
  one pipeline. When it reaches `JOBQ_MAX_QUEUE_DEPTH` (default 50,000), the request gets
  **`429`** with `Retry-After: 1`. The client raises `QueueFullError`, a subclass of
  `JobqHTTPError` with `.retry_after`, so existing callers that catch `JobqHTTPError`
  still work.
- Only the *ready* depth counts. Delayed jobs and backing-off retries are not load yet.
- Schedules bypass the limit, because they are internal and low volume.
- Producers decide how to react to the 429: retry, shed load, or surface the error.

### Consequences

- The limit is checked before the insert. During overload, a duplicate request also gets
  `429` instead of its original job. Retrying it later returns the original job.
- The check costs one Redis round trip per enqueue.
- Concurrent requests can overshoot the limit slightly, because the check and the push
  are not atomic. It is a soft limit, which is enough to protect Redis.
