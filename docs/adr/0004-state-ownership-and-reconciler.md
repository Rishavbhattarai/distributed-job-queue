# ADR 0004: Where state lives, and the reconciler that closes the gap

- **Status:** Accepted
- **Date:** 2026-10-07

## Context

ADR 0001 made Postgres the source of truth and Redis the dispatcher. Each state change
writes Postgres first, then Redis. The gap between the two writes is where jobs can get
stuck.

## The windows where the stores disagree

| Crash point | Postgres | Redis | Who repairs it |
|---|---|---|---|
| API committed the row, push not done | `queued` | missing | reconciler |
| Worker committed a retry, push not done | `queued` (future `run_at`) | lease still in `inflight` | reaper: the lease expires, Postgres says `queued`, so it pushes again |
| Worker committed success, ack not done | `succeeded` | lease in `inflight` | reaper drops the lease |
| Reaper requeued the job, push not done | `queued` | expired lease still present | next reaper pass |
| Redis lost data (no AOF, restore) | `queued` or `running` | missing | reconciler |

## Decision

- **Reconciler.** It runs in the reaper process at startup and then every 30 s:
  1. Takes one `MULTI` snapshot of every id Redis knows (ready lists, delayed set,
     inflight set).
  2. Selects jobs that are `queued` and were last updated more than `reconcile_grace`
     (30 s) ago, plus jobs that are `running` and older than `lease_seconds + grace`.
  3. Pushes queued jobs that are missing from the snapshot. Running jobs with no lease
     are handled like expired leases.
- **The grace period** covers the normal gap between a commit and its push. Without it,
  the reconciler would race the API.
- **Duplicates are allowed, and the claim drops them.** A job pushed twice is claimed once,
  because the worker's claim is `UPDATE … WHERE status = 'queued'`. So no step has to be
  exactly-once. Steps only need to happen at least once, and that is easy to get under
  crashes.
- **Cron ticks get the same treatment** (ADR 0005). The job row commits before the push,
  so the reconciler covers a crash between them.

## Consequences

- Every crash point in the table repairs itself within one reconcile or reap interval.
  The integration tests cover the API, reaper and "Redis lost the lease" cases.
- The reconciler reads every id in Redis on each pass. That is fine at tens of thousands
  of queued jobs. At 10x scale it would need an index instead, such as a Redis set of
  queued ids or a Postgres outbox table drained by a relay.
- Moving dispatch to Postgres (`SELECT … FOR UPDATE SKIP LOCKED`) would remove these
  windows entirely, at the cost of polling Postgres. That is the stretch-goal benchmark
  from ADR 0001.
