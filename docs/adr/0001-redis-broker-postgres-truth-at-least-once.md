# ADR 0001: Redis as broker, Postgres as source of truth, at-least-once delivery with idempotent effects

- **Status:** Accepted
- **Date:** 2026-10-06
- **Deciders:** project author

## Context and problem statement

jobq has to run background jobs so that **no job is lost** and **no job's effect happens
twice**, even when a worker is `kill -9`'d mid-task. We need:

1. A fast way to hand job ids to many workers, with priorities and delays.
2. A durable, queryable record of every job, attempt, error and result.
3. A delivery guarantee we can explain and test honestly.

## Decision drivers

- Correctness under crashes matters more than raw throughput.
- Low dispatch latency at a few thousand jobs/s on a laptop.
- Everything must be free and run under `docker compose`.
- Interview clarity: the guarantee has to be easy to state and to defend.

## Considered options

**Broker**
- A. Redis lists/sorted sets (+ Lua for atomic moves), with Postgres for history
- B. Postgres only: `SELECT … FOR UPDATE SKIP LOCKED`
- C. RabbitMQ / Kafka

**Delivery guarantee**
- X. At-least-once delivery plus idempotent effects ("effectively once")
- Y. Try for exactly-once execution

## Decision

**Option A + X.**

- **Postgres is the source of truth.** `jobs` and `job_attempts` hold status, attempt
  count, errors and results. If Redis and Postgres disagree, Postgres wins.
- **Redis is only the dispatcher.** It holds job *ids*: `ready:{high,normal,low}` lists
  and a `delayed` sorted set. Leases go in an `inflight` sorted set in Week 2. You could
  rebuild all of Redis from the `jobs` table.
- **Write ordering (outbox-style):** the API commits the `jobs` row first and only then
  pushes the id to Redis. A crash between the two leaves a `queued` row that Redis doesn't
  know about. That is recoverable: a reconciler re-pushes `queued` rows it hasn't seen
  dispatched. The reverse order could leave an id in Redis with no row behind it.
- **At-least-once:** a job id may be delivered more than once (redelivery after a lease
  expires, or a duplicate push from the reconciler). Two layers protect against that:
  1. *Job-level guard:* a worker claims a job with a conditional
     `UPDATE jobs SET status='running' … WHERE id=? AND status='queued'`. A duplicate
     delivery of a job that is already running or finished matches no row and is dropped.
  2. *Effect-level idempotency:* handlers must be idempotent. They key side effects on the
     job id or a business key, so a genuine re-run (worker died after the side effect but
     before recording success) doesn't double-apply. API-level `Idempotency-Key` (a
     unique column on `jobs`) stops a client retry from creating a second job.

## Why not exactly-once execution

Exactly-once *execution* is impossible once a worker can die between "did the side effect"
and "recorded that it did". Nothing outside the handler can tell those two crash points
apart. So the system has to either risk losing the job (at-most-once) or risk running it
again (at-least-once). We pick at-least-once and make the second run harmless.

## Consequences

**Good**
- Redis gives O(1) dispatch, blocking pops (no polling) and atomic Lua scripts for leases.
- Postgres gives durable history and ad-hoc SQL over attempts and failures.
- A lost or flushed Redis is recoverable from Postgres.

**Bad / accepted trade-offs**
- Two stores to keep consistent. There is a window where a job is committed but not yet
  dispatchable. The reconciler is what closes it.
- Handler authors must write idempotent handlers. We document this in the handler API.
- Week 1 dispatch used plain `BLPOP`, so a crash mid-job stranded the job as `running`.
  [ADR 0002](0002-leases-heartbeats-and-fencing.md) replaced it with leases and a reaper.

## Revisit when

- Throughput needs exceed what one Redis primary can serve.
- We benchmark option B (`SKIP LOCKED`). If it's close enough, dropping Redis removes the
  dual-write problem entirely. That benchmark is a stretch goal.
