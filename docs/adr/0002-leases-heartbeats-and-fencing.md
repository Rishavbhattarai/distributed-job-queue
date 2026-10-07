# ADR 0002: Leases in a Redis sorted set, heartbeats, and attempt-number fencing

- **Status:** Accepted
- **Date:** 2026-10-07

## Context

Week 1 used `BLPOP`. Once a worker popped a job id, Redis forgot it. A worker killed mid-job
left the job `running` in Postgres forever. We need crash recovery without running a job
twice by mistake when a worker is only slow.

## Decision

1. **Atomic lease.** One Lua script pops the highest-priority id (`LPOP` over
   `ready:high`, `ready:normal`, `ready:low`), adds it to the `inflight` sorted set with
   score `now + lease_seconds`, and stores an owner token in the `owners` hash. There is
   no instant where the id is in neither place.
2. **Redis clock.** The scripts read `TIME` inside Redis, so workers on different hosts
   agree on expiry.
3. **Heartbeats.** While the handler runs, the worker extends its lease every
   `heartbeat_interval` (default `lease_seconds / 3`). The script checks the owner token
   first, so a worker cannot extend a lease it has lost.
4. **Ack checks the owner.** `ack` removes the lease only if the token still matches.
   A slow worker cannot delete the lease of the worker that took the job over.
5. **Reaper.** Every second it lists expired leases. For each one it records the attempt
   as `lease_expired` in Postgres, then applies the retry policy (ADR 0003), then pushes
   the id again. It removes the lease last, and only if the lease is still expired. A
   crash part-way through leaves the lease expired, so the next pass repeats the step.
6. **Fencing.** `jobs.attempts` is a fencing token. A worker that claimed attempt N may
   write a result only `WHERE status = 'running' AND attempts = N`. If the reaper
   reclaimed the job, the late result is discarded and counted as `fenced`.
7. **Polling instead of blocking.** Lua scripts cannot block, so an idle worker retries
   the lease every `idle_sleep` (20 ms). This bounds idle pickup latency at about 20 ms
   and costs about 50 cheap Redis calls per second per idle worker.

## Lease length

| Lease | Recovery after `kill -9` | Risk |
|---|---|---|
| Short (5 s) | Fast | A worker stalled for longer than the lease (GC, CPU starvation) loses its job. Fencing makes this safe but wastes work. |
| Long (60 s) | Slow | Jobs from a crashed worker wait up to a minute. |

We default to 30 s with a 10 s heartbeat. A worker has to miss two heartbeats in a row
before it loses the lease. The chaos test uses 5 s to recover quickly.

## Consequences

- A `kill -9` costs one attempt and `lease_seconds` plus backoff in latency. The chaos run
  in the README shows every reclaimed job finishing.
- A handler can still run twice: the worker dies after the side effect but before the
  Postgres write, or the lease expires during a stall. Fencing stops the second result
  from overwriting state, but it cannot undo the first side effect. Handlers must be
  idempotent (ADR 0001).
- The idle polling gives up some latency (bounded by `idle_sleep`) in exchange for an
  atomic lease. `BLMOVE` would block, but it moves from only one list, so it cannot serve
  three priorities and record an expiry in one step.
