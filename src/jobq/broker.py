"""Redis broker. Redis holds only job ids and scheduling state; Postgres holds the truth.

Key layout (all under ``{prefix}:``):

* ``ready:high`` / ``ready:normal`` / ``ready:low``: lists of job ids, FIFO per priority.
* ``delayed``: sorted set, member ``"{priority}:{job_id}"``, score = run_at (unix seconds).
* ``inflight``: sorted set, member ``job_id``, score = lease expiry (unix seconds).
* ``owners``: hash, ``job_id -> lease token``. Only the token holder can heartbeat or ack.

All lease operations are Lua scripts, so "pop from ready + record lease" is atomic: there is
no instant where a job id is in neither the ready list nor ``inflight``. Lease clocks use
Redis ``TIME``, so workers on different hosts agree on expiry. See docs/adr/0002.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from redis.asyncio import Redis

from jobq.models import PRIORITIES_IN_ORDER, PriorityName

_NOW = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
"""

# KEYS: ready:high, ready:normal, ready:low, inflight, owners. ARGV: lease_seconds, token.
_LEASE = (
    _NOW
    + """
for i = 1, 3 do
  local id = redis.call('LPOP', KEYS[i])
  if id then
    redis.call('ZADD', KEYS[4], now + tonumber(ARGV[1]), id)
    redis.call('HSET', KEYS[5], id, ARGV[2])
    return id
  end
end
return false
"""
)

# KEYS: inflight, owners. ARGV: job_id, lease_seconds, token.
_HEARTBEAT = (
    _NOW
    + """
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[3] then return 0 end
redis.call('ZADD', KEYS[1], 'XX', now + tonumber(ARGV[2]), ARGV[1])
return 1
"""
)

# KEYS: inflight, owners. ARGV: job_id, token.
_ACK = """
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
"""

# KEYS: inflight. ARGV: limit.
_EXPIRED = (
    _NOW
    + """
return redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
"""
)

# KEYS: inflight, owners. ARGV: job_id. Remove the lease only if it is still expired
# (a slow worker may have heartbeated, or a new worker may have leased the job again).
_DROP_IF_EXPIRED = (
    _NOW
    + """
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score and tonumber(score) <= now then
  redis.call('ZREM', KEYS[1], ARGV[1])
  redis.call('HDEL', KEYS[2], ARGV[1])
  return 1
end
return 0
"""
)

# KEYS: delayed. ARGV: limit, ready key prefix.
_PROMOTE_DUE = (
    _NOW
    + """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
for _, member in ipairs(due) do
  redis.call('ZREM', KEYS[1], member)
  local sep = string.find(member, ':', 1, true)
  redis.call('RPUSH', ARGV[2] .. string.sub(member, 1, sep - 1), string.sub(member, sep + 1))
end
return #due
"""
)


@dataclass(frozen=True)
class Lease:
    job_id: uuid.UUID
    token: str


@dataclass(frozen=True)
class QueueCounts:
    ready: dict[str, int]
    delayed: int
    inflight: int

    @property
    def ready_total(self) -> int:
        return sum(self.ready.values())


def _decode(raw: Any) -> str:
    return raw.decode() if isinstance(raw, bytes) else str(raw)


class Broker:
    def __init__(self, redis: Redis, prefix: str = "jobq") -> None:
        self._redis = redis
        self._prefix = prefix
        self._lease = redis.register_script(_LEASE)
        self._heartbeat = redis.register_script(_HEARTBEAT)
        self._ack = redis.register_script(_ACK)
        self._expired = redis.register_script(_EXPIRED)
        self._drop_if_expired = redis.register_script(_DROP_IF_EXPIRED)
        self._promote = redis.register_script(_PROMOTE_DUE)

    @property
    def redis(self) -> Redis:
        return self._redis

    def ready_key(self, priority: PriorityName) -> str:
        return f"{self._prefix}:ready:{priority}"

    @property
    def delayed_key(self) -> str:
        return f"{self._prefix}:delayed"

    @property
    def inflight_key(self) -> str:
        return f"{self._prefix}:inflight"

    @property
    def owners_key(self) -> str:
        return f"{self._prefix}:owners"

    def _ready_keys(self) -> list[str]:
        return [self.ready_key(p) for p in PRIORITIES_IN_ORDER]

    # -- producer side -----------------------------------------------------------------

    async def enqueue(
        self, job_id: uuid.UUID, priority: PriorityName, run_at: datetime | None = None
    ) -> None:
        """Make a job visible to workers now, or at ``run_at`` via the delayed set."""
        if run_at is not None:
            now = await self._redis_time()
            if run_at.timestamp() > now:
                await self._redis.zadd(
                    self.delayed_key, {f"{priority}:{job_id}": run_at.timestamp()}
                )
                return
        await self._redis.rpush(self.ready_key(priority), str(job_id))

    async def enqueue_many(self, jobs: list[tuple[uuid.UUID, PriorityName]]) -> None:
        if not jobs:
            return
        pipe = self._redis.pipeline(transaction=False)
        for job_id, priority in jobs:
            pipe.rpush(self.ready_key(priority), str(job_id))
        await pipe.execute()

    async def promote_due(self, limit: int = 1000) -> int:
        """Move delayed jobs whose run_at has passed onto their ready lists."""
        moved = await self._promote(keys=[self.delayed_key], args=[limit, f"{self._prefix}:ready:"])
        return int(moved)

    # -- worker side -------------------------------------------------------------------

    async def lease(self, lease_seconds: float, owner: str) -> Lease | None:
        """Atomically take the highest-priority ready job and record a lease on it."""
        token = f"{owner}:{uuid.uuid4().hex[:12]}"
        raw = await self._lease(
            keys=[*self._ready_keys(), self.inflight_key, self.owners_key],
            args=[lease_seconds, token],
        )
        if raw is None:
            return None
        return Lease(uuid.UUID(_decode(raw)), token)

    async def heartbeat(self, lease: Lease, lease_seconds: float) -> bool:
        """Extend the lease. False means it was lost (reaped, or re-leased elsewhere)."""
        ok = await self._heartbeat(
            keys=[self.inflight_key, self.owners_key],
            args=[str(lease.job_id), lease_seconds, lease.token],
        )
        return bool(ok)

    async def ack(self, lease: Lease) -> bool:
        """Drop the lease, if we still own it."""
        ok = await self._ack(
            keys=[self.inflight_key, self.owners_key], args=[str(lease.job_id), lease.token]
        )
        return bool(ok)

    # -- reaper side -------------------------------------------------------------------

    async def expired_leases(self, limit: int = 500) -> list[uuid.UUID]:
        raw = await self._expired(keys=[self.inflight_key], args=[limit])
        return [uuid.UUID(_decode(r)) for r in raw]

    async def drop_if_expired(self, job_id: uuid.UUID) -> bool:
        ok = await self._drop_if_expired(
            keys=[self.inflight_key, self.owners_key], args=[str(job_id)]
        )
        return bool(ok)

    async def known_job_ids(self) -> set[uuid.UUID]:
        """Every job id Redis knows about (ready, delayed, inflight), as one MULTI snapshot."""
        pipe = self._redis.pipeline(transaction=True)
        for key in self._ready_keys():
            pipe.lrange(key, 0, -1)
        pipe.zrange(self.delayed_key, 0, -1)
        pipe.zrange(self.inflight_key, 0, -1)
        *ready, delayed, inflight = await pipe.execute()
        ids: set[uuid.UUID] = set()
        for chunk in ready:
            ids.update(uuid.UUID(_decode(r)) for r in chunk)
        ids.update(uuid.UUID(_decode(m).split(":", 1)[1]) for m in delayed)
        ids.update(uuid.UUID(_decode(m)) for m in inflight)
        return ids

    # -- introspection -----------------------------------------------------------------

    async def depth(self) -> int:
        """Jobs waiting in the ready lists. Drives backpressure."""
        pipe = self._redis.pipeline(transaction=False)
        for key in self._ready_keys():
            pipe.llen(key)
        return sum(int(c) for c in await pipe.execute())

    async def counts(self) -> QueueCounts:
        pipe = self._redis.pipeline(transaction=False)
        for key in self._ready_keys():
            pipe.llen(key)
        pipe.zcard(self.delayed_key)
        pipe.zcard(self.inflight_key)
        *ready, delayed, inflight = await pipe.execute()
        return QueueCounts(
            ready={p: int(n) for p, n in zip(PRIORITIES_IN_ORDER, ready, strict=True)},
            delayed=int(delayed),
            inflight=int(inflight),
        )

    async def _redis_time(self) -> float:
        sec, usec = await self._redis.time()
        return float(sec) + float(usec) / 1_000_000
