"""Redis broker. Redis holds only job ids and scheduling state; Postgres holds the truth.

Key layout (all under ``{prefix}:``):

* ``ready:high`` / ``ready:normal`` / ``ready:low`` -- lists of job ids, FIFO per priority.
* ``delayed`` -- sorted set, member ``"{priority}:{job_id}"``, score = run_at (unix seconds).

Week 1 semantics: ``dequeue`` is a plain BLPOP, so a job id leaves Redis the moment a
worker takes it. If that worker dies mid-job, Postgres still says ``running`` but nothing
in Redis will redeliver it.

TODO(week 2 -- leases): replace BLPOP with an atomic Lua "lease" that moves the id into an
``inflight`` sorted set scored by lease expiry; ``ack`` removes it; heartbeats extend it;
a reaper requeues expired entries. The ``dequeue``/``ack`` pair below is the seam for that.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime
from typing import cast

from redis.asyncio import Redis

from jobq.models import PRIORITIES_IN_ORDER, PriorityName

# Atomically move due members of the delayed zset onto their ready list.
_PROMOTE_DUE_LUA = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for _, member in ipairs(due) do
  redis.call('ZREM', KEYS[1], member)
  local sep = string.find(member, ':', 1, true)
  local prio = string.sub(member, 1, sep - 1)
  local job_id = string.sub(member, sep + 1)
  redis.call('RPUSH', ARGV[3] .. prio, job_id)
end
return #due
"""


class Broker:
    def __init__(self, redis: Redis, prefix: str = "jobq") -> None:
        self._redis = redis
        self._prefix = prefix
        self._promote = redis.register_script(_PROMOTE_DUE_LUA)

    def ready_key(self, priority: PriorityName) -> str:
        return f"{self._prefix}:ready:{priority}"

    @property
    def delayed_key(self) -> str:
        return f"{self._prefix}:delayed"

    async def enqueue(
        self, job_id: uuid.UUID, priority: PriorityName, run_at: datetime | None = None
    ) -> None:
        """Make a job visible to workers (now, or at ``run_at``)."""
        if run_at is not None and run_at.timestamp() > time.time():
            await self._redis.zadd(self.delayed_key, {f"{priority}:{job_id}": run_at.timestamp()})
        else:
            await self._redis.rpush(self.ready_key(priority), str(job_id))

    async def promote_due(self, now: float | None = None, limit: int = 100) -> int:
        """Move delayed jobs whose run_at has passed onto their ready lists."""
        now = time.time() if now is None else now
        moved = await self._promote(
            keys=[self.delayed_key], args=[now, limit, f"{self._prefix}:ready:"]
        )
        return int(moved)

    async def dequeue(self, timeout: float) -> uuid.UUID | None:
        """Block up to ``timeout`` seconds for the next job id, highest priority first."""
        keys = [self.ready_key(p) for p in PRIORITIES_IN_ORDER]
        # BLPOP checks keys in the given order, which gives strict priority for free.
        popped = await self._redis.blpop(keys, timeout=timeout)
        if popped is None:
            return None
        _key, raw = cast(tuple[bytes, bytes], popped)
        return uuid.UUID(raw.decode() if isinstance(raw, bytes) else raw)

    async def ack(self, job_id: uuid.UUID) -> None:
        """Tell the broker the job is finished. No-op until leases exist.

        TODO(week 2): remove ``job_id`` from the ``inflight`` zset.
        """
        return None

    async def depth(self) -> int:
        """Number of jobs waiting (ready + delayed). Future use: backpressure / metrics."""
        pipe = self._redis.pipeline()
        for p in PRIORITIES_IN_ORDER:
            pipe.llen(self.ready_key(p))
        pipe.zcard(self.delayed_key)
        counts = await pipe.execute()
        return sum(int(c) for c in counts)
