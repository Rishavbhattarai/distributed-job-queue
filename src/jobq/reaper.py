"""The reaper process: everything that keeps Redis and Postgres honest without a client.

Loops (each on its own interval):

* **promote**: move due ids from the ``delayed`` set to the ready lists (retries, run_at).
* **reap**: find expired leases (a worker crashed or stalled), record the attempt as
  ``lease_expired``, and retry the job with backoff or dead-letter it.
* **schedule**: enqueue one job per due cron schedule tick.
* **reconcile**: find jobs Postgres says are queued (or orphaned in running) that Redis does
  not know about, and push them again. Covers a crash between the Postgres commit and the
  Redis push, and a Redis that lost data.
* **gauges**: queue depth, delayed, in-flight and DLQ size for Prometheus.

Every step is safe to repeat and safe to run in more than one reaper: Postgres updates are
conditional and duplicate pushes are dropped by the worker's claim. See docs/adr/0004.

Run with ``jobq-reaper``.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import structlog
from redis.asyncio import Redis
from sqlalchemy import and_, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobq import cron, lifecycle, metrics
from jobq.broker import Broker
from jobq.config import Settings
from jobq.db import make_engine, make_session_factory
from jobq.lifecycle import RetryPolicy
from jobq.logs import configure_logging
from jobq.models import PRIORITY_NAMES, AttemptOutcome, Job, JobStatus, PriorityName, Schedule
from jobq.worker import install_stop_signals

log = structlog.get_logger("jobq.reaper")

LEASE_EXPIRED_ERROR = "lease expired: worker crashed or stopped heartbeating"


class Reaper:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: Broker,
        settings: Settings,
    ) -> None:
        self._sessions = session_factory
        self._broker = broker
        self._settings = settings
        self._policy = RetryPolicy(settings.backoff_base, settings.backoff_cap)

    async def promote_once(self) -> int:
        return await self._broker.promote_due()

    async def reap_once(self) -> int:
        """Reclaim jobs whose lease expired. Returns how many leases were reclaimed."""
        reclaimed = 0
        for job_id in await self._broker.expired_leases():
            if await self._reclaim(job_id):
                reclaimed += 1
            await self._broker.drop_if_expired(job_id)
        return reclaimed

    async def _reclaim(self, job_id: uuid.UUID) -> bool:
        async with self._sessions() as session, session.begin():
            job = await session.get(Job, job_id)
            if job is None:
                return False
            if job.status == JobStatus.QUEUED.value:
                # Already requeued in Postgres but the Redis push may not have happened
                # (crash between the two). Push again; duplicates are harmless.
                priority = PRIORITY_NAMES[job.priority]
                run_at: datetime | None = job.run_at
                transition = None
            elif job.status == JobStatus.RUNNING.value:
                transition = await lifecycle.fail_attempt(
                    session,
                    job_id,
                    job.attempts,
                    error=LEASE_EXPIRED_ERROR,
                    outcome=AttemptOutcome.LEASE_EXPIRED,
                    policy=self._policy,
                )
                if transition is None:
                    return False
                priority, run_at = transition.priority, transition.run_at
            else:
                return False  # finished; the worker died between commit and ack

        if transition is not None:
            metrics.LEASES_EXPIRED.inc()
            log.warning(
                "job.lease_expired",
                job_id=str(job_id),
                attempt=transition.attempts,
                new_status=transition.status.value,
            )
            if transition.status is JobStatus.DEAD:
                return True
        await self._broker.enqueue(job_id, priority, run_at)
        return transition is not None

    async def schedule_once(self) -> int:
        """Enqueue one job per due schedule tick. Idempotency keys make ticks exactly-once."""
        now = datetime.now(UTC)
        to_push: list[tuple[uuid.UUID, PriorityName]] = []
        async with self._sessions() as session, session.begin():
            due = (
                await session.execute(
                    select(Schedule)
                    .where(Schedule.enabled.is_(True), Schedule.next_run_at <= now)
                    .order_by(Schedule.next_run_at)
                    .limit(100)
                    .with_for_update(skip_locked=True)
                )
            ).scalars()
            for sch in due:
                tick = sch.next_run_at.astimezone(UTC)
                new_id = (
                    await session.execute(
                        pg_insert(Job)
                        .values(
                            id=uuid.uuid4(),
                            type=sch.job_type,
                            payload=sch.payload,
                            priority=sch.priority,
                            status=JobStatus.QUEUED.value,
                            attempts=0,
                            max_attempts=sch.max_attempts
                            or self._settings.max_attempts_for(sch.job_type),
                            run_at=now,
                            idempotency_key=f"schedule:{sch.name}:{tick:%Y-%m-%dT%H:%M:%SZ}",
                        )
                        .on_conflict_do_nothing(index_elements=["idempotency_key"])
                        .returning(Job.id)
                    )
                ).scalar_one_or_none()
                if new_id is not None:
                    to_push.append((new_id, PRIORITY_NAMES[sch.priority]))
                # Missed ticks (reaper was down) collapse into one run; skip to the future.
                sch.next_run_at = cron.next_fire(sch.cron, max(now, tick))
                sch.last_enqueued_at = now
                sch.updated_at = now
        await self._broker.enqueue_many(to_push)
        for job_id, _ in to_push:
            metrics.SCHEDULED.inc()
            log.info("schedule.enqueued", job_id=str(job_id))
        return len(to_push)

    async def reconcile_once(self) -> int:
        """Re-push jobs Postgres expects to be in Redis but Redis does not have."""
        known = await self._broker.known_job_ids()
        now = datetime.now(UTC)
        queued_cutoff = now - timedelta(seconds=self._settings.reconcile_grace)
        running_cutoff = now - timedelta(
            seconds=self._settings.lease_seconds + self._settings.reconcile_grace
        )
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(Job.id, Job.status, Job.priority, Job.run_at, Job.attempts).where(
                        or_(
                            and_(
                                Job.status == JobStatus.QUEUED.value,
                                Job.updated_at < queued_cutoff,
                            ),
                            and_(
                                Job.status == JobStatus.RUNNING.value,
                                Job.updated_at < running_cutoff,
                            ),
                        )
                    )
                )
            ).all()

        fixed = 0
        for row in rows:
            if row.id in known:
                continue
            if row.status == JobStatus.QUEUED.value:
                await self._broker.enqueue(row.id, PRIORITY_NAMES[row.priority], row.run_at)
                metrics.RECONCILED.labels("queued").inc()
                log.warning("job.reconciled", job_id=str(row.id), status="queued")
                fixed += 1
            else:
                # Running in Postgres with no lease anywhere: the lease was lost before
                # the reaper could act on it. Treat it as an expired lease.
                if await self._reclaim(row.id):
                    metrics.RECONCILED.labels("running").inc()
                    log.warning("job.reconciled", job_id=str(row.id), status="running")
                    fixed += 1
        return fixed

    async def update_gauges(self) -> None:
        counts = await self._broker.counts()
        for priority, n in counts.ready.items():
            metrics.QUEUE_DEPTH.labels(priority).set(n)
        metrics.DELAYED.set(counts.delayed)
        metrics.INFLIGHT.set(counts.inflight)
        async with self._sessions() as session:
            rows = await session.execute(select(Job.status, func.count()).group_by(Job.status))
            by_status: dict[str, int] = {st: int(n) for st, n in rows.all()}
        for status in JobStatus:
            metrics.JOBS_BY_STATUS.labels(status.value).set(by_status.get(status.value, 0))
        metrics.DLQ_SIZE.set(by_status.get(JobStatus.DEAD.value, 0))

    async def run_forever(self, stop: asyncio.Event) -> None:
        s = self._settings
        log.info("reaper.started")
        await asyncio.gather(
            _periodic("promote", self.promote_once, s.promote_interval, stop),
            _periodic("reap", self.reap_once, s.reap_interval, stop),
            _periodic("schedule", self.schedule_once, s.schedule_interval, stop),
            _periodic("reconcile", self.reconcile_once, s.reconcile_interval, stop),
            _periodic("gauges", self.update_gauges, 2.0, stop),
        )
        log.info("reaper.stopped")


async def _periodic(
    name: str, fn: Callable[[], Awaitable[object]], interval: float, stop: asyncio.Event
) -> None:
    while not stop.is_set():
        try:
            await fn()
        except Exception:
            log.exception("reaper.loop_error", loop=name)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval)


@asynccontextmanager
async def open_reaper(settings: Settings) -> AsyncIterator[Reaper]:
    engine = make_engine(settings.database_url)
    redis = Redis.from_url(settings.redis_url)
    try:
        yield Reaper(make_session_factory(engine), Broker(redis, settings.redis_prefix), settings)
    finally:
        await redis.aclose()
        await engine.dispose()


async def _amain() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    metrics.serve(settings.metrics_port)
    stop = asyncio.Event()
    install_stop_signals(stop)
    async with open_reaper(settings) as reaper:
        await reaper.run_forever(stop)


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
