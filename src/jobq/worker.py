"""Worker: take a job id from Redis, run its handler, record the attempt in Postgres.

Per job:

1. ``broker.dequeue`` -> job id.
2. Claim in Postgres with a conditional UPDATE (``status='queued'`` -> ``running``,
   ``attempts += 1``) and insert a ``job_attempts`` row, in one transaction. If the claim
   matches no row the job was already taken/finished (duplicate delivery), so we skip it.
   This guard is what makes redelivery under at-least-once safe at the job level.
3. Run the handler (outside any DB transaction).
4. Record outcome on both ``jobs`` and ``job_attempts`` in one transaction.
5. ``broker.ack``.

Run with ``python -m jobq.worker`` or the ``jobq-worker`` console script.
"""

from __future__ import annotations

import asyncio
import importlib
import signal
import traceback
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import structlog
from redis.asyncio import Redis
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobq.broker import Broker
from jobq.config import Settings
from jobq.db import make_engine, make_session_factory
from jobq.handlers import HandlerRegistry, registry
from jobq.logs import configure_logging
from jobq.models import Job, JobAttempt, JobStatus

log = structlog.get_logger("jobq.worker")

_MAX_ERROR_LEN = 4000


class Worker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: Broker,
        handlers: HandlerRegistry,
        worker_id: str,
        poll_timeout: float = 1.0,
    ) -> None:
        self._sessions = session_factory
        self._broker = broker
        self._handlers = handlers
        self.worker_id = worker_id
        self._poll_timeout = poll_timeout

    async def run_once(self, timeout: float | None = None) -> uuid.UUID | None:
        """Process at most one job. Returns its id, or None if nothing arrived in time."""
        # TODO(week 3): move delayed-job promotion into a dedicated scheduler/reaper loop.
        await self._broker.promote_due()
        job_id = await self._broker.dequeue(self._poll_timeout if timeout is None else timeout)
        if job_id is None:
            return None
        await self.process(job_id)
        return job_id

    async def run_forever(self, stop: asyncio.Event) -> None:
        log.info("worker.started", worker_id=self.worker_id, handlers=self._handlers.types())
        while not stop.is_set():
            try:
                await self.run_once()
            except Exception:
                # Infra error (Redis/Postgres blip). Back off briefly and keep going.
                log.exception("worker.loop_error", worker_id=self.worker_id)
                await asyncio.sleep(1.0)
        # TODO(week 3 -- graceful shutdown): today we just stop taking new work between jobs;
        # with leases we must also finish-or-release the in-flight job on SIGTERM.
        log.info("worker.stopped", worker_id=self.worker_id)

    async def process(self, job_id: uuid.UUID) -> None:
        structlog.contextvars.bind_contextvars(job_id=str(job_id), worker_id=self.worker_id)
        try:
            claimed = await self._claim(job_id)
            if claimed is None:
                log.warning("job.skip_not_queued")
                await self._broker.ack(job_id)
                return
            job_type, payload, attempt_number, attempt_id = claimed
            log.info("job.started", type=job_type, attempt=attempt_number)

            try:
                handler = self._handlers.get(job_type)
                result = await handler(payload)
            except Exception as exc:
                error = _format_error(exc)
                log.warning("job.attempt_failed", error=error.splitlines()[-1])
                await self._record_failure(job_id, attempt_id, error)
            else:
                await self._record_success(job_id, attempt_id, result)
                log.info("job.succeeded")
            await self._broker.ack(job_id)
        finally:
            structlog.contextvars.unbind_contextvars("job_id", "worker_id")

    async def _claim(self, job_id: uuid.UUID) -> tuple[str, dict[str, Any], int, int] | None:
        async with self._sessions() as session, session.begin():
            row = (
                await session.execute(
                    update(Job)
                    .where(Job.id == job_id, Job.status == JobStatus.QUEUED.value)
                    .values(
                        status=JobStatus.RUNNING.value,
                        attempts=Job.attempts + 1,
                        updated_at=_now(),
                    )
                    .returning(Job.type, Job.payload, Job.attempts)
                )
            ).one_or_none()
            if row is None:
                return None
            attempt = JobAttempt(
                job_id=job_id,
                attempt_number=row.attempts,
                worker_id=self.worker_id,
                started_at=_now(),
            )
            session.add(attempt)
            await session.flush()
            return row.type, row.payload, row.attempts, attempt.id

    async def _record_success(self, job_id: uuid.UUID, attempt_id: int, result: Any) -> None:
        now = _now()
        async with self._sessions() as session, session.begin():
            await session.execute(
                update(JobAttempt)
                .where(JobAttempt.id == attempt_id)
                .values(finished_at=now, outcome="succeeded")
            )
            await session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(
                    status=JobStatus.SUCCEEDED.value,
                    result=result,
                    last_error=None,
                    updated_at=now,
                )
            )

    async def _record_failure(self, job_id: uuid.UUID, attempt_id: int, error: str) -> None:
        now = _now()
        async with self._sessions() as session, session.begin():
            await session.execute(
                update(JobAttempt)
                .where(JobAttempt.id == attempt_id)
                .values(finished_at=now, outcome="failed", error=error)
            )
            # YOUR TURN (see YOUR_TURN.md, push 2/3): retry vs dead-letter decision goes here.
            # Today every failure is final (status='failed'). Your version should read
            # jobs.attempts / jobs.max_attempts and either
            #   * attempts < max_attempts: set status back to 'queued', set run_at to
            #     now + backoff (exponential + jitter), and after this transaction commits
            #     call self._broker.enqueue(job_id, <priority>, run_at) -- Postgres first,
            #     then Redis, same ordering as the API; or
            #   * attempts >= max_attempts: set status='dead' and put the job in the DLQ.
            await session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(status=JobStatus.FAILED.value, last_error=error, updated_at=now)
            )


def _now() -> datetime:
    return datetime.now(UTC)


def _format_error(exc: BaseException) -> str:
    text = "".join(traceback.format_exception(exc)).strip()
    return text[-_MAX_ERROR_LEN:]


def load_handler_modules(modules: tuple[str, ...]) -> None:
    """Import modules that register extra handlers on ``jobq.handlers.registry``."""
    for name in modules:
        importlib.import_module(name)


@asynccontextmanager
async def open_worker(
    settings: Settings, handlers: HandlerRegistry | None = None
) -> AsyncIterator[Worker]:
    """Build a Worker with its own engine + Redis connection, and clean them up after."""
    engine = make_engine(settings.database_url)
    redis = Redis.from_url(settings.redis_url)
    try:
        yield Worker(
            session_factory=make_session_factory(engine),
            broker=Broker(redis, prefix=settings.redis_prefix),
            handlers=handlers or registry,
            worker_id=settings.worker_id,
            poll_timeout=settings.poll_timeout,
        )
    finally:
        await redis.aclose()
        await engine.dispose()


async def _amain() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    load_handler_modules(settings.handler_modules)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    async with open_worker(settings) as worker:
        await worker.run_forever(stop)


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
