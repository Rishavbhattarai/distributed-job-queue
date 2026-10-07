"""Worker: lease a job from Redis, run its handler, record the outcome in Postgres.

Per job:

1. ``broker.lease``: atomically pop the highest-priority id and record a lease
   (``inflight`` zset, score = expiry) owned by a random token.
2. ``lifecycle.claim``: conditional UPDATE ``queued -> running``, ``attempts += 1``, plus
   the ``job_attempts`` row, in one statement. No row means a duplicate delivery: skip it.
3. Run the handler while a background task heartbeats the lease.
4. Record the outcome, fenced on the attempt number: success, or failure with a retry
   (exponential backoff, full jitter) or dead-letter decision.
5. If retrying, push the id to the delayed set (Postgres first, then Redis), then ``ack``.

On SIGTERM the worker stops leasing, gives the in-flight job ``shutdown_grace`` seconds,
then cancels it and releases the job back to the queue.

Run with ``jobq-worker`` (or ``python -m jobq.worker``).
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import signal
import time
import traceback
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import structlog
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine

from jobq import lifecycle, metrics
from jobq.broker import Broker, Lease
from jobq.config import Settings
from jobq.db import make_autocommit_session_factory, make_engine, make_session_factory
from jobq.handlers import HandlerRegistry, PermanentError, UnknownJobTypeError, registry
from jobq.lifecycle import Claimed, RetryPolicy
from jobq.logs import configure_logging
from jobq.models import AttemptOutcome, JobStatus

log = structlog.get_logger("jobq.worker")

_MAX_ERROR_LEN = 4000


class Worker:
    def __init__(
        self,
        engine: AsyncEngine,
        broker: Broker,
        handlers: HandlerRegistry,
        settings: Settings,
    ) -> None:
        self._sessions = make_session_factory(engine)
        # claim() and complete() are single statements: run them without BEGIN/COMMIT.
        self._autocommit = make_autocommit_session_factory(engine)
        self._broker = broker
        self._handlers = handlers
        self._settings = settings
        self._policy = RetryPolicy(settings.backoff_base, settings.backoff_cap)
        self.worker_id = settings.worker_id

    async def run_once(self, timeout: float | None = None) -> uuid.UUID | None:
        """Process at most one job, polling up to ``timeout`` seconds for one to arrive."""
        deadline = time.monotonic() + (self._settings.poll_timeout if timeout is None else timeout)
        while True:
            lease = await self._broker.lease(self._settings.lease_seconds, self.worker_id)
            if lease is not None:
                await self.process(lease)
                return lease.job_id
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(self._settings.idle_sleep)

    async def run_forever(self, stop: asyncio.Event) -> None:
        log.info("worker.started", worker_id=self.worker_id, handlers=self._handlers.types())
        while not stop.is_set():
            try:
                lease = await self._broker.lease(self._settings.lease_seconds, self.worker_id)
            except Exception:
                # Infra error (Redis blip). Back off briefly and keep going.
                log.exception("worker.lease_error", worker_id=self.worker_id)
                await _wait(stop, 1.0)
                continue
            if lease is None:
                await _wait(stop, self._settings.idle_sleep)
                continue
            await self._process_with_shutdown(lease, stop)
        log.info("worker.stopped", worker_id=self.worker_id)

    async def _process_with_shutdown(self, lease: Lease, stop: asyncio.Event) -> None:
        task = asyncio.create_task(self.process(lease))
        stop_wait = asyncio.create_task(stop.wait())
        try:
            await asyncio.wait({task, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
            if not task.done():
                log.info("worker.draining", job_id=str(lease.job_id))
                done, _ = await asyncio.wait({task}, timeout=self._settings.shutdown_grace)
                if not done:
                    # process() catches the cancellation and releases the job.
                    task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        except Exception:
            log.exception("worker.process_error", job_id=str(lease.job_id))
        finally:
            stop_wait.cancel()

    async def process(self, lease: Lease) -> None:
        job_id = lease.job_id
        structlog.contextvars.bind_contextvars(job_id=str(job_id), worker_id=self.worker_id)
        try:
            async with self._autocommit() as session:
                claimed = await lifecycle.claim(session, job_id, self.worker_id)
            if claimed is None:
                # Duplicate delivery: the job is running elsewhere or already finished.
                log.info("job.skip_not_queued")
                await self._broker.ack(lease)
                return
            await self._run(lease, claimed)
        finally:
            structlog.contextvars.unbind_contextvars("job_id", "worker_id")

    async def _run(self, lease: Lease, claimed: Claimed) -> None:
        metrics.JOBS_STARTED.labels(claimed.type).inc()
        metrics.ENQUEUE_TO_START.observe(
            max(0.0, (claimed.started_at - claimed.runnable_at).total_seconds())
        )
        log.info("job.started", type=claimed.type, attempt=claimed.attempt_number)
        heartbeat = asyncio.create_task(self._heartbeat(lease))
        started = time.perf_counter()
        try:
            try:
                handler = self._handlers.get(claimed.type)
                result = await handler(claimed.payload)
            except asyncio.CancelledError:
                # Graceful-shutdown deadline hit: hand the job back, then stop.
                await asyncio.shield(self._release(lease, claimed))
                raise
            except Exception as exc:
                metrics.JOB_DURATION.labels(claimed.type).observe(time.perf_counter() - started)
                await self._fail(lease, claimed, exc)
            else:
                metrics.JOB_DURATION.labels(claimed.type).observe(time.perf_counter() - started)
                await self._succeed(lease, claimed, result)
        finally:
            heartbeat.cancel()

    async def _succeed(self, lease: Lease, claimed: Claimed, result: Any) -> None:
        async with self._autocommit() as session:
            ok = await lifecycle.complete(session, claimed, result)
        if ok:
            metrics.JOBS_FINISHED.labels(claimed.type, "succeeded").inc()
            log.info("job.succeeded")
        else:
            metrics.JOBS_FINISHED.labels(claimed.type, "fenced").inc()
            log.warning("job.fenced_out", reason="lease lost; result discarded")
        await self._broker.ack(lease)

    async def _fail(self, lease: Lease, claimed: Claimed, exc: Exception) -> None:
        error = _format_error(exc)
        permanent = isinstance(exc, PermanentError | UnknownJobTypeError)
        async with self._sessions() as session, session.begin():
            transition = await lifecycle.fail_attempt(
                session,
                claimed.job_id,
                claimed.attempt_number,
                error=error,
                outcome=AttemptOutcome.FAILED,
                policy=self._policy,
                permanent=permanent,
            )
        last_line = error.splitlines()[-1] if error else ""
        if transition is None:
            metrics.JOBS_FINISHED.labels(claimed.type, "fenced").inc()
            log.warning("job.fenced_out", error=last_line)
        elif transition.status is JobStatus.DEAD:
            metrics.JOBS_FINISHED.labels(claimed.type, "dead").inc()
            log.warning("job.dead", error=last_line, attempts=transition.attempts)
        else:
            # Postgres says queued; now make it visible again (delayed until run_at).
            await self._broker.enqueue(claimed.job_id, transition.priority, transition.run_at)
            metrics.JOBS_FINISHED.labels(claimed.type, "retried").inc()
            log.warning(
                "job.retry_scheduled",
                error=last_line,
                attempt=claimed.attempt_number,
                run_at=transition.run_at.isoformat() if transition.run_at else None,
            )
        await self._broker.ack(lease)

    async def _release(self, lease: Lease, claimed: Claimed) -> None:
        async with self._sessions() as session, session.begin():
            transition = await lifecycle.release(session, claimed.job_id, claimed.attempt_number)
        if transition is not None:
            await self._broker.enqueue(claimed.job_id, transition.priority)
            metrics.JOBS_FINISHED.labels(claimed.type, "released").inc()
            log.info("job.released")
        await self._broker.ack(lease)

    async def _heartbeat(self, lease: Lease) -> None:
        while True:
            await asyncio.sleep(self._settings.heartbeat_interval)
            try:
                alive = await self._broker.heartbeat(lease, self._settings.lease_seconds)
            except Exception:
                log.exception("job.heartbeat_error")
                continue
            if not alive:
                # The reaper reclaimed the job. Keep running; the result will be fenced out.
                metrics.LEASES_LOST.inc()
                log.warning("job.lease_lost")
                return


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


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
            engine=engine,
            broker=Broker(redis, prefix=settings.redis_prefix),
            handlers=handlers or registry,
            settings=settings,
        )
    finally:
        await redis.aclose()
        await engine.dispose()


def install_stop_signals(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)


async def _amain() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    load_handler_modules(settings.handler_modules)
    metrics.serve(settings.metrics_port)
    stop = asyncio.Event()
    install_stop_signals(stop)
    async with open_worker(settings) as worker:
        await worker.run_forever(stop)


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
