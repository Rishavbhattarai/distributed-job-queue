"""FastAPI service.

Write ordering (see docs/adr/0001 and 0004): the job row is committed to Postgres FIRST,
then its id is pushed to Redis. A crash between the two leaves a ``queued`` row that Redis
does not know about, never the reverse. The reaper's reconciler re-pushes such rows.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated

import structlog
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse
from prometheus_client import CollectorRegistry, make_asgi_app, multiprocess
from redis.asyncio import Redis
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.types import ASGIApp

from jobq import cron, lifecycle, metrics
from jobq.broker import Broker
from jobq.config import Settings
from jobq.db import make_engine, make_session_factory
from jobq.logs import configure_logging
from jobq.models import PRIORITY_NAMES, PRIORITY_VALUES, Job, JobStatus, Schedule
from jobq.schemas import JobCreate, JobList, JobOut, Replay, ScheduleIn, ScheduleOut

log = structlog.get_logger("jobq.api")


def _metrics_app() -> ASGIApp:
    """Prometheus ASGI app. With several uvicorn processes (PROMETHEUS_MULTIPROC_DIR set),
    aggregate every process's counters instead of reporting only the one that answered."""
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)  # type: ignore[no-untyped-call]
        return make_asgi_app(registry)
    return make_asgi_app()


def create_app(settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        s = settings or Settings.from_env()
        configure_logging(s.log_level)
        engine = make_engine(s.database_url)
        redis = Redis.from_url(s.redis_url)
        app.state.settings = s
        app.state.session_factory = make_session_factory(engine)
        app.state.redis = redis
        app.state.broker = Broker(redis, prefix=s.redis_prefix)
        try:
            yield
        finally:
            await redis.aclose()
            await engine.dispose()

    app = FastAPI(title="jobq", version="1.0.0", lifespan=lifespan)
    app.mount("/metrics", _metrics_app())

    def sessions(request: Request) -> async_sessionmaker[AsyncSession]:
        sf: async_sessionmaker[AsyncSession] = request.app.state.session_factory
        return sf

    def broker_of(request: Request) -> Broker:
        b: Broker = request.app.state.broker
        return b

    def settings_of(request: Request) -> Settings:
        st: Settings = request.app.state.settings
        return st

    # -- jobs --------------------------------------------------------------------------

    @app.post(
        "/jobs",
        response_model=JobOut,
        status_code=status.HTTP_201_CREATED,
        responses={
            200: {"model": JobOut, "description": "Duplicate Idempotency-Key: original job"},
            409: {"description": "Idempotency-Key reused with a different type or payload"},
            429: {"description": "Queue is full (backpressure); retry after Retry-After"},
        },
    )
    async def create_job(
        body: JobCreate,
        request: Request,
        response: Response,
        idempotency_key: Annotated[str | None, Header(max_length=255)] = None,
    ) -> JobOut | JSONResponse:
        st = settings_of(request)
        broker = broker_of(request)
        if st.max_queue_depth and await broker.depth() >= st.max_queue_depth:
            metrics.ENQUEUE_REJECTED.inc()
            return JSONResponse(
                {"detail": "queue is full, retry later"},
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={"Retry-After": "1"},
            )

        run_at = body.run_at or datetime.now(UTC)
        insert = (
            pg_insert(Job)
            .values(
                id=uuid.uuid4(),
                type=body.type,
                payload=body.payload,
                priority=PRIORITY_VALUES[body.priority],
                status=JobStatus.QUEUED.value,
                attempts=0,
                max_attempts=body.max_attempts or st.max_attempts_for(body.type),
                run_at=run_at,
                idempotency_key=idempotency_key,
            )
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(Job)
        )
        async with sessions(request)() as session:
            job = (await session.execute(select(Job).from_statement(insert))).scalar_one_or_none()
            await session.commit()

            if job is None:
                # Duplicate Idempotency-Key: return the original job, do not enqueue again.
                existing = (
                    await session.execute(select(Job).where(Job.idempotency_key == idempotency_key))
                ).scalar_one()
                if existing.type != body.type or existing.payload != body.payload:
                    raise HTTPException(
                        status_code=409,
                        detail="Idempotency-Key was already used with a different type or payload",
                    )
                metrics.ENQUEUE_DUPLICATES.inc()
                response.status_code = status.HTTP_200_OK
                return JobOut.from_model(existing)

        # Postgres commit happened above; only now make the job visible to workers.
        try:
            await broker.enqueue(job.id, body.priority, body.run_at)
        except Exception:
            # Durably accepted; the reconciler will push it. Don't fail the request.
            metrics.ENQUEUE_PUSH_FAILED.inc()
            log.exception("job.enqueue_failed", job_id=str(job.id))
        metrics.JOBS_ENQUEUED.labels(body.priority).inc()
        response.headers["Location"] = f"/jobs/{job.id}"
        return JobOut.from_model(job)

    @app.get("/jobs/{job_id}", response_model=JobOut)
    async def get_job(job_id: uuid.UUID, request: Request) -> JobOut:
        async with sessions(request)() as session:
            job = await session.get(Job, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return JobOut.from_model(job)

    async def _replay(request: Request, job_id: uuid.UUID, extra_attempts: int) -> JobOut:
        async with sessions(request)() as session:
            job = await lifecycle.replay(session, job_id, extra_attempts)
            await session.commit()
            if job is None:
                exists = await session.get(Job, job_id)
                if exists is None:
                    raise HTTPException(status_code=404, detail="job not found")
                raise HTTPException(
                    status_code=409, detail=f"only dead jobs can be replayed (is {exists.status})"
                )
        await broker_of(request).enqueue(job.id, PRIORITY_NAMES[job.priority])
        log.info("job.replayed", job_id=str(job.id), max_attempts=job.max_attempts)
        return JobOut.from_model(job)

    @app.post("/jobs/{job_id}/retry", response_model=JobOut)
    async def retry_job(job_id: uuid.UUID, request: Request, body: Replay | None = None) -> JobOut:
        """Manually retry a dead job now (same as POST /dlq/{id}/replay)."""
        return await _replay(request, job_id, (body or Replay()).extra_attempts)

    # -- dead-letter queue -------------------------------------------------------------

    @app.get("/dlq", response_model=JobList)
    async def list_dlq(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
        type: Annotated[str | None, Query(description="Filter by job type")] = None,
    ) -> JobList:
        """Dead jobs (out of attempts or permanent errors), most recent first."""
        where = [Job.status == JobStatus.DEAD.value]
        if type is not None:
            where.append(Job.type == type)
        async with sessions(request)() as session:
            total = (await session.execute(select(func.count()).where(*where))).scalar_one()
            jobs = (
                await session.execute(
                    select(Job)
                    .where(*where)
                    .order_by(Job.updated_at.desc())
                    .limit(limit)
                    .offset(offset)
                )
            ).scalars()
            return JobList(items=[JobOut.from_model(j) for j in jobs], total=total)

    @app.post("/dlq/{job_id}/replay", response_model=JobOut)
    async def replay_dlq(job_id: uuid.UUID, request: Request, body: Replay | None = None) -> JobOut:
        """Move a dead job back to the queue with ``extra_attempts`` more tries."""
        return await _replay(request, job_id, (body or Replay()).extra_attempts)

    # -- cron schedules ----------------------------------------------------------------

    @app.put("/schedules/{name}", response_model=ScheduleOut)
    async def upsert_schedule(name: str, body: ScheduleIn, request: Request) -> ScheduleOut:
        """Create or replace a schedule. The next tick is computed from now (UTC)."""
        now = datetime.now(UTC)
        values = {
            "cron": body.cron,
            "job_type": body.job_type,
            "payload": body.payload,
            "priority": PRIORITY_VALUES[body.priority],
            "max_attempts": body.max_attempts,
            "enabled": body.enabled,
            "next_run_at": cron.next_fire(body.cron, now),
            "updated_at": now,
        }
        stmt = (
            pg_insert(Schedule)
            .values(name=name, **values)
            .on_conflict_do_update(index_elements=["name"], set_=values)
            .returning(Schedule)
        )
        async with sessions(request)() as session:
            sch = (await session.execute(select(Schedule).from_statement(stmt))).scalar_one()
            await session.commit()
        return ScheduleOut.from_model(sch)

    @app.get("/schedules", response_model=list[ScheduleOut])
    async def list_schedules(request: Request) -> list[ScheduleOut]:
        async with sessions(request)() as session:
            rows = (await session.execute(select(Schedule).order_by(Schedule.name))).scalars()
            return [ScheduleOut.from_model(s) for s in rows]

    @app.get("/schedules/{name}", response_model=ScheduleOut)
    async def get_schedule(name: str, request: Request) -> ScheduleOut:
        async with sessions(request)() as session:
            sch = await session.get(Schedule, name)
        if sch is None:
            raise HTTPException(status_code=404, detail="schedule not found")
        return ScheduleOut.from_model(sch)

    @app.delete("/schedules/{name}", status_code=204)
    async def delete_schedule(name: str, request: Request) -> Response:
        async with sessions(request)() as session:
            sch = await session.get(Schedule, name)
            if sch is None:
                raise HTTPException(status_code=404, detail="schedule not found")
            await session.delete(sch)
            await session.commit()
        return Response(status_code=204)

    # -- health ------------------------------------------------------------------------

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Liveness: the process is up."""
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz(request: Request) -> JSONResponse:
        """Readiness: Postgres and Redis are reachable."""
        checks: dict[str, str] = {}
        try:
            async with sessions(request)() as session:
                await session.execute(text("SELECT 1"))
            checks["postgres"] = "ok"
        except Exception as exc:
            checks["postgres"] = f"error: {exc.__class__.__name__}"
        try:
            await request.app.state.redis.ping()
            checks["redis"] = "ok"
        except Exception as exc:
            checks["redis"] = f"error: {exc.__class__.__name__}"
        ok = all(v == "ok" for v in checks.values())
        return JSONResponse(
            {"status": "ok" if ok else "degraded", "checks": checks},
            status_code=200 if ok else 503,
        )

    return app


app = create_app()
