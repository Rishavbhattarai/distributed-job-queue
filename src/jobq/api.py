"""FastAPI service: POST /jobs, GET /jobs/{id}, /healthz, /readyz.

Write ordering (see docs/adr/0001): the job row is committed to Postgres FIRST, then its id
is pushed to Redis. A crash between the two leaves a ``queued`` row that Redis does not
know about -- never the reverse (a Redis entry with no row). A reconciler (week 2) will
re-push such rows; until then they are logged.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated

import structlog
from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobq.broker import Broker
from jobq.config import Settings
from jobq.db import make_engine, make_session_factory
from jobq.logs import configure_logging
from jobq.models import PRIORITY_VALUES, Job, JobStatus
from jobq.schemas import JobCreate, JobOut

log = structlog.get_logger("jobq.api")


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

    app = FastAPI(title="jobq", version="0.1.0", lifespan=lifespan)

    def sessions(request: Request) -> async_sessionmaker[AsyncSession]:
        sf: async_sessionmaker[AsyncSession] = request.app.state.session_factory
        return sf

    @app.post(
        "/jobs",
        response_model=JobOut,
        status_code=status.HTTP_201_CREATED,
        responses={200: {"model": JobOut, "description": "Duplicate Idempotency-Key"}},
    )
    async def create_job(
        body: JobCreate,
        request: Request,
        response: Response,
        idempotency_key: Annotated[str | None, Header(max_length=255)] = None,
    ) -> JobOut:
        broker: Broker = request.app.state.broker
        run_at = body.run_at or datetime.now(UTC)
        async with sessions(request)() as session:
            stmt = (
                pg_insert(Job)
                .values(
                    id=uuid.uuid4(),
                    type=body.type,
                    payload=body.payload,
                    priority=PRIORITY_VALUES[body.priority],
                    status=JobStatus.QUEUED.value,
                    attempts=0,
                    max_attempts=body.max_attempts,
                    run_at=run_at,
                    idempotency_key=idempotency_key,
                )
                .on_conflict_do_nothing(index_elements=["idempotency_key"])
                .returning(Job.id)
            )
            new_id = (await session.execute(stmt)).scalar_one_or_none()
            await session.commit()

            if new_id is None:
                # Duplicate Idempotency-Key: return the original job, do not enqueue again.
                # TODO(week 3): reject (422) when the duplicate's body differs from the original.
                existing = (
                    await session.execute(select(Job).where(Job.idempotency_key == idempotency_key))
                ).scalar_one()
                response.status_code = status.HTTP_200_OK
                log.info("job.duplicate", job_id=str(existing.id), idempotency_key=idempotency_key)
                return JobOut.from_model(existing)

            job = await session.get(Job, new_id)
            assert job is not None

        # Postgres commit happened above; only now make it visible to workers.
        try:
            await broker.enqueue(job.id, body.priority, body.run_at)
        except Exception:
            # The job is durably accepted; it is stranded in Redis terms until the
            # reconciler (TODO week 2) re-pushes queued rows. Don't fail the request.
            log.exception("job.enqueue_failed", job_id=str(job.id))
        log.info("job.created", job_id=str(job.id), type=job.type, priority=body.priority)
        response.headers["Location"] = f"/jobs/{job.id}"
        return JobOut.from_model(job)

    @app.get("/jobs/{job_id}", response_model=JobOut)
    async def get_job(job_id: uuid.UUID, request: Request) -> JobOut:
        async with sessions(request)() as session:
            job = await session.get(Job, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return JobOut.from_model(job)

    # YOUR TURN (see YOUR_TURN.md, push 2/3): GET /dlq and POST /dlq/{id}/replay go here.
    # TODO(later weeks): POST /jobs/{id}/retry, 429 backpressure when broker.depth() passes
    # a threshold, /metrics.

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
