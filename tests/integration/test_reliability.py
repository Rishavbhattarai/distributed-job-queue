"""Weeks 2-3: retries, DLQ, leases, reaper, fencing, reconciler, shutdown, backpressure."""

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from typing import Any

import httpx
import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobq import lifecycle
from jobq.broker import Broker
from jobq.client import JobqClient, QueueFullError
from jobq.config import Settings
from jobq.handlers import HandlerRegistry
from jobq.models import Job, JobAttempt
from jobq.reaper import Reaper, open_reaper
from jobq.worker import Worker, open_worker

from .conftest import serve_api
from .helpers import drain

pytestmark = pytest.mark.integration


async def outcomes(sessions: async_sessionmaker[AsyncSession], job_id: uuid.UUID) -> list[str]:
    async with sessions() as s:
        rows = await s.execute(
            select(JobAttempt.outcome)
            .where(JobAttempt.job_id == job_id)
            .order_by(JobAttempt.attempt_number)
        )
        return [r or "" for r in rows.scalars()]


# -- retries and the dead-letter queue ---------------------------------------------------


async def test_retries_with_backoff_then_dead_then_replay(
    api_url: str, worker: Worker, reaper: Reaper, sessions: async_sessionmaker[AsyncSession]
) -> None:
    async with httpx.AsyncClient(base_url=api_url) as http:
        with JobqClient(api_url) as client:
            job = client.enqueue("fail_randomly", {"probability": 1.0}, max_attempts=3)
            done = await drain(worker, reaper, client, job.id)
            assert done.status == "dead"
            assert done.attempts == 3
            assert await outcomes(sessions, job.id) == ["failed"] * 3

            dlq = (await http.get("/dlq")).json()
            assert dlq["total"] == 1
            assert dlq["items"][0]["id"] == str(job.id)
            assert (await http.get("/dlq", params={"type": "other"})).json()["total"] == 0

            replayed = await http.post(f"/dlq/{job.id}/replay", json={"extra_attempts": 1})
            assert replayed.status_code == 200
            assert replayed.json()["status"] == "queued"
            assert replayed.json()["max_attempts"] == 4
            assert (await http.get("/dlq")).json()["total"] == 0

            # Replaying a job that is not dead is a conflict; unknown ids are 404.
            assert (await http.post(f"/dlq/{job.id}/replay")).status_code == 409
            assert (await http.post(f"/dlq/{uuid.uuid4()}/replay")).status_code == 404

            done = await drain(worker, reaper, client, job.id)
            assert (done.status, done.attempts) == ("dead", 4)
            # Manual retry endpoint does the same thing.
            assert (await http.post(f"/jobs/{job.id}/retry")).json()["status"] == "queued"


async def test_transient_failure_retries_then_succeeds(
    api_url: str, settings: Settings, reaper: Reaper, sessions: async_sessionmaker[AsyncSession]
) -> None:
    calls = 0
    reg = HandlerRegistry()

    @reg.handler("flaky")
    async def flaky(payload: dict[str, Any]) -> dict[str, int]:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("downstream timeout")
        return {"calls": calls}

    async with open_worker(settings, reg) as worker:
        with JobqClient(api_url) as client:
            job = client.enqueue("flaky", max_attempts=5)
            done = await drain(worker, reaper, client, job.id)
    assert (done.status, done.attempts, done.result) == ("succeeded", 3, {"calls": 3})
    assert done.last_error is None
    assert await outcomes(sessions, job.id) == ["failed", "failed", "succeeded"]


# -- leases, reaper, fencing ---------------------------------------------------------------


async def _crashed_claim(
    broker: Broker, sessions: async_sessionmaker[AsyncSession], lease_seconds: float
) -> lifecycle.Claimed:
    """Lease and claim a job as a worker would, then 'crash' (never finish or heartbeat)."""
    lease = await broker.lease(lease_seconds, "doomed-worker")
    assert lease is not None
    async with sessions() as s, s.begin():
        claimed = await lifecycle.claim(s, lease.job_id, "doomed-worker")
    assert claimed is not None
    return claimed


async def test_reaper_reclaims_expired_lease(
    api_url: str,
    worker: Worker,
    reaper: Reaper,
    broker: Broker,
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    with JobqClient(api_url) as client:
        job = client.enqueue("echo", {"x": 1})
        await _crashed_claim(broker, sessions, lease_seconds=1.0)
        assert await reaper.reap_once() == 0  # lease still valid
        await asyncio.sleep(1.1)
        assert await reaper.reap_once() == 1
        assert client.get(job.id).status == "queued"
        done = await drain(worker, reaper, client, job.id)
    assert (done.status, done.attempts) == ("succeeded", 2)
    assert await outcomes(sessions, job.id) == ["lease_expired", "succeeded"]
    assert (await broker.counts()).inflight == 0


async def test_late_result_from_expired_lease_is_fenced_out(
    api_url: str,
    worker: Worker,
    reaper: Reaper,
    broker: Broker,
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    with JobqClient(api_url) as client:
        job = client.enqueue("echo", {"v": "fresh"})
        stale = await _crashed_claim(broker, sessions, lease_seconds=0.1)
        await asyncio.sleep(0.2)
        await reaper.reap_once()
        await drain(worker, reaper, client, job.id)
        # The "slow" first worker finally finishes: its write must not land.
        async with sessions() as s, s.begin():
            assert await lifecycle.complete(s, stale, {"v": "stale"}) is False
        final = client.get(job.id)
    assert final.result == {"v": "fresh"}
    assert final.attempts == 2


async def test_heartbeat_keeps_long_job_leased(
    api_url: str, settings: Settings, sessions: async_sessionmaker[AsyncSession]
) -> None:
    # 10 heartbeats per lease, so a loaded CI machine can miss several and still pass.
    s = dataclasses.replace(settings, lease_seconds=1.0, heartbeat_interval=0.1)
    reg = HandlerRegistry()

    @reg.handler("long")
    async def long(payload: dict[str, Any]) -> str:
        await asyncio.sleep(2.5)
        return "done"

    async with open_worker(s, reg) as worker, open_reaper(s) as reaper:
        with JobqClient(api_url) as client:
            job = client.enqueue("long")
        run = asyncio.create_task(worker.run_once(timeout=3))
        reclaimed = 0
        while not run.done():
            reclaimed += await reaper.reap_once()
            await asyncio.sleep(0.05)
        await run
    assert reclaimed == 0
    assert await outcomes(sessions, job.id) == ["succeeded"]


# -- reconciler ------------------------------------------------------------------------------


async def test_reconciler_repushes_job_missing_from_redis(
    api_url: str, worker: Worker, reaper: Reaper, broker: Broker
) -> None:
    with JobqClient(api_url) as client:
        job = client.enqueue("echo")
        # Simulate "committed to Postgres, Redis push lost" (crash or Redis data loss).
        await broker.redis.delete(broker.ready_key("normal"))
        assert await worker.run_once(timeout=0.1) is None
        assert await reaper.reconcile_once() == 1
        assert await reaper.reconcile_once() == 0  # now Redis knows it: nothing to do
        assert await worker.run_once(timeout=1) == job.id
        assert client.get(job.id).status == "succeeded"


async def test_reconciler_recovers_orphaned_running_job(
    api_url: str,
    settings: Settings,
    broker: Broker,
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    s = dataclasses.replace(settings, lease_seconds=0.0)
    with JobqClient(api_url) as client:
        job = client.enqueue("echo")
        await _crashed_claim(broker, sessions, lease_seconds=60)
        # The lease vanished (e.g. Redis restarted without persistence).
        await broker.redis.delete(broker.inflight_key)
        async with open_reaper(s) as reaper:
            assert await reaper.reconcile_once() == 1
        assert client.get(job.id).status == "queued"
    assert await outcomes(sessions, job.id) == ["lease_expired"]


# -- graceful shutdown ---------------------------------------------------------------------


async def _wait_running(sessions: async_sessionmaker[AsyncSession], job_id: uuid.UUID) -> None:
    for _ in range(200):
        async with sessions() as s:
            if (await s.get(Job, job_id, populate_existing=True)).status == "running":  # type: ignore[union-attr]
                return
        await asyncio.sleep(0.02)
    raise TimeoutError


@pytest.mark.parametrize(
    ("job_seconds", "grace", "expected_status", "expected_outcome"),
    [(0.3, 5.0, "succeeded", "succeeded"), (30.0, 0.2, "queued", "released")],
    ids=["finishes-within-grace", "released-after-grace"],
)
async def test_sigterm_finishes_or_releases_inflight_job(
    api_url: str,
    settings: Settings,
    broker: Broker,
    sessions: async_sessionmaker[AsyncSession],
    job_seconds: float,
    grace: float,
    expected_status: str,
    expected_outcome: str,
) -> None:
    s = dataclasses.replace(settings, shutdown_grace=grace)
    stop = asyncio.Event()
    async with open_worker(s) as worker:
        with JobqClient(api_url) as client:
            job = client.enqueue("sleep", {"seconds": job_seconds})
        task = asyncio.create_task(worker.run_forever(stop))
        await _wait_running(sessions, job.id)
        stop.set()
        await asyncio.wait_for(task, timeout=5)
    async with sessions() as sess:
        final = await sess.get(Job, job.id)
    assert final is not None and final.status == expected_status
    assert await outcomes(sessions, job.id) == [expected_outcome]
    counts = await broker.counts()
    assert counts.inflight == 0
    assert counts.ready_total == (1 if expected_status == "queued" else 0)


# -- backpressure and idempotency --------------------------------------------------------------


async def test_backpressure_returns_429(settings: Settings) -> None:
    with serve_api(dataclasses.replace(settings, max_queue_depth=3)) as url, JobqClient(url) as c:
        for _ in range(3):
            c.enqueue("echo")
        with pytest.raises(QueueFullError) as ei:
            c.enqueue("echo")
    assert ei.value.status_code == 429
    assert ei.value.retry_after == 1.0


async def test_duplicate_enqueue_1000_keys_sent_twice_creates_1000_jobs(
    api_url: str, broker: Broker, sessions: async_sessionmaker[AsyncSession]
) -> None:
    limit = asyncio.Semaphore(100)

    async def post(http: httpx.AsyncClient, i: int) -> int:
        async with limit:
            r = await http.post(
                "/jobs",
                json={"type": "echo", "payload": {"i": i}},
                headers={"Idempotency-Key": f"dup-{i}"},
            )
            return r.status_code

    async with httpx.AsyncClient(base_url=api_url, timeout=30) as http:
        # Both copies of each request are in flight at the same time.
        codes = await asyncio.gather(*(post(http, i) for i in range(1000) for _ in range(2)))

    assert sorted(set(codes)) == [200, 201]
    assert codes.count(201) == 1000
    assert codes.count(200) == 1000
    async with sessions() as s:
        assert (await s.execute(text("SELECT count(*) FROM jobs"))).scalar_one() == 1000
    assert await broker.depth() == 1000


async def test_idempotency_key_reuse_with_different_payload_is_409(api_url: str) -> None:
    async with httpx.AsyncClient(base_url=api_url) as http:
        h = {"Idempotency-Key": "same"}
        assert (await http.post("/jobs", json={"type": "echo"}, headers=h)).status_code == 201
        r = await http.post("/jobs", json={"type": "echo", "payload": {"x": 1}}, headers=h)
    assert r.status_code == 409


async def test_per_type_default_max_attempts(settings: Settings) -> None:
    s = dataclasses.replace(settings, max_attempts_by_type={"send_invoice": 10})
    with serve_api(s) as url, JobqClient(url) as c:
        assert c.enqueue("send_invoice").max_attempts == 10
        assert c.enqueue("echo").max_attempts == 3
        assert c.enqueue("send_invoice", max_attempts=2).max_attempts == 2
