"""Week 1 milestone: enqueue -> worker -> succeeded, through the real HTTP API + client."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobq.broker import Broker
from jobq.client import JobNotFoundError, JobqClient
from jobq.models import JobAttempt
from jobq.reaper import Reaper
from jobq.worker import Worker

pytestmark = pytest.mark.integration


async def _attempts(
    sessions: async_sessionmaker[AsyncSession], job_id: uuid.UUID
) -> list[JobAttempt]:
    async with sessions() as s:
        rows = await s.execute(
            select(JobAttempt).where(JobAttempt.job_id == job_id).order_by(JobAttempt.id)
        )
        return list(rows.scalars())


async def test_enqueue_worker_succeeded(
    api_url: str, worker: Worker, sessions: async_sessionmaker[AsyncSession]
) -> None:
    with JobqClient(api_url) as client:
        job = client.enqueue("sleep", {"seconds": 0.01})
        assert job.status == "queued"
        assert job.attempts == 0

        processed = await worker.run_once(timeout=2)
        assert processed == job.id

        done = client.get(job.id)
    assert done.status == "succeeded"
    assert done.attempts == 1
    assert done.result == {"slept": 0.01}
    assert done.last_error is None

    attempts = await _attempts(sessions, job.id)
    assert len(attempts) == 1
    a = attempts[0]
    assert (a.attempt_number, a.outcome, a.worker_id) == (1, "succeeded", "test-worker")
    assert a.finished_at is not None and a.finished_at >= a.started_at


async def test_failing_handler_with_one_attempt_goes_dead(
    api_url: str, worker: Worker, sessions: async_sessionmaker[AsyncSession]
) -> None:
    with JobqClient(api_url) as client:
        job = client.enqueue("fail_randomly", {"probability": 1.0}, max_attempts=1)
        await worker.run_once(timeout=2)
        done = client.get(job.id)
    assert done.status == "dead"
    assert done.last_error is not None and "random failure" in done.last_error
    attempts = await _attempts(sessions, job.id)
    assert [a.outcome for a in attempts] == ["failed"]
    assert attempts[0].error and "RuntimeError" in attempts[0].error


async def test_unknown_job_type_is_dead_without_retries(api_url: str, worker: Worker) -> None:
    with JobqClient(api_url) as client:
        job = client.enqueue("no_such_handler", max_attempts=5)
        await worker.run_once(timeout=2)
        done = client.get(job.id)
    assert done.status == "dead"
    assert done.attempts == 1
    assert done.last_error is not None and "no handler registered" in done.last_error


async def test_duplicate_idempotency_key_returns_original(api_url: str, broker: Broker) -> None:
    key = f"k-{uuid.uuid4()}"
    async with httpx.AsyncClient(base_url=api_url) as http:
        r1 = await http.post("/jobs", json={"type": "echo"}, headers={"Idempotency-Key": key})
        r2 = await http.post("/jobs", json={"type": "echo"}, headers={"Idempotency-Key": key})
    assert (r1.status_code, r2.status_code) == (201, 200)
    assert r1.json()["id"] == r2.json()["id"]
    assert await broker.depth() == 1  # enqueued to Redis exactly once


async def test_redelivered_job_is_not_run_twice(
    api_url: str, worker: Worker, broker: Broker, sessions: async_sessionmaker[AsyncSession]
) -> None:
    with JobqClient(api_url) as client:
        job = client.enqueue("echo", {"n": 1})
    await worker.run_once(timeout=2)
    # Simulate an at-least-once duplicate delivery of the same id.
    await broker.enqueue(job.id, "normal")
    assert await worker.run_once(timeout=2) == job.id
    assert len(await _attempts(sessions, job.id)) == 1


async def test_priority_order(api_url: str, worker: Worker) -> None:
    with JobqClient(api_url) as client:
        low = client.enqueue("echo", priority="low")
        high = client.enqueue("echo", priority="high")
        normal = client.enqueue("echo", priority="normal")
    order = [await worker.run_once(timeout=2) for _ in range(3)]
    assert order == [high.id, normal.id, low.id]


async def test_delayed_job_waits_for_run_at(api_url: str, worker: Worker, reaper: Reaper) -> None:
    with JobqClient(api_url) as client:
        job = client.enqueue("echo", run_at=datetime.now(UTC) + timedelta(seconds=1))
        assert await reaper.promote_once() == 0
        assert await worker.run_once(timeout=0.2) is None
        await asyncio.sleep(1.0)
        assert await reaper.promote_once() == 1
        assert await worker.run_once(timeout=2) == job.id
        assert client.get(job.id).status == "succeeded"


def test_get_unknown_job_404(api_url: str) -> None:
    with JobqClient(api_url) as client, pytest.raises(JobNotFoundError):
        client.get(uuid.uuid4())


def test_health_endpoints(api_url: str) -> None:
    with httpx.Client(base_url=api_url) as http:
        assert http.get("/healthz").json() == {"status": "ok"}
        ready = http.get("/readyz")
    assert ready.status_code == 200
    assert ready.json()["checks"] == {"postgres": "ok", "redis": "ok"}
