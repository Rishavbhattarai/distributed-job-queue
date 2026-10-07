"""Cron schedules, metrics and gauges."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobq import metrics
from jobq.models import Schedule
from jobq.reaper import Reaper
from jobq.worker import Worker

pytestmark = pytest.mark.integration


async def _make_due(sessions: async_sessionmaker[AsyncSession], name: str, at: datetime) -> None:
    async with sessions() as s, s.begin():
        await s.execute(update(Schedule).where(Schedule.name == name).values(next_run_at=at))


async def _job_count(sessions: async_sessionmaker[AsyncSession]) -> int:
    async with sessions() as s:
        return int((await s.execute(text("SELECT count(*) FROM jobs"))).scalar_one())


async def test_schedule_crud_and_exactly_once_per_tick(
    api_url: str, reaper: Reaper, worker: Worker, sessions: async_sessionmaker[AsyncSession]
) -> None:
    async with httpx.AsyncClient(base_url=api_url) as http:
        bad = await http.put("/schedules/x", json={"cron": "every minute", "job_type": "echo"})
        assert bad.status_code == 422

        r = await http.put(
            "/schedules/nightly",
            json={"cron": "0 3 * * *", "job_type": "echo", "payload": {"k": 1}},
        )
        assert r.status_code == 200
        nxt = datetime.fromisoformat(r.json()["next_run_at"])
        assert (nxt.hour, nxt.minute) == (3, 0)
        assert [s["name"] for s in (await http.get("/schedules")).json()] == ["nightly"]

        assert await reaper.schedule_once() == 0  # not due yet

        tick = datetime(2026, 1, 1, 3, 0, tzinfo=UTC)
        await _make_due(sessions, "nightly", tick)
        assert await reaper.schedule_once() == 1
        assert await reaper.schedule_once() == 0  # next_run_at moved to the future

        # A second scheduler replaying the same tick (crash, or two reapers) is a no-op:
        # the job's idempotency key is derived from the schedule name and tick time.
        await _make_due(sessions, "nightly", tick)
        assert await reaper.schedule_once() == 0
        assert await _job_count(sessions) == 1

        got = (await http.get("/schedules/nightly")).json()
        assert datetime.fromisoformat(got["next_run_at"]) > datetime.now(UTC)
        assert got["last_enqueued_at"] is not None

        job_id = await worker.run_once(timeout=1)
        assert job_id is not None
        job = (await http.get(f"/jobs/{job_id}")).json()
        assert job["status"] == "succeeded"
        assert job["idempotency_key"] == "schedule:nightly:2026-01-01T03:00:00Z"

        assert (await http.delete("/schedules/nightly")).status_code == 204
        assert (await http.get("/schedules/nightly")).status_code == 404


async def test_missed_ticks_collapse_into_one_run(
    api_url: str, reaper: Reaper, sessions: async_sessionmaker[AsyncSession]
) -> None:
    async with httpx.AsyncClient(base_url=api_url) as http:
        await http.put("/schedules/every-min", json={"cron": "* * * * *", "job_type": "echo"})
    await _make_due(sessions, "every-min", datetime.now(UTC) - timedelta(hours=2))
    assert await reaper.schedule_once() == 1
    assert await reaper.schedule_once() == 0


async def test_metrics_endpoint_and_gauges(api_url: str, worker: Worker, reaper: Reaper) -> None:
    async with httpx.AsyncClient(base_url=api_url) as http:
        await http.post(
            "/jobs",
            json={"type": "fail_randomly", "payload": {"probability": 1}, "max_attempts": 1},
        )
        await http.post("/jobs", json={"type": "echo"})
        body = (await http.get("/metrics/")).text
    assert "jobq_jobs_enqueued_total" in body
    await worker.run_once(timeout=1)
    await worker.run_once(timeout=1)
    await reaper.update_gauges()
    assert metrics.DLQ_SIZE._value.get() == 1
    assert metrics.INFLIGHT._value.get() == 0
