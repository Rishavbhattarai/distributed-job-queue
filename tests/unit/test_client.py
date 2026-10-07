from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from jobq.client import (
    AsyncJobqClient,
    Job,
    JobNotFoundError,
    JobqClient,
    JobqHTTPError,
)

JOB_ID = uuid.uuid4()


def job_json(**overrides: Any) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    base: dict[str, Any] = {
        "id": str(JOB_ID),
        "type": "sleep",
        "payload": {"seconds": 1},
        "priority": "normal",
        "status": "queued",
        "attempts": 0,
        "max_attempts": 3,
        "run_at": now,
        "idempotency_key": None,
        "result": None,
        "last_error": None,
        "created_at": now,
        "updated_at": now,
    }
    base.update(overrides)
    return base


class Recorder:
    def __init__(self, responses: list[httpx.Response]) -> None:
        self.requests: list[httpx.Request] = []
        self._responses = responses

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._responses.pop(0)


def test_enqueue_sends_body_and_idempotency_header() -> None:
    rec = Recorder([httpx.Response(201, json=job_json(idempotency_key="k1", priority="high"))])
    run_at = datetime.now(UTC) + timedelta(minutes=5)
    with JobqClient("http://jobq.test", transport=httpx.MockTransport(rec)) as c:
        job = c.enqueue(
            "sleep", {"seconds": 1}, idempotency_key="k1", priority="high", run_at=run_at
        )

    req = rec.requests[0]
    assert req.method == "POST"
    assert req.url.path == "/jobs"
    assert req.headers["Idempotency-Key"] == "k1"
    body = json.loads(req.content)
    assert body == {
        "type": "sleep",
        "payload": {"seconds": 1},
        "priority": "high",
        "run_at": run_at.isoformat(),
    }
    assert isinstance(job, Job)
    assert job.id == JOB_ID
    assert job.priority == "high"
    assert not job.is_terminal


def test_enqueue_without_key_sends_no_header_and_defaults() -> None:
    rec = Recorder([httpx.Response(201, json=job_json())])
    with JobqClient("http://jobq.test", transport=httpx.MockTransport(rec)) as c:
        c.enqueue("echo")
    req = rec.requests[0]
    assert "Idempotency-Key" not in req.headers
    assert json.loads(req.content) == {"type": "echo", "payload": {}, "priority": "normal"}


def test_naive_run_at_rejected() -> None:
    with (
        JobqClient("http://jobq.test", transport=httpx.MockTransport(Recorder([]))) as c,
        pytest.raises(ValueError, match="timezone-aware"),
    ):
        c.enqueue("sleep", run_at=datetime(2030, 1, 1))


def test_get_404_raises_not_found() -> None:
    rec = Recorder([httpx.Response(404, json={"detail": "job not found"})])
    with (
        JobqClient("http://jobq.test", transport=httpx.MockTransport(rec)) as c,
        pytest.raises(JobNotFoundError),
    ):
        c.get(JOB_ID)
    assert rec.requests[0].url.path == f"/jobs/{JOB_ID}"


def test_server_error_raises_http_error() -> None:
    rec = Recorder([httpx.Response(500, text="boom")])
    with (
        JobqClient("http://jobq.test", transport=httpx.MockTransport(rec)) as c,
        pytest.raises(JobqHTTPError) as ei,
    ):
        c.enqueue("sleep")
    assert ei.value.status_code == 500


def test_wait_polls_until_terminal() -> None:
    rec = Recorder(
        [
            httpx.Response(200, json=job_json(status="queued")),
            httpx.Response(200, json=job_json(status="running", attempts=1)),
            httpx.Response(200, json=job_json(status="succeeded", attempts=1, result={"ok": 1})),
        ]
    )
    with JobqClient("http://jobq.test", transport=httpx.MockTransport(rec)) as c:
        job = c.wait(JOB_ID, timeout=5, poll_interval=0)
    assert job.status == "succeeded"
    assert job.result == {"ok": 1}
    assert len(rec.requests) == 3


def test_wait_times_out() -> None:
    def always_queued(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=job_json())

    with (
        JobqClient("http://jobq.test", transport=httpx.MockTransport(always_queued)) as c,
        pytest.raises(TimeoutError),
    ):
        c.wait(JOB_ID, timeout=0.05, poll_interval=0.01)


async def test_async_client_roundtrip() -> None:
    rec = Recorder(
        [
            httpx.Response(201, json=job_json()),
            httpx.Response(200, json=job_json(status="failed", last_error="x")),
        ]
    )
    async with AsyncJobqClient("http://jobq.test", transport=httpx.MockTransport(rec)) as c:
        job = await c.enqueue("sleep", {"seconds": 1})
        done = await c.wait(job.id, timeout=1, poll_interval=0)
    assert done.status == "failed"
    assert done.is_terminal


def test_module_level_functions_use_jobq_url(monkeypatch: pytest.MonkeyPatch) -> None:
    import jobq.client as client_mod

    monkeypatch.setenv("JOBQ_URL", "http://from-env.test")
    monkeypatch.setattr(client_mod, "_default_client", None)
    c = client_mod._client()
    try:
        assert str(c._http.base_url) == "http://from-env.test"
    finally:
        c.close()
        monkeypatch.setattr(client_mod, "_default_client", None)


def test_top_level_package_exports_client() -> None:
    import jobq

    assert jobq.enqueue is not None
    assert jobq.get is not None
    assert jobq.JobqClient is JobqClient
