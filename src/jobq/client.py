"""jobq client library -- a thin HTTP client for the jobq API.

STABLE PUBLIC INTERFACE (other projects import this; do not break it):

    from jobq.client import enqueue, get, wait, Job, JobqClient, AsyncJobqClient

    job = enqueue("send_invoice", {"invoice_id": 42}, idempotency_key="invoice-42")
    job = get(job.id)
    job = wait(job.id, timeout=30)   # poll until succeeded/failed/dead

Module-level functions use a shared client pointed at ``$JOBQ_URL``
(default ``http://localhost:8000``). Only depends on ``httpx``.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import httpx

__all__ = [
    "AsyncJobqClient",
    "Job",
    "JobNotFoundError",
    "JobqClient",
    "JobqError",
    "JobqHTTPError",
    "Priority",
    "QueueFullError",
    "enqueue",
    "get",
    "wait",
]

Priority = Literal["high", "normal", "low"]
TERMINAL_STATUSES = frozenset({"succeeded", "failed", "dead"})
DEFAULT_URL = "http://localhost:8000"


class JobqError(Exception):
    """Base class for client errors."""


class JobNotFoundError(JobqError):
    def __init__(self, job_id: uuid.UUID | str) -> None:
        super().__init__(f"job {job_id} not found")
        self.job_id = job_id


class JobqHTTPError(JobqError):
    def __init__(self, status_code: int, body: str) -> None:
        super().__init__(f"jobq API returned HTTP {status_code}: {body[:500]}")
        self.status_code = status_code
        self.body = body


class QueueFullError(JobqHTTPError):
    """HTTP 429: the queue is over its depth limit (backpressure). Retry after a pause."""

    def __init__(self, body: str, retry_after: float) -> None:
        super().__init__(429, body)
        self.retry_after = retry_after


@dataclass(frozen=True)
class Job:
    id: uuid.UUID
    type: str
    payload: dict[str, Any]
    priority: Priority
    status: str  # queued | running | succeeded | dead (in the DLQ)
    attempts: int
    max_attempts: int
    run_at: datetime
    idempotency_key: str | None
    result: Any
    last_error: str | None
    created_at: datetime
    updated_at: datetime

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Job:
        return cls(
            id=uuid.UUID(data["id"]),
            type=data["type"],
            payload=data["payload"],
            priority=data["priority"],
            status=data["status"],
            attempts=data["attempts"],
            max_attempts=data["max_attempts"],
            run_at=datetime.fromisoformat(data["run_at"]),
            idempotency_key=data.get("idempotency_key"),
            result=data.get("result"),
            last_error=data.get("last_error"),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
        )


def _build_request(
    job_type: str,
    payload: dict[str, Any] | None,
    idempotency_key: str | None,
    priority: Priority,
    run_at: datetime | None,
    max_attempts: int | None,
) -> tuple[dict[str, Any], dict[str, str]]:
    if run_at is not None and run_at.tzinfo is None:
        raise ValueError("run_at must be timezone-aware (e.g. datetime.now(UTC))")
    body: dict[str, Any] = {"type": job_type, "payload": payload or {}, "priority": priority}
    if run_at is not None:
        body["run_at"] = run_at.isoformat()
    if max_attempts is not None:
        body["max_attempts"] = max_attempts
    headers = {"Idempotency-Key": idempotency_key} if idempotency_key else {}
    return body, headers


def _parse(resp: httpx.Response, job_id: uuid.UUID | str | None = None) -> Job:
    if resp.status_code == 404 and job_id is not None:
        raise JobNotFoundError(job_id)
    if resp.status_code == 429:
        raise QueueFullError(resp.text, float(resp.headers.get("Retry-After", "1")))
    if resp.status_code >= 400:
        raise JobqHTTPError(resp.status_code, resp.text)
    return Job.from_json(resp.json())


class JobqClient:
    """Synchronous client. Use as a context manager or call ``close()``."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        timeout: float = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._http = httpx.Client(
            base_url=base_url or os.environ.get("JOBQ_URL", DEFAULT_URL),
            timeout=timeout,
            transport=transport,
        )

    def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
        priority: Priority = "normal",
        run_at: datetime | None = None,
        max_attempts: int | None = None,
    ) -> Job:
        """Create a job. With an ``idempotency_key``, repeat calls return the original job."""
        body, headers = _build_request(
            job_type, payload, idempotency_key, priority, run_at, max_attempts
        )
        return _parse(self._http.post("/jobs", json=body, headers=headers))

    def get(self, job_id: uuid.UUID | str) -> Job:
        """Fetch a job's current state. Raises JobNotFoundError on 404."""
        return _parse(self._http.get(f"/jobs/{job_id}"), job_id)

    def wait(
        self, job_id: uuid.UUID | str, *, timeout: float = 30.0, poll_interval: float = 0.2
    ) -> Job:
        """Poll until the job reaches a terminal status. Raises TimeoutError otherwise."""
        deadline = time.monotonic() + timeout
        while True:
            job = self.get(job_id)
            if job.is_terminal:
                return job
            if time.monotonic() >= deadline:
                raise TimeoutError(f"job {job_id} still {job.status!r} after {timeout}s")
            time.sleep(poll_interval)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> JobqClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class AsyncJobqClient:
    """asyncio client with the same methods as JobqClient (awaitable)."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._http = httpx.AsyncClient(
            base_url=base_url or os.environ.get("JOBQ_URL", DEFAULT_URL),
            timeout=timeout,
            transport=transport,
        )

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
        priority: Priority = "normal",
        run_at: datetime | None = None,
        max_attempts: int | None = None,
    ) -> Job:
        body, headers = _build_request(
            job_type, payload, idempotency_key, priority, run_at, max_attempts
        )
        return _parse(await self._http.post("/jobs", json=body, headers=headers))

    async def get(self, job_id: uuid.UUID | str) -> Job:
        return _parse(await self._http.get(f"/jobs/{job_id}"), job_id)

    async def wait(
        self, job_id: uuid.UUID | str, *, timeout: float = 30.0, poll_interval: float = 0.2
    ) -> Job:
        deadline = time.monotonic() + timeout
        while True:
            job = await self.get(job_id)
            if job.is_terminal:
                return job
            if time.monotonic() >= deadline:
                raise TimeoutError(f"job {job_id} still {job.status!r} after {timeout}s")
            await asyncio.sleep(poll_interval)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> AsyncJobqClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()


_default_client: JobqClient | None = None


def _client() -> JobqClient:
    global _default_client
    if _default_client is None:
        _default_client = JobqClient()
    return _default_client


def enqueue(
    job_type: str,
    payload: dict[str, Any] | None = None,
    *,
    idempotency_key: str | None = None,
    priority: Priority = "normal",
    run_at: datetime | None = None,
    max_attempts: int | None = None,
) -> Job:
    """Enqueue a job on the server at ``$JOBQ_URL``. See JobqClient.enqueue."""
    return _client().enqueue(
        job_type,
        payload,
        idempotency_key=idempotency_key,
        priority=priority,
        run_at=run_at,
        max_attempts=max_attempts,
    )


def get(job_id: uuid.UUID | str) -> Job:
    """Fetch a job from the server at ``$JOBQ_URL``. See JobqClient.get."""
    return _client().get(job_id)


def wait(job_id: uuid.UUID | str, *, timeout: float = 30.0, poll_interval: float = 0.2) -> Job:
    """Block until the job is terminal. See JobqClient.wait."""
    return _client().wait(job_id, timeout=timeout, poll_interval=poll_interval)
