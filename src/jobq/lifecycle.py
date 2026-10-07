"""Job state transitions in Postgres. Every transition is a conditional UPDATE.

``jobs.attempts`` doubles as a fencing token: a worker that claimed attempt N may only
finish the job while ``status = 'running' AND attempts = N``. If its lease expired and the
reaper requeued the job (or another worker claimed attempt N+1), its late result is
discarded instead of overwriting newer state. See docs/adr/0002.

Callers own the transaction: call these inside ``session.begin()`` or commit afterwards.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import bindparam, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from jobq.backoff import full_jitter
from jobq.models import PRIORITY_NAMES, AttemptOutcome, Job, JobAttempt, JobStatus, PriorityName


@dataclass(frozen=True)
class Claimed:
    job_id: uuid.UUID
    type: str
    payload: dict[str, Any]
    priority: PriorityName
    attempt_number: int
    max_attempts: int
    # When the job became runnable (created, or its retry delay ended).
    runnable_at: datetime
    started_at: datetime


@dataclass(frozen=True)
class Transition:
    """Result of a failure/release: the job's new status and, if queued, when to run it."""

    job_id: uuid.UUID
    status: JobStatus
    priority: PriorityName
    run_at: datetime | None
    attempts: int


@dataclass(frozen=True)
class RetryPolicy:
    backoff_base: float = 1.0
    backoff_cap: float = 300.0

    def delay(self, attempt: int) -> float:
        return full_jitter(attempt, base=self.backoff_base, cap=self.backoff_cap)


_CLAIM_SQL = text(
    """
    WITH j AS (
        UPDATE jobs
           SET status = 'running', attempts = attempts + 1, updated_at = now()
         WHERE id = :job_id AND status = 'queued'
     RETURNING id, type, payload, priority, attempts, max_attempts, run_at
    ), a AS (
        INSERT INTO job_attempts (job_id, attempt_number, worker_id, started_at)
        SELECT id, attempts, :worker_id, now() FROM j
     RETURNING started_at
    )
    SELECT j.type, j.payload, j.priority, j.attempts, j.max_attempts, j.run_at, a.started_at
      FROM j, a
    """
)

_COMPLETE_SQL = text(
    """
    WITH j AS (
        UPDATE jobs
           SET status = 'succeeded', result = :result, last_error = NULL, updated_at = now()
         WHERE id = :job_id AND status = 'running' AND attempts = :attempt
     RETURNING id
    ), a AS (
        UPDATE job_attempts
           SET finished_at = now(), outcome = 'succeeded'
         WHERE job_id IN (SELECT id FROM j) AND attempt_number = :attempt AND outcome IS NULL
     RETURNING 1
    )
    SELECT count(*) FROM j
    """
).bindparams(bindparam("result", type_=JSONB))


async def claim(session: AsyncSession, job_id: uuid.UUID, worker_id: str) -> Claimed | None:
    """queued -> running, attempts += 1, insert the attempt row. None if not claimable."""
    row = (
        await session.execute(_CLAIM_SQL, {"job_id": job_id, "worker_id": worker_id})
    ).one_or_none()
    if row is None:
        return None
    return Claimed(
        job_id=job_id,
        type=row.type,
        payload=row.payload,
        priority=PRIORITY_NAMES[row.priority],
        attempt_number=row.attempts,
        max_attempts=row.max_attempts,
        runnable_at=row.run_at,
        started_at=row.started_at,
    )


async def complete(session: AsyncSession, claimed: Claimed, result: Any) -> bool:
    """running -> succeeded. False if fenced out (the lease was lost)."""
    count = (
        await session.execute(
            _COMPLETE_SQL,
            {"job_id": claimed.job_id, "attempt": claimed.attempt_number, "result": result},
        )
    ).scalar_one()
    return bool(count)


async def fail_attempt(
    session: AsyncSession,
    job_id: uuid.UUID,
    attempt_number: int,
    *,
    error: str,
    outcome: AttemptOutcome,
    policy: RetryPolicy,
    permanent: bool = False,
) -> Transition | None:
    """Close a running attempt as failed/expired, then retry with backoff or dead-letter.

    Returns None if the job is no longer at this attempt (fenced out).
    """
    job = (
        await session.execute(
            select(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.RUNNING.value,
                Job.attempts == attempt_number,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if job is None:
        return None

    now = datetime.now(UTC)
    await session.execute(
        update(JobAttempt)
        .where(
            JobAttempt.job_id == job_id,
            JobAttempt.attempt_number == attempt_number,
            JobAttempt.outcome.is_(None),
        )
        .values(finished_at=now, outcome=outcome.value, error=error)
    )

    if permanent or job.attempts >= job.max_attempts:
        new_status, run_at = JobStatus.DEAD, None
    else:
        new_status = JobStatus.QUEUED
        run_at = now + timedelta(seconds=policy.delay(job.attempts))

    values: dict[str, Any] = {"status": new_status.value, "last_error": error, "updated_at": now}
    if run_at is not None:
        values["run_at"] = run_at
    await session.execute(update(Job).where(Job.id == job_id).values(**values))
    return Transition(job_id, new_status, PRIORITY_NAMES[job.priority], run_at, job.attempts)


async def release(
    session: AsyncSession, job_id: uuid.UUID, attempt_number: int
) -> Transition | None:
    """running -> queued without a failure (graceful shutdown). The attempt still counts."""
    now = datetime.now(UTC)
    row = (
        await session.execute(
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.RUNNING.value,
                Job.attempts == attempt_number,
            )
            .values(status=JobStatus.QUEUED.value, run_at=now, updated_at=now)
            .returning(Job.priority, Job.attempts)
        )
    ).one_or_none()
    if row is None:
        return None
    await session.execute(
        update(JobAttempt)
        .where(
            JobAttempt.job_id == job_id,
            JobAttempt.attempt_number == attempt_number,
            JobAttempt.outcome.is_(None),
        )
        .values(finished_at=now, outcome=AttemptOutcome.RELEASED.value)
    )
    return Transition(job_id, JobStatus.QUEUED, PRIORITY_NAMES[row.priority], None, row.attempts)


async def replay(session: AsyncSession, job_id: uuid.UUID, extra_attempts: int) -> Job | None:
    """dead -> queued with ``extra_attempts`` more tries. None if the job is not dead.

    ``attempts`` never goes down (it numbers attempt rows and fences workers), so the
    budget grows instead: max_attempts = attempts + extra_attempts.
    """
    now = datetime.now(UTC)
    job = (
        await session.execute(
            update(Job)
            .where(Job.id == job_id, Job.status == JobStatus.DEAD.value)
            .values(
                status=JobStatus.QUEUED.value,
                max_attempts=Job.attempts + extra_attempts,
                run_at=now,
                updated_at=now,
            )
            .returning(Job)
        )
    ).scalar_one_or_none()
    return job
