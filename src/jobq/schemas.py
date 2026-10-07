"""HTTP request/response schemas (the wire contract the client library depends on)."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import AwareDatetime, BaseModel, Field, field_validator

from jobq import cron
from jobq.models import PRIORITY_NAMES, Job, PriorityName, Schedule


class JobCreate(BaseModel):
    type: str = Field(min_length=1, max_length=200, examples=["sleep"])
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: PriorityName = "normal"
    run_at: AwareDatetime | None = Field(
        default=None, description="Earliest time to run (timezone-aware). Omit to run now."
    )
    max_attempts: int | None = Field(
        default=None,
        ge=1,
        le=100,
        description="Omit to use the per-type default (JOBQ_MAX_ATTEMPTS_BY_TYPE) or 3.",
    )


class JobOut(BaseModel):
    id: uuid.UUID
    type: str
    payload: dict[str, Any]
    priority: PriorityName
    status: str
    attempts: int
    max_attempts: int
    run_at: datetime
    idempotency_key: str | None
    result: Any | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_model(cls, job: Job) -> JobOut:
        return cls(
            id=job.id,
            type=job.type,
            payload=job.payload,
            priority=PRIORITY_NAMES[job.priority],
            status=job.status,
            attempts=job.attempts,
            max_attempts=job.max_attempts,
            run_at=job.run_at,
            idempotency_key=job.idempotency_key,
            result=job.result,
            last_error=job.last_error,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )


class JobList(BaseModel):
    items: list[JobOut]
    total: int


class Replay(BaseModel):
    extra_attempts: int = Field(
        default=3, ge=1, le=100, description="How many more attempts the replayed job gets."
    )


class ScheduleIn(BaseModel):
    cron: str = Field(examples=["*/5 * * * *"], description="5-field cron expression, UTC.")
    job_type: str = Field(min_length=1, max_length=200)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: PriorityName = "normal"
    max_attempts: int | None = Field(default=None, ge=1, le=100)
    enabled: bool = True

    @field_validator("cron")
    @classmethod
    def _valid_cron(cls, v: str) -> str:
        if not cron.is_valid(v):
            raise ValueError("invalid 5-field cron expression")
        return v


class ScheduleOut(BaseModel):
    name: str
    cron: str
    job_type: str
    payload: dict[str, Any]
    priority: PriorityName
    max_attempts: int | None
    enabled: bool
    next_run_at: datetime
    last_enqueued_at: datetime | None

    @classmethod
    def from_model(cls, s: Schedule) -> ScheduleOut:
        return cls(
            name=s.name,
            cron=s.cron,
            job_type=s.job_type,
            payload=s.payload,
            priority=PRIORITY_NAMES[s.priority],
            max_attempts=s.max_attempts,
            enabled=s.enabled,
            next_run_at=s.next_run_at,
            last_enqueued_at=s.last_enqueued_at,
        )
