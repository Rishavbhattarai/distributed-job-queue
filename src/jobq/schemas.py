"""HTTP request/response schemas (the wire contract the client library depends on)."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import AwareDatetime, BaseModel, Field

from jobq.models import PRIORITY_NAMES, Job, PriorityName


class JobCreate(BaseModel):
    type: str = Field(min_length=1, max_length=200, examples=["sleep"])
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: PriorityName = "normal"
    run_at: AwareDatetime | None = Field(
        default=None, description="Earliest time to run (timezone-aware). Omit to run now."
    )
    max_attempts: int = Field(default=3, ge=1, le=100)


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
