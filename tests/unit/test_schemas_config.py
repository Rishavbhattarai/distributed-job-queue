from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from jobq.config import Settings
from jobq.models import PRIORITIES_IN_ORDER, PRIORITY_NAMES, PRIORITY_VALUES
from jobq.schemas import JobCreate


def test_job_create_defaults() -> None:
    jc = JobCreate(type="sleep")
    assert jc.priority == "normal"
    assert jc.payload == {}
    assert jc.max_attempts == 3
    assert jc.run_at is None


@pytest.mark.parametrize(
    "bad",
    [
        {"type": ""},
        {"type": "x", "priority": "urgent"},
        {"type": "x", "max_attempts": 0},
        {"type": "x", "run_at": "2030-01-01T00:00:00"},  # naive datetime
        {"type": "x", "payload": [1, 2]},
    ],
)
def test_job_create_rejects(bad: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        JobCreate.model_validate(bad)


def test_job_create_accepts_aware_run_at() -> None:
    jc = JobCreate.model_validate({"type": "x", "run_at": "2030-01-01T00:00:00Z"})
    assert isinstance(jc.run_at, datetime)
    assert jc.run_at.tzinfo is not None


def test_priority_mapping_roundtrips_in_order() -> None:
    assert [PRIORITY_VALUES[p] for p in PRIORITIES_IN_ORDER] == [0, 1, 2]
    for name, value in PRIORITY_VALUES.items():
        assert PRIORITY_NAMES[value] == name


def test_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JOBQ_DATABASE_URL", "postgresql+asyncpg://u:p@db/x")
    monkeypatch.setenv("JOBQ_REDIS_URL", "redis://r:6379/1")
    monkeypatch.setenv("JOBQ_HANDLER_MODULES", "a.b, c.d ,")
    monkeypatch.setenv("JOBQ_WORKER_ID", "w-1")
    monkeypatch.setenv("JOBQ_POLL_TIMEOUT", "0.5")
    s = Settings.from_env()
    assert s.database_url == "postgresql+asyncpg://u:p@db/x"
    assert s.redis_url == "redis://r:6379/1"
    assert s.handler_modules == ("a.b", "c.d")
    assert s.worker_id == "w-1"
    assert s.poll_timeout == 0.5
