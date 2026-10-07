"""Cron expression helpers (standard 5-field syntax, evaluated in UTC)."""

from __future__ import annotations

from datetime import UTC, datetime

from croniter import croniter


def is_valid(expr: str) -> bool:
    return bool(croniter.is_valid(expr)) and len(expr.split()) == 5


def next_fire(expr: str, after: datetime) -> datetime:
    """First fire time strictly after ``after``."""
    nxt = croniter(expr, after.astimezone(UTC)).get_next(datetime)
    assert isinstance(nxt, datetime)
    return nxt.astimezone(UTC)
