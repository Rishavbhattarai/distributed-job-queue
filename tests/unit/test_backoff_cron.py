from __future__ import annotations

import random
from datetime import UTC, datetime

import pytest

from jobq import cron
from jobq.backoff import full_jitter
from jobq.schemas import ScheduleIn


def test_full_jitter_stays_within_exponential_ceiling() -> None:
    rng = random.Random(42)
    for attempt, ceiling in [(1, 1.0), (2, 2.0), (3, 4.0), (5, 16.0)]:
        samples = [full_jitter(attempt, base=1.0, cap=300, rng=rng) for _ in range(500)]
        assert all(0 <= x <= ceiling for x in samples)
        # Full jitter uses the whole window, not just the top of it.
        assert min(samples) < ceiling * 0.1
        assert max(samples) > ceiling * 0.9


def test_full_jitter_respects_cap_and_huge_attempts() -> None:
    rng = random.Random(1)
    assert all(full_jitter(10_000, base=1, cap=30, rng=rng) <= 30 for _ in range(100))


def test_full_jitter_rejects_attempt_zero() -> None:
    with pytest.raises(ValueError):
        full_jitter(0, base=1, cap=1)


def test_cron_next_fire() -> None:
    after = datetime(2026, 1, 1, 3, 0, tzinfo=UTC)
    assert cron.next_fire("0 3 * * *", after) == datetime(2026, 1, 2, 3, 0, tzinfo=UTC)
    assert cron.next_fire("*/15 * * * *", after) == datetime(2026, 1, 1, 3, 15, tzinfo=UTC)


@pytest.mark.parametrize(
    ("expr", "ok"),
    [("*/5 * * * *", True), ("0 0 1 1 *", True), ("bad", False), ("* * * * * *", False)],
)
def test_cron_validation(expr: str, ok: bool) -> None:
    assert cron.is_valid(expr) is ok


def test_schedule_schema_rejects_bad_cron() -> None:
    with pytest.raises(ValueError):
        ScheduleIn(cron="61 * * * *", job_type="x")
