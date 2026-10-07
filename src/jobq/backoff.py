"""Retry delay: exponential backoff with full jitter (see docs/adr/0003).

delay = uniform(0, min(cap, base * 2 ** (attempt - 1)))

Full jitter spreads retries of jobs that failed together (e.g. a downstream outage) across
the whole window, so they do not come back as a synchronized wave.
"""

from __future__ import annotations

import random


def full_jitter(
    attempt: int, *, base: float, cap: float, rng: random.Random | None = None
) -> float:
    """Seconds to wait before retry number ``attempt`` (1 = first retry)."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    # Clamp the exponent so huge attempt counts do not overflow the float.
    ceiling = min(cap, base * 2 ** min(attempt - 1, 62))
    return (rng or random).uniform(0, ceiling)
