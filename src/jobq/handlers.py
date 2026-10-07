"""Handler registry plus a few demo handlers.

A handler is ``async def fn(payload: dict) -> JSON-serialisable result``. Raising any
exception fails the attempt; the job is retried with backoff until ``max_attempts``, then
moved to the dead-letter queue. Raise ``PermanentError`` to skip the remaining retries.

Other code (e.g. Project 3) registers its own handlers with ``@registry.handler("name")``
in a module listed in ``JOBQ_HANDLER_MODULES``; the worker imports those at startup.

Handlers must be idempotent: delivery is at-least-once (see docs/adr/0001).
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import Any

Handler = Callable[[dict[str, Any]], Awaitable[Any]]


class PermanentError(Exception):
    """Raise from a handler when retrying cannot help (bad input, 4xx from a dependency)."""


class UnknownJobTypeError(LookupError):
    """No handler for this job type in this worker. Treated as permanent (dead-lettered)."""


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, job_type: str, fn: Handler) -> None:
        if job_type in self._handlers:
            raise ValueError(f"handler for {job_type!r} already registered")
        self._handlers[job_type] = fn

    def handler(self, job_type: str) -> Callable[[Handler], Handler]:
        def decorator(fn: Handler) -> Handler:
            self.register(job_type, fn)
            return fn

        return decorator

    def get(self, job_type: str) -> Handler:
        try:
            return self._handlers[job_type]
        except KeyError:
            raise UnknownJobTypeError(f"no handler registered for job type {job_type!r}") from None

    def __contains__(self, job_type: object) -> bool:
        return job_type in self._handlers

    def types(self) -> list[str]:
        return sorted(self._handlers)


# The process-wide default registry used by the worker entrypoint.
registry = HandlerRegistry()


@registry.handler("sleep")
async def sleep_handler(payload: dict[str, Any]) -> dict[str, Any]:
    """Sleep for ``payload["seconds"]`` (default 0.1). Simulates I/O-bound work."""
    seconds = float(payload.get("seconds", 0.1))
    if seconds < 0:
        raise ValueError("seconds must be >= 0")
    await asyncio.sleep(seconds)
    return {"slept": seconds}


@registry.handler("fail_randomly")
async def fail_randomly_handler(payload: dict[str, Any]) -> dict[str, Any]:
    """Fail with probability ``payload["probability"]`` (default 0.5)."""
    probability = float(payload.get("probability", 0.5))
    roll = random.random()
    if roll < probability:
        raise RuntimeError(f"random failure (roll={roll:.3f} < p={probability})")
    return {"roll": roll}


@registry.handler("echo")
async def echo_handler(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the payload unchanged. Handy for smoke tests."""
    return payload
