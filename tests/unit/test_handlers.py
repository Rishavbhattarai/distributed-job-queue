from __future__ import annotations

from typing import Any

import pytest

from jobq.handlers import HandlerRegistry, UnknownJobTypeError, registry


async def test_demo_handlers_registered() -> None:
    assert {"sleep", "fail_randomly", "echo"} <= set(registry.types())


async def test_sleep_handler() -> None:
    assert await registry.get("sleep")({"seconds": 0}) == {"slept": 0.0}


async def test_sleep_rejects_negative() -> None:
    with pytest.raises(ValueError):
        await registry.get("sleep")({"seconds": -1})


async def test_fail_randomly_extremes() -> None:
    fn = registry.get("fail_randomly")
    with pytest.raises(RuntimeError, match="random failure"):
        await fn({"probability": 1.0})
    assert "roll" in await fn({"probability": 0.0})


async def test_echo() -> None:
    assert await registry.get("echo")({"a": [1, 2]}) == {"a": [1, 2]}


def test_registry_unknown_and_duplicate() -> None:
    reg = HandlerRegistry()

    @reg.handler("x")
    async def x(payload: dict[str, Any]) -> None:
        return None

    assert "x" in reg
    assert reg.get("x") is x
    with pytest.raises(UnknownJobTypeError):
        reg.get("nope")
    with pytest.raises(ValueError, match="already registered"):
        reg.register("x", x)
