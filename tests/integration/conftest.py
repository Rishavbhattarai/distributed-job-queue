"""Integration fixtures: real Postgres + Redis.

Uses JOBQ_TEST_DATABASE_URL / JOBQ_TEST_REDIS_URL when set (CI service containers),
otherwise starts throwaway containers with testcontainers. Skips if neither is possible.
"""

from __future__ import annotations

import os
import socket
import threading
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import uvicorn
from alembic import command
from alembic.config import Config
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from jobq.api import create_app
from jobq.broker import Broker
from jobq.config import Settings
from jobq.db import make_engine, make_session_factory
from jobq.worker import Worker, open_worker

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Infra:
    database_url: str
    redis_url: str


def _alembic_config(database_url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    cfg.attributes["configure_logger"] = False
    return cfg


@pytest.fixture(scope="session")
def infra() -> Iterator[Infra]:
    db = os.environ.get("JOBQ_TEST_DATABASE_URL")
    rd = os.environ.get("JOBQ_TEST_REDIS_URL")
    if db and rd:
        yield Infra(db, rd)
        return
    try:
        import docker  # testcontainers dependency

        docker.from_env().ping()
    except Exception as exc:
        pytest.skip(f"no JOBQ_TEST_* env vars and Docker unavailable: {exc}")
    from testcontainers.postgres import PostgresContainer
    from testcontainers.redis import RedisContainer

    with (
        PostgresContainer("postgres:16-alpine", driver="asyncpg") as pg,
        RedisContainer("redis:7-alpine") as rc,
    ):
        redis_url = f"redis://{rc.get_container_host_ip()}:{rc.get_exposed_port(6379)}/0"
        yield Infra(pg.get_connection_url(), redis_url)


@pytest.fixture(scope="session")
def migrated(infra: Infra) -> Infra:
    # Sync fixture: alembic's env.py calls asyncio.run(), so it must run outside the test loop.
    cfg = _alembic_config(infra.database_url)
    command.downgrade(cfg, "base")
    command.upgrade(cfg, "head")
    return infra


@pytest.fixture
def settings(migrated: Infra) -> Settings:
    # A unique Redis prefix per test isolates queues without flushing a shared Redis.
    return Settings(
        database_url=migrated.database_url,
        redis_url=migrated.redis_url,
        redis_prefix=f"jobqtest-{uuid.uuid4().hex[:8]}",
        worker_id="test-worker",
        poll_timeout=0.5,
    )


@pytest.fixture
async def engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    eng = make_engine(settings.database_url)
    yield eng
    await eng.dispose()


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return make_session_factory(engine)


@pytest.fixture
async def broker(settings: Settings) -> AsyncIterator[Broker]:
    redis = Redis.from_url(settings.redis_url)
    yield Broker(redis, prefix=settings.redis_prefix)
    await redis.aclose()


@pytest.fixture
async def worker(settings: Settings) -> AsyncIterator[Worker]:
    async with open_worker(settings) as w:
        yield w


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


@pytest.fixture
def api_url(settings: Settings) -> Iterator[str]:
    """Run the real API under uvicorn in a background thread."""
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(create_app(settings), host="127.0.0.1", port=port, log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline or not thread.is_alive():
            raise RuntimeError("API server failed to start")
        time.sleep(0.02)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)
