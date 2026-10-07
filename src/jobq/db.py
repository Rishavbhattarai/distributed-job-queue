"""Async engine / session helpers."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def make_engine(database_url: str) -> AsyncEngine:
    # No pool_pre_ping: it costs a round trip per checkout (3 per job on the hot path).
    # Dead connections raise and are discarded by the pool; recycle bounds their age.
    return create_async_engine(database_url, pool_recycle=1800, pool_size=10, max_overflow=10)


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


def make_autocommit_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Sessions for single-statement writes: no BEGIN/COMMIT round trips.

    Only use for one statement per session (each statement commits on its own).
    """
    return async_sessionmaker(
        engine.execution_options(isolation_level="AUTOCOMMIT"), expire_on_commit=False
    )
