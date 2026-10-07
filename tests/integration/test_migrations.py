"""The Alembic migration must produce exactly the schema the SQLAlchemy models describe."""

from __future__ import annotations

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

from jobq.models import Base

pytestmark = pytest.mark.integration


async def test_models_match_migrations(engine: AsyncEngine) -> None:
    def diff(conn: Connection) -> list[object]:
        ctx = MigrationContext.configure(conn, opts={"compare_type": True})
        return list(compare_metadata(ctx, Base.metadata))

    async with engine.connect() as conn:
        assert await conn.run_sync(diff) == []
