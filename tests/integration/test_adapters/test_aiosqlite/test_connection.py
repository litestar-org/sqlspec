"""Test aiosqlite connection configuration."""

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.sql.result import SelectResult


@pytest.mark.xdist_group("sqlite")
@pytest.mark.asyncio
async def test_connection() -> None:
    """Test connection components."""
    # Test direct connection
    config = AiosqliteConfig()

    async with config.provide_connection() as conn:
        assert conn is not None
        # Test basic query
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            result = await cur.fetchone()
            assert result == (1,)

    # Test session management
    async with config.provide_session() as session:
        assert session is not None
        # Test basic query through session
        sql = "SELECT 1"
        select_result = await session.execute(sql)
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert len(select_result.rows) == 1
        assert select_result.column_names is not None
        result = select_result.rows[0][select_result.column_names[0]]
        assert result == 1
