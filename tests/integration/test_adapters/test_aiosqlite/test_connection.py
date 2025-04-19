"""Test aiosqlite connection configuration."""

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig


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
        result = await session.select_value(sql)
