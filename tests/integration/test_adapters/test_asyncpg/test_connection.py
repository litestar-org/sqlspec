import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig


@pytest.mark.xdist_group("postgres")
async def test_async_connection(postgres_service: PostgresService) -> None:
    """Test asyncpg connection components."""
    # Test direct connection
    async_config = AsyncpgConfig(
        dsn=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        min_size=1,
        max_size=2,
    )

    conn = await async_config.create_connection()
    try:
        assert conn is not None
        # Test basic query
        result = await conn.fetchval("SELECT 1")
        assert result == 1
    finally:
        await conn.close()

    # Test connection pool
    another_config = AsyncpgConfig(
        dsn=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        min_size=1,
        max_size=5,
    )
    # Ensure the pool is created before use if not explicitly managed elsewhere
    await another_config.create_pool()
    try:
        async with another_config.provide_connection() as conn:
            assert conn is not None
            # Test basic query
            result = await conn.fetchval("SELECT 1")
            assert result == 1
    finally:
        await another_config.close_pool()
