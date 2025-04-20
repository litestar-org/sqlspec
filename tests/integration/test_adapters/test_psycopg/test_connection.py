import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import (
    PsycopgAsyncConfig,
    PsycopgAsyncPoolConfig,
    PsycopgSyncConfig,
    PsycopgSyncPoolConfig,
)


@pytest.mark.xdist_group("postgres")
async def test_async_connection(postgres_service: PostgresService) -> None:
    """Test async connection components."""
    # Test direct connection
    async_config = PsycopgAsyncConfig(
        pool_config=PsycopgAsyncPoolConfig(
            conninfo=f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}",
        ),
    )

    async with await async_config.create_connection() as conn:
        assert conn is not None
        # Test basic query
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            result = await cur.fetchone()
            assert result == (1,)
    await async_config.close_pool()
    # Test connection pool
    pool_config = PsycopgAsyncPoolConfig(
        conninfo=f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}",
        min_size=1,
        max_size=5,
    )
    another_config = PsycopgAsyncConfig(pool_config=pool_config)
    # Remove explicit pool creation and manual context management
    async with another_config.provide_connection() as conn:
        assert conn is not None
        # Test basic query
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            result = await cur.fetchone()
            assert result == (1,)
    await another_config.close_pool()


@pytest.mark.xdist_group("postgres")
def test_sync_connection(postgres_service: PostgresService) -> None:
    """Test sync connection components."""
    # Test direct connection
    sync_config = PsycopgSyncConfig(
        pool_config=PsycopgSyncPoolConfig(
            conninfo=f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}",
        ),
    )

    with sync_config.create_connection() as conn:
        assert conn is not None
        # Test basic query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            assert result == (1,)
    sync_config.close_pool()
    # Test connection pool
    pool_config = PsycopgSyncPoolConfig(
        conninfo=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        min_size=1,
        max_size=5,
    )
    another_config = PsycopgSyncConfig(pool_config=pool_config)
    # Remove explicit pool creation and manual context management
    with another_config.provide_connection() as conn:
        assert conn is not None
        # Test basic query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            assert result == (1,)
    another_config.close_pool()
