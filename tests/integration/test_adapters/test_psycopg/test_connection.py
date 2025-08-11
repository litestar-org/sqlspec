import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig


@pytest.mark.xdist_group("postgres")
async def test_async_connection(postgres_service: PostgresService) -> None:
    """Test async connection components."""
    # Test direct connection
    async_config = PsycopgAsyncConfig(
        pool_config={
            "conninfo": f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}"
        }
    )

    async with await async_config.create_connection() as conn:
        assert conn is not None
        # Test basic query
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 AS id")
            result = await cur.fetchone()
            # The config should set DictRow as the row factory
            assert result == {"id": 1}

    # Ensure pool is closed properly with timeout
    await async_config.close_pool()
    # Test connection pool
    another_config = PsycopgAsyncConfig(
        pool_config={
            "conninfo": f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}",
            "min_size": 1,
            "max_size": 5,
        }
    )
    # Remove explicit pool creation and manual context management
    async with another_config.provide_connection() as conn:
        assert conn is not None
        # Test basic query
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 AS value")
            result = await cur.fetchone()
            assert result == {"value": 1}  # type: ignore[comparison-overlap]

    # Ensure pool is closed properly with timeout
    await another_config.close_pool()


@pytest.mark.xdist_group("postgres")
def test_sync_connection(postgres_service: PostgresService) -> None:
    """Test sync connection components."""
    # Test direct connection
    sync_config = PsycopgSyncConfig(
        pool_config={
            "conninfo": f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}"
        }
    )

    try:
        with sync_config.create_connection() as conn:
            assert conn is not None
            # Test basic query
            with conn.cursor() as cur:
                cur.execute("SELECT 1 as id")
                result = cur.fetchone()
                assert result == {"id": 1}
    finally:
        # Ensure pool is closed properly with timeout
        sync_config.close_pool()

    # Test connection pool
    another_config = PsycopgSyncConfig(
        pool_config={
            "conninfo": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "min_size": 1,
            "max_size": 5,
        }
    )
    try:
        # Remove explicit pool creation and manual context management
        with another_config.provide_connection() as conn:
            assert conn is not None
            # Test basic query
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS id")
                result = cur.fetchone()
                assert result == {"id": 1}
    finally:
        # Ensure pool is closed properly with timeout
        another_config.close_pool()
