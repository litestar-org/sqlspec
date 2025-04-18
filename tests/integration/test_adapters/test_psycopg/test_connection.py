import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsync, PsycopgAsyncPool, PsycopgSync, PsycopgSyncPool

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_async_connection(postgres_service: PostgresService) -> None:
    """Test async connection components."""
    # Test direct connection
    async_config = PsycopgAsync(
        pool_config=PsycopgAsyncPool(
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

    # Test connection pool
    pool_config = PsycopgAsyncPool(
        conninfo=f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}",
        min_size=1,
        max_size=5,
    )
    another_config = PsycopgAsync(pool_config=pool_config)
    pool = await another_config.create_pool()
    assert pool is not None
    try:
        async with pool.connection() as conn:
            assert conn is not None
            # Test basic query
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                result = await cur.fetchone()
                assert result == (1,)
    finally:
        await pool.close()


def test_sync_connection(postgres_service: PostgresService) -> None:
    """Test sync connection components."""
    # Test direct connection
    sync_config = PsycopgSync(
        pool_config=PsycopgSyncPool(
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

    # Test connection pool
    pool_config = PsycopgSyncPool(
        conninfo=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        min_size=1,
        max_size=5,
    )
    another_config = PsycopgSync(pool_config=pool_config)
    pool = another_config.create_pool()
    assert pool is not None
    try:
        with pool.connection() as conn:
            assert conn is not None
            # Test basic query
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                assert result == (1,)
    finally:
        pool.close()
