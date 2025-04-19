import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyPoolConfig

pytestmark = pytest.mark.asyncio(loop_scope="session")


@pytest.mark.xdist_group("mysql")
async def test_async_connection(mysql_service: MySQLService) -> None:
    """Test async connection components."""
    # Test direct connection
    async_config = AsyncmyConfig(
        pool_config=AsyncmyPoolConfig(
            host=mysql_service.host,
            port=mysql_service.port,
            user=mysql_service.user,
            password=mysql_service.password,
            database=mysql_service.db,
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
    pool_config = AsyncmyPoolConfig(
        host=mysql_service.host,
        port=mysql_service.port,
        user=mysql_service.user,
        password=mysql_service.password,
        database=mysql_service.db,
        minsize=1,
        maxsize=5,
    )
    another_config = AsyncmyConfig(pool_config=pool_config)
    pool = await another_config.create_pool()
    assert pool is not None
    try:
        async with pool.acquire() as conn:  # Use acquire for asyncmy pool
            assert conn is not None
            # Test basic query
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                result = await cur.fetchone()
                assert result == (1,)
    finally:
        pool.close()
        await pool.wait_closed()  # Ensure pool is closed
