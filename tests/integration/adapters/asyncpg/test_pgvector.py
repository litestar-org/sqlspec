"""Integration tests for asyncpg driver with pgvector extension."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgPoolConfig

pytestmark = pytest.mark.xdist_group("pgvector")


@pytest.fixture(scope="session")
def pgvector_asyncpg_connection_config(pgvector_service: "PostgresService") -> "AsyncpgPoolConfig":
    """Base pool configuration for AsyncPG tests with pgvector."""
    return AsyncpgPoolConfig(
        host=pgvector_service.host,
        port=pgvector_service.port,
        user=pgvector_service.user,
        password=pgvector_service.password,
        database=pgvector_service.database,
    )


@pytest.fixture(scope="function")
async def pgvector_asyncpg_config(
    pgvector_asyncpg_connection_config: "AsyncpgPoolConfig",
) -> "AsyncGenerator[AsyncpgConfig, None]":
    """Provide an AsyncpgConfig instance connected to pgvector postgres."""
    import asyncpg

    # Use individual params for the one-off setup connection
    conn = await asyncpg.connect(
        host=pgvector_asyncpg_connection_config.get("host"),
        port=pgvector_asyncpg_connection_config.get("port"),
        user=pgvector_asyncpg_connection_config.get("user"),
        password=pgvector_asyncpg_connection_config.get("password"),
        database=pgvector_asyncpg_connection_config.get("database"),
    )
    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    finally:
        await conn.close()

    config = AsyncpgConfig(connection_config=pgvector_asyncpg_connection_config)
    try:
        yield config
    finally:
        pool = config.connection_instance
        if pool is not None:
            await pool.close()
            config.connection_instance = None
