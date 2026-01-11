from collections.abc import AsyncGenerator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver


@pytest.fixture(scope="function")
def asyncpg_connection_config(postgres_service: "PostgresService") -> "dict[str, Any]":
    """Base pool configuration for AsyncPG tests."""

    return {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "database": postgres_service.database,
    }


@pytest.fixture(scope="function")
async def asyncpg_config(asyncpg_connection_config: "dict[str, Any]") -> "AsyncGenerator[AsyncpgConfig, None]":
    """Provide an AsyncpgConfig instance with shared pool settings.

    Uses pool.terminate() for forceful cleanup to prevent event loop issues
    when running with pytest-xdist and function-scoped event loops.
    """
    config = AsyncpgConfig(connection_config=dict(asyncpg_connection_config))
    try:
        yield config
    finally:
        pool = config.connection_instance
        if pool is not None:
            # Use terminate() for forceful cleanup - more reliable than close()
            # when dealing with function-scoped event loops in pytest-xdist
            pool.terminate()
            config.connection_instance = None


@pytest.fixture(scope="function")
async def asyncpg_async_driver(asyncpg_config: AsyncpgConfig) -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create an AsyncPG driver for integration tests."""

    async with asyncpg_config.provide_session() as session:
        yield session
