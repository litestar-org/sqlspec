from collections.abc import AsyncGenerator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver


@pytest.fixture(scope="function")
def asyncpg_pool_config(postgres_service: PostgresService) -> "dict[str, Any]":
    """Base pool configuration for AsyncPG tests."""

    return {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "database": postgres_service.database,
    }


@pytest.fixture(scope="function")
async def asyncpg_config(asyncpg_pool_config: "dict[str, Any]") -> "AsyncGenerator[AsyncpgConfig, None]":
    """Provide an AsyncpgConfig instance with shared pool settings."""

    config = AsyncpgConfig(pool_config=dict(asyncpg_pool_config))
    try:
        yield config
    finally:
        if config.pool_instance:
            await config.close_pool()


@pytest.fixture(scope="function")
async def asyncpg_async_driver(asyncpg_config: AsyncpgConfig) -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create an AsyncPG driver for integration tests."""

    async with asyncpg_config.provide_session() as session:
        yield session
