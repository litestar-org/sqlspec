"""Shared PostgreSQL-family integration fixtures."""

from collections.abc import AsyncGenerator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver

__all__ = ("asyncpg_async_driver", "asyncpg_config", "asyncpg_connection_config")


@pytest.fixture(scope="session")
def asyncpg_connection_config(postgres_service: "PostgresService") -> "dict[str, Any]":
    """Base pool configuration for AsyncPG tests."""
    return {
        "host": postgres_service.host,
        "port": postgres_service.port,
        "user": postgres_service.user,
        "password": postgres_service.password,
        "database": postgres_service.database,
    }


@pytest.fixture(scope="session")
async def asyncpg_config(asyncpg_connection_config: "dict[str, Any]") -> "AsyncGenerator[AsyncpgConfig, None]":
    """Provide a session-scoped AsyncpgConfig with a shared pool."""
    config = AsyncpgConfig(connection_config=dict(asyncpg_connection_config))
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            await config.close_pool()
            config.connection_instance = None


@pytest.fixture
async def asyncpg_async_driver(asyncpg_config: "AsyncpgConfig") -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create an AsyncPG driver for integration tests."""
    async with asyncpg_config.provide_session() as session:
        yield session
