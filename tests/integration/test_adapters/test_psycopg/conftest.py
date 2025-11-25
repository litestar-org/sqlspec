"""Pytest configuration for psycopg integration tests."""

from typing import TYPE_CHECKING

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator


@pytest.fixture
def psycopg_sync_config(postgres_service: PostgresService) -> "Generator[PsycopgSyncConfig, None, None]":
    """Create a psycopg sync configuration."""
    config = PsycopgSyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    yield config

    if config.pool_instance:
        config.close_pool()


@pytest.fixture(scope="function")
async def psycopg_async_config(postgres_service: PostgresService) -> "AsyncGenerator[PsycopgAsyncConfig, None]":
    """Create a psycopg async configuration."""
    config = PsycopgAsyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    try:
        yield config
    finally:
        if config.pool_instance:
            await config.close_pool()
