"""Pytest configuration for psycopg integration tests."""

from typing import TYPE_CHECKING

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def psycopg_sync_config(postgres_service: PostgresService) -> "Generator[PsycopgSyncConfig, None, None]":
    """Create a psycopg sync configuration."""
    config = PsycopgSyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    yield config
    # Ensure pool is closed
    if config.pool_instance:
        config.close_pool()


@pytest.fixture
def psycopg_async_config(postgres_service: PostgresService) -> "Generator[PsycopgAsyncConfig, None, None]":
    """Create a psycopg async configuration."""
    config = PsycopgAsyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    yield config
    # Ensure pool is closed
    if config.pool_instance:
        import asyncio

        try:
            # If we're in an async context
            loop = asyncio.get_running_loop()
            if not loop.is_closed():
                loop.run_until_complete(config.close_pool())
        except RuntimeError:
            # Not in an async context, create a new event loop
            # Use a new event loop to avoid conflicts
            new_loop = asyncio.new_event_loop()
            try:
                new_loop.run_until_complete(config.close_pool())
            finally:
                new_loop.close()
