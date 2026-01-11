"""Pytest configuration for psycopg integration tests."""

from typing import TYPE_CHECKING

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.utils.portal import PortalManager

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator


@pytest.fixture(autouse=True)
def _cleanup_portal() -> "Generator[None, None, None]":
    """Clean up the portal manager after each test.

    This prevents state leakage between tests when the portal is used
    for async-to-sync bridging in event channel operations.
    """
    yield
    PortalManager().stop()


@pytest.fixture(scope="session")
def psycopg_sync_config(postgres_service: "PostgresService") -> "Generator[PsycopgSyncConfig, None, None]":
    """Create a psycopg sync configuration."""
    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    yield config

    if config.connection_instance:
        config.close_pool()


@pytest.fixture(scope="function")
async def psycopg_async_config(postgres_service: "PostgresService") -> "AsyncGenerator[PsycopgAsyncConfig, None]":
    """Create a psycopg async configuration."""
    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )
    try:
        yield config
    finally:
        if config.connection_instance:
            await config.close_pool()
        config.connection_instance = None
