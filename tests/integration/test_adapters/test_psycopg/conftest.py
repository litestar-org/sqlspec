"""Pytest configuration for psycopg integration tests."""

import warnings
from typing import TYPE_CHECKING

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture(autouse=True)
def suppress_psycopg_pool_warnings() -> "Generator[None, None, None]":
    """Suppress psycopg pool __del__ warnings during test teardown."""
    # Filter out the specific warnings about pool cleanup
    with warnings.catch_warnings():
        # Suppress sync pool __del__ warnings
        warnings.filterwarnings(
            "ignore",
            message="Exception ignored in: <function ConnectionPool.__del__",
            category=pytest.PytestUnraisableExceptionWarning,
        )
        # Suppress async pool worker coroutine warnings
        warnings.filterwarnings(
            "ignore",
            message="Exception ignored in: <coroutine object AsyncConnectionPool.worker",
            category=pytest.PytestUnraisableExceptionWarning,
        )
        # Suppress thread join warnings
        warnings.filterwarnings("ignore", message="cannot join current thread", category=RuntimeWarning)
        # Suppress event loop closed warnings from async pool cleanup
        warnings.filterwarnings("ignore", message="Event loop is closed", category=RuntimeWarning)
        yield


@pytest.fixture
def psycopg_sync_config(postgres_service: PostgresService) -> "Generator[PsycopgSyncConfig, None, None]":
    """Create a psycopg sync configuration."""
    config = PsycopgSyncConfig(
        host=postgres_service.host,
        port=postgres_service.port,
        user=postgres_service.user,
        password=postgres_service.password,
        dbname=postgres_service.database,
        autocommit=True,  # Enable autocommit for tests
    )
    yield config
    # Ensure pool is closed
    if config.pool_instance:
        config.close_pool()


@pytest.fixture
def psycopg_async_config(postgres_service: PostgresService) -> "Generator[PsycopgAsyncConfig, None, None]":
    """Create a psycopg async configuration."""
    config = PsycopgAsyncConfig(
        host=postgres_service.host,
        port=postgres_service.port,
        user=postgres_service.user,
        password=postgres_service.password,
        dbname=postgres_service.database,
        autocommit=True,  # Enable autocommit for tests
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
