"""Shared fixtures for CockroachDB psycopg integration tests."""

from collections.abc import AsyncGenerator, Generator

import pytest
from pytest_databases.docker.cockroachdb import CockroachDBService

from sqlspec.adapters.cockroach_psycopg import (
    CockroachPsycopgAsyncConfig,
    CockroachPsycopgAsyncDriver,
    CockroachPsycopgSyncConfig,
    CockroachPsycopgSyncDriver,
)


def _conninfo(service: "CockroachDBService") -> str:
    return f"host={service.host} port={service.port} user=root dbname={service.database} sslmode=disable"


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Session-scoped anyio backend for session-scoped async fixtures."""
    return "asyncio"


@pytest.fixture(scope="session")
def cockroach_sync_config(
    cockroachdb_service: "CockroachDBService",
) -> "Generator[CockroachPsycopgSyncConfig, None, None]":
    """Session-scoped Cockroach sync config."""
    config = CockroachPsycopgSyncConfig(connection_config={"conninfo": _conninfo(cockroachdb_service)})
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            config.close_pool()


@pytest.fixture(scope="session")
async def cockroach_async_config(
    cockroachdb_service: "CockroachDBService",
) -> "AsyncGenerator[CockroachPsycopgAsyncConfig, None]":
    """Session-scoped Cockroach async config."""
    config = CockroachPsycopgAsyncConfig(connection_config={"conninfo": _conninfo(cockroachdb_service)})
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            await config.close_pool()


@pytest.fixture
def cockroach_sync_driver(
    cockroach_sync_config: "CockroachPsycopgSyncConfig",
) -> "Generator[CockroachPsycopgSyncDriver, None, None]":
    """Create Cockroach sync driver instance for testing."""
    with cockroach_sync_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def cockroach_async_driver(
    cockroach_async_config: "CockroachPsycopgAsyncConfig",
) -> "AsyncGenerator[CockroachPsycopgAsyncDriver, None]":
    """Create Cockroach async driver instance for testing."""
    async with cockroach_async_config.provide_session() as driver:
        yield driver
