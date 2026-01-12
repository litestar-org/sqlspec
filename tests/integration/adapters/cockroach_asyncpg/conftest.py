"""Shared fixtures for CockroachDB asyncpg integration tests."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.cockroachdb import CockroachDBService

from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgConfig, CockroachAsyncpgDriver


@pytest.fixture(scope="function")
async def cockroach_asyncpg_config(
    cockroachdb_service: "CockroachDBService",
) -> "AsyncGenerator[CockroachAsyncpgConfig, None]":
    """Create Cockroach asyncpg config for testing."""
    config = CockroachAsyncpgConfig(
        connection_config={
            "host": cockroachdb_service.host,
            "port": cockroachdb_service.port,
            "user": "root",
            "password": "",
            "database": cockroachdb_service.database,
            "ssl": None,
        }
    )
    try:
        yield config
    finally:
        await config.close_pool()


@pytest.fixture
async def cockroach_asyncpg_driver(
    cockroach_asyncpg_config: "CockroachAsyncpgConfig",
) -> "AsyncGenerator[CockroachAsyncpgDriver, None]":
    """Create Cockroach asyncpg driver instance for testing."""
    async with cockroach_asyncpg_config.provide_session() as driver:
        yield driver
