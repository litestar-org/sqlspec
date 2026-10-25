"""IBM Db2 integration test fixtures."""

import os
from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2ConnectionParams, Db2SyncConfig

if TYPE_CHECKING:
    from pytest_databases.docker.db2 import Db2Service

    from sqlspec.adapters.db2.driver import Db2AsyncDriver, Db2SyncDriver

__all__ = (
    "db2_async_config",
    "db2_async_session",
    "db2_connection_config",
    "db2_image",
    "db2_session",
    "db2_sync_config",
)


@pytest.fixture(scope="session")
def db2_image() -> str:
    """Return the pinned IBM Db2 Community Edition container image."""
    return os.environ.get("DB2_IMAGE", "icr.io/db2_community/db2:11.5.9.0")


@pytest.fixture(scope="session")
def db2_connection_config(db2_service: "Db2Service") -> Db2ConnectionParams:
    """Build SQLSpec Db2 connection parameters from the Db2 service container."""
    return {
        "database": db2_service.db_name,
        "hostname": db2_service.host,
        "port": db2_service.port,
        "protocol": "TCPIP",
        "user": db2_service.username,
        "password": db2_service.password,
    }


@pytest.fixture(scope="session")
def db2_sync_config(db2_connection_config: Db2ConnectionParams) -> Generator[Db2SyncConfig, None, None]:
    """Yield a session-scoped Db2SyncConfig and close its connection pool on teardown."""
    config = Db2SyncConfig(connection_config=dict(db2_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture(scope="session")
async def db2_async_config(db2_connection_config: Db2ConnectionParams) -> AsyncGenerator[Db2AsyncConfig, None]:
    """Yield a session-scoped Db2AsyncConfig and close its connection pool on teardown."""
    config = Db2AsyncConfig(connection_config=dict(db2_connection_config))
    try:
        yield config
    finally:
        await config.close_pool()


@pytest.fixture
def db2_session(db2_sync_config: Db2SyncConfig) -> "Generator[Db2SyncDriver, None, None]":
    """Yield a function-scoped Db2 synchronous driver session."""
    with db2_sync_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def db2_async_session(db2_async_config: Db2AsyncConfig) -> "AsyncGenerator[Db2AsyncDriver, None]":
    """Yield a function-scoped Db2 asynchronous driver session."""
    async with db2_async_config.provide_session() as driver:
        yield driver
