"""OracleDB test fixtures and configuration."""

from collections.abc import AsyncGenerator, Generator
from typing import Any

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import OracleAsyncConfig, OracleAsyncDriver, OracleSyncConfig, OracleSyncDriver


@pytest.fixture(scope="session")
def oracle_connection_config(oracle_23ai_service: "OracleService") -> "dict[str, Any]":
    """Shared Oracle pool configuration."""

    return {
        "host": oracle_23ai_service.host,
        "port": oracle_23ai_service.port,
        "service_name": oracle_23ai_service.service_name,
        "user": oracle_23ai_service.user,
        "password": oracle_23ai_service.password,
    }


@pytest.fixture(scope="session")
def oracle_sync_config(oracle_connection_config: "dict[str, Any]") -> "OracleSyncConfig":
    """Create Oracle sync configuration."""

    return OracleSyncConfig(connection_config=dict(oracle_connection_config))


@pytest.fixture(scope="function")
async def oracle_async_config(oracle_connection_config: "dict[str, Any]") -> "AsyncGenerator[OracleAsyncConfig, None]":
    """Create Oracle async configuration."""
    connection_config = dict(oracle_connection_config)
    connection_config.setdefault("min", 1)
    connection_config.setdefault("max", 5)
    config = OracleAsyncConfig(connection_config=connection_config)
    try:
        yield config
    finally:
        if config.connection_instance:
            await config.close_pool()
        config.connection_instance = None


@pytest.fixture
def oracle_sync_session(oracle_sync_config: "OracleSyncConfig") -> "Generator[OracleSyncDriver, None, None]":
    """Create Oracle sync driver session."""

    with oracle_sync_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def oracle_async_session(oracle_async_config: "OracleAsyncConfig") -> "AsyncGenerator[OracleAsyncDriver, None]":
    """Create Oracle async driver session."""

    async with oracle_async_config.provide_session() as driver:
        yield driver
