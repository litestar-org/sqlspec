"""Shared Oracle integration fixtures."""

from collections.abc import AsyncGenerator, Generator

import pytest
from pytest_databases.docker.oracle import OracleService

from sqlspec.adapters.oracledb import (
    OracleAsyncConfig,
    OracleAsyncDriver,
    OraclePoolParams,
    OracleSyncConfig,
    OracleSyncDriver,
)

__all__ = (
    "oracle_aq_privileges",
    "oracle_async_config",
    "oracle_async_session",
    "oracle_connection_config",
    "oracle_sync_config",
    "oracle_sync_session",
)


def _oracle_pool_params(oracle_service: "OracleService") -> "OraclePoolParams":
    return OraclePoolParams(
        host=oracle_service.host,
        port=oracle_service.port,
        service_name=oracle_service.service_name,
        user=oracle_service.user,
        password=oracle_service.password,
        min=1,
        max=5,
    )


@pytest.fixture(scope="session")
def oracle_connection_config(oracle_23ai_service: "OracleService") -> "OraclePoolParams":
    """Provide shared Oracle pool parameters."""
    return _oracle_pool_params(oracle_23ai_service)


@pytest.fixture(scope="session")
def oracle_aq_privileges(oracle_23ai_service: "OracleService") -> None:
    """Grant the application user privileges required by Oracle AQ tests."""
    app_user = oracle_23ai_service.user
    config = OracleSyncConfig(
        connection_config={
            "host": oracle_23ai_service.host,
            "port": oracle_23ai_service.port,
            "service_name": oracle_23ai_service.service_name,
            "user": "system",
            "password": oracle_23ai_service.system_password,
        }
    )
    grants = (f"GRANT aq_administrator_role, aq_user_role TO {app_user}", f"GRANT EXECUTE ON dbms_aq TO {app_user}")
    try:
        with config.provide_session() as driver:
            for grant in grants:
                driver.execute_script(grant)
            driver.commit()
    finally:
        if config.connection_instance is not None:
            config.close_pool()


@pytest.fixture(scope="session")
def oracle_sync_config(oracle_connection_config: "OraclePoolParams") -> "Generator[OracleSyncConfig, None, None]":
    """Provide a session-scoped Oracle sync configuration."""
    config = OracleSyncConfig(connection_config=OraclePoolParams(**oracle_connection_config))
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            config.close_pool()


@pytest.fixture(scope="session")
async def oracle_async_config(
    oracle_connection_config: "OraclePoolParams",
) -> "AsyncGenerator[OracleAsyncConfig, None]":
    """Provide a session-scoped Oracle async configuration."""
    config = OracleAsyncConfig(connection_config=OraclePoolParams(**oracle_connection_config))
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            await config.close_pool()
            config.connection_instance = None


@pytest.fixture
def oracle_sync_session(oracle_sync_config: "OracleSyncConfig") -> "Generator[OracleSyncDriver, None, None]":
    """Create an Oracle sync driver session."""
    with oracle_sync_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def oracle_async_session(oracle_async_config: "OracleAsyncConfig") -> "AsyncGenerator[OracleAsyncDriver, None]":
    """Create an Oracle async driver session."""
    async with oracle_async_config.provide_session() as driver:
        yield driver
