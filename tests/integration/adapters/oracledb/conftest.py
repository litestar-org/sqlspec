"""OracleDB test fixtures and configuration."""

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


@pytest.fixture(scope="session")
def oracle_connection_config(oracle_23ai_service: "OracleService") -> "OraclePoolParams":
    """Shared Oracle pool configuration."""

    return OraclePoolParams(
        host=oracle_23ai_service.host,
        port=oracle_23ai_service.port,
        service_name=oracle_23ai_service.service_name,
        user=oracle_23ai_service.user,
        password=oracle_23ai_service.password,
    )


@pytest.fixture(scope="session")
def oracle_aq_privileges(oracle_23ai_service: "OracleService") -> None:
    """Grant the container app user the privileges required to run DBMS_AQADM.

    Unlocks both classic Advanced Queuing and Transactional Event Queues for the
    ``aq`` / ``txeventq`` events backends. The grants persist
    for the container lifetime, so session scope is sufficient and naturally idempotent
    (re-granting an existing role/privilege is a no-op in Oracle).
    """

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
def oracle_sync_config(oracle_connection_config: "OraclePoolParams") -> "OracleSyncConfig":
    """Create Oracle sync configuration."""

    return OracleSyncConfig(connection_config=OraclePoolParams(**oracle_connection_config))


@pytest.fixture(scope="session")
async def oracle_async_config(
    oracle_connection_config: "OraclePoolParams",
) -> "AsyncGenerator[OracleAsyncConfig, None]":
    """Session-scoped Oracle async configuration."""
    connection_config = OraclePoolParams(**oracle_connection_config)
    connection_config.setdefault("min", 1)
    connection_config.setdefault("max", 5)
    config = OracleAsyncConfig(connection_config=connection_config)
    try:
        yield config
    finally:
        if config.connection_instance is not None:
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
