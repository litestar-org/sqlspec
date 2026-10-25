"""Shared Oracle integration fixtures."""

import shlex
from typing import TYPE_CHECKING, cast

import pytest
from pytest_databases.docker.oracle import OracleService
from pytest_databases.helpers import exec_output_to_bytes

from sqlspec.utils.module_loader import import_optional_attr

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from sqlspec.adapters.oracledb import (
        OracleAsyncConfig,
        OracleAsyncDriver,
        OraclePoolParams,
        OracleSyncConfig,
        OracleSyncDriver,
    )
else:
    OracleAsyncConfig = import_optional_attr("sqlspec.adapters.oracledb", "OracleAsyncConfig")
    OracleAsyncDriver = import_optional_attr("sqlspec.adapters.oracledb", "OracleAsyncDriver")
    OraclePoolParams = import_optional_attr("sqlspec.adapters.oracledb", "OraclePoolParams")
    OracleSyncConfig = import_optional_attr("sqlspec.adapters.oracledb", "OracleSyncConfig")
    OracleSyncDriver = import_optional_attr("sqlspec.adapters.oracledb", "OracleSyncDriver")

__all__ = (
    "oracle_18c_async_config",
    "oracle_18c_async_session",
    "oracle_18c_connection_config",
    "oracle_aq_privileges",
    "oracle_async_config",
    "oracle_async_session",
    "oracle_connection_config",
    "oracle_sync_config",
    "oracle_sync_session",
)


@pytest.fixture(scope="session")
def oracle_18c_connection_config(oracle_18c_service: OracleService) -> "OraclePoolParams":
    """Provide Oracle 18c pool parameters for non-native JSON storage coverage."""
    return _oracle_pool_params(oracle_18c_service)


@pytest.fixture(scope="session")
async def oracle_18c_async_config(
    oracle_18c_connection_config: "OraclePoolParams",
) -> "AsyncGenerator[OracleAsyncConfig, None]":
    """Provide a session-scoped Oracle 18c async configuration."""
    pytest.importorskip("oracledb")
    config = OracleAsyncConfig(connection_config=OraclePoolParams(**oracle_18c_connection_config))
    try:
        yield config
    finally:
        if config.connection_instance is not None:
            await config.close_pool()
            config.connection_instance = None


@pytest.fixture
async def oracle_18c_async_session(
    oracle_18c_async_config: "OracleAsyncConfig",
) -> "AsyncGenerator[OracleAsyncDriver, None]":
    """Create an Oracle 18c async driver session."""
    pytest.importorskip("oracledb")
    async with oracle_18c_async_config.provide_session() as driver:
        yield driver


@pytest.fixture(scope="session")
def oracle_connection_config(oracle_23ai_service: OracleService) -> "OraclePoolParams":
    """Provide shared Oracle pool parameters."""
    return _oracle_pool_params(oracle_23ai_service)


@pytest.fixture(scope="session")
def oracle_aq_privileges(oracle_23ai_service: OracleService) -> None:
    """Grant the application user privileges required by Oracle AQ tests via in-container sqlplus."""
    app_user = oracle_23ai_service.user
    script = "\n".join((
        "SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF ECHO OFF",
        "WHENEVER OSERROR EXIT 9",
        "WHENEVER SQLERROR EXIT SQL.SQLCODE",
        f"GRANT aq_administrator_role, aq_user_role TO {app_user};",
        f"GRANT EXECUTE ON dbms_aq TO {app_user};",
        "EXIT",
        "",
    ))
    connect_string = f"system/{oracle_23ai_service.system_password}@//localhost:1521/{oracle_23ai_service.service_name}"
    command = f"printf '%s' {shlex.quote(script)} | sqlplus -L -S {shlex.quote(connect_string)}"
    result = oracle_23ai_service.container.exec_run(["bash", "-lc", command])
    exit_code = result.exit_code if result.exit_code is not None else -1
    if exit_code != 0:
        decoded_output = exec_output_to_bytes(result.output).decode("utf-8", errors="replace")
        msg = f"Failed to grant Oracle AQ privileges to {app_user!r}: {decoded_output}"
        raise RuntimeError(msg)


@pytest.fixture(scope="session")
def oracle_sync_config(oracle_connection_config: "OraclePoolParams") -> "Generator[OracleSyncConfig, None, None]":
    """Provide a session-scoped Oracle sync configuration."""
    pytest.importorskip("oracledb")
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
    pytest.importorskip("oracledb")
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
    pytest.importorskip("oracledb")
    with oracle_sync_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def oracle_async_session(oracle_async_config: "OracleAsyncConfig") -> "AsyncGenerator[OracleAsyncDriver, None]":
    """Create an Oracle async driver session."""
    pytest.importorskip("oracledb")
    async with oracle_async_config.provide_session() as driver:
        yield driver


def _oracle_pool_params(oracle_service: OracleService) -> "OraclePoolParams":
    return cast(
        "OraclePoolParams",
        {
            "host": oracle_service.host,
            "port": oracle_service.port,
            "service_name": oracle_service.service_name,
            "user": oracle_service.user,
            "password": oracle_service.password,
            "min": 1,
            "max": 5,
            "retry_count": 5,
            "retry_delay": 1,
        },
    )
