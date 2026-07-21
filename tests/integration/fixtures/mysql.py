"""Shared MySQL-family integration fixtures."""

from collections.abc import AsyncGenerator, Generator
from typing import Any

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.aiomysql import AiomysqlConfig, AiomysqlDriver
from sqlspec.adapters.aiomysql import default_statement_config as aiomysql_statement_config
from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.adapters.asyncmy import default_statement_config as asyncmy_statement_config
from sqlspec.adapters.mysqlconnector import (
    MysqlConnectorAsyncConfig,
    MysqlConnectorAsyncDriver,
    MysqlConnectorSyncConfig,
    MysqlConnectorSyncDriver,
)
from sqlspec.adapters.mysqlconnector import default_statement_config as mysqlconnector_statement_config
from sqlspec.adapters.pymysql import PyMysqlConfig, PyMysqlDriver
from sqlspec.adapters.pymysql import default_statement_config as pymysql_statement_config
from tests.integration.fixtures.cleanup import mysql_cleanup_statements

__all__ = (
    "aiomysql_clean_driver",
    "aiomysql_config",
    "aiomysql_driver",
    "asyncmy_clean_driver",
    "asyncmy_config",
    "asyncmy_driver",
    "mysqlconnector_async_config",
    "mysqlconnector_async_driver",
    "mysqlconnector_clean_async_driver",
    "mysqlconnector_clean_sync_driver",
    "mysqlconnector_sync_config",
    "mysqlconnector_sync_driver",
    "mysqlconnector_sync_transaction_config",
    "pymysql_clean_driver",
    "pymysql_config",
    "pymysql_driver",
    "pymysql_transaction_config",
)


def _mysql_connection_config(mysql_service: "MySQLService", *, database_key: str = "database") -> "dict[str, Any]":
    return {
        "host": mysql_service.host,
        "port": mysql_service.port,
        "user": mysql_service.user,
        "password": mysql_service.password,
        database_key: mysql_service.db,
        "autocommit": True,
    }


async def _run_async_cleanup(driver: Any, adapter_suffix: str, *, procedure_suffix: str | None = None) -> None:
    for statement in mysql_cleanup_statements(adapter_suffix, procedure_suffix=procedure_suffix):
        await driver.execute_script(statement)


def _run_sync_cleanup(driver: Any, adapter_suffix: str, *, procedure_suffix: str | None = None) -> None:
    for statement in mysql_cleanup_statements(adapter_suffix, procedure_suffix=procedure_suffix):
        driver.execute_script(statement)


@pytest.fixture(scope="session")
async def asyncmy_config(mysql_service: "MySQLService") -> "AsyncGenerator[AsyncmyConfig, None]":
    """Provide a session-scoped AsyncmyConfig with a shared pool."""
    connection_config = _mysql_connection_config(mysql_service)
    connection_config.update({"minsize": 1, "maxsize": 5})
    config = AsyncmyConfig(connection_config=connection_config, statement_config=asyncmy_statement_config)
    try:
        yield config
    finally:
        pool = config.connection_instance
        if pool is not None:
            pool.close()
            await pool.wait_closed()
            config.connection_instance = None


@pytest.fixture
async def asyncmy_driver(asyncmy_config: "AsyncmyConfig") -> "AsyncGenerator[AsyncmyDriver, None]":
    """Create an AsyncMy driver instance for testing."""
    async with asyncmy_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def asyncmy_clean_driver(asyncmy_config: "AsyncmyConfig") -> "AsyncGenerator[AsyncmyDriver, None]":
    """Create an AsyncMy driver with clean database state."""
    async with asyncmy_config.provide_session() as driver:
        await _run_async_cleanup(driver, "asyncmy")
        yield driver
        await _run_async_cleanup(driver, "asyncmy")


@pytest.fixture(scope="session")
async def aiomysql_config(mysql_service: "MySQLService") -> "AsyncGenerator[AiomysqlConfig, None]":
    """Provide a session-scoped AiomysqlConfig with a shared pool."""
    connection_config = _mysql_connection_config(mysql_service, database_key="db")
    connection_config.update({"minsize": 1, "maxsize": 5})
    config = AiomysqlConfig(connection_config=connection_config, statement_config=aiomysql_statement_config)
    try:
        yield config
    finally:
        pool = config.connection_instance
        if pool is not None:
            pool.close()
            await pool.wait_closed()
            config.connection_instance = None


@pytest.fixture
async def aiomysql_driver(aiomysql_config: "AiomysqlConfig") -> "AsyncGenerator[AiomysqlDriver, None]":
    """Create an aiomysql driver instance for testing."""
    async with aiomysql_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def aiomysql_clean_driver(aiomysql_config: "AiomysqlConfig") -> "AsyncGenerator[AiomysqlDriver, None]":
    """Create an aiomysql driver with clean database state."""
    async with aiomysql_config.provide_session() as driver:
        await _run_async_cleanup(driver, "aiomysql")
        yield driver
        await _run_async_cleanup(driver, "aiomysql")


@pytest.fixture(scope="session")
async def mysqlconnector_async_config(
    mysql_service: "MySQLService",
) -> "AsyncGenerator[MysqlConnectorAsyncConfig, None]":
    """Provide a session-scoped MysqlConnector async configuration."""
    connection_config = _mysql_connection_config(mysql_service)
    connection_config["use_pure"] = True
    config = MysqlConnectorAsyncConfig(
        connection_config=connection_config, statement_config=mysqlconnector_statement_config
    )
    try:
        yield config
    finally:
        config.connection_instance = None


@pytest.fixture(scope="session")
def mysqlconnector_sync_config(mysql_service: "MySQLService") -> "Generator[MysqlConnectorSyncConfig, None, None]":
    """Create a MysqlConnector sync configuration for testing."""
    connection_config = _mysql_connection_config(mysql_service)
    connection_config.update({"use_pure": True, "pool_size": 5})
    config = MysqlConnectorSyncConfig(
        connection_config=connection_config, statement_config=mysqlconnector_statement_config
    )
    try:
        yield config
    finally:
        if config.connection_instance:
            config.close_pool()


@pytest.fixture
def mysqlconnector_sync_transaction_config(
    mysql_service: "MySQLService",
) -> "Generator[MysqlConnectorSyncConfig, None, None]":
    """Create a MysqlConnector sync configuration for transaction testing."""
    connection_config = _mysql_connection_config(mysql_service)
    connection_config.update({"autocommit": False, "use_pure": True})
    config = MysqlConnectorSyncConfig(
        connection_config=connection_config, statement_config=mysqlconnector_statement_config
    )
    try:
        yield config
    finally:
        if config.connection_instance:
            config.close_pool()


@pytest.fixture
async def mysqlconnector_async_driver(
    mysqlconnector_async_config: "MysqlConnectorAsyncConfig",
) -> "AsyncGenerator[MysqlConnectorAsyncDriver, None]":
    """Create a MysqlConnector async driver instance for testing."""
    async with mysqlconnector_async_config.provide_session() as driver:
        yield driver


@pytest.fixture
def mysqlconnector_sync_driver(
    mysqlconnector_sync_config: "MysqlConnectorSyncConfig",
) -> "Generator[MysqlConnectorSyncDriver, None, None]":
    """Create a MysqlConnector sync driver instance for testing."""
    with mysqlconnector_sync_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def mysqlconnector_clean_async_driver(
    mysqlconnector_async_config: "MysqlConnectorAsyncConfig",
) -> "AsyncGenerator[MysqlConnectorAsyncDriver, None]":
    """Create a MysqlConnector async driver with clean database state."""
    async with mysqlconnector_async_config.provide_session() as driver:
        await _run_async_cleanup(driver, "mysqlconnector_async", procedure_suffix="mysqlconnector_async")
        yield driver
        await _run_async_cleanup(driver, "mysqlconnector_async", procedure_suffix="mysqlconnector_async")


@pytest.fixture
def mysqlconnector_clean_sync_driver(
    mysqlconnector_sync_config: "MysqlConnectorSyncConfig",
) -> "Generator[MysqlConnectorSyncDriver, None, None]":
    """Create a MysqlConnector sync driver with clean database state."""
    with mysqlconnector_sync_config.provide_session() as driver:
        _run_sync_cleanup(driver, "mysqlconnector_sync", procedure_suffix="mysqlconnector_sync")
        yield driver
        _run_sync_cleanup(driver, "mysqlconnector_sync", procedure_suffix="mysqlconnector_sync")


@pytest.fixture(scope="session")
def pymysql_config(mysql_service: "MySQLService") -> "Generator[PyMysqlConfig, None, None]":
    """Create a PyMySQL config for testing."""
    config = PyMysqlConfig(
        connection_config=_mysql_connection_config(mysql_service), statement_config=pymysql_statement_config
    )
    try:
        yield config
    finally:
        if config.connection_instance:
            config.close_pool()


@pytest.fixture
def pymysql_transaction_config(mysql_service: "MySQLService") -> "Generator[PyMysqlConfig, None, None]":
    """Create a PyMySQL config for transaction testing."""
    connection_config = _mysql_connection_config(mysql_service)
    connection_config["autocommit"] = False
    config = PyMysqlConfig(connection_config=connection_config, statement_config=pymysql_statement_config)
    try:
        yield config
    finally:
        if config.connection_instance:
            config.close_pool()


@pytest.fixture
def pymysql_driver(pymysql_config: "PyMysqlConfig") -> "Generator[PyMysqlDriver, None, None]":
    """Create a PyMySQL driver instance for testing."""
    with pymysql_config.provide_session() as driver:
        yield driver


@pytest.fixture
def pymysql_clean_driver(pymysql_config: "PyMysqlConfig") -> "Generator[PyMysqlDriver, None, None]":
    """Create a PyMySQL driver with clean database state."""
    with pymysql_config.provide_session() as driver:
        _run_sync_cleanup(driver, "pymysql")
        yield driver
        _run_sync_cleanup(driver, "pymysql")
