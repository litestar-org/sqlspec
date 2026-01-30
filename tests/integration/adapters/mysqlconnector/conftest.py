"""Shared fixtures for MysqlConnector integration tests."""

from collections.abc import AsyncGenerator, Generator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.mysqlconnector import (
    MysqlConnectorAsyncConfig,
    MysqlConnectorAsyncDriver,
    MysqlConnectorSyncConfig,
    MysqlConnectorSyncDriver,
    default_statement_config,
)


@pytest.fixture(scope="function")
async def mysqlconnector_async_config(
    mysql_service: "MySQLService",
) -> "AsyncGenerator[MysqlConnectorAsyncConfig, None]":
    """Create MysqlConnector async configuration for testing."""
    config = MysqlConnectorAsyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "use_pure": True,
        },
        statement_config=default_statement_config,
    )
    try:
        yield config
    finally:
        config.connection_instance = None


@pytest.fixture(scope="session")
def mysqlconnector_sync_config(mysql_service: "MySQLService") -> "Generator[MysqlConnectorSyncConfig, None, None]":
    """Create MysqlConnector sync configuration for testing."""
    config = MysqlConnectorSyncConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "use_pure": True,
            "pool_size": 5,
        },
        statement_config=default_statement_config,
    )
    yield config

    if config.connection_instance:
        config.close_pool()


@pytest.fixture
async def mysqlconnector_async_driver(
    mysqlconnector_async_config: "MysqlConnectorAsyncConfig",
) -> "AsyncGenerator[MysqlConnectorAsyncDriver, None]":
    """Create MysqlConnector async driver instance for testing."""
    async with mysqlconnector_async_config.provide_session() as driver:
        yield driver


@pytest.fixture
def mysqlconnector_sync_driver(
    mysqlconnector_sync_config: "MysqlConnectorSyncConfig",
) -> "Generator[MysqlConnectorSyncDriver, None, None]":
    """Create MysqlConnector sync driver instance for testing."""
    with mysqlconnector_sync_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def mysqlconnector_clean_async_driver(
    mysqlconnector_async_config: "MysqlConnectorAsyncConfig",
) -> "AsyncGenerator[MysqlConnectorAsyncDriver, None]":
    """Create MysqlConnector async driver with clean database state."""
    async with mysqlconnector_async_config.provide_session() as driver:
        await driver.execute("SET sql_notes = 0")
        cleanup_tables = [
            "test_table_mysqlconnector_async",
            "data_types_test_mysqlconnector_async",
            "user_profiles_mysqlconnector_async",
            "test_parameter_conversion_mysqlconnector_async",
            "transaction_test_mysqlconnector_async",
            "concurrent_test_mysqlconnector_async",
            "arrow_users_mysqlconnector_async",
            "arrow_table_test_mysqlconnector_async",
            "arrow_batch_test_mysqlconnector_async",
            "arrow_params_test_mysqlconnector_async",
            "arrow_empty_test_mysqlconnector_async",
            "arrow_null_test_mysqlconnector_async",
            "arrow_polars_test_mysqlconnector_async",
            "arrow_large_test_mysqlconnector_async",
            "arrow_types_test_mysqlconnector_async",
            "arrow_json_test_mysqlconnector_async",
            "driver_feature_test_mysqlconnector_async",
        ]

        for table in cleanup_tables:
            await driver.execute_script(f"DROP TABLE IF EXISTS {table}")

        cleanup_procedures = ["test_procedure_mysqlconnector_async", "simple_procedure_mysqlconnector_async"]

        for proc in cleanup_procedures:
            await driver.execute_script(f"DROP PROCEDURE IF EXISTS {proc}")

        await driver.execute("SET sql_notes = 1")

        yield driver

        await driver.execute("SET sql_notes = 0")

        for table in cleanup_tables:
            await driver.execute_script(f"DROP TABLE IF EXISTS {table}")

        for proc in cleanup_procedures:
            await driver.execute_script(f"DROP PROCEDURE IF EXISTS {proc}")

        await driver.execute("SET sql_notes = 1")


@pytest.fixture
def mysqlconnector_clean_sync_driver(
    mysqlconnector_sync_config: "MysqlConnectorSyncConfig",
) -> "Generator[MysqlConnectorSyncDriver, None, None]":
    """Create MysqlConnector sync driver with clean database state."""
    with mysqlconnector_sync_config.provide_session() as driver:
        driver.execute("SET sql_notes = 0")
        cleanup_tables = [
            "test_table_mysqlconnector_sync",
            "data_types_test_mysqlconnector_sync",
            "user_profiles_mysqlconnector_sync",
            "test_parameter_conversion_mysqlconnector_sync",
            "transaction_test_mysqlconnector_sync",
            "concurrent_test_mysqlconnector_sync",
            "arrow_users_mysqlconnector_sync",
            "arrow_table_test_mysqlconnector_sync",
            "arrow_batch_test_mysqlconnector_sync",
            "arrow_params_test_mysqlconnector_sync",
            "arrow_empty_test_mysqlconnector_sync",
            "arrow_null_test_mysqlconnector_sync",
            "arrow_polars_test_mysqlconnector_sync",
            "arrow_large_test_mysqlconnector_sync",
            "arrow_types_test_mysqlconnector_sync",
            "arrow_json_test_mysqlconnector_sync",
            "driver_feature_test_mysqlconnector_sync",
        ]

        for table in cleanup_tables:
            driver.execute_script(f"DROP TABLE IF EXISTS {table}")

        cleanup_procedures = ["test_procedure_mysqlconnector_sync", "simple_procedure_mysqlconnector_sync"]

        for proc in cleanup_procedures:
            driver.execute_script(f"DROP PROCEDURE IF EXISTS {proc}")

        driver.execute("SET sql_notes = 1")

        yield driver

        driver.execute("SET sql_notes = 0")

        for table in cleanup_tables:
            driver.execute_script(f"DROP TABLE IF EXISTS {table}")

        for proc in cleanup_procedures:
            driver.execute_script(f"DROP PROCEDURE IF EXISTS {proc}")

        driver.execute("SET sql_notes = 1")
