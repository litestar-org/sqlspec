"""Shared fixtures for aiomysql integration tests."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.aiomysql import AiomysqlConfig, AiomysqlDriver, default_statement_config


@pytest.fixture(scope="function")
async def aiomysql_config(mysql_service: "MySQLService") -> "AsyncGenerator[AiomysqlConfig, None]":
    """Create aiomysql configuration for testing with proper cleanup."""
    config = AiomysqlConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "db": mysql_service.db,
            "autocommit": True,
            "minsize": 1,
            "maxsize": 5,
        },
        statement_config=default_statement_config,
    )
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
    """Create aiomysql driver instance for testing."""
    async with aiomysql_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def aiomysql_clean_driver(aiomysql_config: "AiomysqlConfig") -> "AsyncGenerator[AiomysqlDriver, None]":
    """Create aiomysql driver with clean database state."""
    async with aiomysql_config.provide_session() as driver:
        await driver.execute("SET sql_notes = 0")
        cleanup_tables = [
            "test_table_aiomysql",
            "data_types_test_aiomysql",
            "user_profiles_aiomysql",
            "test_parameter_conversion_aiomysql",
            "transaction_test_aiomysql",
            "concurrent_test_aiomysql",
            "arrow_users_aiomysql",
            "arrow_table_test_aiomysql",
            "arrow_batch_test_aiomysql",
            "arrow_params_test_aiomysql",
            "arrow_empty_test_aiomysql",
            "arrow_null_test_aiomysql",
            "arrow_polars_test_aiomysql",
            "arrow_large_test_aiomysql",
            "arrow_types_test_aiomysql",
            "arrow_json_test_aiomysql",
            "driver_feature_test_aiomysql",
        ]

        for table in cleanup_tables:
            await driver.execute_script(f"DROP TABLE IF EXISTS {table}")

        cleanup_procedures = ["test_procedure", "simple_procedure"]

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
