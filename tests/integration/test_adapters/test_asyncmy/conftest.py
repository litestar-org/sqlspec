"""Shared fixtures for AsyncMy integration tests."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver, asyncmy_statement_config


@pytest.fixture
async def asyncmy_config(mysql_service: MySQLService) -> AsyncmyConfig:
    """Create AsyncMy configuration for testing."""
    return AsyncmyConfig(
        pool_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,  # Enable autocommit for tests
            "minsize": 1,
            "maxsize": 5,
        },
        statement_config=asyncmy_statement_config,
    )


@pytest.fixture
async def asyncmy_driver(asyncmy_config: AsyncmyConfig) -> AsyncGenerator[AsyncmyDriver, None]:
    """Create AsyncMy driver instance for testing."""
    async with asyncmy_config.provide_session() as driver:
        yield driver


@pytest.fixture
async def asyncmy_clean_driver(asyncmy_config: AsyncmyConfig) -> AsyncGenerator[AsyncmyDriver, None]:
    """Create AsyncMy driver with clean database state."""
    async with asyncmy_config.provide_session() as driver:
        # Silence MySQL "unknown table" notes during fixture cleanup to avoid noisy logs
        await driver.execute("SET sql_notes = 0")
        # Clean up any test tables that might exist
        cleanup_tables = [
            "test_table",
            "data_types_test",
            "user_profiles",
            "test_parameter_conversion",
            "transaction_test",
            "concurrent_test",
            "arrow_users",
            "arrow_table_test",
            "arrow_batch_test",
            "arrow_params_test",
            "arrow_empty_test",
            "arrow_null_test",
            "arrow_polars_test",
            "arrow_large_test",
            "arrow_types_test",
            "arrow_json_test",
        ]

        for table in cleanup_tables:
            await driver.execute_script(f"DROP TABLE IF EXISTS {table}")

        # Clean up stored procedures used in tests
        cleanup_procedures = ["test_procedure", "simple_procedure"]

        for proc in cleanup_procedures:
            await driver.execute_script(f"DROP PROCEDURE IF EXISTS {proc}")

        await driver.execute("SET sql_notes = 1")

        yield driver

        await driver.execute("SET sql_notes = 0")

        # Cleanup after test
        for table in cleanup_tables:
            await driver.execute_script(f"DROP TABLE IF EXISTS {table}")

        for proc in cleanup_procedures:
            await driver.execute_script(f"DROP PROCEDURE IF EXISTS {proc}")

        await driver.execute("SET sql_notes = 1")
