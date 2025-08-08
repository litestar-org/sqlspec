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
        # Clean up any test tables that might exist
        cleanup_tables = [
            "test_table",
            "data_types_test",
            "user_profiles",
            "test_parameter_conversion",
            "transaction_test",
            "concurrent_test",
        ]

        for table in cleanup_tables:
            try:
                await driver.execute_script(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                # Ignore errors if table doesn't exist
                pass

        yield driver

        # Cleanup after test
        for table in cleanup_tables:
            try:
                await driver.execute_script(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                # Ignore errors if table doesn't exist
                pass
