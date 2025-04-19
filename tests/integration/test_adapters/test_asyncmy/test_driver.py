"""Test Asyncmy driver implementation."""

from __future__ import annotations

from typing import Any, Literal

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyPoolConfig

ParamStyle = Literal["tuple_binds", "dict_binds"]

pytestmark = pytest.mark.asyncio(loop_scope="session")


@pytest.fixture
def asyncmy_session(mysql_service: MySQLService) -> AsyncmyConfig:
    """Create an Asyncmy asynchronous session.

    Args:
        mysql_service: MySQL service fixture.

    Returns:
        Configured Asyncmy asynchronous session.
    """
    return AsyncmyConfig(
        pool_config=AsyncmyPoolConfig(
            host=mysql_service.host,
            port=mysql_service.port,
            user=mysql_service.user,
            password=mysql_service.password,
            database=mysql_service.db,
        )
    )


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xfail(reason="MySQL/Asyncmy does not support RETURNING clause directly")
@pytest.mark.xdist_group("mysql")
async def test_async_insert_returning(asyncmy_session: AsyncmyConfig, params: Any, style: ParamStyle) -> None:
    """Test async insert returning functionality with different parameter styles."""
    async with asyncmy_session.provide_session() as driver:
        # Manual cleanup at start of test
        try:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")
        except Exception:
            pass  # Ignore error if table doesn't exist

        sql = """
        CREATE TABLE test_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql)

        # asyncmy uses %s for both tuple and dict binds
        sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        """
        # RETURNING is not standard SQL, get last inserted id separately
        # For dict binds, asyncmy expects the values in order, not by name
        param_values = params if style == "tuple_binds" else list(params.values())
        result = await driver.insert_update_delete_returning(sql, param_values)

        assert result is not None
        assert result["name"] == "test_name"
        assert result["id"] is not None  # Driver should fetch this


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("mysql")
async def test_async_select(asyncmy_session: AsyncmyConfig, params: Any, style: ParamStyle) -> None:
    """Test async select functionality with different parameter styles."""
    async with asyncmy_session.provide_session() as driver:
        # Manual cleanup at start of test
        try:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")
        except Exception:
            pass  # Ignore error if table doesn't exist

        # Create test table
        sql = """
        CREATE TABLE test_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql)

        # Insert test record
        # asyncmy uses %s for both tuple and dict binds
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        """
        # For dict binds, asyncmy expects the values in order, not by name
        param_values = params if style == "tuple_binds" else list(params.values())
        await driver.insert_update_delete(insert_sql, param_values)

        # Select and verify
        # asyncmy uses %s for both tuple and dict binds
        select_sql = """
        SELECT name FROM test_table WHERE name = %s
        """
        results = await driver.select(select_sql, param_values)
        assert len(results) == 1
        assert results[0]["name"] == "test_name"


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("mysql")
async def test_async_select_value(asyncmy_session: AsyncmyConfig, params: Any, style: ParamStyle) -> None:
    """Test async select_value functionality with different parameter styles."""
    async with asyncmy_session.provide_session() as driver:
        # Manual cleanup at start of test
        try:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")
        except Exception:
            pass  # Ignore error if table doesn't exist

        # Create test table
        sql = """
        CREATE TABLE test_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql)

        # Insert test record
        # asyncmy uses %s for both tuple and dict binds
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        """
        # For dict binds, asyncmy expects the values in order, not by name
        param_values = params if style == "tuple_binds" else list(params.values())
        await driver.insert_update_delete(insert_sql, param_values)

        # Get literal string to test with select_value
        select_sql = "SELECT 'test_name' AS test_name"

        # Don't pass parameters with a literal query that has no placeholders
        value = await driver.select_value(select_sql)
        assert value == "test_name"


@pytest.mark.xdist_group("mysql")
async def test_insert(asyncmy_session: AsyncmyConfig) -> None:
    """Test inserting data."""
    async with asyncmy_session.provide_session() as driver:
        # Manual cleanup at start of test
        try:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")
        except Exception:
            pass  # Ignore error if table doesn't exist

        sql = """
        CREATE TABLE test_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql)

        insert_sql = "INSERT INTO test_table (name) VALUES (%s)"
        row_count = await driver.insert_update_delete(insert_sql, ("test",))
        assert row_count == 1


@pytest.mark.xdist_group("mysql")
async def test_select(asyncmy_session: AsyncmyConfig) -> None:
    """Test selecting data."""
    async with asyncmy_session.provide_session() as driver:
        # Manual cleanup at start of test
        try:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")
        except Exception:
            pass  # Ignore error if table doesn't exist

        # Create and populate test table
        sql = """
        CREATE TABLE test_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql)

        insert_sql = "INSERT INTO test_table (name) VALUES (%s)"
        await driver.insert_update_delete(insert_sql, ("test",))

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE id = 1"
        results = await driver.select(select_sql)
        assert len(results) == 1
        assert results[0]["name"] == "test"
