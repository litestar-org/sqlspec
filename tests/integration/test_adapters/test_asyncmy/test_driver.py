"""Test Asyncmy driver implementation."""

from __future__ import annotations

from typing import Any, Literal

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyPoolConfig
from sqlspec.sql.result import ExecuteResult, SelectResult

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
@pytest.mark.xdist_group("mysql")
async def test_async_insert_returning(asyncmy_session: AsyncmyConfig, params: Any, style: ParamStyle) -> None:
    """Test async insert functionality with different parameter styles."""
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

        # Use appropriate SQL for each style (asyncmy driver handles conversion)
        if style == "tuple_binds":
            insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        else:
            insert_sql = "INSERT INTO test_table (name) VALUES (:name)"

        result = await driver.execute(insert_sql, params)
        assert isinstance(result, ExecuteResult)
        assert result.rows_affected == 1
        # MySQL will return the auto-incremented ID
        assert result.last_inserted_id is not None


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
        if style == "tuple_binds":
            insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        else:
            insert_sql = "INSERT INTO test_table (name) VALUES (:name)"

        insert_result = await driver.execute(insert_sql, params)
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        # Select and verify
        if style == "tuple_binds":
            select_sql = "SELECT name FROM test_table WHERE name = ?"
        else:
            select_sql = "SELECT name FROM test_table WHERE name = :name"

        select_result = await driver.execute(select_sql, params)
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert len(select_result.rows) == 1
        assert select_result.rows[0]["name"] == "test_name"


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("mysql")
async def test_async_select_value(asyncmy_session: AsyncmyConfig, params: Any, style: ParamStyle) -> None:
    """Test async select value functionality with different parameter styles."""
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
        if style == "tuple_binds":
            insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        else:
            insert_sql = "INSERT INTO test_table (name) VALUES (:name)"

        insert_result = await driver.execute(insert_sql, params)
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        # Get literal string to test with select value
        select_sql = "SELECT 'test_name' AS test_name"

        # Don't pass parameters with a literal query that has no placeholders
        value_result = await driver.execute(select_sql)
        assert isinstance(value_result, SelectResult)
        assert value_result.rows is not None
        assert len(value_result.rows) == 1
        assert value_result.column_names is not None

        # Extract single value using column name
        value = value_result.rows[0][value_result.column_names[0]]
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

        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        insert_result = await driver.execute(insert_sql, ("test",))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1


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

        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        insert_result = await driver.execute(insert_sql, ("test",))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE id = 1"
        select_result = await driver.execute(select_sql)
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert len(select_result.rows) == 1
        assert select_result.rows[0]["name"] == "test"


@pytest.mark.xdist_group("mysql")
async def test_execute_many_insert(asyncmy_session: AsyncmyConfig) -> None:
    """Test execute_many functionality for batch inserts."""
    async with asyncmy_session.provide_session() as driver:
        # Manual cleanup at start of test
        try:
            await driver.execute_script("DROP TABLE IF EXISTS test_many_table")
        except Exception:
            pass  # Ignore error if table doesn't exist

        sql_create = """
        CREATE TABLE test_many_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_many_table (name) VALUES (?)"
        params_list = [("name1",), ("name2",), ("name3",)]

        result = await driver.execute_many(insert_sql, params_list)
        assert isinstance(result, ExecuteResult)
        assert result.rows_affected == len(params_list)

        select_sql = "SELECT COUNT(*) as count FROM test_many_table"
        count_result = await driver.execute(select_sql)
        assert isinstance(count_result, SelectResult)
        assert count_result.rows is not None
        assert count_result.rows[0]["count"] == len(params_list)


@pytest.mark.xdist_group("mysql")
async def test_execute_script(asyncmy_session: AsyncmyConfig) -> None:
    """Test execute_script functionality for multi-statement scripts."""
    async with asyncmy_session.provide_session() as driver:
        # Manual cleanup at start of test
        try:
            await driver.execute_script("DROP TABLE IF EXISTS test_script_table")
        except Exception:
            pass  # Ignore error if table doesn't exist

        script = """
        CREATE TABLE test_script_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50)
        );
        INSERT INTO test_script_table (name) VALUES ('script_name1');
        INSERT INTO test_script_table (name) VALUES ('script_name2');
        """

        result = await driver.execute_script(script)
        assert isinstance(result, str)

        # Verify script executed successfully
        select_result = await driver.execute("SELECT COUNT(*) as count FROM test_script_table")
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert select_result.rows[0]["count"] == 2


@pytest.mark.xdist_group("mysql")
async def test_update_operation(asyncmy_session: AsyncmyConfig) -> None:
    """Test UPDATE operations."""
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
        )
        """
        await driver.execute_script(sql)

        # Insert a record first
        insert_result = await driver.execute("INSERT INTO test_table (name) VALUES (?)", ("original_name",))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        # Update the record
        update_result = await driver.execute("UPDATE test_table SET name = ? WHERE id = ?", ("updated_name", 1))
        assert isinstance(update_result, ExecuteResult)
        assert update_result.rows_affected == 1

        # Verify the update
        select_result = await driver.execute("SELECT name FROM test_table WHERE id = ?", (1,))
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert select_result.rows[0]["name"] == "updated_name"


@pytest.mark.xdist_group("mysql")
async def test_delete_operation(asyncmy_session: AsyncmyConfig) -> None:
    """Test DELETE operations."""
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
        )
        """
        await driver.execute_script(sql)

        # Insert a record first
        insert_result = await driver.execute("INSERT INTO test_table (name) VALUES (?)", ("to_delete",))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        # Delete the record
        delete_result = await driver.execute("DELETE FROM test_table WHERE id = ?", (1,))
        assert isinstance(delete_result, ExecuteResult)
        assert delete_result.rows_affected == 1

        # Verify the deletion
        select_result = await driver.execute("SELECT COUNT(*) as count FROM test_table")
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert select_result.rows[0]["count"] == 0
