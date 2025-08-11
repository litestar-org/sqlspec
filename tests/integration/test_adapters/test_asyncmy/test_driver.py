"""Integration tests for asyncmy driver implementation.

This serves as a comprehensive test template for database drivers,
covering all core functionality including CRUD operations, parameter styles,
transaction management, and error handling.
"""

from __future__ import annotations

import math
from collections.abc import AsyncGenerator
from typing import Literal

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver, asyncmy_statement_config
from sqlspec.core.result import SQLResult
from sqlspec.core.statement import SQL

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


@pytest.fixture
async def asyncmy_driver(asyncmy_session: AsyncmyDriver) -> AsyncmyDriver:
    """Create and manage test table lifecycle."""
    # Create test table
    create_sql = """
        CREATE TABLE IF NOT EXISTS test_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    await asyncmy_session.execute_script(create_sql)

    # Clear any existing data
    await asyncmy_session.execute_script("TRUNCATE TABLE test_table")

    return asyncmy_session


@pytest.fixture
async def asyncmy_session(mysql_service: MySQLService) -> AsyncGenerator[AsyncmyDriver, None]:
    """Create an asyncmy session with test database."""
    config = AsyncmyConfig(
        pool_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,  # Enable autocommit for tests
        },
        statement_config=asyncmy_statement_config,
    )

    async with config.provide_session() as session:
        yield session


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_basic_crud(asyncmy_driver: AsyncmyDriver) -> None:
    """Test basic CRUD operations."""
    driver = asyncmy_driver

    # INSERT
    insert_result = await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("test_user", 42))
    assert insert_result.num_rows == 1

    # SELECT
    select_result = await driver.execute("SELECT * FROM test_table WHERE name = ?", ("test_user",))
    assert select_result.num_rows == 1
    assert len(select_result.get_data()) == 1
    row = select_result.get_data()[0]
    assert row["name"] == "test_user"
    assert row["value"] == 42

    # UPDATE
    update_result = await driver.execute("UPDATE test_table SET value = ? WHERE name = ?", (100, "test_user"))
    assert update_result.num_rows == 1

    # Verify update
    updated_result = await driver.execute("SELECT value FROM test_table WHERE name = ?", ("test_user",))
    assert updated_result.get_data()[0]["value"] == 100

    # DELETE
    delete_result = await driver.execute("DELETE FROM test_table WHERE name = ?", ("test_user",))
    assert delete_result.num_rows == 1

    # Verify deletion
    verify_result = await driver.execute("SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("test_user",))
    assert verify_result.get_data()[0]["count"] == 0


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_parameter_styles(asyncmy_driver: AsyncmyDriver) -> None:
    """Test different parameter binding styles."""
    driver = asyncmy_driver

    # Test positional parameters (tuple)
    result1 = await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("user1", 10))
    assert result1.num_rows == 1

    # Test positional parameters (list)
    result2 = await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ["user2", 20])
    assert result2.num_rows == 1

    # Verify both inserts
    select_result = await driver.execute("SELECT name, value FROM test_table ORDER BY name")
    assert len(select_result.get_data()) == 2
    assert select_result.get_data()[0]["name"] == "user1"
    assert select_result.get_data()[0]["value"] == 10
    assert select_result.get_data()[1]["name"] == "user2"
    assert select_result.get_data()[1]["value"] == 20


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many(asyncmy_driver: AsyncmyDriver) -> None:
    """Test execute_many functionality."""
    driver = asyncmy_driver

    # Test batch insert
    data = [("batch_user_1", 100), ("batch_user_2", 200), ("batch_user_3", 300)]

    result = await driver.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", data)
    assert result.num_rows == 3

    # Verify batch insert
    select_result = await driver.execute(
        "SELECT name, value FROM test_table WHERE name LIKE ? ORDER BY name", ("batch_user_%",)
    )
    assert len(select_result.get_data()) == 3
    assert select_result.get_data()[0]["name"] == "batch_user_1"
    assert select_result.get_data()[0]["value"] == 100


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_script(asyncmy_driver: AsyncmyDriver) -> None:
    """Test script execution with multiple statements."""
    driver = asyncmy_driver

    script = """
        INSERT INTO test_table (name, value) VALUES ('script_user_1', 1000);
        INSERT INTO test_table (name, value) VALUES ('script_user_2', 2000);
        UPDATE test_table SET value = value * 2 WHERE name LIKE 'script_user_%';
    """

    result = await driver.execute_script(script)
    assert result.operation_type == "SCRIPT"  # Check operation type

    # Verify script execution
    select_result = await driver.execute(
        "SELECT name, value FROM test_table WHERE name LIKE ? ORDER BY name", ("script_user_%",)
    )
    assert len(select_result.get_data()) == 2
    assert select_result.get_data()[0]["value"] == 2000  # 1000 * 2
    assert select_result.get_data()[1]["value"] == 4000  # 2000 * 2


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_data_types(asyncmy_driver: AsyncmyDriver) -> None:
    """Test handling of various MySQL data types."""
    driver = asyncmy_driver

    # Create table with various data types
    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS data_types_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            text_col VARCHAR(255),
            int_col INT,
            float_col FLOAT,
            bool_col BOOLEAN,
            date_col DATE,
            datetime_col DATETIME,
            json_col JSON
        )
    """)

    # Insert test data
    from datetime import date, datetime

    test_data = ("test_string", 42, math.pi, True, date(2023, 1, 1), datetime(2023, 1, 1, 12, 0, 0), '{"key": "value"}')

    result = await driver.execute(
        """INSERT INTO data_types_test
           (text_col, int_col, float_col, bool_col, date_col, datetime_col, json_col)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        test_data,
    )
    assert result.rows_affected == 1

    # Retrieve and verify data using unique test values
    select_result = await driver.execute(
        "SELECT * FROM data_types_test WHERE text_col = ? AND int_col = ?", ("test_string", 42)
    )
    assert len(select_result.get_data()) == 1
    row = select_result.get_data()[0]
    assert row["text_col"] == "test_string"
    assert row["int_col"] == 42
    assert abs(row["float_col"] - math.pi) < 0.01
    assert row["bool_col"] == 1  # MySQL returns boolean as int


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_transaction_management(asyncmy_driver: AsyncmyDriver) -> None:
    """Test transaction management (begin, commit, rollback)."""
    driver = asyncmy_driver

    # Test successful transaction
    await driver.begin()
    await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("tx_user_1", 100))
    await driver.commit()

    # Verify commit
    result = await driver.execute("SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("tx_user_1",))
    assert result.get_data()[0]["count"] == 1

    # Test rollback transaction
    await driver.begin()
    await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("tx_user_2", 200))
    await driver.rollback()

    # Verify rollback
    result = await driver.execute("SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("tx_user_2",))
    assert result.get_data()[0]["count"] == 0


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_null_parameters(asyncmy_driver: AsyncmyDriver) -> None:
    """Test handling of NULL parameters."""
    driver = asyncmy_driver

    # Insert with NULL value
    result = await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("null_test", None))
    assert result.num_rows == 1

    # Verify NULL handling
    select_result = await driver.execute("SELECT name, value FROM test_table WHERE name = ?", ("null_test",))
    assert len(select_result.get_data()) == 1
    assert select_result.get_data()[0]["name"] == "null_test"
    assert select_result.get_data()[0]["value"] is None


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_error_handling(asyncmy_driver: AsyncmyDriver) -> None:
    """Test error handling and exception wrapping."""
    driver = asyncmy_driver

    # Test SQL syntax error
    with pytest.raises(Exception):  # Should be wrapped as SQLSpecError
        await driver.execute("INVALID SQL STATEMENT")

    # Test constraint violation (duplicate primary key)
    await driver.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "user1", 100))

    with pytest.raises(Exception):  # Should be wrapped as SQLSpecError
        await driver.execute(
            "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
            (1, "user2", 200),  # Same ID
        )


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_large_result_set(asyncmy_driver: AsyncmyDriver) -> None:
    """Test handling of large result sets."""
    driver = asyncmy_driver

    # Insert multiple rows for testing
    batch_data = [(f"user_{i}", i * 10) for i in range(100)]
    await driver.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", batch_data)

    # Test large SELECT
    result = await driver.execute("SELECT * FROM test_table ORDER BY value")
    assert result.num_rows == 100
    assert len(result.get_data()) == 100
    assert result.get_data()[0]["name"] == "user_0"
    assert result.get_data()[99]["name"] == "user_99"


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_mysql_specific_features(asyncmy_driver: AsyncmyDriver) -> None:
    """Test MySQL-specific features and SQL constructs."""
    driver = asyncmy_driver

    # Test MySQL INSERT ... ON DUPLICATE KEY UPDATE
    await driver.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "duplicate_test", 100))

    # Use ON DUPLICATE KEY UPDATE
    _ = await driver.execute(
        """INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)
           ON DUPLICATE KEY UPDATE value = VALUES(value) + 50""",
        (1, "duplicate_test_updated", 200),
    )

    # Verify the update occurred
    select_result = await driver.execute("SELECT name, value FROM test_table WHERE id = ?", (1,))
    assert select_result.get_data()[0]["value"] == 250  # 200 + 50


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_complex_queries(asyncmy_driver: AsyncmyDriver) -> None:
    """Test complex SQL queries with JOINs, subqueries, etc."""
    driver = asyncmy_driver

    # Create additional table for JOINs
    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id INT PRIMARY KEY,
            email VARCHAR(255),
            age INT
        )
    """)

    # Insert test data
    await driver.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "john_doe", 100))
    await driver.execute(
        "INSERT INTO user_profiles (user_id, email, age) VALUES (?, ?, ?)", (1, "john@example.com", 30)
    )

    # Test JOIN query
    result = await driver.execute(
        """
        SELECT t.name, t.value, p.email, p.age
        FROM test_table t
        JOIN user_profiles p ON t.id = p.user_id
        WHERE t.name = ?
    """,
        ("john_doe",),
    )

    assert len(result.get_data()) == 1
    row = result.get_data()[0]
    assert row["name"] == "john_doe"
    assert row["email"] == "john@example.com"
    assert row["age"] == 30


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_edge_cases(asyncmy_driver: AsyncmyDriver) -> None:
    """Test edge cases and boundary conditions."""
    driver = asyncmy_driver

    # Test empty parameter list
    result = await driver.execute("SELECT 1 as test_col", ())
    assert len(result.get_data()) == 1
    assert result.get_data()[0]["test_col"] == 1

    # Test single parameter
    result = await driver.execute("SELECT ? as param_value", (42,))
    assert result.get_data()[0]["param_value"] == 42

    # Test None parameters in execute_many
    data_with_nulls = [("user1", 100), ("user2", None), ("user3", 300)]

    result = await driver.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", data_with_nulls)
    assert result.num_rows == 3

    # Verify NULL handling in batch
    select_result = await driver.execute(
        "SELECT name, value FROM test_table WHERE name IN (?, ?, ?) ORDER BY name", ("user1", "user2", "user3")
    )
    assert len(select_result.get_data()) == 3
    assert select_result.get_data()[1]["value"] is None  # user2


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_result_metadata(asyncmy_driver: AsyncmyDriver) -> None:
    """Test SQL result metadata and properties."""
    driver = asyncmy_driver

    # Test INSERT result metadata
    insert_result = await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("metadata_test", 500))
    assert insert_result.num_rows == 1
    assert insert_result.operation_type == "INSERT"  # Check operation type
    assert insert_result.column_names is None or len(insert_result.column_names) == 0

    # Test SELECT result metadata
    select_result = await driver.execute("SELECT id, name, value FROM test_table WHERE name = ?", ("metadata_test",))
    assert select_result.num_rows == 1
    assert select_result.operation_type == "SELECT"
    assert select_result.column_names == ["id", "name", "value"]
    assert len(select_result.get_data()) == 1

    # Test no-results query metadata
    empty_result = await driver.execute("SELECT * FROM test_table WHERE name = ?", ("nonexistent",))
    assert empty_result.num_rows == 0
    assert empty_result.operation_type == "SELECT"
    assert len(empty_result.get_data()) == 0


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_sql_object_execution(asyncmy_driver: AsyncmyDriver) -> None:
    """Test execution of SQL objects with CORE_ROUND_3 architecture."""
    driver = asyncmy_driver

    # Test SQL object with positional parameters
    sql_obj = SQL("INSERT INTO test_table (name, value) VALUES (?, ?)", "sql_obj_test", 999)
    result = await driver.execute(sql_obj)
    assert isinstance(result, SQLResult)
    assert result.num_rows == 1

    # Verify the insert
    verify_result = await driver.execute("SELECT name, value FROM test_table WHERE name = ?", ("sql_obj_test",))
    assert len(verify_result.get_data()) == 1
    assert verify_result.get_data()[0]["name"] == "sql_obj_test"
    assert verify_result.get_data()[0]["value"] == 999

    # Test SQL object for SELECT
    select_sql = SQL("SELECT * FROM test_table WHERE value > ?", 500)
    select_result = await driver.execute(select_sql)
    assert isinstance(select_result, SQLResult)
    assert select_result.num_rows >= 1
    assert select_result.operation_type == "SELECT"
