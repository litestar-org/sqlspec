"""Integration tests for asyncmy driver implementation.

This serves as a comprehensive test template for database drivers,
covering all core functionality including CRUD operations, parameter styles,
transaction management, and error handling.
"""

import math
from typing import Literal

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec import SQL, SQLResult, StatementStack, sql
from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.utils.serializers import from_json, to_json
from tests.conftest import requires_interpreted

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]

pytestmark = pytest.mark.xdist_group("mysql")


@pytest.fixture
async def asyncmy_driver(asyncmy_clean_driver: AsyncmyDriver) -> AsyncmyDriver:
    """Create and manage test table lifecycle."""

    create_sql = """
        CREATE TABLE IF NOT EXISTS test_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    await asyncmy_clean_driver.execute_script(create_sql)
    await asyncmy_clean_driver.execute_script("TRUNCATE TABLE test_table")

    return asyncmy_clean_driver


async def test_asyncmy_basic_crud(asyncmy_driver: AsyncmyDriver) -> None:
    """Test basic CRUD operations."""
    driver = asyncmy_driver

    insert_result = await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("test_user", 42))
    assert insert_result.num_rows == 1

    select_result = await driver.execute("SELECT * FROM test_table WHERE name = ?", ("test_user",))
    assert select_result.num_rows == 1
    assert len(select_result.get_data()) == 1
    row = select_result.get_data()[0]
    assert row["name"] == "test_user"
    assert row["value"] == 42

    update_result = await driver.execute("UPDATE test_table SET value = ? WHERE name = ?", (100, "test_user"))
    assert update_result.num_rows == 1

    updated_result = await driver.execute("SELECT value FROM test_table WHERE name = ?", ("test_user",))
    assert updated_result.get_data()[0]["value"] == 100

    delete_result = await driver.execute("DELETE FROM test_table WHERE name = ?", ("test_user",))
    assert delete_result.num_rows == 1

    verify_result = await driver.execute("SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("test_user",))
    assert verify_result.get_data()[0]["count"] == 0


async def test_asyncmy_parameter_styles(asyncmy_driver: AsyncmyDriver) -> None:
    """Test different parameter binding styles."""
    driver = asyncmy_driver

    result1 = await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("user1", 10))
    assert result1.num_rows == 1

    result2 = await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ["user2", 20])
    assert result2.num_rows == 1

    select_result = await driver.execute("SELECT name, value FROM test_table ORDER BY name")
    assert len(select_result.get_data()) == 2
    assert select_result.get_data()[0]["name"] == "user1"
    assert select_result.get_data()[0]["value"] == 10
    assert select_result.get_data()[1]["name"] == "user2"
    assert select_result.get_data()[1]["value"] == 20


async def test_asyncmy_execute_many(asyncmy_driver: AsyncmyDriver) -> None:
    """Test execute_many functionality."""
    driver = asyncmy_driver

    data = [("batch_user_1", 100), ("batch_user_2", 200), ("batch_user_3", 300)]

    result = await driver.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", data)
    assert result.num_rows == 3

    select_result = await driver.execute(
        "SELECT name, value FROM test_table WHERE name LIKE ? ORDER BY name", ("batch_user_%",)
    )
    assert len(select_result.get_data()) == 3
    assert select_result.get_data()[0]["name"] == "batch_user_1"
    assert select_result.get_data()[0]["value"] == 100


async def test_asyncmy_execute_script(asyncmy_driver: AsyncmyDriver) -> None:
    """Test script execution with multiple statements."""
    driver = asyncmy_driver

    script = """
        INSERT INTO test_table (name, value) VALUES ('script_user_1', 1000);
        INSERT INTO test_table (name, value) VALUES ('script_user_2', 2000);
        UPDATE test_table SET value = value * 2 WHERE name LIKE 'script_user_%';
    """

    result = await driver.execute_script(script)
    assert result.operation_type == "SCRIPT"

    select_result = await driver.execute(
        "SELECT name, value FROM test_table WHERE name LIKE ? ORDER BY name", ("script_user_%",)
    )
    assert len(select_result.get_data()) == 2
    assert select_result.get_data()[0]["value"] == 2000
    assert select_result.get_data()[1]["value"] == 4000


async def test_asyncmy_data_types(asyncmy_driver: AsyncmyDriver) -> None:
    """Test handling of various MySQL data types."""
    driver = asyncmy_driver

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS asyncmy_data_types_test (
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

    from datetime import date, datetime

    test_data = ("test_string", 42, math.pi, True, date(2023, 1, 1), datetime(2023, 1, 1, 12, 0, 0), '{"key": "value"}')

    result = await driver.execute(
        """INSERT INTO asyncmy_data_types_test
           (text_col, int_col, float_col, bool_col, date_col, datetime_col, json_col)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        test_data,
    )
    assert result.rows_affected == 1

    select_result = await driver.execute(
        "SELECT * FROM asyncmy_data_types_test WHERE text_col = ? AND int_col = ?", ("test_string", 42)
    )
    assert len(select_result.get_data()) == 1
    row = select_result.get_data()[0]
    assert row["text_col"] == "test_string"
    assert row["int_col"] == 42
    assert abs(row["float_col"] - math.pi) < 0.01
    assert row["bool_col"] == 1
    assert isinstance(row["json_col"], dict)
    assert row["json_col"]["key"] == "value"


async def test_asyncmy_statement_stack_sequential(asyncmy_driver: AsyncmyDriver) -> None:
    """StatementStack should execute sequentially for asyncmy (no native batching)."""

    await asyncmy_driver.execute_script("TRUNCATE TABLE test_table")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "mysql-stack-one", 11))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (2, "mysql-stack-two", 22))
        .push_execute("SELECT COUNT(*) AS total FROM test_table WHERE name LIKE ?", ("mysql-stack-%",))
    )

    results = await asyncmy_driver.execute_stack(stack)

    assert len(results) == 3
    assert results[0].rows_affected == 1
    assert results[1].rows_affected == 1
    final_result = results[2].result
    assert isinstance(final_result, SQLResult)
    data = final_result.get_data()
    assert data
    assert data[0]["total"] == 2


@requires_interpreted
async def test_asyncmy_statement_stack_continue_on_error(asyncmy_driver: AsyncmyDriver) -> None:
    """Continue-on-error should still work with sequential fallback."""

    await asyncmy_driver.execute_script("TRUNCATE TABLE test_table")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "mysql-initial", 5))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "mysql-duplicate", 15))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (2, "mysql-final", 25))
    )

    results = await asyncmy_driver.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[0].rows_affected == 1
    assert results[1].error is not None
    assert results[2].rows_affected == 1

    verify = await asyncmy_driver.execute("SELECT COUNT(*) AS total FROM test_table WHERE name LIKE ?", ("mysql-%",))
    assert verify.get_data()[0]["total"] == 2


async def test_asyncmy_driver_features_custom_serializers(mysql_service: MySQLService) -> None:
    """Ensure custom serializer and deserializer driver features are applied."""

    serializer_calls: list[object] = []

    def tracking_serializer(value: object) -> str:
        serializer_calls.append(value)
        return to_json(value)

    def tracking_deserializer(value: str | bytes) -> object:
        decoded = from_json(value)
        if isinstance(decoded, dict):
            decoded["extra_marker"] = True
        return decoded

    config = AsyncmyConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
        },
        driver_features={"json_serializer": tracking_serializer, "json_deserializer": tracking_deserializer},
    )

    async with config.provide_session() as session:
        await session.execute_script(
            """
            CREATE TABLE IF NOT EXISTS driver_feature_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                payload JSON
            );
            TRUNCATE TABLE driver_feature_test;
            """
        )

        payload = {"foo": "bar"}
        await session.execute("INSERT INTO driver_feature_test (payload) VALUES (?)", (payload,))

        assert serializer_calls
        assert serializer_calls[0] == payload

        select_result = await session.execute("SELECT payload FROM driver_feature_test ORDER BY id DESC LIMIT 1")
        stored_row = select_result.get_data()[0]
        assert stored_row["payload"]["foo"] == "bar"
        assert stored_row["payload"]["extra_marker"] is True


async def test_asyncmy_transaction_management(asyncmy_driver: AsyncmyDriver) -> None:
    """Test transaction management (begin, commit, rollback)."""
    driver = asyncmy_driver

    await driver.begin()
    await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("tx_user_1", 100))
    await driver.commit()

    result = await driver.execute("SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("tx_user_1",))
    assert result.get_data()[0]["count"] == 1

    await driver.begin()
    await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("tx_user_2", 200))
    await driver.rollback()

    result = await driver.execute("SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("tx_user_2",))
    assert result.get_data()[0]["count"] == 0


async def test_asyncmy_null_parameters(asyncmy_driver: AsyncmyDriver) -> None:
    """Test handling of NULL parameters."""
    driver = asyncmy_driver

    result = await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("null_test", None))
    assert result.num_rows == 1

    select_result = await driver.execute("SELECT name, value FROM test_table WHERE name = ?", ("null_test",))
    assert len(select_result.get_data()) == 1
    assert select_result.get_data()[0]["name"] == "null_test"
    assert select_result.get_data()[0]["value"] is None


async def test_asyncmy_error_handling(asyncmy_driver: AsyncmyDriver) -> None:
    """Test error handling and exception wrapping."""
    driver = asyncmy_driver

    with pytest.raises(Exception):
        await driver.execute("INVALID SQL STATEMENT")

    await driver.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "user1", 100))

    with pytest.raises(Exception):
        await driver.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "user2", 200))


async def test_asyncmy_large_result_set(asyncmy_driver: AsyncmyDriver) -> None:
    """Test handling of large result sets."""
    driver = asyncmy_driver

    batch_data = [(f"user_{i}", i * 10) for i in range(100)]
    await driver.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", batch_data)

    result = await driver.execute("SELECT * FROM test_table ORDER BY value")
    assert result.num_rows == 100
    assert len(result.get_data()) == 100
    assert result.get_data()[0]["name"] == "user_0"
    assert result.get_data()[99]["name"] == "user_99"


async def test_asyncmy_mysql_specific_features(asyncmy_driver: AsyncmyDriver) -> None:
    """Test MySQL-specific features and SQL constructs."""
    driver = asyncmy_driver

    await driver.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "duplicate_test", 100))

    _ = await driver.execute(
        """INSERT INTO test_table (id, name, value) VALUES (?, ?, ?) AS new
           ON DUPLICATE KEY UPDATE value = new.value + 50""",
        (1, "duplicate_test_updated", 200),
    )

    select_result = await driver.execute("SELECT name, value FROM test_table WHERE id = ?", (1,))
    assert select_result.get_data()[0]["value"] == 250


async def test_asyncmy_complex_queries(asyncmy_driver: AsyncmyDriver) -> None:
    """Test complex SQL queries with JOINs, subqueries, etc."""
    driver = asyncmy_driver

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id INT PRIMARY KEY,
            email VARCHAR(255),
            age INT
        )
    """)

    await driver.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "john_doe", 100))
    await driver.execute(
        "INSERT INTO user_profiles (user_id, email, age) VALUES (?, ?, ?)", (1, "john@example.com", 30)
    )

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


async def test_asyncmy_edge_cases(asyncmy_driver: AsyncmyDriver) -> None:
    """Test edge cases and boundary conditions."""
    driver = asyncmy_driver

    result = await driver.execute("SELECT 1 as test_col", ())
    assert len(result.get_data()) == 1
    assert result.get_data()[0]["test_col"] == 1

    result = await driver.execute("SELECT ? as param_value", (42,))
    assert result.get_data()[0]["param_value"] == 42

    data_with_nulls = [("user1", 100), ("user2", None), ("user3", 300)]

    result = await driver.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", data_with_nulls)
    assert result.num_rows == 3

    select_result = await driver.execute(
        "SELECT name, value FROM test_table WHERE name IN (?, ?, ?) ORDER BY name", ("user1", "user2", "user3")
    )
    assert len(select_result.get_data()) == 3
    assert select_result.get_data()[1]["value"] is None


async def test_asyncmy_result_metadata(asyncmy_driver: AsyncmyDriver) -> None:
    """Test SQL result metadata and properties."""
    driver = asyncmy_driver

    insert_result = await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("metadata_test", 500))
    assert insert_result.num_rows == 1
    assert insert_result.operation_type == "INSERT"
    assert insert_result.column_names is None or len(insert_result.column_names) == 0

    select_result = await driver.execute("SELECT id, name, value FROM test_table WHERE name = ?", ("metadata_test",))
    assert select_result.num_rows == 1
    assert select_result.operation_type == "SELECT"
    assert select_result.column_names == ["id", "name", "value"]
    assert len(select_result.get_data()) == 1

    empty_result = await driver.execute("SELECT * FROM test_table WHERE name = ?", ("nonexistent",))
    assert empty_result.num_rows == 0
    assert empty_result.operation_type == "SELECT"
    assert len(empty_result.get_data()) == 0


async def test_asyncmy_sql_object_execution(asyncmy_driver: AsyncmyDriver) -> None:
    """Test execution of SQL objects."""
    driver = asyncmy_driver

    sql_obj = SQL("INSERT INTO test_table (name, value) VALUES (?, ?)", "sql_obj_test", 999)
    result = await driver.execute(sql_obj)
    assert isinstance(result, SQLResult)
    assert result.num_rows == 1

    verify_result = await driver.execute("SELECT name, value FROM test_table WHERE name = ?", ("sql_obj_test",))
    assert len(verify_result.get_data()) == 1
    assert verify_result.get_data()[0]["name"] == "sql_obj_test"
    assert verify_result.get_data()[0]["value"] == 999

    select_sql = SQL("SELECT * FROM test_table WHERE value > ?", 500)
    select_result = await driver.execute(select_sql)
    assert isinstance(select_result, SQLResult)
    assert select_result.num_rows >= 1
    assert select_result.operation_type == "SELECT"


async def test_asyncmy_for_update_locking(asyncmy_driver: AsyncmyDriver) -> None:
    """Test FOR UPDATE row locking with MySQL."""

    driver = asyncmy_driver

    # Insert test data
    await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("mysql_lock", 100))

    try:
        await driver.begin()

        # Test basic FOR UPDATE
        result = await driver.select_one(
            sql.select("id", "name", "value").from_("test_table").where_eq("name", "mysql_lock").for_update()
        )
        assert result is not None
        assert result["name"] == "mysql_lock"
        assert result["value"] == 100

        await driver.commit()
    except Exception:
        await driver.rollback()
        raise


async def test_asyncmy_for_update_skip_locked(asyncmy_driver: AsyncmyDriver) -> None:
    """Test FOR UPDATE SKIP LOCKED with MySQL (MySQL 8.0+ feature)."""

    driver = asyncmy_driver

    # Insert test data
    await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("mysql_skip", 200))

    try:
        await driver.begin()

        # Test FOR UPDATE SKIP LOCKED
        result = await driver.select_one(
            sql.select("*").from_("test_table").where_eq("name", "mysql_skip").for_update(skip_locked=True)
        )
        assert result is not None
        assert result["name"] == "mysql_skip"

        await driver.commit()
    except Exception:
        await driver.rollback()
        raise


async def test_asyncmy_for_share_locking(asyncmy_driver: AsyncmyDriver) -> None:
    """Test FOR SHARE row locking with MySQL."""

    driver = asyncmy_driver

    # Insert test data
    await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("mysql_share", 300))

    try:
        await driver.begin()

        # Test basic FOR SHARE (MySQL uses FOR SHARE syntax like PostgreSQL)
        result = await driver.select_one(
            sql.select("id", "name", "value").from_("test_table").where_eq("name", "mysql_share").for_share()
        )
        assert result is not None
        assert result["name"] == "mysql_share"
        assert result["value"] == 300

        await driver.commit()
    except Exception:
        await driver.rollback()
        raise
