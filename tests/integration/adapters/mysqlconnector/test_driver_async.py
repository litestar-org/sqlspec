"""Integration tests for MysqlConnector async driver implementation."""

import math

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec import SQL, SQLResult, StatementStack, sql
from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncConfig, MysqlConnectorAsyncDriver
from sqlspec.utils.serializers import from_json, to_json

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql_connector, pytest.mark.asyncio]


@pytest.fixture
async def mysqlconnector_async_driver(
    mysqlconnector_clean_async_driver: MysqlConnectorAsyncDriver,
) -> MysqlConnectorAsyncDriver:
    """Create and manage test table lifecycle."""
    create_sql = """
        CREATE TABLE IF NOT EXISTS test_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    await mysqlconnector_clean_async_driver.execute_script(create_sql)
    await mysqlconnector_clean_async_driver.execute_script("TRUNCATE TABLE test_table")
    return mysqlconnector_clean_async_driver


async def test_mysqlconnector_async_basic_crud(mysqlconnector_async_driver: MysqlConnectorAsyncDriver) -> None:
    """Test basic CRUD operations."""
    driver = mysqlconnector_async_driver

    insert_result = await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("test_user", 42))
    assert insert_result.num_rows == 1

    select_result = await driver.execute("SELECT * FROM test_table WHERE name = ?", ("test_user",))
    assert select_result.num_rows == 1
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


async def test_mysqlconnector_async_execute_many(mysqlconnector_async_driver: MysqlConnectorAsyncDriver) -> None:
    """Test execute_many functionality."""
    driver = mysqlconnector_async_driver

    data = [("batch_user_1", 100), ("batch_user_2", 200), ("batch_user_3", 300)]
    result = await driver.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", data)
    assert result.num_rows == 3

    select_result = await driver.execute(
        "SELECT name, value FROM test_table WHERE name LIKE ? ORDER BY name", ("batch_user_%",)
    )
    assert len(select_result.get_data()) == 3


async def test_mysqlconnector_async_execute_script(mysqlconnector_async_driver: MysqlConnectorAsyncDriver) -> None:
    """Test script execution with multiple statements."""
    driver = mysqlconnector_async_driver

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


async def test_mysqlconnector_async_data_types(mysqlconnector_async_driver: MysqlConnectorAsyncDriver) -> None:
    """Test handling of various MySQL data types."""
    driver = mysqlconnector_async_driver

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

    from datetime import date, datetime

    test_data = ("test_string", 42, math.pi, True, date(2023, 1, 1), datetime(2023, 1, 1, 12, 0, 0), '{"key": "value"}')

    result = await driver.execute(
        """INSERT INTO data_types_test
           (text_col, int_col, float_col, bool_col, date_col, datetime_col, json_col)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        test_data,
    )
    assert result.rows_affected == 1

    select_result = await driver.execute(
        "SELECT * FROM data_types_test WHERE text_col = ? AND int_col = ?", ("test_string", 42)
    )
    row = select_result.get_data()[0]
    assert row["text_col"] == "test_string"
    assert row["int_col"] == 42
    assert abs(row["float_col"] - math.pi) < 0.01
    assert row["bool_col"] in (True, 1)
    assert isinstance(row["json_col"], dict)
    assert row["json_col"]["key"] == "value"


async def test_mysqlconnector_async_statement_stack(mysqlconnector_async_driver: MysqlConnectorAsyncDriver) -> None:
    """StatementStack should execute sequentially for mysql-connector."""
    await mysqlconnector_async_driver.execute_script("TRUNCATE TABLE test_table")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "mysql-stack-one", 11))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (2, "mysql-stack-two", 22))
        .push_execute("SELECT COUNT(*) AS total FROM test_table WHERE name LIKE ?", ("mysql-stack-%",))
    )

    results = await mysqlconnector_async_driver.execute_stack(stack)

    assert len(results) == 3
    final_result = results[2].result
    assert isinstance(final_result, SQLResult)
    data = final_result.get_data()
    assert data[0]["total"] == 2


async def test_mysqlconnector_async_driver_features_custom_serializers(mysql_service: MySQLService) -> None:
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


async def test_mysqlconnector_async_transactions(mysqlconnector_async_driver: MysqlConnectorAsyncDriver) -> None:
    """Test transaction management (begin, commit, rollback)."""
    driver = mysqlconnector_async_driver

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


async def test_mysqlconnector_async_sql_object_execution(
    mysqlconnector_async_driver: MysqlConnectorAsyncDriver,
) -> None:
    """Test execution of SQL objects."""
    driver = mysqlconnector_async_driver

    sql_obj = SQL("INSERT INTO test_table (name, value) VALUES (?, ?)", "sql_obj_test", 999)
    result = await driver.execute(sql_obj)
    assert isinstance(result, SQLResult)
    assert result.num_rows == 1

    verify_result = await driver.execute("SELECT name, value FROM test_table WHERE name = ?", ("sql_obj_test",))
    assert verify_result.get_data()[0]["name"] == "sql_obj_test"
    assert verify_result.get_data()[0]["value"] == 999

    select_sql = SQL("SELECT * FROM test_table WHERE value > ?", 500)
    select_result = await driver.execute(select_sql)
    assert isinstance(select_result, SQLResult)
    assert select_result.num_rows >= 1


async def test_mysqlconnector_async_for_update(mysqlconnector_async_driver: MysqlConnectorAsyncDriver) -> None:
    """Test FOR UPDATE row locking with MySQL."""
    driver = mysqlconnector_async_driver

    await driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("mysql_lock", 100))

    try:
        await driver.begin()
        result = await driver.select_one(
            sql.select("id", "name", "value").from_("test_table").where_eq("name", "mysql_lock").for_update()
        )
        assert result is not None
        assert result["name"] == "mysql_lock"
        await driver.commit()
    except Exception:
        await driver.rollback()
        raise
