"""Integration tests for MysqlConnector sync driver implementation."""

import math

import pytest

from sqlspec import SQL, SQLResult, StatementStack
from sqlspec.adapters.mysqlconnector import MysqlConnectorSyncDriver

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql_connector]


@pytest.fixture
def mysqlconnector_sync_driver(mysqlconnector_clean_sync_driver: MysqlConnectorSyncDriver) -> MysqlConnectorSyncDriver:
    """Create and manage test table lifecycle."""
    create_sql = """
        CREATE TABLE IF NOT EXISTS test_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    mysqlconnector_clean_sync_driver.execute_script(create_sql)
    mysqlconnector_clean_sync_driver.execute_script("TRUNCATE TABLE test_table")
    return mysqlconnector_clean_sync_driver


def test_mysqlconnector_sync_basic_crud(mysqlconnector_sync_driver: MysqlConnectorSyncDriver) -> None:
    """Test basic CRUD operations."""
    driver = mysqlconnector_sync_driver

    insert_result = driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("test_user", 42))
    assert insert_result.num_rows == 1

    select_result = driver.execute("SELECT * FROM test_table WHERE name = ?", ("test_user",))
    assert select_result.num_rows == 1
    row = select_result.get_data()[0]
    assert row["name"] == "test_user"
    assert row["value"] == 42

    update_result = driver.execute("UPDATE test_table SET value = ? WHERE name = ?", (100, "test_user"))
    assert update_result.num_rows == 1

    updated_result = driver.execute("SELECT value FROM test_table WHERE name = ?", ("test_user",))
    assert updated_result.get_data()[0]["value"] == 100

    delete_result = driver.execute("DELETE FROM test_table WHERE name = ?", ("test_user",))
    assert delete_result.num_rows == 1

    verify_result = driver.execute("SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("test_user",))
    assert verify_result.get_data()[0]["count"] == 0


def test_mysqlconnector_sync_execute_many(mysqlconnector_sync_driver: MysqlConnectorSyncDriver) -> None:
    """Test execute_many functionality."""
    driver = mysqlconnector_sync_driver

    data = [("batch_user_1", 100), ("batch_user_2", 200), ("batch_user_3", 300)]
    result = driver.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", data)
    assert result.num_rows == 3

    select_result = driver.execute(
        "SELECT name, value FROM test_table WHERE name LIKE ? ORDER BY name", ("batch_user_%",)
    )
    assert len(select_result.get_data()) == 3


def test_mysqlconnector_sync_execute_script(mysqlconnector_sync_driver: MysqlConnectorSyncDriver) -> None:
    """Test script execution with multiple statements."""
    driver = mysqlconnector_sync_driver

    script = """
        INSERT INTO test_table (name, value) VALUES ('script_user_1', 1000);
        INSERT INTO test_table (name, value) VALUES ('script_user_2', 2000);
        UPDATE test_table SET value = value * 2 WHERE name LIKE 'script_user_%';
    """

    result = driver.execute_script(script)
    assert result.operation_type == "SCRIPT"

    select_result = driver.execute(
        "SELECT name, value FROM test_table WHERE name LIKE ? ORDER BY name", ("script_user_%",)
    )
    assert len(select_result.get_data()) == 2


def test_mysqlconnector_sync_data_types(mysqlconnector_sync_driver: MysqlConnectorSyncDriver) -> None:
    """Test handling of various MySQL data types."""
    driver = mysqlconnector_sync_driver

    driver.execute_script("""
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

    result = driver.execute(
        """INSERT INTO data_types_test
           (text_col, int_col, float_col, bool_col, date_col, datetime_col, json_col)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        test_data,
    )
    assert result.rows_affected == 1

    select_result = driver.execute(
        "SELECT * FROM data_types_test WHERE text_col = ? AND int_col = ?", ("test_string", 42)
    )
    row = select_result.get_data()[0]
    assert row["text_col"] == "test_string"
    assert row["int_col"] == 42
    assert abs(row["float_col"] - math.pi) < 0.01
    assert row["bool_col"] in (True, 1)
    assert isinstance(row["json_col"], dict)
    assert row["json_col"]["key"] == "value"


def test_mysqlconnector_sync_statement_stack(mysqlconnector_sync_driver: MysqlConnectorSyncDriver) -> None:
    """StatementStack should execute sequentially for mysql-connector sync."""
    mysqlconnector_sync_driver.execute_script("TRUNCATE TABLE test_table")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "mysql-stack-one", 11))
        .push_execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (2, "mysql-stack-two", 22))
        .push_execute("SELECT COUNT(*) AS total FROM test_table WHERE name LIKE ?", ("mysql-stack-%",))
    )

    results = mysqlconnector_sync_driver.execute_stack(stack)

    assert len(results) == 3
    final_result = results[2].result
    assert isinstance(final_result, SQLResult)
    data = final_result.get_data()
    assert data[0]["total"] == 2


def test_mysqlconnector_sync_transactions(mysqlconnector_sync_driver: MysqlConnectorSyncDriver) -> None:
    """Test transaction management (begin, commit, rollback)."""
    driver = mysqlconnector_sync_driver

    driver.begin()
    driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("tx_user_1", 100))
    driver.commit()

    result = driver.execute("SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("tx_user_1",))
    assert result.get_data()[0]["count"] == 1

    driver.begin()
    driver.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("tx_user_2", 200))
    driver.rollback()

    result = driver.execute("SELECT COUNT(*) as count FROM test_table WHERE name = ?", ("tx_user_2",))
    assert result.get_data()[0]["count"] == 0


def test_mysqlconnector_sync_sql_object_execution(mysqlconnector_sync_driver: MysqlConnectorSyncDriver) -> None:
    """Test execution of SQL objects."""
    driver = mysqlconnector_sync_driver

    sql_obj = SQL("INSERT INTO test_table (name, value) VALUES (?, ?)", "sql_obj_test", 999)
    result = driver.execute(sql_obj)
    assert isinstance(result, SQLResult)
    assert result.num_rows == 1

    verify_result = driver.execute("SELECT name, value FROM test_table WHERE name = ?", ("sql_obj_test",))
    assert verify_result.get_data()[0]["name"] == "sql_obj_test"
    assert verify_result.get_data()[0]["value"] == 999
