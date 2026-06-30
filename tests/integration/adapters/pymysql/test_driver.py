"""Integration tests for PyMySQL driver implementation."""

import pytest

from sqlspec import StatementStack, sql
from sqlspec.adapters.pymysql import PyMysqlConfig, PyMysqlDriver, default_statement_config

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql, pytest.mark.pymysql]


def _autocommit_reader_config(config: "PyMysqlConfig") -> PyMysqlConfig:
    connection_config = dict(config.connection_config)
    connection_config["autocommit"] = True
    return PyMysqlConfig(connection_config=connection_config, statement_config=default_statement_config)


def _create_transaction_table(driver: PyMysqlDriver) -> None:
    driver.execute_script("""
        CREATE TABLE IF NOT EXISTS transaction_test_pymysql (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0
        ) ENGINE=InnoDB
    """)


def _count_transaction_rows(driver: PyMysqlDriver, name: str) -> int:
    result = driver.execute("SELECT COUNT(*) as count FROM transaction_test_pymysql WHERE name = ?", (name,))
    return int(result.get_data()[0]["count"])


@pytest.fixture
def pymysql_driver(pymysql_clean_driver: PyMysqlDriver) -> PyMysqlDriver:
    """Create and manage test table lifecycle."""
    create_sql = """
        CREATE TABLE IF NOT EXISTS test_table_pymysql (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB
    """
    pymysql_clean_driver.execute_script(create_sql)
    pymysql_clean_driver.execute_script("DELETE FROM test_table_pymysql")
    return pymysql_clean_driver


def test_pymysql_transactions(pymysql_transaction_config: "PyMysqlConfig") -> None:
    """Test transaction management (begin, commit, rollback).

    Note: Uses a dedicated fixture with autocommit=False for proper transaction support.
    """
    import uuid

    test_id = uuid.uuid4().hex[:8]
    committed_name = f"tx_commit_{test_id}"
    rolled_back_name = f"tx_rollback_{test_id}"

    with pymysql_transaction_config.provide_session() as driver:
        # Create table for transaction testing (DDL auto-commits in MySQL)
        driver.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table_pymysql (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
        """)

        # Test commit - insert and commit should persist
        driver.execute("INSERT INTO test_table_pymysql (name, value) VALUES (?, ?)", (committed_name, 100))
        driver.commit()

        result = driver.execute("SELECT COUNT(*) as count FROM test_table_pymysql WHERE name = ?", (committed_name,))
        assert result.get_data()[0]["count"] == 1, "Committed data should be visible"

        # Test rollback - insert and rollback should NOT persist
        driver.execute("INSERT INTO test_table_pymysql (name, value) VALUES (?, ?)", (rolled_back_name, 200))
        driver.rollback()

        result = driver.execute("SELECT COUNT(*) as count FROM test_table_pymysql WHERE name = ?", (rolled_back_name,))
        assert result.get_data()[0]["count"] == 0, "Rolled back data should NOT be visible"


def test_pymysql_execute_stack_commits_owned_transaction_when_autocommit_disabled(
    pymysql_transaction_config: "PyMysqlConfig",
) -> None:
    """Owned StatementStack transactions commit even when the PyMySQL connection has autocommit disabled."""
    name = "stack_owned_autocommit_off"
    reader_config = _autocommit_reader_config(pymysql_transaction_config)
    try:
        with pymysql_transaction_config.provide_session() as driver:
            _create_transaction_table(driver)
            driver.execute("DELETE FROM transaction_test_pymysql WHERE name = ?", (name,))
            driver.commit()

            stack = StatementStack().push_execute(
                "INSERT INTO transaction_test_pymysql (name, value) VALUES (?, ?)", (name, 561)
            )
            driver.execute_stack(stack)

        pymysql_transaction_config.close_pool()

        with reader_config.provide_session() as reader:
            assert _count_transaction_rows(reader, name) == 1
            reader.execute("DELETE FROM transaction_test_pymysql WHERE name = ?", (name,))
            reader.commit()
    finally:
        pymysql_transaction_config.close_pool()
        reader_config.close_pool()


def test_pymysql_execute_stack_preserves_explicit_caller_transaction(
    pymysql_transaction_config: "PyMysqlConfig",
) -> None:
    """Explicit caller transactions remain caller-owned around StatementStack execution."""
    name = "stack_caller_owned"
    reader_config = _autocommit_reader_config(pymysql_transaction_config)
    try:
        with pymysql_transaction_config.provide_session() as driver:
            _create_transaction_table(driver)
            driver.execute("DELETE FROM transaction_test_pymysql WHERE name = ?", (name,))
            driver.commit()

            driver.begin()
            stack = StatementStack().push_execute(
                "INSERT INTO transaction_test_pymysql (name, value) VALUES (?, ?)", (name, 561)
            )
            driver.execute_stack(stack)

            with reader_config.provide_session() as reader:
                assert _count_transaction_rows(reader, name) == 0

            driver.commit()

        with reader_config.provide_session() as reader:
            assert _count_transaction_rows(reader, name) == 1
            reader.execute("DELETE FROM transaction_test_pymysql WHERE name = ?", (name,))
            reader.commit()
    finally:
        pymysql_transaction_config.close_pool()
        reader_config.close_pool()


def test_pymysql_for_update(pymysql_driver: PyMysqlDriver) -> None:
    """Test FOR UPDATE row locking with MySQL."""
    driver = pymysql_driver

    driver.execute("INSERT INTO test_table_pymysql (name, value) VALUES (?, ?)", ("mysql_lock", 100))

    try:
        driver.begin()
        result = driver.select_one(
            sql.select("id", "name", "value").from_("test_table_pymysql").where_eq("name", "mysql_lock").for_update()
        )
        assert result is not None
        assert result["name"] == "mysql_lock"
        driver.commit()
    except Exception:
        driver.rollback()
        raise
