"""Integration tests for PyMySQL driver implementation."""

from typing import TYPE_CHECKING

import pytest

from sqlspec import sql
from sqlspec.adapters.pymysql import PyMysqlDriver

if TYPE_CHECKING:
    from sqlspec.adapters.pymysql import PyMysqlConfig

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql, pytest.mark.pymysql]


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
