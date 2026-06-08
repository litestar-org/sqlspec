"""Integration tests for MysqlConnector async driver implementation."""

import pytest

from sqlspec import sql
from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncDriver

pytestmark = [pytest.mark.xdist_group("mysql"), pytest.mark.mysql_connector]


@pytest.fixture
async def mysqlconnector_async_driver(
    mysqlconnector_clean_async_driver: MysqlConnectorAsyncDriver,
) -> MysqlConnectorAsyncDriver:
    """Create and manage test table lifecycle."""
    create_sql = """
        CREATE TABLE IF NOT EXISTS test_table_mysqlconnector_async (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    await mysqlconnector_clean_async_driver.execute_script(create_sql)
    await mysqlconnector_clean_async_driver.execute_script("DELETE FROM test_table_mysqlconnector_async")
    return mysqlconnector_clean_async_driver


async def test_mysqlconnector_async_for_update(mysqlconnector_async_driver: MysqlConnectorAsyncDriver) -> None:
    """Test FOR UPDATE row locking with MySQL."""
    driver = mysqlconnector_async_driver

    await driver.execute("INSERT INTO test_table_mysqlconnector_async (name, value) VALUES (?, ?)", ("mysql_lock", 100))

    try:
        await driver.begin()
        result = await driver.select_one(
            sql
            .select("id", "name", "value")
            .from_("test_table_mysqlconnector_async")
            .where_eq("name", "mysql_lock")
            .for_update()
        )
        assert result is not None
        assert result["name"] == "mysql_lock"
        await driver.commit()
    except Exception:
        await driver.rollback()
        raise
