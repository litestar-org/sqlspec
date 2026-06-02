"""mysql-connector-specific parameter variants not covered by generic contracts."""

from collections.abc import AsyncGenerator, Generator

import pytest

from sqlspec.adapters.mysqlconnector import MysqlConnectorAsyncDriver, MysqlConnectorSyncDriver
from sqlspec.core import SQLResult

pytestmark = pytest.mark.xdist_group("mysql")


@pytest.fixture
def mysqlconnector_sync_parameter_variants(
    mysqlconnector_clean_sync_driver: MysqlConnectorSyncDriver,
) -> Generator[MysqlConnectorSyncDriver, None, None]:
    """Provide mysql-connector sync data for native parameter variants."""
    mysqlconnector_clean_sync_driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_parameter_variants_mysqlconnector_sync (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0,
            active TINYINT(1)
        )
    """)
    mysqlconnector_clean_sync_driver.execute_script("DELETE FROM test_parameter_variants_mysqlconnector_sync")
    mysqlconnector_clean_sync_driver.execute_many(
        "INSERT INTO test_parameter_variants_mysqlconnector_sync (name, value, active) VALUES (?, ?, ?)",
        [("test1", 100, True), ("test2", 200, False), ("test3", 300, True)],
    )
    yield mysqlconnector_clean_sync_driver
    mysqlconnector_clean_sync_driver.execute_script("DROP TABLE IF EXISTS test_parameter_variants_mysqlconnector_sync")


@pytest.fixture
async def mysqlconnector_async_parameter_variants(
    mysqlconnector_clean_async_driver: MysqlConnectorAsyncDriver,
) -> AsyncGenerator[MysqlConnectorAsyncDriver, None]:
    """Provide mysql-connector async data for native parameter variants."""
    await mysqlconnector_clean_async_driver.execute_script("""
        CREATE TABLE IF NOT EXISTS test_parameter_variants_mysqlconnector_async (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT DEFAULT 0,
            active TINYINT(1)
        )
    """)
    await mysqlconnector_clean_async_driver.execute_script("DELETE FROM test_parameter_variants_mysqlconnector_async")
    await mysqlconnector_clean_async_driver.execute_many(
        "INSERT INTO test_parameter_variants_mysqlconnector_async (name, value, active) VALUES (?, ?, ?)",
        [("test1", 100, True), ("test2", 200, False), ("test3", 300, True)],
    )
    yield mysqlconnector_clean_async_driver
    await mysqlconnector_clean_async_driver.execute_script(
        "DROP TABLE IF EXISTS test_parameter_variants_mysqlconnector_async"
    )


def test_mysqlconnector_sync_native_pyformat_select(
    mysqlconnector_sync_parameter_variants: MysqlConnectorSyncDriver,
) -> None:
    """mysql-connector sync accepts native positional pyformat parameters."""
    result = mysqlconnector_sync_parameter_variants.execute(
        "SELECT name, value FROM test_parameter_variants_mysqlconnector_sync WHERE name = %s AND value > %s",
        ("test2", 150),
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test2", "value": 200}]


def test_mysqlconnector_sync_named_pyformat_select(
    mysqlconnector_sync_parameter_variants: MysqlConnectorSyncDriver,
) -> None:
    """mysql-connector sync converts named pyformat parameters."""
    result = mysqlconnector_sync_parameter_variants.execute(
        "SELECT name, value FROM test_parameter_variants_mysqlconnector_sync "
        "WHERE name = %(name)s AND value < %(maximum)s",
        {"name": "test3", "maximum": 350},
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test3", "value": 300}]


def test_mysqlconnector_sync_boolean_parameter(
    mysqlconnector_sync_parameter_variants: MysqlConnectorSyncDriver,
) -> None:
    """mysql-connector sync binds Python bools to TINYINT parameters."""
    result = mysqlconnector_sync_parameter_variants.execute(
        "SELECT name FROM test_parameter_variants_mysqlconnector_sync WHERE active = ? ORDER BY value", (True,)
    )

    assert result.get_data() == [{"name": "test1"}, {"name": "test3"}]


async def test_mysqlconnector_async_native_pyformat_select(
    mysqlconnector_async_parameter_variants: MysqlConnectorAsyncDriver,
) -> None:
    """mysql-connector async accepts native positional pyformat parameters."""
    result = await mysqlconnector_async_parameter_variants.execute(
        "SELECT name, value FROM test_parameter_variants_mysqlconnector_async WHERE name = %s AND value > %s",
        ("test2", 150),
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test2", "value": 200}]


async def test_mysqlconnector_async_named_pyformat_select(
    mysqlconnector_async_parameter_variants: MysqlConnectorAsyncDriver,
) -> None:
    """mysql-connector async converts named pyformat parameters."""
    result = await mysqlconnector_async_parameter_variants.execute(
        "SELECT name, value FROM test_parameter_variants_mysqlconnector_async "
        "WHERE name = %(name)s AND value < %(maximum)s",
        {"name": "test3", "maximum": 350},
    )

    assert isinstance(result, SQLResult)
    assert result.get_data() == [{"name": "test3", "value": 300}]


async def test_mysqlconnector_async_boolean_parameter(
    mysqlconnector_async_parameter_variants: MysqlConnectorAsyncDriver,
) -> None:
    """mysql-connector async binds Python bools to TINYINT parameters."""
    result = await mysqlconnector_async_parameter_variants.execute(
        "SELECT name FROM test_parameter_variants_mysqlconnector_async WHERE active = ? ORDER BY value", (True,)
    )

    assert result.get_data() == [{"name": "test1"}, {"name": "test3"}]
