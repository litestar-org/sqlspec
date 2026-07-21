"""Shared feature coverage for the async MySQL adapters."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.aiomysql import AiomysqlConfig, AiomysqlDriver
from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("mysql")

MySQLAsyncConfig = AsyncmyConfig | AiomysqlConfig
MySQLAsyncDriver = AsyncmyDriver | AiomysqlDriver

_MYSQL_FEATURE_TABLES = (
    "concurrent_test",
    "json_test",
    "mysql_features",
    "isolation_test",
    "bulk_test",
    "error_test",
    "advanced_test",
)


async def _clean_feature_objects(driver: MySQLAsyncDriver) -> None:
    await driver.execute("SET sql_notes = 0")
    for table in _MYSQL_FEATURE_TABLES:
        await driver.execute_script(f"DROP TABLE IF EXISTS {table}")
    for procedure in ("test_procedure", "simple_procedure"):
        await driver.execute_script(f"DROP PROCEDURE IF EXISTS {procedure}")
    await driver.execute("SET sql_notes = 1")


@pytest.fixture(params=("asyncmy", "aiomysql"))
async def mysql_async_driver(
    request: pytest.FixtureRequest, asyncmy_config: AsyncmyConfig, aiomysql_config: AiomysqlConfig
) -> AsyncGenerator[MySQLAsyncDriver, None]:
    """Provide equivalent sessions for both async MySQL adapters."""
    config: MySQLAsyncConfig = asyncmy_config if request.param == "asyncmy" else aiomysql_config
    async with config.provide_session() as driver:
        await _clean_feature_objects(driver)
        await driver.execute_script("""
            CREATE TABLE concurrent_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                thread_id VARCHAR(50),
                value INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        try:
            yield driver
        finally:
            await _clean_feature_objects(driver)


async def test_mysql_async_mysql_json_operations(mysql_async_driver: MySQLAsyncDriver) -> None:
    """Test MySQL JSON column operations."""
    driver = mysql_async_driver

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS json_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            data JSON,
            metadata JSON
        )
    """)

    json_data = '{"name": "test", "values": [1, 2, 3], "nested": {"key": "value"}}'
    metadata = '{"created_by": "test_suite", "version": 1}'

    result = await driver.execute("INSERT INTO json_test (data, metadata) VALUES (?, ?)", (json_data, metadata))
    assert result.num_rows == 1

    json_result = await driver.execute(
        "SELECT data->>'$.name' as name, JSON_EXTRACT(data, '$.values[1]') as second_value FROM json_test WHERE id = ?",
        (result.last_inserted_id,),
    )

    assert len(json_result.get_data()) == 1
    row = json_result.get_data()[0]
    assert row["name"] == "test"
    assert str(row["second_value"]) == "2"

    contains_result = await driver.execute(
        "SELECT COUNT(*) as count FROM json_test WHERE JSON_CONTAINS(data, ?, '$.values')", ("2",)
    )
    assert contains_result.get_data()[0]["count"] == 1


async def test_mysql_async_mysql_specific_sql_features(mysql_async_driver: MySQLAsyncDriver) -> None:
    """Test MySQL-specific SQL features and syntax."""
    driver = mysql_async_driver

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS mysql_features (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            value INT,
            status ENUM('active', 'inactive', 'pending') DEFAULT 'pending',
            tags SET('urgent', 'important', 'normal', 'low') DEFAULT 'normal'
        );
        DELETE FROM mysql_features;
    """)

    await driver.execute(
        "INSERT INTO mysql_features (id, name, value, status) VALUES (?, ?, ?, ?) AS new_vals ON DUPLICATE KEY UPDATE value = new_vals.value + ?, status = new_vals.status",
        (1, "duplicate_test", 100, "active", 50),
    )

    await driver.execute(
        "INSERT INTO mysql_features (id, name, value, status) VALUES (?, ?, ?, ?) AS new_vals ON DUPLICATE KEY UPDATE value = new_vals.value + ?, status = new_vals.status",
        (1, "duplicate_test_updated", 200, "inactive", 50),
    )
    await driver.commit()

    result = await driver.execute("SELECT name, value, status FROM mysql_features WHERE id = ?", (1,))
    row = result.get_data()[0]
    assert row["value"] == 250
    assert row["status"] == "inactive"

    await driver.execute(
        "INSERT INTO mysql_features (name, value, status, tags) VALUES (?, ?, ?, ?)",
        ("enum_set_test", 300, "active", "urgent,important"),
    )

    enum_result = await driver.execute("SELECT status, tags FROM mysql_features WHERE name = ?", ("enum_set_test",))
    enum_row = enum_result.get_data()[0]
    assert enum_row["status"] == "active"
    assert "urgent" in enum_row["tags"]
    assert "important" in enum_row["tags"]


async def test_mysql_async_transaction_isolation_levels(mysql_async_driver: MySQLAsyncDriver) -> None:
    """Test MySQL transaction isolation level handling."""
    driver = mysql_async_driver

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS isolation_test (
            id INT PRIMARY KEY,
            value VARCHAR(50)
        )
    """)

    await driver.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

    await driver.begin()

    await driver.execute("INSERT INTO isolation_test (id, value) VALUES (?, ?)", (1, "transaction_data"))

    result = await driver.execute("SELECT COUNT(*) as count FROM isolation_test WHERE id = ?", (1,))
    assert result.get_data()[0]["count"] == 1

    await driver.commit()

    committed_result = await driver.execute("SELECT value FROM isolation_test WHERE id = ?", (1,))
    assert committed_result.get_data()[0]["value"] == "transaction_data"


async def test_mysql_async_stored_procedures(mysql_async_driver: MySQLAsyncDriver) -> None:
    """Test stored procedure execution."""
    driver = mysql_async_driver

    await driver.execute_script("""
        DROP PROCEDURE IF EXISTS test_procedure;

        CREATE PROCEDURE test_procedure(IN input_value INT, OUT output_value INT)
        BEGIN
            SET output_value = input_value * 2;
        END;
    """)

    await driver.execute_script("""
        DROP PROCEDURE IF EXISTS simple_procedure;

        CREATE PROCEDURE simple_procedure(IN multiplier INT)
        BEGIN
            CREATE TEMPORARY TABLE IF NOT EXISTS proc_result (result_value INT);
            INSERT INTO proc_result (result_value) VALUES (multiplier * 10);
        END;
    """)

    await driver.execute("CALL simple_procedure(?)", (5,))


async def test_mysql_async_bulk_operations_performance(mysql_async_driver: MySQLAsyncDriver) -> None:
    """Test bulk operations for performance characteristics."""
    driver = mysql_async_driver

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS bulk_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id VARCHAR(50),
            sequence_num INT,
            data VARCHAR(100)
        )
    """)

    batch_size = 100
    batch_data = [("batch_001", i, f"data_item_{i:04d}") for i in range(batch_size)]

    result = await driver.execute_many(
        "INSERT INTO bulk_test (batch_id, sequence_num, data) VALUES (?, ?, ?)", batch_data
    )

    assert result.num_rows == batch_size

    count_result = await driver.execute("SELECT COUNT(*) as total FROM bulk_test WHERE batch_id = ?", ("batch_001",))
    assert count_result.get_data()[0]["total"] == batch_size

    select_result = await driver.execute(
        "SELECT sequence_num, data FROM bulk_test WHERE batch_id = ? ORDER BY sequence_num", ("batch_001",)
    )

    assert len(select_result.get_data()) == batch_size
    assert select_result.get_data()[0]["sequence_num"] == 0
    assert select_result.get_data()[99]["sequence_num"] == 99


async def test_mysql_async_error_recovery(mysql_async_driver: MySQLAsyncDriver) -> None:
    """Test error handling and connection recovery."""
    driver = mysql_async_driver

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS error_test (
            id INT PRIMARY KEY,
            value VARCHAR(50) NOT NULL
        )
    """)

    await driver.execute("INSERT INTO error_test (id, value) VALUES (?, ?)", (1, "test_value"))

    with pytest.raises(Exception):
        await driver.execute("INSERT INTO error_test (id, value) VALUES (?, ?)", (1, "duplicate"))

    recovery_result = await driver.execute("SELECT COUNT(*) as count FROM error_test")
    assert recovery_result.get_data()[0]["count"] == 1

    with pytest.raises(Exception):
        await driver.execute("INSERT INTO error_test (id, value) VALUES (?, ?)", (2, None))

    final_result = await driver.execute("SELECT value FROM error_test WHERE id = ?", (1,))
    assert final_result.get_data()[0]["value"] == "test_value"


async def test_mysql_async_sql_object_advanced_features(mysql_async_driver: MySQLAsyncDriver) -> None:
    """Test SQL object integration with advanced MySQL async features."""
    driver = mysql_async_driver

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS advanced_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            metadata JSON,
            score DECIMAL(10,2)
        )
    """)

    complex_sql = SQL(
        """
        INSERT INTO advanced_test (name, metadata, score)
        VALUES (?, ?, ?)
        AS new_vals
        ON DUPLICATE KEY UPDATE
        score = new_vals.score + ?,
        metadata = JSON_MERGE_PATCH(advanced_test.metadata, new_vals.metadata)
        """,
        "complex_test",
        '{"type": "advanced", "priority": 1}',
        95.5,
        10.0,
    )

    result = await driver.execute(complex_sql)
    assert isinstance(result, SQLResult)
    assert result.num_rows == 1

    verify_sql = SQL(
        "SELECT name, metadata->>'$.type' as type, score FROM advanced_test WHERE name = ?", "complex_test"
    )

    verify_result = await driver.execute(verify_sql)
    assert len(verify_result.get_data()) == 1
    row = verify_result.get_data()[0]
    assert row["name"] == "complex_test"
    assert row["type"] == "advanced"
    assert float(row["score"]) == 95.5
