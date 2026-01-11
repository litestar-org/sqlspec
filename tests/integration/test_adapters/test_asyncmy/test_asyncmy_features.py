"""AsyncMy-specific feature tests.

This test suite focuses on AsyncMy adapter specific functionality including:
- Connection pooling behavior
- MySQL-specific SQL features
- Async transaction handling
- Error handling and recovery
- Performance characteristics
"""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver, default_statement_config
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("mysql")


@pytest.fixture
async def asyncmy_pooled_session(mysql_service: MySQLService) -> AsyncGenerator[AsyncmyDriver, None]:
    """Create AsyncMy session with connection pooling."""
    config = AsyncmyConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "minsize": 2,
            "maxsize": 10,
            "echo": False,
        },
        statement_config=default_statement_config,
    )

    async with config.provide_session() as session:
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS concurrent_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                thread_id VARCHAR(50),
                value INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await session.execute_script("TRUNCATE TABLE concurrent_test")

        yield session


async def test_asyncmy_mysql_json_operations(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test MySQL JSON column operations."""
    driver = asyncmy_pooled_session

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


async def test_asyncmy_mysql_specific_sql_features(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test MySQL-specific SQL features and syntax."""
    driver = asyncmy_pooled_session

    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS mysql_features (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            value INT,
            status ENUM('active', 'inactive', 'pending') DEFAULT 'pending',
            tags SET('urgent', 'important', 'normal', 'low') DEFAULT 'normal'
        );
        TRUNCATE TABLE mysql_features;
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


async def test_asyncmy_transaction_isolation_levels(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test MySQL transaction isolation level handling."""
    driver = asyncmy_pooled_session

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


async def test_asyncmy_stored_procedures(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test stored procedure execution."""
    driver = asyncmy_pooled_session

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


async def test_asyncmy_bulk_operations_performance(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test bulk operations for performance characteristics."""
    driver = asyncmy_pooled_session

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


async def test_asyncmy_error_recovery(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test error handling and connection recovery."""
    driver = asyncmy_pooled_session

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


async def test_asyncmy_sql_object_advanced_features(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test SQL object integration with advanced AsyncMy features."""
    driver = asyncmy_pooled_session

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
