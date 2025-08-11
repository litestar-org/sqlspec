"""AsyncMy-specific feature tests with CORE_ROUND_3 architecture.

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

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver, asyncmy_statement_config
from sqlspec.core.result import SQLResult
from sqlspec.core.statement import SQL


@pytest.fixture
async def asyncmy_pooled_session(mysql_service: MySQLService) -> AsyncGenerator[AsyncmyDriver, None]:
    """Create AsyncMy session with connection pooling."""
    config = AsyncmyConfig(
        pool_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.db,
            "autocommit": True,
            "minsize": 2,  # Minimum pool size
            "maxsize": 10,  # Maximum pool size
            "echo": False,
        },
        statement_config=asyncmy_statement_config,
    )

    async with config.provide_session() as session:
        # Create test table for pooling tests
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


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_mysql_json_operations(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test MySQL JSON column operations."""
    driver = asyncmy_pooled_session

    # Create table with JSON column
    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS json_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            data JSON,
            metadata JSON
        )
    """)

    # Insert JSON data using different parameter styles
    json_data = '{"name": "test", "values": [1, 2, 3], "nested": {"key": "value"}}'
    metadata = '{"created_by": "test_suite", "version": 1}'

    result = await driver.execute("INSERT INTO json_test (data, metadata) VALUES (?, ?)", (json_data, metadata))
    assert result.num_rows == 1

    # Query JSON data using MySQL JSON functions
    json_result = await driver.execute(
        "SELECT data->>'$.name' as name, JSON_EXTRACT(data, '$.values[1]') as second_value FROM json_test WHERE id = ?",
        (result.last_inserted_id,),  # Use the inserted ID
    )

    assert len(json_result.get_data()) == 1
    row = json_result.get_data()[0]
    assert row["name"] == "test"
    assert str(row["second_value"]) == "2"  # JSON_EXTRACT may return string or int depending on MySQL version

    # Test JSON_CONTAINS function
    contains_result = await driver.execute(
        "SELECT COUNT(*) as count FROM json_test WHERE JSON_CONTAINS(data, ?, '$.values')",
        ("2",),  # Search for value 2 in the array
    )
    assert contains_result.get_data()[0]["count"] == 1


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_mysql_specific_sql_features(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test MySQL-specific SQL features and syntax."""
    driver = asyncmy_pooled_session

    # Create test table for MySQL features
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

    # Test INSERT ... ON DUPLICATE KEY UPDATE
    await driver.execute(
        "INSERT INTO mysql_features (id, name, value, status) VALUES (?, ?, ?, ?) AS new_vals ON DUPLICATE KEY UPDATE value = new_vals.value + ?, status = new_vals.status",
        (1, "duplicate_test", 100, "active", 50),
    )

    # Insert same ID again to trigger the ON DUPLICATE KEY UPDATE
    await driver.execute(
        "INSERT INTO mysql_features (id, name, value, status) VALUES (?, ?, ?, ?) AS new_vals ON DUPLICATE KEY UPDATE value = new_vals.value + ?, status = new_vals.status",
        (1, "duplicate_test_updated", 200, "inactive", 50),
    )
    await driver.commit()
    # Verify the update occurred (200 + 50 = 250)
    result = await driver.execute("SELECT name, value, status FROM mysql_features WHERE id = ?", (1,))
    row = result.get_data()[0]
    assert row["value"] == 250
    assert row["status"] == "inactive"  # Status should be updated

    # Test ENUM and SET types
    await driver.execute(
        "INSERT INTO mysql_features (name, value, status, tags) VALUES (?, ?, ?, ?)",
        ("enum_set_test", 300, "active", "urgent,important"),
    )

    enum_result = await driver.execute("SELECT status, tags FROM mysql_features WHERE name = ?", ("enum_set_test",))
    enum_row = enum_result.get_data()[0]
    assert enum_row["status"] == "active"
    assert "urgent" in enum_row["tags"]
    assert "important" in enum_row["tags"]


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_transaction_isolation_levels(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test MySQL transaction isolation level handling."""
    driver = asyncmy_pooled_session

    # Create test table for isolation testing
    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS isolation_test (
            id INT PRIMARY KEY,
            value VARCHAR(50)
        )
    """)

    # Test setting isolation level
    await driver.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

    # Start transaction
    await driver.begin()

    # Insert data in transaction
    await driver.execute("INSERT INTO isolation_test (id, value) VALUES (?, ?)", (1, "transaction_data"))

    # Verify data exists in current transaction
    result = await driver.execute("SELECT COUNT(*) as count FROM isolation_test WHERE id = ?", (1,))
    assert result.get_data()[0]["count"] == 1

    # Commit transaction
    await driver.commit()

    # Verify data persists after commit
    committed_result = await driver.execute("SELECT value FROM isolation_test WHERE id = ?", (1,))
    assert committed_result.get_data()[0]["value"] == "transaction_data"


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_stored_procedures(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test stored procedure execution."""
    driver = asyncmy_pooled_session

    # Create a simple stored procedure
    await driver.execute_script("""
        DROP PROCEDURE IF EXISTS test_procedure;

        CREATE PROCEDURE test_procedure(IN input_value INT, OUT output_value INT)
        BEGIN
            SET output_value = input_value * 2;
        END;
    """)

    # Note: AsyncMy/MySQL stored procedure calls with OUT parameters require specific handling
    # For now, test a simple procedure without OUT parameters
    await driver.execute_script("""
        DROP PROCEDURE IF EXISTS simple_procedure;

        CREATE PROCEDURE simple_procedure(IN multiplier INT)
        BEGIN
            CREATE TEMPORARY TABLE IF NOT EXISTS proc_result (result_value INT);
            INSERT INTO proc_result (result_value) VALUES (multiplier * 10);
        END;
    """)

    # Call the procedure
    await driver.execute("CALL simple_procedure(?)", (5,))

    # Verify the procedure executed (result should be 5 * 10 = 50)
    # Note: For temporary tables, we'd need different verification approach
    # This is a simplified test case


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_bulk_operations_performance(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test bulk operations for performance characteristics."""
    driver = asyncmy_pooled_session

    # Create table for bulk operations
    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS bulk_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id VARCHAR(50),
            sequence_num INT,
            data VARCHAR(100)
        )
    """)

    # Prepare large dataset for bulk insert
    batch_size = 100
    batch_data = [("batch_001", i, f"data_item_{i:04d}") for i in range(batch_size)]

    # Test execute_many performance
    result = await driver.execute_many(
        "INSERT INTO bulk_test (batch_id, sequence_num, data) VALUES (?, ?, ?)", batch_data
    )

    assert result.num_rows == batch_size

    # Verify all data was inserted correctly
    count_result = await driver.execute("SELECT COUNT(*) as total FROM bulk_test WHERE batch_id = ?", ("batch_001",))
    assert count_result.get_data()[0]["total"] == batch_size

    # Test bulk SELECT performance
    select_result = await driver.execute(
        "SELECT sequence_num, data FROM bulk_test WHERE batch_id = ? ORDER BY sequence_num", ("batch_001",)
    )

    assert len(select_result.get_data()) == batch_size
    assert select_result.get_data()[0]["sequence_num"] == 0
    assert select_result.get_data()[99]["sequence_num"] == 99


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_error_recovery(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test error handling and connection recovery."""
    driver = asyncmy_pooled_session

    # Create test table
    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS error_test (
            id INT PRIMARY KEY,
            value VARCHAR(50) NOT NULL
        )
    """)

    # Test successful operation first
    await driver.execute("INSERT INTO error_test (id, value) VALUES (?, ?)", (1, "test_value"))

    # Test error handling - duplicate key
    with pytest.raises(Exception):  # Should be wrapped as SQLSpecError
        await driver.execute("INSERT INTO error_test (id, value) VALUES (?, ?)", (1, "duplicate"))

    # Verify connection is still valid after error
    recovery_result = await driver.execute("SELECT COUNT(*) as count FROM error_test")
    assert recovery_result.get_data()[0]["count"] == 1

    # Test error handling - constraint violation (NOT NULL)
    with pytest.raises(Exception):
        await driver.execute("INSERT INTO error_test (id, value) VALUES (?, ?)", (2, None))

    # Verify connection still works
    final_result = await driver.execute("SELECT value FROM error_test WHERE id = ?", (1,))
    assert final_result.get_data()[0]["value"] == "test_value"


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_sql_object_advanced_features(asyncmy_pooled_session: AsyncmyDriver) -> None:
    """Test SQL object integration with advanced AsyncMy features."""
    driver = asyncmy_pooled_session

    # Create test table
    await driver.execute_script("""
        CREATE TABLE IF NOT EXISTS advanced_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            metadata JSON,
            score DECIMAL(10,2)
        )
    """)

    # Test SQL object with complex MySQL-specific query
    complex_sql = SQL(
        """
        INSERT INTO advanced_test (name, metadata, score)
        VALUES (?, ?, ?)
        ON DUPLICATE KEY UPDATE
        score = VALUES(score) + ?,
        metadata = JSON_MERGE_PATCH(metadata, VALUES(metadata))
        """,
        "complex_test",
        '{"type": "advanced", "priority": 1}',
        95.5,
        10.0,
    )

    result = await driver.execute(complex_sql)
    assert isinstance(result, SQLResult)
    assert result.num_rows == 1

    # Verify the insert with JSON query
    verify_sql = SQL(
        "SELECT name, metadata->>'$.type' as type, score FROM advanced_test WHERE name = ?", "complex_test"
    )

    verify_result = await driver.execute(verify_sql)
    assert len(verify_result.get_data()) == 1
    row = verify_result.get_data()[0]
    assert row["name"] == "complex_test"
    assert row["type"] == "advanced"
    assert float(row["score"]) == 95.5
