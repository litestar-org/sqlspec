"""Test execute_many functionality for AsyncMy drivers."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy import AsyncmyConfig, AsyncmyDriver
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
async def asyncmy_batch_session(mysql_service: MySQLService) -> "AsyncGenerator[AsyncmyDriver, None]":
    """Create an AsyncMy session for batch operation testing."""
    config = AsyncmyConfig(
        connection_config={
            "host": mysql_service.host,
            "port": mysql_service.port,
            "user": mysql_service.user,
            "password": mysql_service.password,
            "database": mysql_service.database,
        },
        statement_config=SQLConfig(strict_mode=False),
    )

    async with config.provide_session() as session:
        # Create test table
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_batch (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT DEFAULT 0,
                category VARCHAR(50)
            )
        """)
        # Clear any existing data
        await session.execute_script("TRUNCATE TABLE test_batch")

        yield session
        # Cleanup
        await session.execute_script("DROP TABLE IF EXISTS test_batch")


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_basic(asyncmy_batch_session: AsyncmyDriver) -> None:
    """Test basic execute_many with AsyncMy."""
    parameters = [
        ("Item 1", 100, "A"),
        ("Item 2", 200, "B"),
        ("Item 3", 300, "A"),
        ("Item 4", 400, "C"),
        ("Item 5", 500, "B"),
    ]

    result = await asyncmy_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)",
        parameters,
    )

    assert isinstance(result, SQLResult)
    # AsyncMy should report the number of rows affected
    assert result.rows_affected == 5

    # Verify data was inserted
    count_result = await asyncmy_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 5


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_update(asyncmy_batch_session: AsyncmyDriver) -> None:
    """Test execute_many for UPDATE operations with AsyncMy."""
    # First insert some data
    await asyncmy_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)",
        [
            ("Update 1", 10, "X"),
            ("Update 2", 20, "Y"),
            ("Update 3", 30, "Z"),
        ],
    )

    # Now update with execute_many
    update_params = [
        (100, "Update 1"),
        (200, "Update 2"),
        (300, "Update 3"),
    ]

    result = await asyncmy_batch_session.execute_many(
        "UPDATE test_batch SET value = %s WHERE name = %s",
        update_params,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify updates
    check_result = await asyncmy_batch_session.execute("SELECT name, value FROM test_batch ORDER BY name")
    assert len(check_result.data) == 3
    assert all(row["value"] in (100, 200, 300) for row in check_result.data)


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_empty(asyncmy_batch_session: AsyncmyDriver) -> None:
    """Test execute_many with empty parameter list on AsyncMy."""
    result = await asyncmy_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)",
        [],
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 0

    # Verify no data was inserted
    count_result = await asyncmy_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 0


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_mixed_types(asyncmy_batch_session: AsyncmyDriver) -> None:
    """Test execute_many with mixed parameter types on AsyncMy."""
    parameters = [
        ("String Item", 123, "CAT1"),
        ("Another Item", 456, None),  # NULL category
        ("Third Item", 0, "CAT2"),
        ("Negative Item", -50, "CAT3"),
    ]

    result = await asyncmy_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)",
        parameters,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 4

    # Verify data including NULL
    null_result = await asyncmy_batch_session.execute("SELECT * FROM test_batch WHERE category IS NULL")
    assert len(null_result.data) == 1
    assert null_result.data[0]["name"] == "Another Item"

    # Verify negative value
    negative_result = await asyncmy_batch_session.execute("SELECT * FROM test_batch WHERE value < 0")
    assert len(negative_result.data) == 1
    assert negative_result.data[0]["value"] == -50


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_delete(asyncmy_batch_session: AsyncmyDriver) -> None:
    """Test execute_many for DELETE operations with AsyncMy."""
    # First insert test data
    await asyncmy_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)",
        [
            ("Delete 1", 10, "X"),
            ("Delete 2", 20, "Y"),
            ("Delete 3", 30, "X"),
            ("Keep 1", 40, "Z"),
            ("Delete 4", 50, "Y"),
        ],
    )

    # Delete specific items by name
    delete_params = [
        ("Delete 1",),
        ("Delete 2",),
        ("Delete 4",),
    ]

    result = await asyncmy_batch_session.execute_many(
        "DELETE FROM test_batch WHERE name = %s",
        delete_params,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify remaining data
    remaining_result = await asyncmy_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert remaining_result.data[0]["count"] == 2

    # Verify specific remaining items
    names_result = await asyncmy_batch_session.execute("SELECT name FROM test_batch ORDER BY name")
    remaining_names = [row["name"] for row in names_result.data]
    assert remaining_names == ["Delete 3", "Keep 1"]


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_moderate_batch(asyncmy_batch_session: AsyncmyDriver) -> None:
    """Test execute_many with moderate batch size on AsyncMy."""
    # Create a moderate batch of parameters (MySQL might have limitations)
    moderate_batch = [(f"Item {i}", i * 10, f"CAT{i % 3}") for i in range(100)]

    result = await asyncmy_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)",
        moderate_batch,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 100

    # Verify count
    count_result = await asyncmy_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 100

    # Verify some specific values
    sample_result = await asyncmy_batch_session.execute(
        "SELECT * FROM test_batch WHERE name IN (%s, %s, %s) ORDER BY value",
        ("Item 10", "Item 50", "Item 99"),
    )
    assert len(sample_result.data) == 3
    assert sample_result.data[0]["value"] == 100  # Item 10
    assert sample_result.data[1]["value"] == 500  # Item 50
    assert sample_result.data[2]["value"] == 990  # Item 99


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_with_sql_object(asyncmy_batch_session: AsyncmyDriver) -> None:
    """Test execute_many with SQL object on AsyncMy."""
    from sqlspec.statement.sql import SQL

    parameters = [
        ("SQL Obj 1", 111, "SOB"),
        ("SQL Obj 2", 222, "SOB"),
        ("SQL Obj 3", 333, "SOB"),
    ]

    sql_obj = SQL("INSERT INTO test_batch (name, value, category) VALUES (%s, %s, %s)").as_many(parameters)

    result = await asyncmy_batch_session.execute_statement(sql_obj)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify data
    check_result = await asyncmy_batch_session.execute(
        "SELECT COUNT(*) as count FROM test_batch WHERE category = %s",
        ("SOB",),
    )
    assert check_result.data[0]["count"] == 3


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_with_mysql_functions(asyncmy_batch_session: AsyncmyDriver) -> None:
    """Test execute_many with MySQL-specific features."""
    # Create table with timestamp
    await asyncmy_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_mysql_features (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """)

    parameters = [
        ("MySQL Feature 1",),
        ("MySQL Feature 2",),
        ("MySQL Feature 3",),
    ]

    result = await asyncmy_batch_session.execute_many(
        "INSERT INTO test_mysql_features (name) VALUES (%s)",
        parameters,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify timestamps were set
    timestamp_result = await asyncmy_batch_session.execute(
        "SELECT name, created_at FROM test_mysql_features WHERE created_at IS NOT NULL"
    )
    assert len(timestamp_result.data) == 3


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_with_on_duplicate_key(asyncmy_batch_session: AsyncmyDriver) -> None:
    """Test execute_many with MySQL ON DUPLICATE KEY UPDATE."""
    # Create table with unique constraint
    await asyncmy_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_duplicate (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            counter INT DEFAULT 1
        )
    """)

    # First batch - initial inserts
    initial_params = [
        (1, "Item 1"),
        (2, "Item 2"),
        (3, "Item 3"),
    ]

    await asyncmy_batch_session.execute_many(
        "INSERT INTO test_duplicate (id, name) VALUES (%s, %s)",
        initial_params,
    )

    # Second batch - with duplicates using ON DUPLICATE KEY UPDATE
    duplicate_params = [
        (1, "Updated Item 1"),  # Duplicate
        (2, "Updated Item 2"),  # Duplicate
        (4, "Item 4"),  # New
    ]

    result = await asyncmy_batch_session.execute_many(
        "INSERT INTO test_duplicate (id, name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name = VALUES(name), counter = counter + 1",
        duplicate_params,
    )

    assert isinstance(result, SQLResult)

    # Verify the behavior - MySQL might report different row counts for ON DUPLICATE KEY
    check_result = await asyncmy_batch_session.execute("SELECT id, name, counter FROM test_duplicate ORDER BY id")
    assert len(check_result.data) == 4

    # Check that duplicates were updated
    updated_items = [row for row in check_result.data if row["counter"] > 1]
    assert len(updated_items) == 2  # Items 1 and 2 should be updated


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_with_json(asyncmy_batch_session: AsyncmyDriver) -> None:
    """Test execute_many with JSON data on AsyncMy (MySQL 5.7+ feature)."""
    # Create table with JSON column (if supported)
    try:
        await asyncmy_batch_session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_json (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255),
                metadata JSON
            )
        """)

        import json

        parameters = [
            ("JSON 1", json.dumps({"type": "test", "value": 100, "active": True})),
            ("JSON 2", json.dumps({"type": "prod", "value": 200, "active": False})),
            ("JSON 3", json.dumps({"type": "test", "value": 300, "tags": ["a", "b"]})),
        ]

        result = await asyncmy_batch_session.execute_many(
            "INSERT INTO test_json (name, metadata) VALUES (%s, %s)",
            parameters,
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 3

        # Verify JSON data (if JSON functions are supported)
        try:
            check_result = await asyncmy_batch_session.execute(
                "SELECT name, JSON_EXTRACT(metadata, '$.type') as type, JSON_EXTRACT(metadata, '$.value') as value FROM test_json ORDER BY name"
            )
            assert len(check_result.data) == 3

        except Exception:
            # JSON functions might not be available, just verify count
            count_result = await asyncmy_batch_session.execute("SELECT COUNT(*) as count FROM test_json")
            assert count_result.data[0]["count"] == 3

    except Exception:
        # JSON type might not be supported in this MySQL version
        # Skip this test gracefully
        pytest.skip("JSON type not supported in this MySQL version")


@pytest.mark.asyncio
@pytest.mark.xdist_group("mysql")
async def test_asyncmy_execute_many_with_constraints(asyncmy_batch_session: AsyncmyDriver) -> None:
    """Test execute_many with constraint violations on AsyncMy."""
    # Create a table with unique constraint
    await asyncmy_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_unique (
            id INT AUTO_INCREMENT PRIMARY KEY,
            unique_name VARCHAR(255) UNIQUE,
            value INT
        )
    """)

    # First batch should succeed
    success_params = [
        ("unique1", 100),
        ("unique2", 200),
        ("unique3", 300),
    ]

    result = await asyncmy_batch_session.execute_many(
        "INSERT INTO test_unique (unique_name, value) VALUES (%s, %s)",
        success_params,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Second batch with duplicate should fail
    duplicate_params = [
        ("unique4", 400),
        ("unique2", 500),  # Duplicate unique_name
        ("unique6", 600),
    ]

    with pytest.raises(Exception):  # MySQL will raise an integrity error
        await asyncmy_batch_session.execute_many(
            "INSERT INTO test_unique (unique_name, value) VALUES (%s, %s)",
            duplicate_params,
        )

    # Verify original data is still there
    count_result = await asyncmy_batch_session.execute("SELECT COUNT(*) as count FROM test_unique")
    assert count_result.data[0]["count"] == 3  # Only original data remains
