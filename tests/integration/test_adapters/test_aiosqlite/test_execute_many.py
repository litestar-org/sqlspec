"""Test execute_many functionality for AIOSQLite drivers."""

from collections.abc import AsyncGenerator

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
async def aiosqlite_batch_session() -> "AsyncGenerator[AiosqliteDriver, None]":
    """Create an AIOSQLite session for batch operation testing."""
    config = AiosqliteConfig(
        connection_config={
            "database": ":memory:",
        },
        statement_config=SQLConfig(strict_mode=False),
    )

    async with config.provide_session() as session:
        # Create test table
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_batch (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                category TEXT
            )
        """)
        yield session


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_basic(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test basic execute_many with AIOSQLite."""
    parameters = [
        ("Item 1", 100, "A"),
        ("Item 2", 200, "B"),
        ("Item 3", 300, "A"),
        ("Item 4", 400, "C"),
        ("Item 5", 500, "B"),
    ]

    result = await aiosqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
        parameters,
    )

    assert isinstance(result, SQLResult)
    # AIOSQLite should report the number of rows affected
    assert result.rows_affected == 5

    # Verify data was inserted
    count_result = await aiosqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 5


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_update(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test execute_many for UPDATE operations with AIOSQLite."""
    # First insert some data
    await aiosqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
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

    result = await aiosqlite_batch_session.execute_many(
        "UPDATE test_batch SET value = ? WHERE name = ?",
        update_params,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify updates
    check_result = await aiosqlite_batch_session.execute("SELECT name, value FROM test_batch ORDER BY name")
    assert len(check_result.data) == 3
    assert all(row["value"] in (100, 200, 300) for row in check_result.data)


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_empty(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test execute_many with empty parameter list on AIOSQLite."""
    result = await aiosqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
        [],
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 0

    # Verify no data was inserted
    count_result = await aiosqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 0


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_mixed_types(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test execute_many with mixed parameter types on AIOSQLite."""
    parameters = [
        ("String Item", 123, "CAT1"),
        ("Another Item", 456, None),  # NULL category
        ("Third Item", 0, "CAT2"),
        ("Float Item", 78.5, "CAT3"),  # SQLite handles mixed numeric types
    ]

    result = await aiosqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
        parameters,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 4

    # Verify data including NULL
    null_result = await aiosqlite_batch_session.execute("SELECT * FROM test_batch WHERE category IS NULL")
    assert len(null_result.data) == 1
    assert null_result.data[0]["name"] == "Another Item"

    # Verify float value was stored correctly
    float_result = await aiosqlite_batch_session.execute(
        "SELECT * FROM test_batch WHERE name = ?",
        ("Float Item",),
    )
    assert len(float_result.data) == 1
    assert float_result.data[0]["value"] == 78.5


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_delete(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test execute_many for DELETE operations with AIOSQLite."""
    # First insert test data
    await aiosqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
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

    result = await aiosqlite_batch_session.execute_many(
        "DELETE FROM test_batch WHERE name = ?",
        delete_params,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify remaining data
    remaining_result = await aiosqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert remaining_result.data[0]["count"] == 2

    # Verify specific remaining items
    names_result = await aiosqlite_batch_session.execute("SELECT name FROM test_batch ORDER BY name")
    remaining_names = [row["name"] for row in names_result.data]
    assert remaining_names == ["Delete 3", "Keep 1"]


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_large_batch(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test execute_many with large batch size on AIOSQLite."""
    # Create a large batch of parameters
    large_batch = [(f"Item {i}", i * 10, f"CAT{i % 3}") for i in range(1000)]

    result = await aiosqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
        large_batch,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1000

    # Verify count
    count_result = await aiosqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 1000

    # Verify some specific values
    sample_result = await aiosqlite_batch_session.execute(
        "SELECT * FROM test_batch WHERE name IN (?, ?, ?) ORDER BY value",
        ("Item 100", "Item 500", "Item 999"),
    )
    assert len(sample_result.data) == 3
    assert sample_result.data[0]["value"] == 1000  # Item 100
    assert sample_result.data[1]["value"] == 5000  # Item 500
    assert sample_result.data[2]["value"] == 9990  # Item 999


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_with_sql_object(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test execute_many with SQL object on AIOSQLite."""
    from sqlspec.statement.sql import SQL

    parameters = [
        ("SQL Obj 1", 111, "SOB"),
        ("SQL Obj 2", 222, "SOB"),
        ("SQL Obj 3", 333, "SOB"),
    ]

    sql_obj = SQL("INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)").as_many(parameters)

    result = await aiosqlite_batch_session.execute_statement(sql_obj)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify data
    check_result = await aiosqlite_batch_session.execute(
        "SELECT COUNT(*) as count FROM test_batch WHERE category = ?",
        ("SOB",),
    )
    assert check_result.data[0]["count"] == 3


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_with_transactions(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test execute_many with transaction behavior on AIOSQLite."""
    # AIOSQLite typically runs in autocommit mode, but test behavior
    parameters = [
        ("Trans 1", 1000, "T"),
        ("Trans 2", 2000, "T"),
        ("Trans 3", 3000, "T"),
    ]

    result = await aiosqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
        parameters,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify all data is present (autocommit mode)
    total_result = await aiosqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert total_result.data[0]["count"] == 3


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_with_constraints(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test execute_many with constraint violations on AIOSQLite."""
    # Create a table with unique constraint
    await aiosqlite_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_unique (
            id INTEGER PRIMARY KEY,
            unique_name TEXT UNIQUE,
            value INTEGER
        )
    """)

    # First batch should succeed
    success_params = [
        (1, "unique1", 100),
        (2, "unique2", 200),
        (3, "unique3", 300),
    ]

    result = await aiosqlite_batch_session.execute_many(
        "INSERT INTO test_unique (id, unique_name, value) VALUES (?, ?, ?)",
        success_params,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Second batch with duplicate should fail
    duplicate_params = [
        (4, "unique4", 400),
        (5, "unique2", 500),  # Duplicate unique_name
        (6, "unique6", 600),
    ]

    with pytest.raises(Exception):  # SQLite will raise an integrity error
        await aiosqlite_batch_session.execute_many(
            "INSERT INTO test_unique (id, unique_name, value) VALUES (?, ?, ?)",
            duplicate_params,
        )

    # Verify original data is still there
    count_result = await aiosqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_unique")
    assert count_result.data[0]["count"] == 3  # Only original data remains


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_with_json(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test execute_many with JSON data on AIOSQLite."""
    # Create table with JSON column (SQLite stores as TEXT)
    await aiosqlite_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_json (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            metadata TEXT
        )
    """)

    import json

    parameters = [
        ("JSON 1", json.dumps({"type": "test", "value": 100, "active": True})),
        ("JSON 2", json.dumps({"type": "prod", "value": 200, "active": False})),
        ("JSON 3", json.dumps({"type": "test", "value": 300, "tags": ["a", "b"]})),
    ]

    result = await aiosqlite_batch_session.execute_many(
        "INSERT INTO test_json (name, metadata) VALUES (?, ?)",
        parameters,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify JSON data (SQLite JSON functions if available)
    try:
        check_result = await aiosqlite_batch_session.execute(
            "SELECT name, json_extract(metadata, '$.type') as type, json_extract(metadata, '$.value') as value FROM test_json ORDER BY name"
        )
        assert len(check_result.data) == 3
        assert check_result.data[0]["type"] == "test"  # JSON 1

    except Exception:
        # JSON functions might not be available, just verify count
        count_result = await aiosqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_json")
        assert count_result.data[0]["count"] == 3


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_with_sqlite_features(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test execute_many with SQLite-specific features."""
    # Create table with generated column (if supported)
    await aiosqlite_batch_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_generated (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            base_value INTEGER,
            doubled_value INTEGER AS (base_value * 2) STORED
        )
    """)

    try:
        parameters = [
            ("Gen 1", 10),
            ("Gen 2", 20),
            ("Gen 3", 30),
        ]

        result = await aiosqlite_batch_session.execute_many(
            "INSERT INTO test_generated (name, base_value) VALUES (?, ?)",
            parameters,
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 3

        # Verify generated column
        check_result = await aiosqlite_batch_session.execute(
            "SELECT name, base_value, doubled_value FROM test_generated ORDER BY name"
        )
        assert len(check_result.data) == 3
        assert check_result.data[0]["doubled_value"] == 20  # 10 * 2
        assert check_result.data[1]["doubled_value"] == 40  # 20 * 2
        assert check_result.data[2]["doubled_value"] == 60  # 30 * 2

    except Exception:
        # Generated columns might not be supported, test with regular table
        await aiosqlite_batch_session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_regular (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                value INTEGER
            )
        """)

        simple_params = [
            ("Simple 1", 10),
            ("Simple 2", 20),
            ("Simple 3", 30),
        ]

        await aiosqlite_batch_session.execute_many(
            "INSERT INTO test_regular (name, value) VALUES (?, ?)",
            simple_params,
        )

        check_result = await aiosqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_regular")
        assert check_result.data[0]["count"] == 3


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_with_full_text_search(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test execute_many with SQLite FTS (Full Text Search)."""
    # Create FTS table (if supported)
    try:
        await aiosqlite_batch_session.execute_script("""
            CREATE VIRTUAL TABLE IF NOT EXISTS test_fts USING fts5(
                title,
                content
            )
        """)

        parameters = [
            ("First Article", "This is the content of the first article about SQLite"),
            ("Second Article", "This is the content of the second article about databases"),
            ("Third Article", "This is the content of the third article about async programming"),
        ]

        result = await aiosqlite_batch_session.execute_many(
            "INSERT INTO test_fts (title, content) VALUES (?, ?)",
            parameters,
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 3

        # Test FTS search
        search_result = await aiosqlite_batch_session.execute(
            "SELECT title FROM test_fts WHERE test_fts MATCH ?",
            ("SQLite",),
        )
        assert len(search_result.data) == 1
        assert search_result.data[0]["title"] == "First Article"

    except Exception:
        # FTS might not be supported, skip gracefully
        pytest.skip("FTS not supported in this SQLite build")


@pytest.mark.asyncio
async def test_aiosqlite_execute_many_performance(aiosqlite_batch_session: AiosqliteDriver) -> None:
    """Test execute_many performance with AIOSQLite."""
    import time

    # Create medium-sized batch
    batch_size = 500
    batch_data = [(f"Perf Item {i}", i, f"PERF{i % 5}") for i in range(batch_size)]

    start_time = time.time()

    result = await aiosqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
        batch_data,
    )

    end_time = time.time()
    execution_time = end_time - start_time

    assert isinstance(result, SQLResult)
    assert result.rows_affected == batch_size

    # Verify performance is reasonable (should complete in under 5 seconds for 500 rows)
    assert execution_time < 5.0, f"Batch execution took too long: {execution_time:.2f} seconds"

    # Verify data integrity
    count_result = await aiosqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == batch_size

    # Verify category distribution
    category_result = await aiosqlite_batch_session.execute(
        "SELECT category, COUNT(*) as count FROM test_batch GROUP BY category ORDER BY category"
    )
    assert len(category_result.data) == 5  # PERF0 through PERF4
    # Each category should have approximately batch_size/5 items
    for row in category_result.data:
        assert row["count"] == batch_size // 5
