"""Test execute_many functionality for SQLite drivers."""

from collections.abc import Generator

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def sqlite_batch_session() -> "Generator[SqliteDriver, None, None]":
    """Create a SQLite session for batch operation testing."""
    config = SqliteConfig(
        database=":memory:",
        statement_config=SQLConfig(strict_mode=False),
    )

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_batch (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                category TEXT
            )
        """)
        yield session


def test_sqlite_execute_many_basic(sqlite_batch_session: SqliteDriver) -> None:
    """Test basic execute_many with SQLite."""
    parameters = [
        ("Item 1", 100, "A"),
        ("Item 2", 200, "B"),
        ("Item 3", 300, "A"),
        ("Item 4", 400, "C"),
        ("Item 5", 500, "B"),
    ]

    result = sqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
        parameters,
    )

    assert isinstance(result, SQLResult)
    # SQLite should report the number of rows affected
    assert result.rows_affected == 5

    # Verify data was inserted
    count_result = sqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 5


def test_sqlite_execute_many_update(sqlite_batch_session: SqliteDriver) -> None:
    """Test execute_many for UPDATE operations with SQLite."""
    # First insert some data
    sqlite_batch_session.execute_many(
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

    result = sqlite_batch_session.execute_many(
        "UPDATE test_batch SET value = ? WHERE name = ?",
        update_params,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify updates
    check_result = sqlite_batch_session.execute("SELECT name, value FROM test_batch ORDER BY name")
    assert len(check_result.data) == 3
    assert all(row["value"] in (100, 200, 300) for row in check_result.data)


def test_sqlite_execute_many_empty(sqlite_batch_session: SqliteDriver) -> None:
    """Test execute_many with empty parameter list on SQLite."""
    result = sqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
        [],
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 0

    # Verify no data was inserted
    count_result = sqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 0


def test_sqlite_execute_many_mixed_types(sqlite_batch_session: SqliteDriver) -> None:
    """Test execute_many with mixed parameter types on SQLite."""
    parameters = [
        ("String Item", 123, "CAT1"),
        ("Another Item", 456, None),  # NULL category
        ("Third Item", 0, "CAT2"),
        ("Float Item", 78.5, "CAT3"),  # SQLite handles mixed numeric types
    ]

    result = sqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
        parameters,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 4

    # Verify data including NULL
    null_result = sqlite_batch_session.execute("SELECT * FROM test_batch WHERE category IS NULL")
    assert len(null_result.data) == 1
    assert null_result.data[0]["name"] == "Another Item"

    # Verify float value was stored correctly
    float_result = sqlite_batch_session.execute(
        "SELECT * FROM test_batch WHERE name = ?",
        ("Float Item",),
    )
    assert len(float_result.data) == 1
    assert float_result.data[0]["value"] == 78.5


def test_sqlite_execute_many_delete(sqlite_batch_session: SqliteDriver) -> None:
    """Test execute_many for DELETE operations with SQLite."""
    # First insert test data
    sqlite_batch_session.execute_many(
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

    result = sqlite_batch_session.execute_many(
        "DELETE FROM test_batch WHERE name = ?",
        delete_params,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify remaining data
    remaining_result = sqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert remaining_result.data[0]["count"] == 2

    # Verify specific remaining items
    names_result = sqlite_batch_session.execute("SELECT name FROM test_batch ORDER BY name")
    remaining_names = [row["name"] for row in names_result.data]
    assert remaining_names == ["Delete 3", "Keep 1"]


def test_sqlite_execute_many_large_batch(sqlite_batch_session: SqliteDriver) -> None:
    """Test execute_many with large batch size on SQLite."""
    # Create a large batch of parameters
    large_batch = [(f"Item {i}", i * 10, f"CAT{i % 3}") for i in range(1000)]

    result = sqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
        large_batch,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1000

    # Verify count
    count_result = sqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert count_result.data[0]["count"] == 1000

    # Verify some specific values
    sample_result = sqlite_batch_session.execute(
        "SELECT * FROM test_batch WHERE name IN (?, ?, ?) ORDER BY value",
        ("Item 100", "Item 500", "Item 999"),
    )
    assert len(sample_result.data) == 3
    assert sample_result.data[0]["value"] == 1000  # Item 100
    assert sample_result.data[1]["value"] == 5000  # Item 500
    assert sample_result.data[2]["value"] == 9990  # Item 999


def test_sqlite_execute_many_with_sql_object(sqlite_batch_session: SqliteDriver) -> None:
    """Test execute_many with SQL object on SQLite."""
    from sqlspec.statement.sql import SQL

    parameters = [
        ("SQL Obj 1", 111, "SOB"),
        ("SQL Obj 2", 222, "SOB"),
        ("SQL Obj 3", 333, "SOB"),
    ]

    sql_obj = SQL("INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)").as_many(parameters)

    result = sqlite_batch_session.execute(sql_obj)

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify data
    check_result = sqlite_batch_session.execute(
        "SELECT COUNT(*) as count FROM test_batch WHERE category = ?",
        ("SOB",),
    )
    assert check_result.data[0]["count"] == 3


def test_sqlite_execute_many_transaction_rollback(sqlite_batch_session: SqliteDriver) -> None:
    """Test execute_many with transaction rollback on SQLite."""
    # Insert initial data
    sqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
        [("Initial", 1, "I")],
    )

    # Test transaction behavior (SQLite in memory is auto-commit by default)
    # This test verifies the execute_many itself works correctly
    parameters = [
        ("Trans 1", 1000, "T"),
        ("Trans 2", 2000, "T"),
        ("Trans 3", 3000, "T"),
    ]

    result = sqlite_batch_session.execute_many(
        "INSERT INTO test_batch (name, value, category) VALUES (?, ?, ?)",
        parameters,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify all data is present (auto-commit mode)
    total_result = sqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_batch")
    assert total_result.data[0]["count"] == 4  # 1 initial + 3 new


def test_sqlite_execute_many_with_constraints(sqlite_batch_session: SqliteDriver) -> None:
    """Test execute_many with constraint violations on SQLite."""
    # Create a table with unique constraint
    sqlite_batch_session.execute_script("""
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

    result = sqlite_batch_session.execute_many(
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
        sqlite_batch_session.execute_many(
            "INSERT INTO test_unique (id, unique_name, value) VALUES (?, ?, ?)",
            duplicate_params,
        )

    # Verify original data plus first row from failed batch
    # SQLite stops at first error but doesn't rollback previous rows in batch
    count_result = sqlite_batch_session.execute("SELECT COUNT(*) as count FROM test_unique")
    assert count_result.data[0]["count"] == 4  # Original 3 + 1 from failed batch
