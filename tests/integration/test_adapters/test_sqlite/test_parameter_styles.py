"""Test different parameter styles for SQLite drivers."""

from collections.abc import Generator
from typing import Any

import pytest

from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def sqlite_params_session() -> "Generator[SqliteDriver, None, None]":
    """Create a SQLite session for parameter style testing."""
    config = SqliteConfig(database=":memory:", statement_config=SQLConfig(strict_mode=False))

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_params (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                description TEXT
            )
        """)
        # Insert test data
        session.execute(
            "INSERT INTO test_params (name, value, description) VALUES (?, ?, ?)", ("test1", 100, "First test")
        )
        session.execute(
            "INSERT INTO test_params (name, value, description) VALUES (?, ?, ?)", ("test2", 200, "Second test")
        )
        session.execute(
            "INSERT INTO test_params (name, value, description) VALUES (?, ?, ?)", ("test3", 300, None)
        )  # NULL description
        yield session


@pytest.mark.parametrize(
    "params,expected_count",
    [
        (("test1",), 1),  # Tuple parameter
        (["test1"], 1),  # List parameter
    ],
)
def test_sqlite_qmark_parameter_types(sqlite_params_session: SqliteDriver, params: Any, expected_count: int) -> None:
    """Test different parameter types with SQLite qmark style."""
    result = sqlite_params_session.execute("SELECT * FROM test_params WHERE name = ?", params)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == expected_count
    if expected_count > 0:
        assert result.data[0]["name"] == "test1"


@pytest.mark.parametrize(
    "params,style,query",
    [
        (("test1",), "qmark", "SELECT * FROM test_params WHERE name = ?"),
        ({"name": "test1"}, "named_colon", "SELECT * FROM test_params WHERE name = :name"),
    ],
)
def test_sqlite_parameter_styles(sqlite_params_session: SqliteDriver, params: Any, style: str, query: str) -> None:
    """Test different parameter styles with SQLite."""
    result = sqlite_params_session.execute(query, params)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"


def test_sqlite_multiple_parameters_qmark(sqlite_params_session: SqliteDriver) -> None:
    """Test queries with multiple parameters using qmark style."""
    result = sqlite_params_session.execute(
        "SELECT * FROM test_params WHERE value >= ? AND value <= ? ORDER BY value", (50, 150)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 100


def test_sqlite_multiple_parameters_named(sqlite_params_session: SqliteDriver) -> None:
    """Test queries with multiple parameters using named style."""
    result = sqlite_params_session.execute(
        "SELECT * FROM test_params WHERE value >= :min_val AND value <= :max_val ORDER BY value",
        {"min_val": 50, "max_val": 150},
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 100


def test_sqlite_null_parameters(sqlite_params_session: SqliteDriver) -> None:
    """Test handling of NULL parameters on SQLite."""
    # Query for NULL values
    result = sqlite_params_session.execute("SELECT * FROM test_params WHERE description IS NULL")

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test3"
    assert result.data[0]["description"] is None

    # Test inserting NULL with parameters
    sqlite_params_session.execute(
        "INSERT INTO test_params (name, value, description) VALUES (?, ?, ?)", ("null_param_test", 400, None)
    )

    null_result = sqlite_params_session.execute("SELECT * FROM test_params WHERE name = ?", ("null_param_test",))
    assert len(null_result.data) == 1
    assert null_result.data[0]["description"] is None


def test_sqlite_parameter_escaping(sqlite_params_session: SqliteDriver) -> None:
    """Test parameter escaping prevents SQL injection."""
    # This should safely search for a literal string with quotes
    malicious_input = "'; DROP TABLE test_params; --"

    result = sqlite_params_session.execute("SELECT * FROM test_params WHERE name = ?", (malicious_input,))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 0  # No matches, but table should still exist

    # Verify table still exists by counting all records
    count_result = sqlite_params_session.execute("SELECT COUNT(*) as count FROM test_params")
    assert count_result.data[0]["count"] >= 3  # Our test data should still be there


def test_sqlite_parameter_with_like(sqlite_params_session: SqliteDriver) -> None:
    """Test parameters with LIKE operations."""
    result = sqlite_params_session.execute("SELECT * FROM test_params WHERE name LIKE ?", ("test%",))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) >= 3  # test1, test2, test3

    # Test with named parameter
    named_result = sqlite_params_session.execute(
        "SELECT * FROM test_params WHERE name LIKE :pattern", {"pattern": "test1%"}
    )
    assert len(named_result.data) == 1
    assert named_result.data[0]["name"] == "test1"


def test_sqlite_parameter_with_in_clause(sqlite_params_session: SqliteDriver) -> None:
    """Test parameters with IN clause."""
    # Insert additional test data
    sqlite_params_session.execute_many(
        "INSERT INTO test_params (name, value, description) VALUES (?, ?, ?)",
        [("alpha", 10, "Alpha test"), ("beta", 20, "Beta test"), ("gamma", 30, "Gamma test")],
    )

    # Test IN clause with multiple values (tricky with parameters)
    result = sqlite_params_session.execute(
        "SELECT * FROM test_params WHERE name IN (?, ?, ?) ORDER BY name", ("alpha", "beta", "test1")
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 3
    assert result.data[0]["name"] == "alpha"
    assert result.data[1]["name"] == "beta"
    assert result.data[2]["name"] == "test1"


def test_sqlite_parameter_with_sql_object(sqlite_params_session: SqliteDriver) -> None:
    """Test parameters with SQL object."""
    from sqlspec.statement.sql import SQL

    # Test with qmark style
    sql_obj = SQL("SELECT * FROM test_params WHERE value > ?", parameters=[150])
    result = sqlite_params_session.execute(sql_obj)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) >= 1
    assert all(row["value"] > 150 for row in result.data)

    # Test with named style
    named_sql = SQL("SELECT * FROM test_params WHERE value < :max_value", parameters={"max_value": 150})
    named_result = sqlite_params_session.execute(named_sql)

    assert isinstance(named_result, SQLResult)
    assert named_result.data is not None
    assert len(named_result.data) >= 1
    assert all(row["value"] < 150 for row in named_result.data)


def test_sqlite_parameter_data_types(sqlite_params_session: SqliteDriver) -> None:
    """Test different parameter data types with SQLite."""
    # Create table for different data types
    sqlite_params_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_types (
            id INTEGER PRIMARY KEY,
            int_val INTEGER,
            real_val REAL,
            text_val TEXT,
            blob_val BLOB
        )
    """)

    # Test different data types
    test_data = [(1, 42, 3.14, "hello", b"binary_data"), (2, -100, -2.5, "world", b"more_binary"), (3, 0, 0.0, "", b"")]

    for data in test_data:
        sqlite_params_session.execute(
            "INSERT INTO test_types (id, int_val, real_val, text_val, blob_val) VALUES (?, ?, ?, ?, ?)", data
        )

    # Verify data with parameters
    result = sqlite_params_session.execute("SELECT * FROM test_types WHERE int_val = ? AND real_val = ?", (42, 3.14))

    assert len(result.data) == 1
    assert result.data[0]["text_val"] == "hello"
    assert result.data[0]["blob_val"] == b"binary_data"


def test_sqlite_parameter_edge_cases(sqlite_params_session: SqliteDriver) -> None:
    """Test edge cases for SQLite parameters."""
    # Empty string parameter
    sqlite_params_session.execute(
        "INSERT INTO test_params (name, value, description) VALUES (?, ?, ?)", ("", 999, "Empty name test")
    )

    empty_result = sqlite_params_session.execute("SELECT * FROM test_params WHERE name = ?", ("",))
    assert len(empty_result.data) == 1
    assert empty_result.data[0]["value"] == 999

    # Very long string parameter
    long_string = "x" * 1000
    sqlite_params_session.execute(
        "INSERT INTO test_params (name, value, description) VALUES (?, ?, ?)", ("long_test", 1000, long_string)
    )

    long_result = sqlite_params_session.execute("SELECT * FROM test_params WHERE description = ?", (long_string,))
    assert len(long_result.data) == 1
    assert len(long_result.data[0]["description"]) == 1000


def test_sqlite_parameter_with_functions(sqlite_params_session: SqliteDriver) -> None:
    """Test parameters with SQLite functions."""
    # Test with string functions
    result = sqlite_params_session.execute(
        "SELECT * FROM test_params WHERE LENGTH(name) > ? AND UPPER(name) LIKE ?", (4, "TEST%")
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    # Should find test1, test2, test3 (all have length > 4 and start with "test")
    assert len(result.data) >= 3

    # Test with math functions
    math_result = sqlite_params_session.execute(
        "SELECT name, value, ROUND(value * ?, 2) as multiplied FROM test_params WHERE value >= ?", (1.5, 100)
    )
    assert len(math_result.data) >= 3
    for row in math_result.data:
        expected = round(row["value"] * 1.5, 2)
        assert row["multiplied"] == expected
