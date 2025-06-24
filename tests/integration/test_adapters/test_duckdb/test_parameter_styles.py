"""Test different parameter styles for DuckDB drivers."""

from collections.abc import Generator
from typing import Any

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def duckdb_params_session() -> "Generator[DuckDBDriver, None, None]":
    """Create a DuckDB session for parameter style testing."""
    config = DuckDBConfig(database=":memory:", statement_config=SQLConfig(strict_mode=False))

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_params (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                value INTEGER DEFAULT 0,
                description VARCHAR
            )
        """)
        # Insert test data
        session.execute(
            "INSERT INTO test_params (id, name, value, description) VALUES (?, ?, ?, ?)",
            (1, "test1", 100, "First test"),
        )
        session.execute(
            "INSERT INTO test_params (id, name, value, description) VALUES (?, ?, ?, ?)",
            (2, "test2", 200, "Second test"),
        )
        session.execute(
            "INSERT INTO test_params (id, name, value, description) VALUES (?, ?, ?, ?)", (3, "test3", 300, None)
        )  # NULL description
        yield session


@pytest.mark.parametrize(
    "params,expected_count",
    [
        (("test1",), 1),  # Tuple parameter
        (["test1"], 1),  # List parameter
    ],
)
def test_duckdb_qmark_parameter_types(duckdb_params_session: DuckDBDriver, params: Any, expected_count: int) -> None:
    """Test different parameter types with DuckDB qmark style."""
    result = duckdb_params_session.execute("SELECT * FROM test_params WHERE name = ?", params)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == expected_count
    if expected_count > 0:
        assert result.data[0]["name"] == "test1"


@pytest.mark.parametrize(
    "params,style,query",
    [
        (("test1",), "qmark", "SELECT * FROM test_params WHERE name = ?"),
        (("test1",), "numeric", "SELECT * FROM test_params WHERE name = $1"),
    ],
)
def test_duckdb_parameter_styles(duckdb_params_session: DuckDBDriver, params: Any, style: str, query: str) -> None:
    """Test different parameter styles with DuckDB."""
    result = duckdb_params_session.execute(query, params)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"


def test_duckdb_multiple_parameters_qmark(duckdb_params_session: DuckDBDriver) -> None:
    """Test queries with multiple parameters using qmark style."""
    result = duckdb_params_session.execute(
        "SELECT * FROM test_params WHERE value >= ? AND value <= ? ORDER BY value", (50, 150)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 100


def test_duckdb_multiple_parameters_numeric(duckdb_params_session: DuckDBDriver) -> None:
    """Test queries with multiple parameters using numeric style."""
    result = duckdb_params_session.execute(
        "SELECT * FROM test_params WHERE value >= $1 AND value <= $2 ORDER BY value", (50, 150)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 100


def test_duckdb_null_parameters(duckdb_params_session: DuckDBDriver) -> None:
    """Test handling of NULL parameters on DuckDB."""
    # Query for NULL values
    result = duckdb_params_session.execute("SELECT * FROM test_params WHERE description IS NULL")

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test3"
    assert result.data[0]["description"] is None

    # Test inserting NULL with parameters
    duckdb_params_session.execute(
        "INSERT INTO test_params (id, name, value, description) VALUES (?, ?, ?, ?)", (4, "null_param_test", 400, None)
    )

    null_result = duckdb_params_session.execute("SELECT * FROM test_params WHERE name = ?", ("null_param_test",))
    assert len(null_result.data) == 1
    assert null_result.data[0]["description"] is None


def test_duckdb_parameter_escaping(duckdb_params_session: DuckDBDriver) -> None:
    """Test parameter escaping prevents SQL injection."""
    # This should safely search for a literal string with quotes
    malicious_input = "'; DROP TABLE test_params; --"

    result = duckdb_params_session.execute("SELECT * FROM test_params WHERE name = ?", (malicious_input,))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 0  # No matches, but table should still exist

    # Verify table still exists by counting all records
    count_result = duckdb_params_session.execute("SELECT COUNT(*) as count FROM test_params")
    assert count_result.data[0]["count"] >= 3  # Our test data should still be there


def test_duckdb_parameter_with_like(duckdb_params_session: DuckDBDriver) -> None:
    """Test parameters with LIKE operations."""
    result = duckdb_params_session.execute("SELECT * FROM test_params WHERE name LIKE ?", ("test%",))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) >= 3  # test1, test2, test3

    # Test with numeric parameter style
    numeric_result = duckdb_params_session.execute("SELECT * FROM test_params WHERE name LIKE $1", ("test1%",))
    assert len(numeric_result.data) == 1
    assert numeric_result.data[0]["name"] == "test1"


def test_duckdb_parameter_with_in_clause(duckdb_params_session: DuckDBDriver) -> None:
    """Test parameters with IN clause."""
    # Insert additional test data
    duckdb_params_session.execute_many(
        "INSERT INTO test_params (id, name, value, description) VALUES (?, ?, ?, ?)",
        [(5, "alpha", 10, "Alpha test"), (6, "beta", 20, "Beta test"), (7, "gamma", 30, "Gamma test")],
    )

    # Test IN clause with multiple values
    result = duckdb_params_session.execute(
        "SELECT * FROM test_params WHERE name IN (?, ?, ?) ORDER BY name", ("alpha", "beta", "test1")
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 3
    assert result.data[0]["name"] == "alpha"
    assert result.data[1]["name"] == "beta"
    assert result.data[2]["name"] == "test1"


def test_duckdb_parameter_with_sql_object(duckdb_params_session: DuckDBDriver) -> None:
    """Test parameters with SQL object."""
    from sqlspec.statement.sql import SQL

    # Test with qmark style
    sql_obj = SQL("SELECT * FROM test_params WHERE value > ?", parameters=[150])
    result = duckdb_params_session.execute(sql_obj)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) >= 1
    assert all(row["value"] > 150 for row in result.data)

    # Test with numeric style
    numeric_sql = SQL("SELECT * FROM test_params WHERE value < $1", parameters=[150])
    numeric_result = duckdb_params_session.execute(numeric_sql)

    assert isinstance(numeric_result, SQLResult)
    assert numeric_result.data is not None
    assert len(numeric_result.data) >= 1
    assert all(row["value"] < 150 for row in numeric_result.data)


def test_duckdb_parameter_data_types(duckdb_params_session: DuckDBDriver) -> None:
    """Test different parameter data types with DuckDB."""
    # Create table for different data types
    duckdb_params_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_types (
            id INTEGER PRIMARY KEY,
            int_val INTEGER,
            real_val REAL,
            text_val VARCHAR,
            bool_val BOOLEAN,
            list_val INTEGER[]
        )
    """)

    # Test different data types
    test_data = [
        (1, 42, 3.14, "hello", True, [1, 2, 3]),
        (2, -100, -2.5, "world", False, [4, 5, 6]),
        (3, 0, 0.0, "", None, []),
    ]

    for data in test_data:
        duckdb_params_session.execute(
            "INSERT INTO test_types (id, int_val, real_val, text_val, bool_val, list_val) VALUES (?, ?, ?, ?, ?, ?)",
            data,
        )

    # Verify data with parameters
    # This test is about parameter data types working correctly, not floating point precision
    result = duckdb_params_session.execute("SELECT * FROM test_types WHERE int_val = ?", (42,))

    assert len(result.data) == 1
    assert result.data[0]["text_val"] == "hello"
    assert result.data[0]["bool_val"] is True
    assert result.data[0]["list_val"] == [1, 2, 3]
    # Also verify the float was stored (even if not exactly 3.14)
    assert 3.13 < result.data[0]["real_val"] < 3.15


def test_duckdb_parameter_edge_cases(duckdb_params_session: DuckDBDriver) -> None:
    """Test edge cases for DuckDB parameters."""
    # Empty string parameter
    duckdb_params_session.execute(
        "INSERT INTO test_params (id, name, value, description) VALUES (?, ?, ?, ?)", (8, "", 999, "Empty name test")
    )

    empty_result = duckdb_params_session.execute("SELECT * FROM test_params WHERE name = ?", ("",))
    assert len(empty_result.data) == 1
    assert empty_result.data[0]["value"] == 999

    # Very long string parameter
    long_string = "x" * 1000
    duckdb_params_session.execute(
        "INSERT INTO test_params (id, name, value, description) VALUES (?, ?, ?, ?)",
        (9, "long_test", 1000, long_string),
    )

    long_result = duckdb_params_session.execute("SELECT * FROM test_params WHERE description = ?", (long_string,))
    assert len(long_result.data) == 1
    assert len(long_result.data[0]["description"]) == 1000


def test_duckdb_parameter_with_analytics_functions(duckdb_params_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB analytics functions."""
    # Insert time series data for analytics
    duckdb_params_session.execute_many(
        "INSERT INTO test_params (id, name, value, description) VALUES (?, ?, ?, ?)",
        [
            (10, "analytics1", 10, "2023-01-01"),
            (11, "analytics2", 20, "2023-01-02"),
            (12, "analytics3", 30, "2023-01-03"),
            (13, "analytics4", 40, "2023-01-04"),
            (14, "analytics5", 50, "2023-01-05"),
        ],
    )

    # Test with window functions and parameters
    result = duckdb_params_session.execute(
        """
        SELECT
            name,
            value,
            LAG(value, 1) OVER (ORDER BY name) as prev_value,
            value - LAG(value, 1) OVER (ORDER BY name) as diff
        FROM test_params
        WHERE value >= ?
        ORDER BY name
    """,
        (15,),
    )

    assert len(result.data) >= 4
    # Check that analytics function worked
    non_null_diffs = [row for row in result.data if row["diff"] is not None]
    assert len(non_null_diffs) >= 3


def test_duckdb_parameter_with_array_functions(duckdb_params_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB array/list functions."""
    # Create table with array data
    duckdb_params_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_arrays (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            numbers INTEGER[],
            tags VARCHAR[]
        )
    """)

    # Insert array data with parameters
    array_data = [
        (1, "Array 1", [1, 2, 3, 4, 5], ["tag1", "tag2"]),
        (2, "Array 2", [10, 20, 30], ["tag3"]),
        (3, "Array 3", [100, 200], ["tag4", "tag5", "tag6"]),
    ]

    for data in array_data:
        duckdb_params_session.execute("INSERT INTO test_arrays (id, name, numbers, tags) VALUES (?, ?, ?, ?)", data)

    # Test array functions with parameters
    result = duckdb_params_session.execute(
        "SELECT name, len(numbers) as num_count, len(tags) as tag_count FROM test_arrays WHERE len(numbers) >= ?", (3,)
    )

    assert len(result.data) == 2  # Array 1 and Array 2
    assert all(row["num_count"] >= 3 for row in result.data)

    # Test array element access with parameters
    element_result = duckdb_params_session.execute(
        "SELECT name FROM test_arrays WHERE numbers[?] > ?",
        (1, 5),  # First element > 5
    )
    assert len(element_result.data) >= 1


def test_duckdb_parameter_with_json_functions(duckdb_params_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB JSON functions."""
    # Create table with JSON data
    duckdb_params_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_json (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            metadata VARCHAR
        )
    """)

    import json

    # Insert JSON data with parameters
    json_data = [
        (1, "JSON 1", json.dumps({"type": "test", "value": 100, "active": True})),
        (2, "JSON 2", json.dumps({"type": "prod", "value": 200, "active": False})),
        (3, "JSON 3", json.dumps({"type": "test", "value": 300, "tags": ["a", "b"]})),
    ]

    for data in json_data:
        duckdb_params_session.execute("INSERT INTO test_json (id, name, metadata) VALUES (?, ?, ?)", data)

    # Test JSON extraction with parameters
    try:
        result = duckdb_params_session.execute(
            "SELECT name, json_extract_string(metadata, '$.type') as type FROM test_json WHERE json_extract_string(metadata, '$.type') = ?",
            ("test",),
        )
        assert len(result.data) == 2  # JSON 1 and JSON 3
        assert all(row["type"] == "test" for row in result.data)

    except Exception:
        # JSON functions might not be available, test simpler string operations
        result = duckdb_params_session.execute("SELECT name FROM test_json WHERE metadata LIKE ?", ('%"type":"test"%',))
        assert len(result.data) >= 1


def test_duckdb_parameter_with_date_functions(duckdb_params_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB date/time functions."""
    # Create table with date data
    duckdb_params_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_dates (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            created_date DATE,
            created_timestamp TIMESTAMP
        )
    """)

    # Insert date data with parameters
    date_data = [
        (1, "Date 1", "2023-01-01", "2023-01-01 10:00:00"),
        (2, "Date 2", "2023-06-15", "2023-06-15 14:30:00"),
        (3, "Date 3", "2023-12-31", "2023-12-31 23:59:59"),
    ]

    for data in date_data:
        duckdb_params_session.execute(
            "INSERT INTO test_dates (id, name, created_date, created_timestamp) VALUES (?, ?, ?, ?)", data
        )

    # Test date functions with parameters
    result = duckdb_params_session.execute(
        "SELECT name, EXTRACT(month FROM created_date) as month FROM test_dates WHERE created_date >= ?",
        ("2023-06-01",),
    )

    assert len(result.data) == 2  # Date 2 and Date 3
    assert all(row["month"] >= 6 for row in result.data)

    # Test timestamp functions with parameters
    timestamp_result = duckdb_params_session.execute(
        "SELECT name FROM test_dates WHERE EXTRACT(hour FROM created_timestamp) >= ?", (14,)
    )
    assert len(timestamp_result.data) >= 1


def test_duckdb_parameter_with_string_functions(duckdb_params_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB string functions."""
    # Test with string functions
    result = duckdb_params_session.execute(
        "SELECT * FROM test_params WHERE LENGTH(name) > ? AND UPPER(name) LIKE ?", (4, "TEST%")
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    # Should find test1, test2, test3 (all have length > 4 and start with "test")
    assert len(result.data) >= 3

    # Test with string manipulation and parameters
    manipulation_result = duckdb_params_session.execute(
        "SELECT name, CONCAT(name, ?) as extended_name FROM test_params WHERE POSITION(? IN name) > 0",
        ("_suffix", "test"),
    )
    assert len(manipulation_result.data) >= 3
    for row in manipulation_result.data:
        assert row["extended_name"].endswith("_suffix")


def test_duckdb_parameter_with_math_functions(duckdb_params_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB mathematical functions."""
    # Test with math functions
    math_result = duckdb_params_session.execute(
        "SELECT name, value, ROUND(value * ?, 2) as multiplied, POW(value, ?) as powered FROM test_params WHERE value >= ?",
        (1.5, 2, 100),
    )

    assert len(math_result.data) >= 3
    for row in math_result.data:
        expected_multiplied = round(row["value"] * 1.5, 2)
        expected_powered = row["value"] ** 2
        assert row["multiplied"] == expected_multiplied
        assert row["powered"] == expected_powered


def test_duckdb_parameter_with_aggregate_functions(duckdb_params_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB aggregate functions."""
    # Insert more test data for aggregation
    duckdb_params_session.execute_many(
        "INSERT INTO test_params (id, name, value, description) VALUES (?, ?, ?, ?)",
        [
            (15, "agg1", 15, "Group A"),
            (16, "agg2", 25, "Group A"),
            (17, "agg3", 35, "Group B"),
            (18, "agg4", 45, "Group B"),
        ],
    )

    # Test aggregate functions with parameters
    result = duckdb_params_session.execute(
        """
        SELECT
            description,
            COUNT(*) as count,
            AVG(value) as avg_value,
            MAX(value) as max_value
        FROM test_params
        WHERE value >= ? AND description IS NOT NULL
        GROUP BY description
        HAVING COUNT(*) >= ?
        ORDER BY description
    """,
        (10, 2),
    )

    assert len(result.data) == 2  # Group A and Group B
    for row in result.data:
        assert row["count"] >= 2
        assert row["avg_value"] is not None
        assert row["max_value"] >= 10


def test_duckdb_parameter_performance(duckdb_params_session: DuckDBDriver) -> None:
    """Test parameter performance with DuckDB."""
    import time

    # Create larger dataset for performance testing
    batch_data = [(i + 19, f"Perf Item {i}", i, f"PERF{i % 5}") for i in range(1000)]

    start_time = time.time()
    duckdb_params_session.execute_many(
        "INSERT INTO test_params (id, name, value, description) VALUES (?, ?, ?, ?)", batch_data
    )
    end_time = time.time()

    insert_time = end_time - start_time
    assert insert_time < 2.0, f"Batch insert took too long: {insert_time:.2f} seconds"

    # Test query performance with parameters
    start_time = time.time()
    result = duckdb_params_session.execute(
        "SELECT COUNT(*) as count FROM test_params WHERE value >= ? AND value <= ?", (100, 900)
    )
    end_time = time.time()

    query_time = end_time - start_time
    assert query_time < 1.0, f"Query took too long: {query_time:.2f} seconds"
    assert result.data[0]["count"] >= 800  # Should find many records in range
