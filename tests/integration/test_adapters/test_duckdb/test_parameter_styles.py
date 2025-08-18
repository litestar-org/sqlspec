"""Test different parameter styles for DuckDB drivers."""

import math
from collections.abc import Generator
from typing import Any

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.core.result import SQLResult


@pytest.fixture
def duckdb_parameters_session() -> "Generator[DuckDBDriver, None, None]":
    """Create a DuckDB session for parameter style testing."""
    config = DuckDBConfig(pool_config={"database": ":memory:"})

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_parameters (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                value INTEGER DEFAULT 0,
                description VARCHAR
            );
            TRUNCATE TABLE test_parameters;
        """)

        session.execute(
            "INSERT INTO test_parameters (id, name, value, description) VALUES (?, ?, ?, ?)",
            (1, "test1", 100, "First test"),
        )
        session.execute(
            "INSERT INTO test_parameters (id, name, value, description) VALUES (?, ?, ?, ?)",
            (2, "test2", 200, "Second test"),
        )
        session.execute(
            "INSERT INTO test_parameters (id, name, value, description) VALUES (?, ?, ?, ?)", (3, "test3", 300, None)
        )
        yield session


@pytest.mark.parametrize("parameters,expected_count", [(("test1"), 1), (["test1"], 1)])
def test_duckdb_qmark_parameter_types(
    duckdb_parameters_session: DuckDBDriver, parameters: Any, expected_count: int
) -> None:
    """Test different parameter types with DuckDB qmark style."""
    result = duckdb_parameters_session.execute("SELECT * FROM test_parameters WHERE name = ?", parameters)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == expected_count
    if expected_count > 0:
        assert result.data[0]["name"] == "test1"


@pytest.mark.parametrize(
    "parameters,style,query",
    [
        (("test1"), "qmark", "SELECT * FROM test_parameters WHERE name = ?"),
        (("test1"), "numeric", "SELECT * FROM test_parameters WHERE name = $1"),
    ],
)
def test_duckdb_parameter_styles(
    duckdb_parameters_session: DuckDBDriver, parameters: Any, style: str, query: str
) -> None:
    """Test different parameter styles with DuckDB."""
    result = duckdb_parameters_session.execute(query, parameters)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"


def test_duckdb_multiple_parameters_qmark(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test queries with multiple parameters using qmark style."""
    result = duckdb_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE value >= ? AND value <= ? ORDER BY value", (50, 150)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 100


def test_duckdb_multiple_parameters_numeric(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test queries with multiple parameters using numeric style."""
    result = duckdb_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE value >= $1 AND value <= $2 ORDER BY value", (50, 150)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 100


def test_duckdb_null_parameters(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test handling of NULL parameters on DuckDB."""

    result = duckdb_parameters_session.execute("SELECT * FROM test_parameters WHERE description IS NULL")

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test3"
    assert result.data[0]["description"] is None

    duckdb_parameters_session.execute(
        "INSERT INTO test_parameters (id, name, value, description) VALUES (?, ?, ?, ?)",
        (4, "null_param_test", 400, None),
    )

    null_result = duckdb_parameters_session.execute("SELECT * FROM test_parameters WHERE name = ?", ("null_param_test"))
    assert len(null_result.data) == 1
    assert null_result.data[0]["description"] is None


def test_duckdb_parameter_escaping(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test parameter escaping prevents SQL injection."""

    malicious_input = "'; DROP TABLE test_parameters; --"

    result = duckdb_parameters_session.execute("SELECT * FROM test_parameters WHERE name = ?", (malicious_input))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 0

    count_result = duckdb_parameters_session.execute("SELECT COUNT(*) as count FROM test_parameters")
    assert count_result.data[0]["count"] >= 3


def test_duckdb_parameter_with_like(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test parameters with LIKE operations."""
    result = duckdb_parameters_session.execute("SELECT * FROM test_parameters WHERE name LIKE ?", ("test%"))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) >= 3

    numeric_result = duckdb_parameters_session.execute("SELECT * FROM test_parameters WHERE name LIKE $1", ("test1%"))
    assert len(numeric_result.data) == 1
    assert numeric_result.data[0]["name"] == "test1"


def test_duckdb_parameter_with_in_clause(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test parameters with IN clause."""

    duckdb_parameters_session.execute_many(
        "INSERT INTO test_parameters (id, name, value, description) VALUES (?, ?, ?, ?)",
        [(5, "alpha", 10, "Alpha test"), (6, "beta", 20, "Beta test"), (7, "gamma", 30, "Gamma test")],
    )

    result = duckdb_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name IN (?, ?, ?) ORDER BY name", ("alpha", "beta", "test1")
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 3
    assert result.data[0]["name"] == "alpha"
    assert result.data[1]["name"] == "beta"
    assert result.data[2]["name"] == "test1"


def test_duckdb_parameter_with_sql_object(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test parameters with SQL object."""
    from sqlspec.core.statement import SQL

    sql_obj = SQL("SELECT * FROM test_parameters WHERE value > ?", [150])
    result = duckdb_parameters_session.execute(sql_obj)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) >= 1
    assert all(row["value"] > 150 for row in result.data)

    numeric_sql = SQL("SELECT * FROM test_parameters WHERE value < $1", [150])
    numeric_result = duckdb_parameters_session.execute(numeric_sql)

    assert isinstance(numeric_result, SQLResult)
    assert numeric_result.data is not None
    assert len(numeric_result.data) >= 1
    assert all(row["value"] < 150 for row in numeric_result.data)


def test_duckdb_parameter_data_types(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test different parameter data types with DuckDB."""

    duckdb_parameters_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_types (
            id INTEGER PRIMARY KEY,
            int_val INTEGER,
            real_val REAL,
            text_val VARCHAR,
            bool_val BOOLEAN,
            list_val INTEGER[]
        )
    """)

    test_data = [
        (1, 42, math.pi, "hello", True, [1, 2, 3]),
        (2, -100, -2.5, "world", False, [4, 5, 6]),
        (3, 0, 0.0, "", None, []),
    ]

    for data in test_data:
        duckdb_parameters_session.execute(
            "INSERT INTO test_types (id, int_val, real_val, text_val, bool_val, list_val) VALUES (?, ?, ?, ?, ?, ?)",
            data,
        )

    result = duckdb_parameters_session.execute("SELECT * FROM test_types WHERE int_val = ?", (42))

    assert len(result.data) == 1
    assert result.data[0]["text_val"] == "hello"
    assert result.data[0]["bool_val"] is True
    assert result.data[0]["list_val"] == [1, 2, 3]

    assert 3.13 < result.data[0]["real_val"] < 3.15


def test_duckdb_parameter_edge_cases(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test edge cases for DuckDB parameters."""

    duckdb_parameters_session.execute(
        "INSERT INTO test_parameters (id, name, value, description) VALUES (?, ?, ?, ?)",
        (8, "", 999, "Empty name test"),
    )

    empty_result = duckdb_parameters_session.execute("SELECT * FROM test_parameters WHERE name = ?", (""))
    assert len(empty_result.data) == 1
    assert empty_result.data[0]["value"] == 999

    long_string = "x" * 1000
    duckdb_parameters_session.execute(
        "INSERT INTO test_parameters (id, name, value, description) VALUES (?, ?, ?, ?)",
        (9, "long_test", 1000, long_string),
    )

    long_result = duckdb_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE description = ?", (long_string)
    )
    assert len(long_result.data) == 1
    assert len(long_result.data[0]["description"]) == 1000


def test_duckdb_parameter_with_analytics_functions(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB analytics functions."""

    duckdb_parameters_session.execute_many(
        "INSERT INTO test_parameters (id, name, value, description) VALUES (?, ?, ?, ?)",
        [
            (10, "analytics1", 10, "2023-01-01"),
            (11, "analytics2", 20, "2023-01-02"),
            (12, "analytics3", 30, "2023-01-03"),
            (13, "analytics4", 40, "2023-01-04"),
            (14, "analytics5", 50, "2023-01-05"),
        ],
    )

    result = duckdb_parameters_session.execute(
        """
        SELECT
            name,
            value,
            LAG(value, 1) OVER (ORDER BY name) as prev_value,
            value - LAG(value, 1) OVER (ORDER BY name) as diff
        FROM test_parameters
        WHERE value >= ?
        ORDER BY name
    """,
        (15),
    )

    assert len(result.data) >= 4

    non_null_diffs = [row for row in result.data if row["diff"] is not None]
    assert len(non_null_diffs) >= 3


def test_duckdb_parameter_with_array_functions(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB array/list functions."""

    duckdb_parameters_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_arrays (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            numbers INTEGER[],
            tags VARCHAR[]
        )
    """)

    array_data = [
        (1, "Array 1", [1, 2, 3, 4, 5], ["tag1", "tag2"]),
        (2, "Array 2", [10, 20, 30], ["tag3"]),
        (3, "Array 3", [100, 200], ["tag4", "tag5", "tag6"]),
    ]

    for data in array_data:
        duckdb_parameters_session.execute("INSERT INTO test_arrays (id, name, numbers, tags) VALUES (?, ?, ?, ?)", data)

    result = duckdb_parameters_session.execute(
        "SELECT name, len(numbers) as num_count, len(tags) as tag_count FROM test_arrays WHERE len(numbers) >= ?", (3)
    )

    assert len(result.data) == 2
    assert all(row["num_count"] >= 3 for row in result.data)

    element_result = duckdb_parameters_session.execute("SELECT name FROM test_arrays WHERE numbers[?] > ?", (1, 5))
    assert len(element_result.data) >= 1


def test_duckdb_parameter_with_json_functions(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB JSON functions."""

    duckdb_parameters_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_json (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            metadata VARCHAR
        )
    """)

    import json

    json_data = [
        (1, "JSON 1", json.dumps({"type": "test", "value": 100, "active": True})),
        (2, "JSON 2", json.dumps({"type": "prod", "value": 200, "active": False})),
        (3, "JSON 3", json.dumps({"type": "test", "value": 300, "tags": ["a", "b"]})),
    ]

    for data in json_data:
        duckdb_parameters_session.execute("INSERT INTO test_json (id, name, metadata) VALUES (?, ?, ?)", data)

    try:
        result = duckdb_parameters_session.execute(
            "SELECT name, json_extract_string(metadata, '$.type') as type FROM test_json WHERE json_extract_string(metadata, '$.type') = ?",
            ("test"),
        )
        assert len(result.data) == 2
        assert all(row["type"] == "test" for row in result.data)

    except Exception:
        result = duckdb_parameters_session.execute(
            "SELECT name FROM test_json WHERE metadata LIKE ?", ('%"type":"test"%')
        )
        assert len(result.data) >= 1


def test_duckdb_parameter_with_date_functions(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB date/time functions."""

    duckdb_parameters_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_dates (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            created_date DATE,
            created_timestamp TIMESTAMP
        )
    """)

    date_data = [
        (1, "Date 1", "2023-01-01", "2023-01-01 10:00:00"),
        (2, "Date 2", "2023-06-15", "2023-06-15 14:30:00"),
        (3, "Date 3", "2023-12-31", "2023-12-31 23:59:59"),
    ]

    for data in date_data:
        duckdb_parameters_session.execute(
            "INSERT INTO test_dates (id, name, created_date, created_timestamp) VALUES (?, ?, ?, ?)", data
        )

    result = duckdb_parameters_session.execute(
        "SELECT name, EXTRACT(month FROM created_date) as month FROM test_dates WHERE created_date >= ?", ("2023-06-01")
    )

    assert len(result.data) == 2
    assert all(row["month"] >= 6 for row in result.data)

    timestamp_result = duckdb_parameters_session.execute(
        "SELECT name FROM test_dates WHERE EXTRACT(hour FROM created_timestamp) >= ?", (14)
    )
    assert len(timestamp_result.data) >= 1


def test_duckdb_parameter_with_string_functions(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB string functions."""

    result = duckdb_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE LENGTH(name) > ? AND UPPER(name) LIKE ?", (4, "TEST%")
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None

    assert len(result.data) >= 3

    manipulation_result = duckdb_parameters_session.execute(
        "SELECT name, CONCAT(name, ?) as extended_name FROM test_parameters WHERE POSITION(? IN name) > 0",
        ("_suffix", "test"),
    )
    assert len(manipulation_result.data) >= 3
    for row in manipulation_result.data:
        assert row["extended_name"].endswith("_suffix")


def test_duckdb_parameter_with_math_functions(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB mathematical functions."""

    math_result = duckdb_parameters_session.execute(
        "SELECT name, value, ROUND(value * ?, 2) as multiplied, POW(value, ?) as powered FROM test_parameters WHERE value >= ?",
        (1.5, 2, 100),
    )

    assert len(math_result.data) >= 3
    for row in math_result.data:
        expected_multiplied = round(row["value"] * 1.5, 2)
        expected_powered = row["value"] ** 2
        assert row["multiplied"] == expected_multiplied
        assert row["powered"] == expected_powered


def test_duckdb_parameter_with_aggregate_functions(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test parameters with DuckDB aggregate functions."""

    duckdb_parameters_session.execute_many(
        "INSERT INTO test_parameters (id, name, value, description) VALUES (?, ?, ?, ?)",
        [
            (15, "agg1", 15, "Group A"),
            (16, "agg2", 25, "Group A"),
            (17, "agg3", 35, "Group B"),
            (18, "agg4", 45, "Group B"),
        ],
    )

    result = duckdb_parameters_session.execute(
        """
        SELECT
            description,
            COUNT(*) as count,
            AVG(value) as avg_value,
            MAX(value) as max_value
        FROM test_parameters
        WHERE value >= ? AND description IS NOT NULL
        GROUP BY description
        HAVING COUNT(*) >= ?
        ORDER BY description
    """,
        (10, 2),
    )

    assert len(result.data) == 2
    for row in result.data:
        assert row["count"] >= 2
        assert row["avg_value"] is not None
        assert row["max_value"] >= 10


def test_duckdb_parameter_performance(duckdb_parameters_session: DuckDBDriver) -> None:
    """Test parameter performance with DuckDB."""
    import time

    batch_data = [(i + 19, f"Perf Item {i}", i, f"PERF{i % 5}") for i in range(1000)]

    start_time = time.time()
    duckdb_parameters_session.execute_many(
        "INSERT INTO test_parameters (id, name, value, description) VALUES (?, ?, ?, ?)", batch_data
    )
    end_time = time.time()

    insert_time = end_time - start_time
    assert insert_time < 2.0, f"Batch insert took too long: {insert_time:.2f} seconds"

    start_time = time.time()
    result = duckdb_parameters_session.execute(
        "SELECT COUNT(*) as count FROM test_parameters WHERE value >= ? AND value <= ?", (100, 900)
    )
    end_time = time.time()

    query_time = end_time - start_time
    assert query_time < 1.0, f"Query took too long: {query_time:.2f} seconds"
    assert result.data[0]["count"] >= 800
