"""Test different parameter styles for DuckDB drivers."""

import math
from collections.abc import Generator
from datetime import date
from typing import Any
from uuid import uuid4

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("duckdb")


@pytest.fixture
def duckdb_parameters_session() -> "Generator[DuckDBDriver, None, None]":
    """Create a DuckDB session for parameter style testing."""
    import uuid

    # Use unique database for each test to avoid data contamination
    config = DuckDBConfig(connection_config={"database": f":memory:{uuid.uuid4().hex}"})

    with config.provide_session() as session:
        session.execute_script("""
            CREATE TABLE test_parameters (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                value INTEGER DEFAULT 0,
                description VARCHAR
            )
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


# ===== None Parameter Tests =====
# Tests consolidated from test_none_parameters.py


def test_duckdb_none_parameters() -> None:
    """Test that None values in named parameters are handled correctly by DuckDB."""
    config = DuckDBConfig(connection_config={"database": ":memory:shared_db_none_test"})

    with config.provide_session() as driver:
        # Create test table
        driver.execute("""
            CREATE TABLE test_none_values (
                id VARCHAR PRIMARY KEY,
                text_col VARCHAR,
                nullable_text VARCHAR,
                int_col INTEGER,
                nullable_int INTEGER,
                bool_col BOOLEAN,
                nullable_bool BOOLEAN,
                date_col DATE,
                nullable_date DATE
            )
        """)

        # Test INSERT with None values using named parameters (dollar style)
        test_id = str(uuid4())
        params = {
            "id": test_id,
            "text_col": "test_value",
            "nullable_text": None,  # None value
            "int_col": 42,
            "nullable_int": None,  # None value
            "bool_col": True,
            "nullable_bool": None,  # None value
            "date_col": date(2025, 1, 21),
            "nullable_date": None,  # None value
        }

        result = driver.execute(
            """
            INSERT INTO test_none_values (
                id, text_col, nullable_text, int_col, nullable_int,
                bool_col, nullable_bool, date_col, nullable_date
            )
            VALUES (
                $id, $text_col, $nullable_text, $int_col, $nullable_int,
                $bool_col, $nullable_bool, $date_col, $nullable_date
            )
        """,
            statement_config=None,
            **params,
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Verify the insert worked
        select_result = driver.select_one("SELECT * FROM test_none_values WHERE id = $id", id=test_id)

        assert select_result is not None
        assert select_result["id"] == test_id
        assert select_result["text_col"] == "test_value"
        assert select_result["nullable_text"] is None
        assert select_result["int_col"] == 42
        assert select_result["nullable_int"] is None
        assert select_result["bool_col"] is True
        assert select_result["nullable_bool"] is None
        assert select_result["date_col"] is not None  # Date object
        assert select_result["nullable_date"] is None


def test_duckdb_none_parameters_qmark_style() -> None:
    """Test None values with QMARK (?) parameter style - DuckDB default."""
    config = DuckDBConfig(connection_config={"database": ":memory:shared_db_qmark_test"})

    with config.provide_session() as driver:
        # Create test table without primary key constraint to allow None insertion test
        driver.execute("""
            CREATE TABLE test_none_qmark (
                id INTEGER,
                col1 VARCHAR,
                col2 INTEGER,
                col3 BOOLEAN
            )
        """)

        # Test INSERT with None values using positional parameters
        params = (1, "test_value", None, None)  # Provide explicit ID, None in positions 2 and 3

        result = driver.execute("INSERT INTO test_none_qmark (id, col1, col2, col3) VALUES (?, ?, ?, ?)", params)

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Verify the insert worked
        select_result = driver.select_one("SELECT * FROM test_none_qmark WHERE col1 = ?", ("test_value",))

        assert select_result is not None
        assert select_result["col1"] == "test_value"
        assert select_result["col2"] is None
        assert select_result["col3"] is None


def test_duckdb_none_parameters_numeric_style() -> None:
    """Test None values with NUMERIC ($1, $2) parameter style."""
    config = DuckDBConfig(connection_config={"database": ":memory:shared_db_numeric_test"})

    with config.provide_session() as driver:
        # Create test table without primary key constraint
        driver.execute("""
            CREATE TABLE test_none_numeric (
                id INTEGER,
                col1 VARCHAR,
                col2 INTEGER,
                col3 BOOLEAN
            )
        """)

        # Test INSERT with None values using numeric parameters
        params = (1, "test_value", None, None)  # Provide explicit ID, None in positions 2 and 3

        result = driver.execute("INSERT INTO test_none_numeric (id, col1, col2, col3) VALUES ($1, $2, $3, $4)", params)

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Verify the insert worked
        select_result = driver.select_one("SELECT * FROM test_none_numeric WHERE col1 = $1", ("test_value",))

        assert select_result is not None
        assert select_result["col1"] == "test_value"
        assert select_result["col2"] is None
        assert select_result["col3"] is None


def test_duckdb_all_none_parameters() -> None:
    """Test when all parameter values are None."""
    config = DuckDBConfig(connection_config={"database": ":memory:shared_db_all_none_test"})

    with config.provide_session() as driver:
        # Create test table with auto-increment ID
        driver.execute("""
            CREATE SEQUENCE test_all_none_seq;
            CREATE TABLE test_all_none (
                id INTEGER PRIMARY KEY DEFAULT nextval('test_all_none_seq'),
                col1 VARCHAR,
                col2 INTEGER,
                col3 BOOLEAN
            )
        """)

        # Insert with all None values using named parameters
        params = {"col1": None, "col2": None, "col3": None}

        result = driver.execute(
            """
            INSERT INTO test_all_none (col1, col2, col3)
            VALUES ($col1, $col2, $col3)
        """,
            **params,
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Verify the insert worked - find the most recent row
        select_result = driver.select_one("SELECT * FROM test_all_none ORDER BY id DESC LIMIT 1")

        assert select_result is not None
        assert select_result["id"] is not None  # Auto-generated
        assert select_result["col1"] is None
        assert select_result["col2"] is None
        assert select_result["col3"] is None


def test_duckdb_none_with_execute_many() -> None:
    """Test None values work correctly with execute_many."""
    config = DuckDBConfig(connection_config={"database": ":memory:shared_db_many_test"})

    with config.provide_session() as driver:
        # Create test table
        driver.execute("""
            CREATE TABLE test_none_many (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                value INTEGER
            )
        """)

        # Test execute_many with some None values
        params = [
            (1, "first", 10),
            (2, None, 20),  # None name
            (3, "third", None),  # None value
            (4, None, None),  # Both None
        ]

        result = driver.execute_many("INSERT INTO test_none_many (id, name, value) VALUES (?, ?, ?)", params)

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 4

        # Verify all rows were inserted correctly
        select_result = driver.execute("SELECT * FROM test_none_many ORDER BY id")
        assert isinstance(select_result, SQLResult)
        assert select_result.data is not None
        assert len(select_result.data) == 4

        # Check specific None handling
        rows = select_result.data
        assert rows[0]["name"] == "first" and rows[0]["value"] == 10
        assert rows[1]["name"] is None and rows[1]["value"] == 20
        assert rows[2]["name"] == "third" and rows[2]["value"] is None
        assert rows[3]["name"] is None and rows[3]["value"] is None


def test_duckdb_none_in_where_clause() -> None:
    """Test None values in WHERE clauses work correctly."""
    config = DuckDBConfig(connection_config={"database": ":memory:shared_db_where_test"})

    with config.provide_session() as driver:
        # Create test table
        driver.execute("""
            CREATE TABLE test_none_where (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                category VARCHAR
            )
        """)

        # Insert test data
        test_data = [(1, "item1", "A"), (2, "item2", None), (3, "item3", "B"), (4, "item4", None)]
        driver.execute_many("INSERT INTO test_none_where (id, name, category) VALUES (?, ?, ?)", test_data)

        # Test WHERE with None parameter using IS NULL comparison
        result = driver.execute("SELECT * FROM test_none_where WHERE category IS NULL")

        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 2  # Two rows with NULL category

        # Verify the correct rows were found
        found_ids = {row["id"] for row in result.data}
        assert found_ids == {2, 4}

        # Test direct comparison with None parameter (should work with parameters)
        none_result = driver.execute("SELECT * FROM test_none_where WHERE category = ? OR ? IS NULL", (None, None))

        # The second condition should be TRUE since None IS NULL
        assert isinstance(none_result, SQLResult)
        assert none_result.data is not None
        assert len(none_result.data) == 4  # All rows because condition is always true


def test_duckdb_none_complex_parameter_scenarios() -> None:
    """Test complex scenarios with None parameters that might cause issues."""
    config = DuckDBConfig(connection_config={"database": ":memory:shared_db_complex_test"})

    with config.provide_session() as driver:
        # Create test table
        driver.execute("""
            CREATE TABLE test_complex_none (
                id INTEGER,
                col1 VARCHAR,
                col2 INTEGER,
                col3 REAL,
                col4 BOOLEAN,
                col5 DATE,
                col6 VARCHAR[]
            )
        """)

        # Test 1: Mix of None and complex values
        complex_params = {
            "id": 1,
            "col1": "complex_test",
            "col2": None,
            "col3": 3.14159,
            "col4": None,
            "col5": date(2025, 1, 21),
            "col6": ["array", "with", "values"],
        }

        result = driver.execute(
            """
            INSERT INTO test_complex_none (id, col1, col2, col3, col4, col5, col6)
            VALUES ($id, $col1, $col2, $col3, $col4, $col5, $col6)
        """,
            statement_config=None,
            **complex_params,
        )

        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1

        # Test 2: Correct parameter count with None values
        params_for_count_test = (2, "test2", None, None, None)  # 5 parameters for 5 placeholders

        # Should NOT raise a parameter count error
        driver.execute(
            "INSERT INTO test_complex_none (id, col1, col2, col3, col4) VALUES (?, ?, ?, ?, ?)", params_for_count_test
        )

        # Test 3: Verify complex insert worked correctly
        verify_result = driver.select_one("SELECT * FROM test_complex_none WHERE id = ?", (1,))

        assert verify_result is not None
        assert verify_result["col1"] == "complex_test"
        assert verify_result["col2"] is None
        assert abs(verify_result["col3"] - 3.14159) < 0.00001
        assert verify_result["col4"] is None
        assert verify_result["col5"] is not None
        assert verify_result["col6"] == ["array", "with", "values"]


def test_duckdb_none_parameter_edge_cases() -> None:
    """Test edge cases that might reveal parameter handling bugs."""
    config = DuckDBConfig(connection_config={"database": ":memory:shared_db_edge_test"})

    with config.provide_session() as driver:
        # Test 1: Empty parameter list with None
        driver.execute("CREATE TABLE test_edge (id INTEGER)")

        # Test 2: Single None parameter
        driver.execute("CREATE TABLE test_single_none (id INTEGER, value VARCHAR)")
        driver.execute("INSERT INTO test_single_none VALUES (1, ?)", (None,))

        result = driver.select_one("SELECT * FROM test_single_none WHERE id = 1")
        assert result is not None
        assert result["value"] is None

        # Test 3: Multiple consecutive None parameters
        driver.execute("CREATE TABLE test_consecutive_none (a INTEGER, b VARCHAR, c VARCHAR, d VARCHAR)")
        driver.execute("INSERT INTO test_consecutive_none VALUES (?, ?, ?, ?)", (1, None, None, None))

        result = driver.select_one("SELECT * FROM test_consecutive_none WHERE a = 1")
        assert result is not None
        assert result["b"] is None
        assert result["c"] is None
        assert result["d"] is None

        # Test 4: None at beginning, middle, and end positions
        driver.execute("CREATE TABLE test_position_none (a VARCHAR, b VARCHAR, c VARCHAR)")
        test_cases = [
            (None, "middle", "end"),  # None at start
            ("start", None, "end"),  # None at middle
            ("start", "middle", None),  # None at end
            (None, None, "end"),  # Multiple None at start
            ("start", None, None),  # Multiple None at end
        ]

        for i, params in enumerate(test_cases):
            driver.execute("INSERT INTO test_position_none VALUES (?, ?, ?)", params)

        # Verify all rows were inserted
        all_results = driver.execute("SELECT COUNT(*) as count FROM test_position_none")
        assert all_results.data[0]["count"] == 5


def test_duckdb_parameter_count_mismatch_with_none() -> None:
    """Test that parameter count mismatches are properly detected even when None values are involved.

    This test verifies the bug mentioned in the original issue where parameter
    count mismatches might be missed when None values are present.
    """
    config = DuckDBConfig(connection_config={"database": ":memory:shared_db_param_count_test"})

    with config.provide_session() as driver:
        driver.execute("CREATE TABLE test_param_count (col1 VARCHAR, col2 INTEGER)")

        # Test: Too many parameters - should raise an error
        with pytest.raises(Exception) as exc_info:
            driver.execute(
                "INSERT INTO test_param_count (col1, col2) VALUES (?, ?)",  # 2 placeholders
                ("value1", None, "extra_param"),  # 3 parameters
            )

        # Should be a parameter count error
        assert "mismatch" in str(exc_info.value).lower()

        # Test: Too few parameters - should raise an error
        with pytest.raises(Exception) as exc_info:
            driver.execute(
                "INSERT INTO test_param_count (col1, col2) VALUES (?, ?)",  # 2 placeholders
                ("value1",),  # Only 1 parameter
            )

        # Should be a parameter count error
        assert "mismatch" in str(exc_info.value).lower() or "parameter" in str(exc_info.value).lower()

        # Test: Correct count with None should work fine
        result = driver.execute("INSERT INTO test_param_count (col1, col2) VALUES (?, ?)", ("value1", None))
        assert isinstance(result, SQLResult)
        assert result.rows_affected == 1
