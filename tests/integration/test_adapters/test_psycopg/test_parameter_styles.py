"""Test different parameter styles for Psycopg drivers."""

import math
from collections.abc import Generator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
def psycopg_params_session(postgres_service: PostgresService) -> "Generator[PsycopgSyncDriver, None, None]":
    """Create a Psycopg session for parameter style testing."""
    config = PsycopgSyncConfig(
        host=postgres_service.host,
        port=postgres_service.port,
        user=postgres_service.user,
        password=postgres_service.password,
        dbname=postgres_service.database,
        autocommit=True,  # Enable autocommit for tests
        statement_config=SQLConfig(strict_mode=False),
    )

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_params (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                description TEXT
            )
        """)
        # Clear any existing data
        session.execute_script("TRUNCATE TABLE test_params RESTART IDENTITY")

        # Insert test data
        session.execute(
            "INSERT INTO test_params (name, value, description) VALUES (%s, %s, %s)", ("test1", 100, "First test")
        )
        session.execute(
            "INSERT INTO test_params (name, value, description) VALUES (%s, %s, %s)", ("test2", 200, "Second test")
        )
        session.execute(
            "INSERT INTO test_params (name, value, description) VALUES (%s, %s, %s)", ("test3", 300, None)
        )  # NULL description
        yield session
        # Cleanup
        session.execute_script("DROP TABLE IF EXISTS test_params")


@pytest.mark.xdist_group("postgres")
@pytest.mark.parametrize(
    "params,expected_count",
    [
        (("test1",), 1),  # Tuple parameter
        (["test1"], 1),  # List parameter
    ],
)
def test_psycopg_pyformat_parameter_types(
    psycopg_params_session: PsycopgSyncDriver, params: Any, expected_count: int
) -> None:
    """Test different parameter types with Psycopg pyformat style."""
    result = psycopg_params_session.execute("SELECT * FROM test_params WHERE name = %s", params)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == expected_count
    if expected_count > 0:
        assert result.data[0]["name"] == "test1"


@pytest.mark.xdist_group("postgres")
@pytest.mark.parametrize(
    "params,style,query",
    [
        (("test1",), "pyformat_positional", "SELECT * FROM test_params WHERE name = %s"),
        ({"name": "test1"}, "pyformat_named", "SELECT * FROM test_params WHERE name = %(name)s"),
    ],
)
def test_psycopg_parameter_styles(
    psycopg_params_session: PsycopgSyncDriver, params: Any, style: str, query: str
) -> None:
    """Test different parameter styles with Psycopg."""
    result = psycopg_params_session.execute(query, params)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"


@pytest.mark.xdist_group("postgres")
def test_psycopg_multiple_parameters_pyformat(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test queries with multiple parameters using pyformat style."""
    result = psycopg_params_session.execute(
        "SELECT * FROM test_params WHERE value >= %s AND value <= %s ORDER BY value", (50, 150)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 100


@pytest.mark.xdist_group("postgres")
def test_psycopg_multiple_parameters_named(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test queries with multiple parameters using named style."""
    result = psycopg_params_session.execute(
        "SELECT * FROM test_params WHERE value >= %(min_val)s AND value <= %(max_val)s ORDER BY value",
        {"min_val": 50, "max_val": 150},
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 100


@pytest.mark.xdist_group("postgres")
def test_psycopg_null_parameters(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test handling of NULL parameters on Psycopg."""
    # Query for NULL values
    result = psycopg_params_session.execute("SELECT * FROM test_params WHERE description IS NULL")

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test3"
    assert result.data[0]["description"] is None

    # Test inserting NULL with parameters
    psycopg_params_session.execute(
        "INSERT INTO test_params (name, value, description) VALUES (%s, %s, %s)", ("null_param_test", 400, None)
    )

    null_result = psycopg_params_session.execute("SELECT * FROM test_params WHERE name = %s", ("null_param_test",))
    assert len(null_result.data) == 1
    assert null_result.data[0]["description"] is None


@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_escaping(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test parameter escaping prevents SQL injection."""
    # This should safely search for a literal string with quotes
    malicious_input = "'; DROP TABLE test_params; --"

    result = psycopg_params_session.execute("SELECT * FROM test_params WHERE name = %s", (malicious_input,))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 0  # No matches, but table should still exist

    # Verify table still exists by counting all records
    count_result = psycopg_params_session.execute("SELECT COUNT(*) as count FROM test_params")
    assert count_result.data[0]["count"] >= 3  # Our test data should still be there


@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_with_like(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test parameters with LIKE operations."""
    result = psycopg_params_session.execute("SELECT * FROM test_params WHERE name LIKE %s", ("test%",))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) >= 3  # test1, test2, test3

    # Test with named parameter
    named_result = psycopg_params_session.execute(
        "SELECT * FROM test_params WHERE name LIKE %(pattern)s", {"pattern": "test1%"}
    )
    assert len(named_result.data) == 1
    assert named_result.data[0]["name"] == "test1"


@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_with_any_array(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test parameters with PostgreSQL ANY and arrays."""
    # Insert additional test data
    psycopg_params_session.execute_many(
        "INSERT INTO test_params (name, value, description) VALUES (%s, %s, %s)",
        [("alpha", 10, "Alpha test"), ("beta", 20, "Beta test"), ("gamma", 30, "Gamma test")],
    )

    # Test ANY with array parameter
    result = psycopg_params_session.execute(
        "SELECT * FROM test_params WHERE name = ANY(%s) ORDER BY name", (["alpha", "beta", "test1"],)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 3
    assert result.data[0]["name"] == "alpha"
    assert result.data[1]["name"] == "beta"
    assert result.data[2]["name"] == "test1"


@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_with_sql_object(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test parameters with SQL object."""
    from sqlspec.statement.sql import SQL

    # Test with pyformat style
    sql_obj = SQL("SELECT * FROM test_params WHERE value > %s", parameters=[150])
    result = psycopg_params_session.execute(sql_obj)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) >= 1
    assert all(row["value"] > 150 for row in result.data)

    # Test with named style
    named_sql = SQL("SELECT * FROM test_params WHERE value < %(max_value)s", parameters={"max_value": 150})
    named_result = psycopg_params_session.execute(named_sql)

    assert isinstance(named_result, SQLResult)
    assert named_result.data is not None
    assert len(named_result.data) >= 1
    assert all(row["value"] < 150 for row in named_result.data)


@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_data_types(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test different parameter data types with Psycopg."""
    # Drop and recreate table to ensure clean state
    psycopg_params_session.execute_script("""
        DROP TABLE IF EXISTS test_types;
        CREATE TABLE test_types (
            id SERIAL PRIMARY KEY,
            int_val INTEGER,
            real_val REAL,
            text_val TEXT,
            bool_val BOOLEAN,
            array_val INTEGER[]
        )
    """)

    # Test different data types
    test_data = [
        (42, math.pi, "hello", True, [1, 2, 3]),
        (-100, -2.5, "world", False, [4, 5, 6]),
        (0, 0.0, "", None, []),
    ]

    for data in test_data:
        psycopg_params_session.execute(
            "INSERT INTO test_types (int_val, real_val, text_val, bool_val, array_val) VALUES (%s, %s, %s, %s, %s)",
            data,
        )

    # Verify data with parameters
    # First check if data was inserted
    all_data_result = psycopg_params_session.execute("SELECT * FROM test_types")
    assert len(all_data_result.data) == 3  # We inserted 3 rows

    # Now test with specific parameters - use int comparison only to avoid float precision issues
    result = psycopg_params_session.execute("SELECT * FROM test_types WHERE int_val = %s", (42,))

    assert len(result.data) == 1
    assert result.data[0]["text_val"] == "hello"
    assert result.data[0]["bool_val"] is True
    assert result.data[0]["array_val"] == [1, 2, 3]
    assert abs(result.data[0]["real_val"] - math.pi) < 0.001  # Use approximate comparison for float


@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_edge_cases(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test edge cases for Psycopg parameters."""
    # Empty string parameter
    psycopg_params_session.execute(
        "INSERT INTO test_params (name, value, description) VALUES (%s, %s, %s)", ("", 999, "Empty name test")
    )

    empty_result = psycopg_params_session.execute("SELECT * FROM test_params WHERE name = %s", ("",))
    assert len(empty_result.data) == 1
    assert empty_result.data[0]["value"] == 999

    # Very long string parameter
    long_string = "x" * 1000
    psycopg_params_session.execute(
        "INSERT INTO test_params (name, value, description) VALUES (%s, %s, %s)", ("long_test", 1000, long_string)
    )

    long_result = psycopg_params_session.execute("SELECT * FROM test_params WHERE description = %s", (long_string,))
    assert len(long_result.data) == 1
    assert len(long_result.data[0]["description"]) == 1000


@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_with_postgresql_functions(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test parameters with PostgreSQL functions."""
    # Test with string functions
    result = psycopg_params_session.execute(
        "SELECT * FROM test_params WHERE LENGTH(name) > %s AND UPPER(name) LIKE %s", (4, "TEST%")
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    # Should find test1, test2, test3 (all have length > 4 and start with "test")
    assert len(result.data) >= 3

    # Test with math functions and named parameters
    math_result = psycopg_params_session.execute(
        "SELECT name, value, ROUND(CAST(value * %(multiplier)s AS NUMERIC), 2) as multiplied FROM test_params WHERE value >= %(min_val)s",
        {"multiplier": 1.5, "min_val": 100},
    )
    assert len(math_result.data) >= 3
    for row in math_result.data:
        expected = round(row["value"] * 1.5, 2)
        assert row["multiplied"] == expected


@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_with_json(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test parameters with PostgreSQL JSON operations."""
    # Create table with JSONB column
    psycopg_params_session.execute_script("""
        DROP TABLE IF EXISTS test_json;
        CREATE TABLE test_json (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB
        )
    """)

    import json

    # Test inserting JSON data with parameters
    json_data = [
        ("JSON 1", {"type": "test", "value": 100, "active": True}),
        ("JSON 2", {"type": "prod", "value": 200, "active": False}),
        ("JSON 3", {"type": "test", "value": 300, "tags": ["a", "b"]}),
    ]

    for name, metadata in json_data:
        psycopg_params_session.execute(
            "INSERT INTO test_json (name, metadata) VALUES (%s, %s)", (name, json.dumps(metadata))
        )

    # Test querying JSON with parameters
    result = psycopg_params_session.execute(
        "SELECT name, metadata->>'type' as type, (metadata->>'value')::INTEGER as value FROM test_json WHERE metadata->>'type' = %s",
        ("test",),
    )

    assert len(result.data) == 2  # JSON 1 and JSON 3
    assert all(row["type"] == "test" for row in result.data)

    # Test with named parameters
    named_result = psycopg_params_session.execute(
        "SELECT name FROM test_json WHERE (metadata->>'value')::INTEGER > %(min_value)s", {"min_value": 150}
    )
    assert len(named_result.data) >= 1


@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_with_arrays(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test parameters with PostgreSQL array operations."""
    # Create table with array columns
    psycopg_params_session.execute_script("""
        DROP TABLE IF EXISTS test_arrays;
        CREATE TABLE test_arrays (
            id SERIAL PRIMARY KEY,
            name TEXT,
            tags TEXT[],
            scores INTEGER[]
        )
    """)

    # Test inserting array data with parameters
    array_data = [
        ("Array 1", ["tag1", "tag2"], [10, 20, 30]),
        ("Array 2", ["tag3"], [40, 50]),
        ("Array 3", ["tag4", "tag5", "tag6"], [60]),
    ]

    for name, tags, scores in array_data:
        psycopg_params_session.execute(
            "INSERT INTO test_arrays (name, tags, scores) VALUES (%s, %s, %s)", (name, tags, scores)
        )

    # Test querying arrays with parameters
    result = psycopg_params_session.execute("SELECT name FROM test_arrays WHERE %s = ANY(tags)", ("tag2",))

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Array 1"

    # Test with named parameters
    named_result = psycopg_params_session.execute(
        "SELECT name FROM test_arrays WHERE array_length(scores, 1) > %(min_length)s", {"min_length": 1}
    )
    assert len(named_result.data) == 2  # Array 1 and Array 2


@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_with_window_functions(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test parameters with PostgreSQL window functions."""
    # Insert some test data for window functions
    psycopg_params_session.execute_many(
        "INSERT INTO test_params (name, value, description) VALUES (%s, %s, %s)",
        [
            ("window1", 50, "Group A"),
            ("window2", 75, "Group A"),
            ("window3", 25, "Group B"),
            ("window4", 100, "Group B"),
        ],
    )

    # Test window function with parameter
    result = psycopg_params_session.execute(
        """
        SELECT
            name,
            value,
            description,
            ROW_NUMBER() OVER (PARTITION BY description ORDER BY value) as row_num
        FROM test_params
        WHERE value > %s
        ORDER BY description, value
    """,
        (30,),
    )

    assert len(result.data) >= 4
    # Verify window function worked correctly
    group_a_rows = [row for row in result.data if row["description"] == "Group A"]
    assert len(group_a_rows) == 2
    assert group_a_rows[0]["row_num"] == 1  # First in partition
    assert group_a_rows[1]["row_num"] == 2  # Second in partition


@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_with_copy_operations(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test parameters in queries alongside COPY operations."""
    # First use parameters to find specific data
    filter_result = psycopg_params_session.execute(
        "SELECT COUNT(*) as count FROM test_params WHERE value >= %s", (100,)
    )
    filter_result.data[0]["count"]

    # Insert data that would be suitable for COPY operations
    batch_data = [(f"Copy Item {i}", i * 50, "COPY_DATA") for i in range(10)]
    psycopg_params_session.execute_many(
        "INSERT INTO test_params (name, value, description) VALUES (%s, %s, %s)", batch_data
    )

    # Use parameters to verify the data was inserted correctly
    verify_result = psycopg_params_session.execute(
        "SELECT COUNT(*) as count FROM test_params WHERE description = %s AND value >= %s", ("COPY_DATA", 100)
    )

    assert verify_result.data[0]["count"] >= 8  # Should have items with value >= 100


@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_mixed_styles_same_query(psycopg_params_session: PsycopgSyncDriver) -> None:
    """Test edge case where mixing parameter styles might occur."""
    # This should work with named parameters
    result = psycopg_params_session.execute(
        "SELECT * FROM test_params WHERE name = %(name)s AND value > %(min_value)s", {"name": "test1", "min_value": 50}
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"
    assert result.data[0]["value"] == 100
