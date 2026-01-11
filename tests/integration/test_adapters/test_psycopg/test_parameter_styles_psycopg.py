"""Test different parameter styles for Psycopg drivers."""

import math
from collections.abc import Generator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver, default_statement_config
from sqlspec.core import SQL, SQLResult

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
def psycopg_parameters_session(postgres_service: "PostgresService") -> "Generator[PsycopgSyncDriver, None, None]":
    """Create a Psycopg session for parameter style testing."""
    config = PsycopgSyncConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "dbname": postgres_service.database,
            "autocommit": True,
        },
        statement_config=default_statement_config,
    )

    try:
        with config.provide_session() as session:
            session.execute_script("""
                CREATE TABLE IF NOT EXISTS test_parameters (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    description TEXT
                )
            """)

            session.execute_script("TRUNCATE TABLE test_parameters RESTART IDENTITY")

            session.execute(
                "INSERT INTO test_parameters (name, value, description) VALUES (%s, %s, %s)",
                ("test1", 100, "First test"),
            )
            session.execute(
                "INSERT INTO test_parameters (name, value, description) VALUES (%s, %s, %s)",
                ("test2", 200, "Second test"),
            )
            session.execute(
                "INSERT INTO test_parameters (name, value, description) VALUES (%s, %s, %s)", ("test3", 300, None)
            )
            yield session

            session.execute_script("DROP TABLE IF EXISTS test_parameters")
    finally:
        config.close_pool()


@pytest.mark.parametrize("parameters,expected_count", [(("test1"), 1), (["test1"], 1)])
def test_psycopg_pyformat_parameter_types(
    psycopg_parameters_session: PsycopgSyncDriver, parameters: Any, expected_count: int
) -> None:
    """Test different parameter types with Psycopg pyformat style."""
    result = psycopg_parameters_session.execute("SELECT * FROM test_parameters WHERE name = %s", parameters)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == expected_count
    if expected_count > 0:
        assert result.data[0]["name"] == "test1"


@pytest.mark.parametrize(
    "parameters,style,query",
    [
        (("test1"), "pyformat_positional", "SELECT * FROM test_parameters WHERE name = %s"),
        ({"name": "test1"}, "pyformat_named", "SELECT * FROM test_parameters WHERE name = %(name)s"),
    ],
)
def test_psycopg_parameter_styles(
    psycopg_parameters_session: PsycopgSyncDriver, parameters: Any, style: str, query: str
) -> None:
    """Test different parameter styles with Psycopg."""
    result = psycopg_parameters_session.execute(query, parameters)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"


def test_psycopg_multiple_parameters_pyformat(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test queries with multiple parameters using pyformat style."""
    result = psycopg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE value >= %s AND value <= %s ORDER BY value", (50, 150)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 100


def test_psycopg_multiple_parameters_named(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test queries with multiple parameters using named style."""
    result = psycopg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE value >= %(min_val)s AND value <= %(max_val)s ORDER BY value",
        {"min_val": 50, "max_val": 150},
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] == 100


def test_psycopg_null_parameters(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test handling of NULL parameters on Psycopg."""

    result = psycopg_parameters_session.execute("SELECT * FROM test_parameters WHERE description IS NULL")

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test3"
    assert result.data[0]["description"] is None

    psycopg_parameters_session.execute(
        "INSERT INTO test_parameters (name, value, description) VALUES (%s, %s, %s)", ("null_param_test", 400, None)
    )

    null_result = psycopg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name = %s", ("null_param_test")
    )
    assert len(null_result.data) == 1
    assert null_result.data[0]["description"] is None


def test_psycopg_parameter_escaping(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test parameter escaping prevents SQL injection."""

    malicious_input = "'; DROP TABLE test_parameters; --"

    result = psycopg_parameters_session.execute("SELECT * FROM test_parameters WHERE name = %s", (malicious_input))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 0

    count_result = psycopg_parameters_session.execute("SELECT COUNT(*) as count FROM test_parameters")
    assert count_result.data[0]["count"] >= 3


def test_psycopg_parameter_with_like(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test parameters with LIKE operations."""
    result = psycopg_parameters_session.execute("SELECT * FROM test_parameters WHERE name LIKE %s", ("test%"))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) >= 3

    named_result = psycopg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name LIKE %(pattern)s", {"pattern": "test1%"}
    )
    assert len(named_result.data) == 1
    assert named_result.data[0]["name"] == "test1"


def test_psycopg_parameter_with_any_array(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test parameters with PostgreSQL ANY and arrays."""

    psycopg_parameters_session.execute_many(
        "INSERT INTO test_parameters (name, value, description) VALUES (%s, %s, %s)",
        [("alpha", 10, "Alpha test"), ("beta", 20, "Beta test"), ("gamma", 30, "Gamma test")],
    )

    result = psycopg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name = ANY(%s) ORDER BY name", (["alpha", "beta", "test1"],)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 3
    assert result.data[0]["name"] == "alpha"
    assert result.data[1]["name"] == "beta"
    assert result.data[2]["name"] == "test1"


def test_psycopg_parameter_with_sql_object(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test parameters with SQL object."""

    sql_obj = SQL("SELECT * FROM test_parameters WHERE value > %s", [150])
    result = psycopg_parameters_session.execute(sql_obj)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) >= 1
    assert all(row["value"] > 150 for row in result.data)

    named_sql = SQL("SELECT * FROM test_parameters WHERE value < %(max_value)s", {"max_value": 150})
    named_result = psycopg_parameters_session.execute(named_sql)

    assert isinstance(named_result, SQLResult)
    assert named_result.data is not None
    assert len(named_result.data) >= 1
    assert all(row["value"] < 150 for row in named_result.data)


def test_psycopg_parameter_data_types(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test different parameter data types with Psycopg."""

    psycopg_parameters_session.execute_script("""
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

    test_data = [
        (42, math.pi, "hello", True, [1, 2, 3]),
        (-100, -2.5, "world", False, [4, 5, 6]),
        (0, 0.0, "", None, []),
    ]

    for data in test_data:
        psycopg_parameters_session.execute(
            "INSERT INTO test_types (int_val, real_val, text_val, bool_val, array_val) VALUES (%s, %s, %s, %s, %s)",
            data,
        )

    all_data_result = psycopg_parameters_session.execute("SELECT * FROM test_types")
    assert len(all_data_result.data) == 3

    result = psycopg_parameters_session.execute("SELECT * FROM test_types WHERE int_val = %s", (42))

    assert len(result.data) == 1
    assert result.data[0]["text_val"] == "hello"
    assert result.data[0]["bool_val"] is True
    assert result.data[0]["array_val"] == [1, 2, 3]
    assert abs(result.data[0]["real_val"] - math.pi) < 0.001


def test_psycopg_parameter_edge_cases(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test edge cases for Psycopg parameters."""

    psycopg_parameters_session.execute(
        "INSERT INTO test_parameters (name, value, description) VALUES (%s, %s, %s)", ("", 999, "Empty name test")
    )

    empty_result = psycopg_parameters_session.execute("SELECT * FROM test_parameters WHERE name = %s", (""))
    assert len(empty_result.data) == 1
    assert empty_result.data[0]["value"] == 999

    long_string = "x" * 1000
    psycopg_parameters_session.execute(
        "INSERT INTO test_parameters (name, value, description) VALUES (%s, %s, %s)", ("long_test", 1000, long_string)
    )

    long_result = psycopg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE description = %s", (long_string)
    )
    assert len(long_result.data) == 1
    assert len(long_result.data[0]["description"]) == 1000


def test_psycopg_parameter_with_postgresql_functions(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test parameters with PostgreSQL functions."""

    result = psycopg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE LENGTH(name) > %s AND UPPER(name) LIKE %s", (4, "TEST%")
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None

    assert len(result.data) >= 3

    math_result = psycopg_parameters_session.execute(
        "SELECT name, value, ROUND(CAST(value * %(multiplier)s AS NUMERIC), 2) as multiplied FROM test_parameters WHERE value >= %(min_val)s",
        {"multiplier": 1.5, "min_val": 100},
    )
    assert len(math_result.data) >= 3
    for row in math_result.data:
        expected = round(row["value"] * 1.5, 2)
        assert row["multiplied"] == expected


def test_psycopg_parameter_with_json(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test parameters with PostgreSQL JSON operations."""

    psycopg_parameters_session.execute_script("""
        DROP TABLE IF EXISTS test_json;
        CREATE TABLE test_json (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB
        )
    """)

    import json

    json_data = [
        ("JSON 1", {"type": "test", "value": 100, "active": True}),
        ("JSON 2", {"type": "prod", "value": 200, "active": False}),
        ("JSON 3", {"type": "test", "value": 300, "tags": ["a", "b"]}),
    ]

    for name, metadata in json_data:
        psycopg_parameters_session.execute(
            "INSERT INTO test_json (name, metadata) VALUES (%s, %s)", (name, json.dumps(metadata))
        )

    result = psycopg_parameters_session.execute(
        "SELECT name, metadata->>'type' as type, (metadata->>'value')::INTEGER as value FROM test_json WHERE metadata->>'type' = %s",
        ("test"),
    )

    assert len(result.data) == 2
    assert all(row["type"] == "test" for row in result.data)

    named_result = psycopg_parameters_session.execute(
        "SELECT name FROM test_json WHERE (metadata->>'value')::INTEGER > %(min_value)s", {"min_value": 150}
    )
    assert len(named_result.data) >= 1


def test_psycopg_parameter_with_arrays(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test parameters with PostgreSQL array operations."""

    psycopg_parameters_session.execute_script("""
        DROP TABLE IF EXISTS test_arrays;
        CREATE TABLE test_arrays (
            id SERIAL PRIMARY KEY,
            name TEXT,
            tags TEXT[],
            scores INTEGER[]
        )
    """)

    array_data = [
        ("Array 1", ["tag1", "tag2"], [10, 20, 30]),
        ("Array 2", ["tag3"], [40, 50]),
        ("Array 3", ["tag4", "tag5", "tag6"], [60]),
    ]

    for name, tags, scores in array_data:
        psycopg_parameters_session.execute(
            "INSERT INTO test_arrays (name, tags, scores) VALUES (%s, %s, %s)", (name, tags, scores)
        )

    result = psycopg_parameters_session.execute("SELECT name FROM test_arrays WHERE %s = ANY(tags)", ("tag2"))

    assert len(result.data) == 1
    assert result.data[0]["name"] == "Array 1"

    named_result = psycopg_parameters_session.execute(
        "SELECT name FROM test_arrays WHERE array_length(scores, 1) > %(min_length)s", {"min_length": 1}
    )
    assert len(named_result.data) == 2


def test_psycopg_parameter_with_window_functions(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test parameters with PostgreSQL window functions."""

    psycopg_parameters_session.execute_many(
        "INSERT INTO test_parameters (name, value, description) VALUES (%s, %s, %s)",
        [
            ("window1", 50, "Group A"),
            ("window2", 75, "Group A"),
            ("window3", 25, "Group B"),
            ("window4", 100, "Group B"),
        ],
    )

    result = psycopg_parameters_session.execute(
        """
        SELECT
            name,
            value,
            description,
            ROW_NUMBER() OVER (PARTITION BY description ORDER BY value) as row_num
        FROM test_parameters
        WHERE value > %s
        ORDER BY description, value
    """,
        (30),
    )

    assert len(result.data) >= 4

    group_a_rows = [row for row in result.data if row["description"] == "Group A"]
    assert len(group_a_rows) == 2
    assert group_a_rows[0]["row_num"] == 1
    assert group_a_rows[1]["row_num"] == 2


def test_psycopg_parameter_with_copy_operations(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test parameters in queries alongside COPY operations."""

    filter_result = psycopg_parameters_session.execute(
        "SELECT COUNT(*) as count FROM test_parameters WHERE value >= %s", (100)
    )
    filter_result.data[0]["count"]

    batch_data = [(f"Copy Item {i}", i * 50, "COPY_DATA") for i in range(10)]
    psycopg_parameters_session.execute_many(
        "INSERT INTO test_parameters (name, value, description) VALUES (%s, %s, %s)", batch_data
    )

    verify_result = psycopg_parameters_session.execute(
        "SELECT COUNT(*) as count FROM test_parameters WHERE description = %s AND value >= %s", ("COPY_DATA", 100)
    )

    assert verify_result.data[0]["count"] >= 8


def test_psycopg_parameter_mixed_styles_same_query(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test edge case where mixing parameter styles might occur."""

    result = psycopg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name = %(name)s AND value > %(min_value)s",
        {"name": "test1", "min_value": 50},
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"
    assert result.data[0]["value"] == 100


def test_psycopg_named_pyformat_parameter_conversion(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test that NAMED_PYFORMAT parameters are converted correctly through the pipeline."""

    result = psycopg_parameters_session.execute(
        "SELECT * FROM test_parameters WHERE name = %(target_name)s AND value > %(min_value)s",
        {"target_name": "test1", "min_value": 50},
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test1"
    assert result.data[0]["value"] == 100


def test_psycopg_mixed_null_parameters(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test edge cases with mixed NULL and non-NULL parameters."""

    test_data = [("test_null_1", 10, None), ("test_null_2", 20, "non-null"), ("test_null_3", 30, None)]

    for name, value, description in test_data:
        psycopg_parameters_session.execute(
            "INSERT INTO test_parameters (name, value, description) VALUES (%(name)s, %(value)s, %(description)s)",
            {"name": name, "value": value, "description": description},
        )

    result = psycopg_parameters_session.execute(
        "SELECT COUNT(*) as count FROM test_parameters WHERE description IS NULL AND value > %(min_val)s AND name LIKE %(pattern)s",
        {"min_val": 15, "pattern": "test_null_%"},
    )

    assert result.data[0]["count"] == 1


def test_psycopg_parameter_consistency_check(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test that different parameter styles produce consistent results."""

    named_result = psycopg_parameters_session.execute(
        "SELECT COUNT(*) as count FROM test_parameters WHERE value > %(threshold)s", {"threshold": 150}
    )

    positional_result = psycopg_parameters_session.execute(
        "SELECT COUNT(*) as count FROM test_parameters WHERE value > %s", (150,)
    )

    assert named_result.data[0]["count"] == positional_result.data[0]["count"]


def test_psycopg_none_values_in_named_parameters(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test that None values in named parameters are handled correctly."""
    from datetime import date
    from uuid import uuid4

    psycopg_parameters_session.execute("""
        CREATE TABLE IF NOT EXISTS test_none_values (
            id UUID PRIMARY KEY,
            text_col TEXT,
            nullable_text TEXT,
            int_col INTEGER,
            nullable_int INTEGER,
            bool_col BOOLEAN,
            nullable_bool BOOLEAN,
            date_col DATE,
            nullable_date DATE
        )
    """)

    # Test INSERT with None values using named parameters
    test_id = uuid4()
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

    psycopg_parameters_session.execute(
        """
        INSERT INTO test_none_values (
            id, text_col, nullable_text, int_col, nullable_int,
            bool_col, nullable_bool, date_col, nullable_date
        )
        VALUES (
            %(id)s, %(text_col)s, %(nullable_text)s, %(int_col)s, %(nullable_int)s,
            %(bool_col)s, %(nullable_bool)s, %(date_col)s, %(nullable_date)s
        )
    """,
        statement_config=None,
        **params,
    )

    # Verify the insert worked
    result = psycopg_parameters_session.select_one("SELECT * FROM test_none_values WHERE id = %(id)s", id=test_id)

    assert result is not None
    assert result["id"] == test_id
    assert result["text_col"] == "test_value"
    assert result["nullable_text"] is None
    assert result["int_col"] == 42
    assert result["nullable_int"] is None
    assert result["bool_col"] is True
    assert result["nullable_bool"] is None
    assert result["date_col"] is not None  # Date object
    assert result["nullable_date"] is None

    psycopg_parameters_session.execute("DROP TABLE IF EXISTS test_none_values")


def test_psycopg_all_none_parameters(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test when all parameter values are None."""
    psycopg_parameters_session.execute("""
        CREATE TABLE IF NOT EXISTS test_all_none (
            id SERIAL PRIMARY KEY,
            col1 TEXT,
            col2 INTEGER,
            col3 BOOLEAN
        )
    """)

    # Insert with all None values
    params = {"col1": None, "col2": None, "col3": None}

    result = psycopg_parameters_session.select_one(
        """
        INSERT INTO test_all_none (col1, col2, col3)
        VALUES (%(col1)s, %(col2)s, %(col3)s)
        RETURNING id, col1, col2, col3
    """,
        **params,
    )

    assert result is not None
    assert result["id"] is not None  # Auto-generated
    assert result["col1"] is None
    assert result["col2"] is None
    assert result["col3"] is None

    psycopg_parameters_session.execute("DROP TABLE IF EXISTS test_all_none")


def test_psycopg_none_with_execute_many(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test None values work correctly with execute_many."""
    psycopg_parameters_session.execute("""
        CREATE TABLE IF NOT EXISTS test_none_many (
            id INTEGER PRIMARY KEY,
            name TEXT,
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

    result = psycopg_parameters_session.execute_many(
        "INSERT INTO test_none_many (id, name, value) VALUES (%s, %s, %s)", params
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 4

    # Verify all rows were inserted correctly
    select_result = psycopg_parameters_session.execute("SELECT * FROM test_none_many ORDER BY id")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 4

    # Check specific None handling
    rows = select_result.data
    assert rows[0]["name"] == "first" and rows[0]["value"] == 10
    assert rows[1]["name"] is None and rows[1]["value"] == 20
    assert rows[2]["name"] == "third" and rows[2]["value"] is None
    assert rows[3]["name"] is None and rows[3]["value"] is None

    psycopg_parameters_session.execute("DROP TABLE IF EXISTS test_none_many")


def test_psycopg_none_in_where_clause(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test None values in WHERE clauses work correctly."""
    psycopg_parameters_session.execute("""
        CREATE TABLE IF NOT EXISTS test_none_where (
            id INTEGER PRIMARY KEY,
            name TEXT,
            category TEXT
        )
    """)

    # Insert test data
    test_data = [(1, "item1", "A"), (2, "item2", None), (3, "item3", "B"), (4, "item4", None)]
    psycopg_parameters_session.execute_many(
        "INSERT INTO test_none_where (id, name, category) VALUES (%s, %s, %s)", test_data
    )

    # Test WHERE with None parameter using IS NULL comparison
    result = psycopg_parameters_session.execute("SELECT * FROM test_none_where WHERE category IS NULL")

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 2  # Two rows with NULL category

    # Verify the correct rows were found
    found_ids = {row["id"] for row in result.data}
    assert found_ids == {2, 4}

    # Test direct comparison with None parameter using explicit NULL casting
    # Note: psycopg requires explicit casting for None values in some contexts
    none_result = psycopg_parameters_session.execute("SELECT * FROM test_none_where WHERE category IS NULL OR 1 = 1")

    # The condition should return all rows since 1=1 is always true
    assert isinstance(none_result, SQLResult)
    assert none_result.data is not None
    assert len(none_result.data) == 4  # All rows because condition is always true

    # Test None parameter in comparison with explicit type casting
    typed_none_result = psycopg_parameters_session.execute(
        "SELECT * FROM test_none_where WHERE category = %s::TEXT", (None,)
    )

    # This should return no rows because NULL = NULL evaluates to NULL (not true)
    assert isinstance(typed_none_result, SQLResult)
    assert typed_none_result.data is not None
    assert len(typed_none_result.data) == 0  # No rows because NULL comparison returns NULL

    psycopg_parameters_session.execute("DROP TABLE IF EXISTS test_none_where")


def test_psycopg_parameter_count_mismatch_with_none(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test that parameter count mismatches are properly detected even when None values are involved.

    This test verifies the bug mentioned in the original issue where parameter
    count mismatches might be missed when None values are present.
    """
    psycopg_parameters_session.execute("CREATE TABLE IF NOT EXISTS test_param_count (col1 TEXT, col2 INTEGER)")

    # Test: Too many parameters - should raise an error
    with pytest.raises(Exception) as exc_info:
        psycopg_parameters_session.execute(
            "INSERT INTO test_param_count (col1, col2) VALUES (%s, %s)",  # 2 placeholders
            ("value1", None, "extra_param"),  # 3 parameters
        )

    # Should be a parameter count error
    error_msg = str(exc_info.value).lower()
    assert "mismatch" in error_msg or "parameter" in error_msg

    # Test: Too few parameters - should raise an error
    with pytest.raises(Exception) as exc_info:
        psycopg_parameters_session.execute(
            "INSERT INTO test_param_count (col1, col2) VALUES (%s, %s)",  # 2 placeholders
            ("value1",),  # Only 1 parameter
        )

    # Should be a parameter count error
    error_msg = str(exc_info.value).lower()
    assert "mismatch" in error_msg or "parameter" in error_msg

    # Test: Correct count with None should work fine
    result = psycopg_parameters_session.execute(
        "INSERT INTO test_param_count (col1, col2) VALUES (%s, %s)", ("value1", None)
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    psycopg_parameters_session.execute("DROP TABLE IF EXISTS test_param_count")


def test_psycopg_none_complex_parameter_scenarios(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test complex scenarios with None parameters that might cause issues."""
    from datetime import date

    psycopg_parameters_session.execute("""
        CREATE TABLE IF NOT EXISTS test_complex_none (
            id INTEGER,
            col1 TEXT,
            col2 INTEGER,
            col3 REAL,
            col4 BOOLEAN,
            col5 DATE,
            col6 TEXT[]
        )
    """)

    # Test 1: Mix of None and complex values
    complex_params = {
        "id": 1,
        "col1": "complex_test",
        "col2": None,
        "col3": math.pi,
        "col4": None,
        "col5": date(2025, 1, 21),
        "col6": ["array", "with", "values"],
    }

    result = psycopg_parameters_session.execute(
        """
        INSERT INTO test_complex_none (id, col1, col2, col3, col4, col5, col6)
        VALUES (%(id)s, %(col1)s, %(col2)s, %(col3)s, %(col4)s, %(col5)s, %(col6)s)
    """,
        statement_config=None,
        **complex_params,
    )

    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Test 2: Correct parameter count with None values
    params_for_count_test = (2, "test2", None, None, None)  # 5 parameters for 5 placeholders

    # Should NOT raise a parameter count error
    psycopg_parameters_session.execute(
        "INSERT INTO test_complex_none (id, col1, col2, col3, col4) VALUES (%s, %s, %s, %s, %s)", params_for_count_test
    )

    # Test 3: Verify complex insert worked correctly
    verify_result = psycopg_parameters_session.select_one("SELECT * FROM test_complex_none WHERE id = %s", (1,))

    assert verify_result is not None
    assert verify_result["col1"] == "complex_test"
    assert verify_result["col2"] is None
    assert abs(verify_result["col3"] - math.pi) < 0.00001
    assert verify_result["col4"] is None
    assert verify_result["col5"] is not None
    assert verify_result["col6"] == ["array", "with", "values"]

    psycopg_parameters_session.execute("DROP TABLE IF EXISTS test_complex_none")


def test_psycopg_none_parameter_edge_cases(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test edge cases that might reveal parameter handling bugs."""
    # Test 1: Empty parameter list with None
    psycopg_parameters_session.execute("CREATE TABLE IF NOT EXISTS test_edge (id INTEGER)")

    # Test 2: Single None parameter
    psycopg_parameters_session.execute("CREATE TABLE IF NOT EXISTS test_single_none (id INTEGER, value TEXT)")
    psycopg_parameters_session.execute("INSERT INTO test_single_none VALUES (1, %s)", (None,))

    result = psycopg_parameters_session.select_one("SELECT * FROM test_single_none WHERE id = 1")
    assert result is not None
    assert result["value"] is None

    # Test 3: Multiple consecutive None parameters
    psycopg_parameters_session.execute(
        "CREATE TABLE IF NOT EXISTS test_consecutive_none (a INTEGER, b TEXT, c TEXT, d TEXT)"
    )
    psycopg_parameters_session.execute(
        "INSERT INTO test_consecutive_none VALUES (%s, %s, %s, %s)", (1, None, None, None)
    )

    result = psycopg_parameters_session.select_one("SELECT * FROM test_consecutive_none WHERE a = 1")
    assert result is not None
    assert result["b"] is None
    assert result["c"] is None
    assert result["d"] is None

    # Test 4: None at beginning, middle, and end positions
    psycopg_parameters_session.execute("CREATE TABLE IF NOT EXISTS test_position_none (a TEXT, b TEXT, c TEXT)")
    test_cases = [
        (None, "middle", "end"),  # None at start
        ("start", None, "end"),  # None at middle
        ("start", "middle", None),  # None at end
        (None, None, "end"),  # Multiple None at start
        ("start", None, None),  # Multiple None at end
    ]

    for i, params in enumerate(test_cases):
        psycopg_parameters_session.execute("INSERT INTO test_position_none VALUES (%s, %s, %s)", params)

    # Verify all rows were inserted
    all_results = psycopg_parameters_session.execute("SELECT COUNT(*) as count FROM test_position_none")
    assert all_results.data[0]["count"] == 5

    psycopg_parameters_session.execute("DROP TABLE IF EXISTS test_edge")
    psycopg_parameters_session.execute("DROP TABLE IF EXISTS test_single_none")
    psycopg_parameters_session.execute("DROP TABLE IF EXISTS test_consecutive_none")
    psycopg_parameters_session.execute("DROP TABLE IF EXISTS test_position_none")


def test_psycopg_jsonb_none_parameters(psycopg_parameters_session: PsycopgSyncDriver) -> None:
    """Test JSONB column None parameter handling comprehensively."""
    import json

    psycopg_parameters_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_jsonb_none (
            id SERIAL PRIMARY KEY,
            name TEXT,
            metadata JSONB,
            config JSONB,
            tags JSONB
        );
        delete from test_jsonb_none;
        commit;
    """)

    # Test 1: Insert None values into JSONB columns using positional parameters
    result1 = psycopg_parameters_session.execute(
        "INSERT INTO test_jsonb_none (name, metadata, config, tags) VALUES (%s, %s, %s, %s) RETURNING id, name, metadata, config, tags",
        ("test_none_jsonb", None, None, None),
    )

    assert isinstance(result1, SQLResult)
    assert result1.data is not None
    assert len(result1.data) == 1
    assert result1.data[0]["name"] == "test_none_jsonb"
    assert result1.data[0]["metadata"] is None
    assert result1.data[0]["config"] is None
    assert result1.data[0]["tags"] is None

    # Test 2: Insert mixed JSON data and None values using positional parameters
    json_data = {"user_id": 123, "preferences": {"theme": "dark", "notifications": True}}
    complex_json = {"items": [{"id": 1, "name": "item1"}, {"id": 2, "name": "item2"}], "total": 2}

    result2 = psycopg_parameters_session.execute(
        "INSERT INTO test_jsonb_none (name, metadata, config, tags) VALUES (%s, %s, %s, %s) RETURNING id, metadata, config, tags",
        ("test_mixed_jsonb", json.dumps(json_data), None, json.dumps(complex_json)),
    )

    assert isinstance(result2, SQLResult)
    assert result2.data is not None
    assert len(result2.data) == 1
    # Psycopg automatically parses JSONB, so we get back dict objects
    assert isinstance(result2.data[0]["metadata"], dict)
    assert result2.data[0]["metadata"]["user_id"] == 123
    assert result2.data[0]["config"] is None
    assert isinstance(result2.data[0]["tags"], dict)
    assert result2.data[0]["tags"]["total"] == 2

    # Test 3: Query JSONB columns with None values
    result3 = psycopg_parameters_session.execute("SELECT * FROM test_jsonb_none WHERE metadata IS NULL")

    assert isinstance(result3, SQLResult)
    assert result3.data is not None
    assert len(result3.data) == 1
    assert result3.data[0]["name"] == "test_none_jsonb"

    # Test 4: Query JSONB columns filtering by JSON content
    result4 = psycopg_parameters_session.execute(
        "SELECT * FROM test_jsonb_none WHERE metadata->>'user_id' = %s", ("123",)
    )

    assert isinstance(result4, SQLResult)
    assert result4.data is not None
    assert len(result4.data) == 1
    assert result4.data[0]["name"] == "test_mixed_jsonb"

    # Test 5: Insert using named parameters with JSONB and None
    params = {
        "name": "named_jsonb_test",
        "metadata": json.dumps({"type": "test", "version": "1.0"}),
        "config": None,
        "tags": json.dumps(["tag1", "tag2", "tag3"]),
    }

    result5 = psycopg_parameters_session.execute(
        """INSERT INTO test_jsonb_none (name, metadata, config, tags)
           VALUES (%(name)s, %(metadata)s, %(config)s, %(tags)s)
           RETURNING name, metadata, config, tags""",
        params,
    )

    assert isinstance(result5, SQLResult)
    assert result5.data is not None
    assert len(result5.data) == 1
    assert result5.data[0]["name"] == "named_jsonb_test"
    assert result5.data[0]["metadata"]["type"] == "test"
    assert result5.data[0]["config"] is None
    assert result5.data[0]["tags"] == ["tag1", "tag2", "tag3"]

    # Test 6: Update JSONB columns with None values using positional parameters
    psycopg_parameters_session.execute(
        "UPDATE test_jsonb_none SET metadata = %s, config = %s WHERE name = %s",
        (None, json.dumps({"updated": True}), "named_jsonb_test"),
    )

    result6 = psycopg_parameters_session.execute(
        "SELECT metadata, config FROM test_jsonb_none WHERE name = %s", ("named_jsonb_test",)
    )

    assert isinstance(result6, SQLResult)
    assert result6.data is not None
    assert len(result6.data) == 1
    assert result6.data[0]["metadata"] is None
    assert result6.data[0]["config"]["updated"] is True

    # Test 7: Update JSONB columns with None values using named parameters
    psycopg_parameters_session.execute(
        "UPDATE test_jsonb_none SET tags = %(new_tags)s WHERE name = %(target_name)s",
        {"new_tags": None, "target_name": "test_mixed_jsonb"},
    )

    result7 = psycopg_parameters_session.execute(
        "SELECT name, tags FROM test_jsonb_none WHERE name = %(name)s", {"name": "test_mixed_jsonb"}
    )

    assert isinstance(result7, SQLResult)
    assert result7.data is not None
    assert len(result7.data) == 1
    assert result7.data[0]["tags"] is None

    # Test 8: Test JSONB operations with None parameters
    result8 = psycopg_parameters_session.execute(
        "SELECT name FROM test_jsonb_none WHERE metadata IS NULL AND config IS NOT NULL"
    )

    assert isinstance(result8, SQLResult)
    assert result8.data is not None
    assert len(result8.data) == 1
    assert result8.data[0]["name"] == "named_jsonb_test"

    # Test 9: Test COALESCE with JSONB and None values
    result9 = psycopg_parameters_session.execute(
        "SELECT name, COALESCE(metadata, %s::jsonb) as metadata_or_default FROM test_jsonb_none WHERE name = %s",
        (json.dumps({"default": "value"}), "test_none_jsonb"),
    )

    assert isinstance(result9, SQLResult)
    assert result9.data is not None
    assert len(result9.data) == 1
    assert result9.data[0]["metadata_or_default"]["default"] == "value"

    # Test 10: execute_many with JSONB None values
    batch_data = [
        ("batch1", json.dumps({"batch": 1}), None, json.dumps(["batch"])),
        ("batch2", None, json.dumps({"config": "batch2"}), None),
        ("batch3", None, None, None),
    ]

    result10 = psycopg_parameters_session.execute_many(
        "INSERT INTO test_jsonb_none (name, metadata, config, tags) VALUES (%s, %s, %s, %s)", batch_data
    )

    assert isinstance(result10, SQLResult)
    assert result10.rows_affected == 3

    # Verify batch insert
    result11 = psycopg_parameters_session.execute(
        "SELECT name, metadata, config, tags FROM test_jsonb_none WHERE name LIKE %s ORDER BY name", ("batch%",)
    )

    assert isinstance(result11, SQLResult)
    assert result11.data is not None
    assert len(result11.data) == 3

    # Verify batch1
    assert result11.data[0]["name"] == "batch1"
    assert result11.data[0]["metadata"]["batch"] == 1
    assert result11.data[0]["config"] is None
    assert result11.data[0]["tags"] == ["batch"]

    # Verify batch2
    assert result11.data[1]["name"] == "batch2"
    assert result11.data[1]["metadata"] is None
    assert result11.data[1]["config"]["config"] == "batch2"
    assert result11.data[1]["tags"] is None

    # Verify batch3
    assert result11.data[2]["name"] == "batch3"
    assert result11.data[2]["metadata"] is None
    assert result11.data[2]["config"] is None
    assert result11.data[2]["tags"] is None

    # Test 11: Test JSONB array operations with None values
    psycopg_parameters_session.execute(
        "INSERT INTO test_jsonb_none (name, metadata, config, tags) VALUES (%s, %s, %s, %s)",
        ("array_test", json.dumps([1, 2, 3]), None, json.dumps({"array": [None, "value", None]})),
    )

    result12 = psycopg_parameters_session.execute("SELECT tags FROM test_jsonb_none WHERE name = %s", ("array_test",))

    assert isinstance(result12, SQLResult)
    assert result12.data is not None
    assert len(result12.data) == 1
    assert result12.data[0]["tags"]["array"] == [None, "value", None]

    # Test 12: Test JSONB path operations with None parameters
    result13 = psycopg_parameters_session.execute(
        "SELECT name FROM test_jsonb_none WHERE metadata #> %s IS NULL", ("{nonexistent}",)
    )

    assert isinstance(result13, SQLResult)
    assert result13.data is not None
    # Should include all records since nonexistent path returns NULL for all
    assert len(result13.data) >= 2

    psycopg_parameters_session.execute("DROP TABLE IF EXISTS test_jsonb_none")
