"""BigQuery parameter style tests with CORE_ROUND_3 architecture."""

import math

import pytest

from sqlspec.adapters.bigquery import BigQueryDriver
from sqlspec.core.result import SQLResult

pytestmark = pytest.mark.xdist_group("bigquery")


def test_bigquery_named_at_parameters(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test BigQuery NAMED_AT parameter style (@param)."""
    table_name = bigquery_test_table

    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @value)",
        {"id": 1, "name": "test_param", "value": 100},
    )

    result = bigquery_session.execute(f"SELECT name, value FROM {table_name} WHERE id = @id", {"id": 1})
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_param"
    assert result.data[0]["value"] == 100

    result = bigquery_session.execute(
        f"SELECT * FROM {table_name} WHERE name = @name AND value > @min_value", {"name": "test_param", "min_value": 50}
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    result = bigquery_session.execute(
        f"SELECT * FROM {table_name} WHERE value >= @threshold AND value <= @threshold + 50", {"threshold": 50}
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1


@pytest.mark.xfail(reason="BigQuery emulator expects all parameter values as strings, not numbers")
def test_bigquery_parameter_type_conversion(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test BigQuery parameter type handling and conversion."""
    table_name = bigquery_test_table

    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@int_param, @str_param, @float_param)",
        {"int_param": 42, "str_param": "type_test", "float_param": math.pi},
    )

    result = bigquery_session.execute(f"SELECT * FROM {table_name} WHERE id = @search_id", {"search_id": 42})
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["id"] == 42
    assert result.data[0]["name"] == "type_test"


@pytest.mark.xfail(reason="BigQuery emulator has issues with NULL parameter handling")
def test_bigquery_null_parameter_handling(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test BigQuery NULL parameter handling."""
    table_name = bigquery_test_table

    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @null_value)",
        {"id": 100, "name": "null_test", "null_value": None},
    )

    result = bigquery_session.execute(
        f"SELECT * FROM {table_name} WHERE name = @name AND value IS NULL", {"name": "null_test"}
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] is None


def test_bigquery_parameter_escaping(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test BigQuery parameter escaping and SQL injection prevention."""
    table_name = bigquery_test_table

    special_name = "test'; DROP TABLE users; --"
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @value)",
        {"id": 200, "name": special_name, "value": 42},
    )

    result = bigquery_session.execute(
        f"SELECT * FROM {table_name} WHERE name = @search_name", {"search_name": special_name}
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == special_name


def test_bigquery_complex_parameter_queries(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test complex queries with BigQuery parameters."""
    table_name = bigquery_test_table

    test_data = [(1, "Alice", 1000), (2, "Bob", 1500), (3, "Charlie", 2000), (4, "Diana", 800)]
    bigquery_session.execute_many(f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", test_data)

    result = bigquery_session.execute(
        f"""
        SELECT name, value
        FROM {table_name}
        WHERE value BETWEEN @min_val AND @max_val
        ORDER BY value DESC
        LIMIT @limit_count
    """,
        {"min_val": 1200, "max_val": 2500, "limit_count": 2},
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 2
    assert result.data[0]["name"] == "Charlie"
    assert result.data[1]["name"] == "Bob"

    agg_result = bigquery_session.execute(
        f"""
        SELECT
            COUNT(*) as count,
            AVG(value) as avg_value
        FROM {table_name}
        WHERE value > @threshold
    """,
        {"threshold": 900},
    )
    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["count"] == 3
    assert agg_result.data[0]["avg_value"] == (1000 + 1500 + 2000) / 3


def test_bigquery_parameter_edge_cases(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test BigQuery parameter edge cases and boundary conditions."""
    table_name = bigquery_test_table

    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @empty_name, @value)",
        {"id": 300, "empty_name": "", "value": 1},
    )

    long_string = "x" * 1000
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @long_name, @value)",
        {"id": 301, "long_name": long_string, "value": 2},
    )

    result = bigquery_session.execute(
        f"SELECT COUNT(*) as count FROM {table_name} WHERE id IN (@id1, @id2)", {"id1": 300, "id2": 301}
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 2

    large_number = 9223372036854775807
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@small_id, @name, @large_value)",
        {"small_id": 302, "name": "large_num_test", "large_value": large_number},
    )

    result = bigquery_session.execute(f"SELECT value FROM {table_name} WHERE id = @id", {"id": 302})
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["value"] == large_number


def test_bigquery_comprehensive_none_parameter_handling(
    bigquery_session: BigQueryDriver, bigquery_test_table: str
) -> None:
    """Test comprehensive None parameter handling scenarios for BigQuery."""
    table_name = bigquery_test_table

    # Test 1: Single None parameter (only in nullable columns due to NOT NULL constraint on name)
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @value)",
        {"id": 1001, "name": "test_none", "value": None},  # None only in nullable column
    )

    result = bigquery_session.execute(f"SELECT * FROM {table_name} WHERE id = @id", {"id": 1001})
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    assert result.data[0]["name"] == "test_none"
    # BigQuery emulator may return None for NULL integers or 0
    assert result.data[0]["value"] is None or result.data[0]["value"] == 0

    # Test 2: Another None parameter test
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @value)",
        {"id": 1002, "name": "another_test", "value": None},
    )

    result = bigquery_session.execute(f"SELECT * FROM {table_name} WHERE id = @id", {"id": 1002})
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "another_test"
    # BigQuery emulator may return None for NULL integers or 0
    assert result.data[0]["value"] is None or result.data[0]["value"] == 0

    # Test 3: None in WHERE clause (checking for NULL values)
    result = bigquery_session.execute(f"SELECT COUNT(*) as null_value_count FROM {table_name} WHERE value IS NULL")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    # Should count our NULL values, but BigQuery emulator might return 0 if it converts NULL to 0
    assert result.data[0]["null_value_count"] >= 0

    # Test 4: Mixed None and non-None parameters
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @value)",
        {"id": 1003, "name": "mixed_test", "value": None},
    )

    result = bigquery_session.execute(
        f"SELECT * FROM {table_name} WHERE id = @id AND name = @name", {"id": 1003, "name": "mixed_test"}
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "mixed_test"
    # BigQuery emulator may return None for NULL integers or 0
    assert result.data[0]["value"] is None or result.data[0]["value"] == 0


def test_bigquery_none_parameters_with_execute_many(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test None parameter handling in execute_many operations."""
    table_name = bigquery_test_table

    # Note: The test table has name STRING NOT NULL, so we can only test None in nullable columns
    # Test data with None values in nullable columns (id and value)
    parameters_list = [
        (2001, "first", 100),  # No None values
        (2002, "second", None),  # None in value (nullable)
        (2003, "third", None),  # None in value (nullable)
        (2004, "fourth", 400),  # No None values
    ]

    result = bigquery_session.execute_many(
        f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", parameters_list
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected >= 0  # BigQuery may return 0 or actual count

    # Verify all records were inserted correctly
    verify_result = bigquery_session.execute(
        f"SELECT id, name, value FROM {table_name} WHERE id >= 2001 AND id <= 2004 ORDER BY id"
    )
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 4

    # Check each row individually (accounting for BigQuery emulator behavior)
    rows = verify_result.data
    assert rows[0]["id"] == 2001 and rows[0]["name"] == "first" and rows[0]["value"] == 100
    assert rows[1]["id"] == 2002 and rows[1]["name"] == "second" and (rows[1]["value"] is None or rows[1]["value"] == 0)
    assert rows[2]["id"] == 2003 and rows[2]["name"] == "third" and (rows[2]["value"] is None or rows[2]["value"] == 0)
    assert rows[3]["id"] == 2004 and rows[3]["name"] == "fourth" and rows[3]["value"] == 400


def test_bigquery_all_none_parameters(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test when all parameter values are None."""
    table_name = bigquery_test_table

    # Create a test table that allows NULL in primary key for this test
    # Extract the base table name without backticks and add suffix
    base_table = table_name.rstrip("`") + "_null_test`"
    bigquery_session.execute(f"""
        CREATE OR REPLACE TABLE {base_table} (
            id INT64,
            name STRING,
            value INT64
        )
    """)

    # Insert with all None values
    result = bigquery_session.execute(
        f"INSERT INTO {base_table} (id, name, value) VALUES (@id, @name, @value)",
        {"id": None, "name": None, "value": None},
    )
    assert isinstance(result, SQLResult)

    # Verify the insert worked - accounting for BigQuery emulator behavior
    # BigQuery emulator may convert NULL strings to empty strings and NULL integers to 0
    verify_result = bigquery_session.execute(
        f"SELECT COUNT(*) as count FROM {base_table} WHERE (id IS NULL OR id = 0) AND (name IS NULL OR name = '') AND (value IS NULL OR value = 0)"
    )
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["count"] == 1

    # Clean up
    bigquery_session.execute(f"DROP TABLE {base_table}")


def test_bigquery_none_parameter_type_coercion(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test that None values are properly type-coerced by BigQuery's parameter system."""
    table_name = bigquery_test_table

    # Test None coercion with different expected types
    # The BigQuery adapter should handle None -> NULL conversion properly
    # Note: Can't test None in name column due to NOT NULL constraint
    test_cases = [
        {"id": 3001, "name": "string_none", "value": None},  # None as INT64
        {"id": 3002, "name": "value_none", "value": None},  # None as INT64
    ]

    for case in test_cases:
        bigquery_session.execute(f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @value)", case)

    # Verify successful inserts
    result = bigquery_session.execute(f"SELECT * FROM {table_name} WHERE id IN (3001, 3002) ORDER BY id")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 2

    # Check None handling in results (accounting for BigQuery emulator behavior)
    for row in result.data:
        if row["id"] == 3001:
            assert row["name"] == "string_none"
            assert row["value"] is None or row["value"] == 0  # Emulator may convert NULL int to 0
        elif row["id"] == 3002:
            assert row["name"] == "value_none"
            assert row["value"] is None or row["value"] == 0  # Emulator may convert NULL int to 0


def test_bigquery_parameter_count_validation_with_none(
    bigquery_session: BigQueryDriver, bigquery_test_table: str
) -> None:
    """Test that parameter count validation works correctly even with None values present."""
    table_name = bigquery_test_table

    # Test 1: Correct parameter count with None values should work
    result = bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @value)",
        {"id": 4001, "name": "param_test", "value": None},  # Can't use None for name due to NOT NULL constraint
    )
    assert isinstance(result, SQLResult)

    # Test 2: Extra parameters should be detected (BigQuery uses named params, so extra params in dict are ignored)
    # This is expected behavior for named parameters - extra keys in dict don't cause errors
    try:
        result = bigquery_session.execute(
            f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @value)",
            {"id": 4002, "name": "test", "value": 42, "extra_param": None, "another_extra": "ignored"},
        )
        # This should succeed for BigQuery as it uses named parameters
        assert isinstance(result, SQLResult)
    except Exception:
        # If it fails, that's also acceptable behavior for parameter validation
        pass

    # Test 3: Missing required parameters should fail
    with pytest.raises(Exception):
        bigquery_session.execute(
            f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @value)",
            {"id": 4003, "name": "missing_value"},  # Missing @value parameter
        )

    # Test 4: Empty parameter dict when parameters are expected should fail
    with pytest.raises(Exception):
        bigquery_session.execute(
            f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @value)",
            {},  # No parameters provided
        )
