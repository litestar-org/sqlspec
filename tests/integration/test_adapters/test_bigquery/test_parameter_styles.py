"""BigQuery parameter style tests with CORE_ROUND_3 architecture."""

import math

import pytest

from sqlspec.adapters.bigquery import BigQueryDriver
from sqlspec.core.result import SQLResult


@pytest.mark.xdist_group("bigquery")
def test_bigquery_named_at_parameters(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test BigQuery NAMED_AT parameter style (@param)."""
    table_name = bigquery_test_table

    # Test single named parameter
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @value)",
        {"id": 1, "name": "test_param", "value": 100},
    )

    # Verify insertion
    result = bigquery_session.execute(f"SELECT name, value FROM {table_name} WHERE id = @id", {"id": 1})
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_param"
    assert result.data[0]["value"] == 100

    # Test multiple parameters
    result = bigquery_session.execute(
        f"SELECT * FROM {table_name} WHERE name = @name AND value > @min_value", {"name": "test_param", "min_value": 50}
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1

    # Test parameter reuse
    result = bigquery_session.execute(
        f"SELECT * FROM {table_name} WHERE value >= @threshold AND value <= @threshold + 50", {"threshold": 50}
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1


@pytest.mark.xdist_group("bigquery")
@pytest.mark.xfail(reason="BigQuery emulator expects all parameter values as strings, not numbers")
def test_bigquery_parameter_type_conversion(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test BigQuery parameter type handling and conversion."""
    table_name = bigquery_test_table

    # Test various parameter types
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@int_param, @str_param, @float_param)",
        {"int_param": 42, "str_param": "type_test", "float_param": math.pi},
    )

    # Verify type preservation
    result = bigquery_session.execute(f"SELECT * FROM {table_name} WHERE id = @search_id", {"search_id": 42})
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["id"] == 42
    assert result.data[0]["name"] == "type_test"
    # Note: BigQuery may convert float to int if it's a whole number


@pytest.mark.xdist_group("bigquery")
@pytest.mark.xfail(reason="BigQuery emulator has issues with NULL parameter handling")
def test_bigquery_null_parameter_handling(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test BigQuery NULL parameter handling."""
    table_name = bigquery_test_table

    # Insert with NULL value
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @null_value)",
        {"id": 100, "name": "null_test", "null_value": None},
    )

    # Test NULL comparison
    result = bigquery_session.execute(
        f"SELECT * FROM {table_name} WHERE name = @name AND value IS NULL", {"name": "null_test"}
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["value"] is None


@pytest.mark.xdist_group("bigquery")
def test_bigquery_parameter_escaping(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test BigQuery parameter escaping and SQL injection prevention."""
    table_name = bigquery_test_table

    # Test special characters in parameters
    special_name = "test'; DROP TABLE users; --"
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @name, @value)",
        {"id": 200, "name": special_name, "value": 42},
    )

    # Verify data was inserted safely
    result = bigquery_session.execute(
        f"SELECT * FROM {table_name} WHERE name = @search_name", {"search_name": special_name}
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == special_name


@pytest.mark.xdist_group("bigquery")
def test_bigquery_complex_parameter_queries(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test complex queries with BigQuery parameters."""
    table_name = bigquery_test_table

    # Insert test data
    test_data = [(1, "Alice", 1000), (2, "Bob", 1500), (3, "Charlie", 2000), (4, "Diana", 800)]
    bigquery_session.execute_many(f"INSERT INTO {table_name} (id, name, value) VALUES (?, ?, ?)", test_data)

    # Test parameters in complex WHERE clauses
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

    # Test parameters in aggregation queries
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
    assert agg_result.data[0]["count"] == 3  # Alice, Bob, Charlie
    assert agg_result.data[0]["avg_value"] == (1000 + 1500 + 2000) / 3


@pytest.mark.xdist_group("bigquery")
def test_bigquery_parameter_edge_cases(bigquery_session: BigQueryDriver, bigquery_test_table: str) -> None:
    """Test BigQuery parameter edge cases and boundary conditions."""
    table_name = bigquery_test_table

    # Test empty string parameter
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @empty_name, @value)",
        {"id": 300, "empty_name": "", "value": 1},
    )

    # Test very long string parameter
    long_string = "x" * 1000
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@id, @long_name, @value)",
        {"id": 301, "long_name": long_string, "value": 2},
    )

    # Verify both insertions worked
    result = bigquery_session.execute(
        f"SELECT COUNT(*) as count FROM {table_name} WHERE id IN (@id1, @id2)", {"id1": 300, "id2": 301}
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 2

    # Test large numeric parameter
    large_number = 9223372036854775807  # Max INT64
    bigquery_session.execute(
        f"INSERT INTO {table_name} (id, name, value) VALUES (@small_id, @name, @large_value)",
        {"small_id": 302, "name": "large_num_test", "large_value": large_number},
    )

    # Verify large number handling
    result = bigquery_session.execute(f"SELECT value FROM {table_name} WHERE id = @id", {"id": 302})
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["value"] == large_number
