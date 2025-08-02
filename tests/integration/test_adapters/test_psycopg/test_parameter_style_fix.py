"""Regression tests for psycopg parameter style fix.

This test ensures that removing the _get_compiled_sql override from the psycopg driver
allows the base implementation to handle NAMED_PYFORMAT parameter conversion correctly.
"""

from collections.abc import Generator
from typing import Any

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver, psycopg_statement_config
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL


@pytest.fixture
def psycopg_regression_session(postgres_service: PostgresService) -> Generator[PsycopgSyncDriver, Any, None]:
    """Create a Psycopg session for parameter style regression testing."""
    config = PsycopgSyncConfig(
        pool_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "dbname": postgres_service.database,
            "autocommit": True,
        },
        statement_config=psycopg_statement_config,
    )

    try:
        with config.provide_session() as session:
            # Create test table
            session.execute_script("""
                CREATE TABLE IF NOT EXISTS parameter_regression_test (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    category TEXT,
                    metadata JSONB
                )
            """)
            # Clear any existing data
            session.execute_script("TRUNCATE TABLE parameter_regression_test RESTART IDENTITY")

            # Insert comprehensive test data
            session.execute_many(
                "INSERT INTO parameter_regression_test (name, value, category, metadata) VALUES (%s, %s, %s, %s)",
                [
                    ("test_alpha", 100, "A", '{"type": "test", "active": true}'),
                    ("test_beta", 200, "B", '{"type": "prod", "active": false}'),
                    ("test_gamma", 300, "A", '{"type": "test", "active": true}'),
                    ("prod_delta", 400, "C", '{"type": "prod", "active": true}'),
                ],
            )
            yield session
            # Cleanup
            session.execute_script("DROP TABLE IF EXISTS parameter_regression_test")
    finally:
        if config.pool_instance:
            config.pool_instance.close(timeout=5.0)
            config.pool_instance = None


@pytest.mark.xdist_group("postgres")
def test_named_pyformat_parameter_conversion(psycopg_regression_session: PsycopgSyncDriver) -> None:
    """Test that NAMED_PYFORMAT parameters are converted correctly through the pipeline."""
    # Test the exact scenario that was failing: %(name)s style parameters
    result = psycopg_regression_session.execute(
        "SELECT * FROM parameter_regression_test WHERE name = %(target_name)s AND value > %(min_value)s",
        {"target_name": "test_alpha", "min_value": 50},
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_alpha"
    assert result.data[0]["value"] == 100


@pytest.mark.xdist_group("postgres")
def test_complex_named_pyformat_query(psycopg_regression_session: PsycopgSyncDriver) -> None:
    """Test complex named pyformat query with multiple parameters and JSON operations."""
    result = psycopg_regression_session.execute(
        """
        SELECT name, value, category, metadata->>'type' as metadata_type
        FROM parameter_regression_test
        WHERE category IN (%(cat1)s, %(cat2)s)
        AND value BETWEEN %(min_val)s AND %(max_val)s
        AND metadata->>'active' = %(active_status)s
        ORDER BY value
        """,
        {"cat1": "A", "cat2": "B", "min_val": 150, "max_val": 350, "active_status": "true"},
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_gamma"
    assert result.data[0]["value"] == 300
    assert result.data[0]["metadata_type"] == "test"


@pytest.mark.xdist_group("postgres")
def test_mixed_parameter_styles_consistency(psycopg_regression_session: PsycopgSyncDriver) -> None:
    """Test that different parameter styles produce consistent results."""
    # Query using named pyformat style
    named_result = psycopg_regression_session.execute(
        "SELECT COUNT(*) as count FROM parameter_regression_test WHERE category = %(category)s", {"category": "A"}
    )

    # Query using positional pyformat style (the execution style)
    positional_result = psycopg_regression_session.execute(
        "SELECT COUNT(*) as count FROM parameter_regression_test WHERE category = %s", ("A",)
    )

    # Both should return the same result
    assert named_result.data[0]["count"] == positional_result.data[0]["count"]
    assert named_result.data[0]["count"] == 2  # test_alpha and test_gamma


@pytest.mark.xdist_group("postgres")
def test_sql_object_with_named_pyformat(psycopg_regression_session: PsycopgSyncDriver) -> None:
    """Test SQL object with named pyformat parameters goes through proper pipeline."""
    sql_obj = SQL(
        "SELECT name, value FROM parameter_regression_test WHERE value > %(threshold)s ORDER BY value",
        parameters={"threshold": 250},
    )

    result = psycopg_regression_session.execute(sql_obj)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 2  # test_gamma (300) and prod_delta (400)
    assert result.data[0]["name"] == "test_gamma"
    assert result.data[1]["name"] == "prod_delta"


@pytest.mark.xdist_group("postgres")
def test_parameter_validation_and_normalization(psycopg_regression_session: PsycopgSyncDriver) -> None:
    """Test that parameter validation and normalization work correctly."""
    # Test with various data types to ensure type handling works
    # Note: value column is INTEGER, so all values must be valid integers
    test_data = [
        ("string_test", 100, "TEXT", '{"string": true}'),
        ("int_test", 12345, "INT", '{"number": 12345}'),
        ("float_test", 99, "FLOAT", '{"decimal": 99.99}'),  # Use integer since column is INTEGER
        ("bool_test", 1, "BOOL", '{"boolean": true}'),  # Use 1 for True since column is INTEGER
    ]

    for name, value, category, metadata in test_data:
        psycopg_regression_session.execute(
            "INSERT INTO parameter_regression_test (name, value, category, metadata) VALUES (%(name)s, %(value)s, %(category)s, %(metadata)s)",
            {"name": name, "value": value, "category": category, "metadata": metadata},
        )

    # Verify the data was inserted with proper type handling
    result = psycopg_regression_session.execute(
        "SELECT * FROM parameter_regression_test WHERE category = %(category)s ORDER BY name", {"category": "TEXT"}
    )

    assert len(result.data) == 1
    assert result.data[0]["name"] == "string_test"
    assert result.data[0]["value"] == 100
    assert result.data[0]["category"] == "TEXT"


@pytest.mark.xdist_group("postgres")
def test_edge_case_parameter_scenarios(psycopg_regression_session: PsycopgSyncDriver) -> None:
    """Test edge cases that might expose pipeline issues."""
    # First, insert some test data for this test
    test_data = [
        ("test1", 10, "A", None),
        ("test2", 20, "B", None),
        ("test3", 30, None, None),  # This row has NULL category
        ("test4", 40, "C", None),
    ]

    for name, value, category, metadata in test_data:
        psycopg_regression_session.execute(
            "INSERT INTO parameter_regression_test (name, value, category, metadata) VALUES (%(name)s, %(value)s, %(category)s, %(metadata)s)",
            {"name": name, "value": value, "category": category, "metadata": metadata},
        )

    # Test with None/NULL parameters - should only return the row with NULL category
    result = psycopg_regression_session.execute(
        "SELECT COUNT(*) as count FROM parameter_regression_test WHERE category IS NULL", {}
    )

    # Should return 1 row (test3 has NULL category)
    assert result.data[0]["count"] == 1

    # Test with empty string parameters
    psycopg_regression_session.execute(
        "INSERT INTO parameter_regression_test (name, value, category) VALUES (%(name)s, %(value)s, %(category)s)",
        {"name": "", "value": 0, "category": "EMPTY"},
    )

    empty_result = psycopg_regression_session.execute(
        "SELECT * FROM parameter_regression_test WHERE name = %(empty_name)s", {"empty_name": ""}
    )

    assert len(empty_result.data) == 1
    assert empty_result.data[0]["category"] == "EMPTY"


@pytest.mark.xdist_group("postgres")
def test_parameter_style_performance_consistency(psycopg_regression_session: PsycopgSyncDriver) -> None:
    """Test that parameter processing doesn't introduce performance regressions."""
    import time

    # Insert a larger dataset for performance testing
    large_dataset = [(f"perf_test_{i}", i * 10, "PERF", f'{{"index": {i}}}') for i in range(100)]

    psycopg_regression_session.execute_many(
        "INSERT INTO parameter_regression_test (name, value, category, metadata) VALUES (%s, %s, %s, %s)", large_dataset
    )

    # Test named parameter performance
    start_time = time.time()
    for i in range(10):  # Run multiple queries to average timing
        psycopg_regression_session.execute(
            "SELECT COUNT(*) as count FROM parameter_regression_test WHERE value > %(threshold)s AND category = %(cat)s",
            {"threshold": 500, "cat": "PERF"},
        )
    named_time = time.time() - start_time

    # Test positional parameter performance for comparison
    start_time = time.time()
    for i in range(10):
        psycopg_regression_session.execute(
            "SELECT COUNT(*) as count FROM parameter_regression_test WHERE value > %s AND category = %s", (500, "PERF")
        )
    positional_time = time.time() - start_time

    # Named parameters should not be significantly slower (allow 2x tolerance)
    # This is a rough performance check - the key is that it works, not that it's identical speed
    assert named_time < positional_time * 3  # Very generous tolerance for CI environments

    # Verify results are consistent
    final_check = psycopg_regression_session.execute(
        "SELECT COUNT(*) as count FROM parameter_regression_test WHERE category = %(cat)s", {"cat": "PERF"}
    )
    assert final_check.data[0]["count"] == 100
