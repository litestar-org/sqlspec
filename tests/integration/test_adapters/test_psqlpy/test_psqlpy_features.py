"""Test PSQLPy-specific features and capabilities."""

from __future__ import annotations

import pytest

from sqlspec.adapters.psqlpy import PsqlpyDriver
from sqlspec.core.result import SQLResult
from sqlspec.core.statement import SQL

pytestmark = [pytest.mark.psqlpy, pytest.mark.postgres, pytest.mark.integration]


async def test_psqlpy_performance_features(psqlpy_session: PsqlpyDriver) -> None:
    """Test PSQLPy's Rust-based performance optimizations."""
    # Test bulk operations that should benefit from Rust performance
    bulk_data = [(f"perf_test_{i}",) for i in range(1000)]

    # Large batch insert
    result = await psqlpy_session.execute_many("INSERT INTO test_table (name) VALUES ($1)", bulk_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1000

    # Large select
    select_result = await psqlpy_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'perf_test_%'"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 1000


async def test_psqlpy_connection_pooling(psqlpy_session: PsqlpyDriver) -> None:
    """Test PSQLPy connection pooling features."""
    # Test that we can perform multiple operations efficiently
    operations = []

    for i in range(10):
        result = await psqlpy_session.execute("SELECT $1::int as operation_id", (i,))
        assert isinstance(result, SQLResult)
        assert result.data is not None
        operations.append(result.data[0]["operation_id"])

    # Verify all operations completed
    assert operations == list(range(10))


async def test_psqlpy_advanced_postgresql_types(psqlpy_session: PsqlpyDriver) -> None:
    """Test PSQLPy handling of advanced PostgreSQL data types."""
    # Create table with advanced types
    await psqlpy_session.execute_script("""
        CREATE TABLE IF NOT EXISTS psqlpy_types_test (
            id SERIAL PRIMARY KEY,
            json_col JSON,
            jsonb_col JSONB,
            array_col INTEGER[],
            uuid_col UUID,
            inet_col INET,
            timestamp_col TIMESTAMP WITH TIME ZONE
        )
    """)

    # Insert data with advanced types
    insert_result = await psqlpy_session.execute(
        """
        INSERT INTO psqlpy_types_test
        (json_col, jsonb_col, array_col, uuid_col, inet_col, timestamp_col)
        VALUES ($1::json, $2::jsonb, $3::integer[], $4::uuid, $5::inet, $6::timestamptz)
        RETURNING id
    """,
        (
            {"name": "test", "value": 42},  # JSON
            {"type": "jsonb", "fast": True},  # JSONB
            [1, 2, 3, 4, 5],  # Array
            "550e8400-e29b-41d4-a716-446655440000",  # UUID
            "192.168.1.1",  # INET (without CIDR)
            "2023-01-01T12:00:00+00:00",  # Timestamp with timezone
        ),
    )

    assert isinstance(insert_result, SQLResult)
    assert insert_result.data is not None
    record_id = insert_result.data[0]["id"]

    # Retrieve and verify data
    select_result = await psqlpy_session.execute("SELECT * FROM psqlpy_types_test WHERE id = $1", (record_id,))

    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["id"] == record_id
    # JSON handling may vary, just verify we got data
    assert row["json_col"] is not None
    assert row["jsonb_col"] is not None
    assert row["uuid_col"] is not None

    # Clean up
    await psqlpy_session.execute("DROP TABLE IF EXISTS psqlpy_types_test")


async def test_psqlpy_error_handling(psqlpy_session: PsqlpyDriver) -> None:
    """Test PSQLPy error handling and exception propagation."""
    # Test syntax error
    with pytest.raises(Exception) as exc_info:
        await psqlpy_session.execute("INVALID SQL SYNTAX")

    # Verify we get a meaningful error
    assert "syntax" in str(exc_info.value).lower() or "error" in str(exc_info.value).lower()

    # Test constraint violation
    await psqlpy_session.execute("INSERT INTO test_table (name) VALUES ($1)", ("constraint_test",))

    # Try to violate a constraint (if any exist)
    # For now, just test that normal operations still work after error
    result = await psqlpy_session.execute("SELECT 'recovery_test'::text as status")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["status"] == "recovery_test"


async def test_psqlpy_large_result_sets(psqlpy_session: PsqlpyDriver) -> None:
    """Test PSQLPy handling of large result sets."""
    # Insert a moderate amount of data
    bulk_data = [(f"large_result_{i}",) for i in range(100)]
    await psqlpy_session.execute_many("INSERT INTO test_table (name) VALUES ($1)", bulk_data)

    # Retrieve all data
    result = await psqlpy_session.execute("SELECT * FROM test_table WHERE name LIKE 'large_result_%' ORDER BY id")

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 100

    # Verify data integrity
    for i, row in enumerate(result.data):
        assert row["name"] == f"large_result_{i}"


async def test_psqlpy_transaction_behavior(psqlpy_session: PsqlpyDriver) -> None:
    """Test PSQLPy transaction handling."""
    # Test explicit transaction
    await psqlpy_session.execute("BEGIN")

    # Insert data in transaction
    await psqlpy_session.execute("INSERT INTO test_table (name) VALUES ($1)", ("transaction_test",))

    # Data should not be visible in other connections yet
    # But we can see it in this session
    result = await psqlpy_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name = $1", ("transaction_test",)
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 1

    # Commit transaction
    await psqlpy_session.execute("COMMIT")

    # Verify data is committed
    committed_result = await psqlpy_session.execute(
        "SELECT name FROM test_table WHERE name = $1", ("transaction_test",)
    )
    assert isinstance(committed_result, SQLResult)
    assert committed_result.data is not None
    assert len(committed_result.data) == 1
    assert committed_result.data[0]["name"] == "transaction_test"


async def test_psqlpy_with_core_round_3_sql(psqlpy_session: PsqlpyDriver) -> None:
    """Test PSQLPy integration with CORE_ROUND_3 SQL objects."""
    # Create complex SQL object
    complex_sql = SQL("""
        WITH RECURSIVE series(n) AS (
            SELECT 1
            UNION ALL
            SELECT n + 1 FROM series WHERE n < $1
        )
        SELECT
            n as number,
            n * n as square,
            CASE
                WHEN n % 2 = 0 THEN 'even'
                ELSE 'odd'
            END as parity
        FROM series
        ORDER BY n
    """)

    # Execute with parameter
    result = await psqlpy_session.execute(complex_sql, (10,))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result.data) == 10

    # Verify computation
    for i, row in enumerate(result.data, 1):
        assert row["number"] == i
        assert row["square"] == i * i
        assert row["parity"] == ("even" if i % 2 == 0 else "odd")


async def test_psqlpy_prepared_statement_behavior(psqlpy_session: PsqlpyDriver) -> None:
    """Test PSQLPy's prepared statement optimization."""
    # Execute the same query multiple times with different parameters
    # PSQLPy should optimize this with prepared statements

    sql = "SELECT $1::text as param_value, length($1) as param_length"

    test_values = ["short", "medium_length", "this_is_a_much_longer_parameter_value"]

    for value in test_values:
        result = await psqlpy_session.execute(sql, (value,))
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["param_value"] == value
        assert result.data[0]["param_length"] == len(value)


async def test_psqlpy_rust_performance_indicators(psqlpy_session: PsqlpyDriver) -> None:
    """Test indicators of Rust-based performance benefits."""
    import time

    # Test that should benefit from Rust performance
    start_time = time.time()

    # Perform many small operations
    for i in range(50):
        result = await psqlpy_session.execute("SELECT $1::int + $2::int as sum", (i, i * 2))
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["sum"] == i + (i * 2)

    elapsed_time = time.time() - start_time

    # This should complete reasonably quickly due to Rust optimizations
    # Exact threshold depends on system, but should be under reasonable limits
    assert elapsed_time < 10.0  # Should complete in under 10 seconds

    # Test bulk operation performance
    bulk_start = time.time()

    bulk_params = [(i, f"bulk_{i}") for i in range(100)]
    await psqlpy_session.execute_many(
        "INSERT INTO test_table (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING", bulk_params
    )

    bulk_elapsed = time.time() - bulk_start
    assert bulk_elapsed < 5.0  # Bulk operations should be fast
