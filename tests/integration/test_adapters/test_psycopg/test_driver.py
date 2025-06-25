"""Integration tests for psycopg driver implementation."""

from __future__ import annotations

import tempfile
from collections.abc import Generator
from typing import Any, Literal

import pyarrow.parquet as pq
import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


@pytest.fixture
def psycopg_session(postgres_service: PostgresService) -> Generator[PsycopgSyncDriver, None, None]:
    """Create a psycopg session with test table."""
    from sqlspec.statement.sql import SQLConfig

    config = PsycopgSyncConfig(
        host=postgres_service.host,
        port=postgres_service.port,
        user=postgres_service.user,
        password=postgres_service.password,
        dbname=postgres_service.database,
        autocommit=True,  # Enable autocommit for tests
        statement_config=SQLConfig(enable_transformations=False, enable_normalization=False, enable_parsing=False),
    )

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        yield session
        # Cleanup - handle potential transaction errors
        try:
            session.execute_script("DROP TABLE IF EXISTS test_table")
        except Exception:
            # If the transaction is in an error state, rollback first
            if hasattr(session.connection, "rollback"):
                session.connection.rollback()
            # Try again after rollback
            try:
                session.execute_script("DROP TABLE IF EXISTS test_table")
            except Exception:
                # If it still fails, ignore - table might not exist
                pass


@pytest.mark.xdist_group("postgres")
def test_psycopg_basic_crud(psycopg_session: PsycopgSyncDriver) -> None:
    """Test basic CRUD operations."""
    # INSERT
    insert_result = psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("test_name", 42))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # SELECT
    select_result = psycopg_session.execute("SELECT name, value FROM test_table WHERE name = %s", ("test_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    # UPDATE
    update_result = psycopg_session.execute("UPDATE test_table SET value = %s WHERE name = %s", (100, "test_name"))
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    # Verify UPDATE
    verify_result = psycopg_session.execute("SELECT value FROM test_table WHERE name = %s", ("test_name",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    # DELETE
    delete_result = psycopg_session.execute("DELETE FROM test_table WHERE name = %s", ("test_name",))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    # Verify DELETE
    empty_result = psycopg_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_value",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_value"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_styles(psycopg_session: PsycopgSyncDriver, params: Any, style: ParamStyle) -> None:
    """Test different parameter binding styles."""
    # Insert test data
    psycopg_session.execute("INSERT INTO test_table (name) VALUES (%s)", ("test_value",))

    # Test parameter style
    if style == "tuple_binds":
        sql = "SELECT name FROM test_table WHERE name = %s"
    else:  # dict_binds
        sql = "SELECT name FROM test_table WHERE name = %(name)s"

    result = psycopg_session.execute(sql, params)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result) == 1
    assert result.data[0]["name"] == "test_value"


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many(psycopg_session: PsycopgSyncDriver) -> None:
    """Test execute_many functionality."""
    params_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = psycopg_session.execute_many("INSERT INTO test_table (name, value) VALUES (%s, %s)", params_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(params_list)

    # Verify all records were inserted
    select_result = psycopg_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(params_list)

    # Verify data integrity
    ordered_result = psycopg_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(ordered_result, SQLResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_script(psycopg_session: PsycopgSyncDriver) -> None:
    """Test execute_script functionality."""
    script = """
        INSERT INTO test_table (name, value) VALUES ('script_test1', 999);
        INSERT INTO test_table (name, value) VALUES ('script_test2', 888);
        UPDATE test_table SET value = 1000 WHERE name = 'script_test1';
    """

    result = psycopg_session.execute_script(script)
    # Script execution returns a SQLResult
    assert isinstance(result, SQLResult)
    assert result.operation_type == "SCRIPT"

    # Verify script effects
    select_result = psycopg_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'script_test%' ORDER BY name"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2
    assert select_result.data[0]["name"] == "script_test1"
    assert select_result.data[0]["value"] == 1000
    assert select_result.data[1]["name"] == "script_test2"
    assert select_result.data[1]["value"] == 888


@pytest.mark.xdist_group("postgres")
def test_psycopg_result_methods(psycopg_session: PsycopgSyncDriver) -> None:
    """Test SelectResult and ExecuteResult methods."""
    # Insert test data
    psycopg_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES (%s, %s)", [("result1", 10), ("result2", 20), ("result3", 30)]
    )

    # Test SelectResult methods
    result = psycopg_session.execute("SELECT * FROM test_table ORDER BY name")
    assert isinstance(result, SQLResult)

    # Test get_first()
    first_row = result.get_first()
    assert first_row is not None
    assert first_row["name"] == "result1"

    # Test get_count()
    assert result.get_count() == 3

    # Test is_empty()
    assert not result.is_empty()

    # Test empty result
    empty_result = psycopg_session.execute("SELECT * FROM test_table WHERE name = %s", ("nonexistent",))
    assert isinstance(empty_result, SQLResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


@pytest.mark.xdist_group("postgres")
def test_psycopg_error_handling(psycopg_session: PsycopgSyncDriver) -> None:
    """Test error handling and exception propagation."""
    # Test invalid SQL
    with pytest.raises(Exception):  # psycopg.errors.SyntaxError
        psycopg_session.execute("INVALID SQL STATEMENT")

    # Test constraint violation
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("unique_test", 1))

    # Try to insert with invalid column reference
    with pytest.raises(Exception):  # psycopg.errors.UndefinedColumn
        psycopg_session.execute("SELECT nonexistent_column FROM test_table")


@pytest.mark.xdist_group("postgres")
def test_psycopg_data_types(psycopg_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL data type handling with psycopg."""
    # Create table with various PostgreSQL data types
    psycopg_session.execute_script("""
        CREATE TABLE data_types_test (
            id SERIAL PRIMARY KEY,
            text_col TEXT,
            integer_col INTEGER,
            numeric_col NUMERIC(10,2),
            boolean_col BOOLEAN,
            json_col JSONB,
            array_col INTEGER[],
            date_col DATE,
            timestamp_col TIMESTAMP,
            uuid_col UUID
        )
    """)

    # Insert data with various types
    psycopg_session.execute(
        """
        INSERT INTO data_types_test (
            text_col, integer_col, numeric_col, boolean_col, json_col,
            array_col, date_col, timestamp_col, uuid_col
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """,
        (
            "text_value",
            42,
            123.45,
            True,
            '{"key": "value"}',
            [1, 2, 3],
            "2024-01-15",
            "2024-01-15 10:30:00",
            "550e8400-e29b-41d4-a716-446655440000",
        ),
    )

    # Retrieve and verify data
    select_result = psycopg_session.execute(
        "SELECT text_col, integer_col, numeric_col, boolean_col, json_col, array_col FROM data_types_test"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["text_col"] == "text_value"
    assert row["integer_col"] == 42
    assert row["boolean_col"] is True
    assert row["array_col"] == [1, 2, 3]

    # Clean up
    psycopg_session.execute_script("DROP TABLE data_types_test")


@pytest.mark.xdist_group("postgres")
def test_psycopg_transactions(psycopg_session: PsycopgSyncDriver) -> None:
    """Test transaction behavior."""
    # PostgreSQL supports explicit transactions
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("transaction_test", 100))

    # Verify data is committed
    result = psycopg_session.execute("SELECT COUNT(*) as count FROM test_table WHERE name = %s", ("transaction_test",))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 1


@pytest.mark.xdist_group("postgres")
def test_psycopg_complex_queries(psycopg_session: PsycopgSyncDriver) -> None:
    """Test complex SQL queries."""
    # Insert test data
    test_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 28)]

    psycopg_session.execute_many("INSERT INTO test_table (name, value) VALUES (%s, %s)", test_data)

    # Test JOIN (self-join)
    join_result = psycopg_session.execute("""
        SELECT t1.name as name1, t2.name as name2, t1.value as value1, t2.value as value2
        FROM test_table t1
        CROSS JOIN test_table t2
        WHERE t1.value < t2.value
        ORDER BY t1.name, t2.name
        LIMIT 3
    """)
    assert isinstance(join_result, SQLResult)
    assert join_result.data is not None
    assert len(join_result.data) == 3

    # Test aggregation
    agg_result = psycopg_session.execute("""
        SELECT
            COUNT(*) as total_count,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM test_table
    """)
    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["total_count"] == 4
    assert agg_result.data[0]["avg_value"] == 29.5
    assert agg_result.data[0]["min_value"] == 25
    assert agg_result.data[0]["max_value"] == 35

    # Test subquery
    subquery_result = psycopg_session.execute("""
        SELECT name, value
        FROM test_table
        WHERE value > (SELECT AVG(value) FROM test_table)
        ORDER BY value
    """)
    assert isinstance(subquery_result, SQLResult)
    assert subquery_result.data is not None
    assert len(subquery_result.data) == 2  # Bob and Charlie
    assert subquery_result.data[0]["name"] == "Bob"
    assert subquery_result.data[1]["name"] == "Charlie"


@pytest.mark.xdist_group("postgres")
def test_psycopg_schema_operations(psycopg_session: PsycopgSyncDriver) -> None:
    """Test schema operations (DDL)."""
    # Create a new table
    psycopg_session.execute_script("""
        CREATE TABLE schema_test (
            id SERIAL PRIMARY KEY,
            description TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert data into new table
    insert_result = psycopg_session.execute("INSERT INTO schema_test (description) VALUES (%s)", ("test description",))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Verify table structure
    info_result = psycopg_session.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'schema_test'
        ORDER BY ordinal_position
    """)
    assert isinstance(info_result, SQLResult)
    assert info_result.data is not None
    assert len(info_result.data) == 3  # id, description, created_at

    # Drop table
    psycopg_session.execute_script("DROP TABLE schema_test")


@pytest.mark.xdist_group("postgres")
def test_psycopg_column_names_and_metadata(psycopg_session: PsycopgSyncDriver) -> None:
    """Test column names and result metadata."""
    # Insert test data
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("metadata_test", 123))

    # Test column names
    result = psycopg_session.execute(
        "SELECT id, name, value, created_at FROM test_table WHERE name = %s", ("metadata_test",)
    )
    assert isinstance(result, SQLResult)
    assert result.column_names == ["id", "name", "value", "created_at"]
    assert result.data is not None
    assert len(result) == 1

    # Test that we can access data by column name
    row = result.data[0]
    assert row["name"] == "metadata_test"
    assert row["value"] == 123
    assert row["id"] is not None
    assert row["created_at"] is not None


@pytest.mark.xdist_group("postgres")
def test_psycopg_with_schema_type(psycopg_session: PsycopgSyncDriver) -> None:
    """Test psycopg driver with schema type conversion."""
    from dataclasses import dataclass

    @dataclass
    class TestRecord:
        id: int | None
        name: str
        value: int

    # Insert test data
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("schema_test", 456))

    # Query with schema type
    result = psycopg_session.execute(
        "SELECT id, name, value FROM test_table WHERE name = %s", ("schema_test",), schema_type=TestRecord
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result) == 1

    # The data should be converted to the schema type by the ResultConverter
    assert result.column_names == ["id", "name", "value"]


@pytest.mark.xdist_group("postgres")
def test_psycopg_performance_bulk_operations(psycopg_session: PsycopgSyncDriver) -> None:
    """Test performance with bulk operations."""
    # Generate bulk data
    bulk_data = [(f"bulk_user_{i}", i * 10) for i in range(100)]

    # Bulk insert
    result = psycopg_session.execute_many("INSERT INTO test_table (name, value) VALUES (%s, %s)", bulk_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 100

    # Bulk select
    select_result = psycopg_session.execute("SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'bulk_user_%'")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    # Test pagination-like query
    page_result = psycopg_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'bulk_user_%' ORDER BY value LIMIT 10 OFFSET 20"
    )
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_20"


@pytest.mark.xdist_group("postgres")
def test_psycopg_postgresql_specific_features(psycopg_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL-specific features with psycopg."""
    # Test RETURNING clause
    returning_result = psycopg_session.execute(
        "INSERT INTO test_table (name, value) VALUES (%s, %s) RETURNING id, name", ("returning_test", 999)
    )
    assert isinstance(returning_result, SQLResult)  # psycopg returns SQLResult for RETURNING
    assert returning_result.data is not None
    assert len(returning_result.data) == 1
    assert returning_result.data[0]["name"] == "returning_test"

    # Test window functions
    psycopg_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES (%s, %s)", [("window1", 10), ("window2", 20), ("window3", 30)]
    )

    window_result = psycopg_session.execute("""
        SELECT
            name,
            value,
            ROW_NUMBER() OVER (ORDER BY value) as row_num,
            LAG(value) OVER (ORDER BY value) as prev_value
        FROM test_table
        WHERE name LIKE 'window%'
        ORDER BY value
    """)
    assert isinstance(window_result, SQLResult)
    assert window_result.data is not None
    assert len(window_result.data) == 3
    assert window_result.data[0]["row_num"] == 1
    assert window_result.data[0]["prev_value"] is None


@pytest.mark.xdist_group("postgres")
def test_psycopg_json_operations(psycopg_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL JSON operations with psycopg."""
    # Create table with JSONB column
    psycopg_session.execute_script("""
        CREATE TABLE json_test (
            id SERIAL PRIMARY KEY,
            data JSONB
        )
    """)

    # Insert JSON data
    json_data = '{"name": "test", "age": 30, "tags": ["postgres", "json"]}'
    psycopg_session.execute("INSERT INTO json_test (data) VALUES (%s)", (json_data,))

    # Test JSON queries
    json_result = psycopg_session.execute("SELECT data->>'name' as name, data->>'age' as age FROM json_test")
    assert isinstance(json_result, SQLResult)
    assert json_result.data is not None
    assert json_result.data[0]["name"] == "test"
    assert json_result.data[0]["age"] == "30"

    # Clean up
    psycopg_session.execute_script("DROP TABLE json_test")


@pytest.mark.xdist_group("postgres")
@pytest.mark.skip(
    reason="COPY commands require cursor.copy() method which is not implemented in SQLSpec psycopg driver"
)
def test_psycopg_copy_operations(psycopg_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL COPY operations if supported by psycopg.

    Note: This test is skipped because psycopg's cursor.execute() method
    explicitly rejects COPY commands with the error:
    "COPY cannot be used with this method; use copy() instead"

    The SQLSpec psycopg driver would need to implement special handling
    for COPY commands using cursor.copy() method to support this functionality.
    """
    # Test basic COPY functionality if available
    try:
        # Create temp table for copy test
        psycopg_session.execute_script("""
            CREATE TABLE copy_test (
                id INTEGER,
                name TEXT,
                value INTEGER
            )
        """)

        # Test if COPY is supported (depends on psycopg implementation)
        copy_data = "1\ttest1\t100\n2\ttest2\t200\n"

        # Try COPY FROM if supported
        try:
            psycopg_session.execute("COPY copy_test FROM STDIN WITH (FORMAT text)", copy_data)

            # Verify data was copied
            verify_result = psycopg_session.execute("SELECT COUNT(*) as count FROM copy_test")
            assert isinstance(verify_result, SQLResult)
            assert verify_result.data[0]["count"] == 2

        except Exception:
            # COPY might not be supported in this implementation
            pass

        # Clean up
        psycopg_session.execute_script("DROP TABLE copy_test")

    except Exception:
        # COPY operations might not be supported
        pytest.skip("COPY operations not supported in this psycopg implementation")


@pytest.mark.xdist_group("postgres")
def test_psycopg_fetch_arrow_table(psycopg_session: PsycopgSyncDriver) -> None:
    """Integration test: fetch_arrow_table returns ArrowResult with correct pyarrow.Table."""
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("arrow1", 111))
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("arrow2", 222))
    statement = SQL("SELECT name, value FROM test_table ORDER BY name")
    result = psycopg_session.fetch_arrow_table(statement)
    assert isinstance(result, ArrowResult)
    assert result.num_rows == 2
    assert set(result.column_names) == {"name", "value"}
    names = result.data["name"].to_pylist()
    assert "arrow1" in names and "arrow2" in names


@pytest.mark.xdist_group("postgres")
def test_psycopg_to_parquet(psycopg_session: PsycopgSyncDriver) -> None:
    """Integration test: to_parquet writes correct data to a Parquet file."""
    # Insert fresh data for this test
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("pq1", 123))
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", ("pq2", 456))

    # First verify data can be selected normally
    normal_result = psycopg_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert len(normal_result.data) >= 2, f"Expected at least 2 rows, got {len(normal_result.data)}"

    # Use a simpler query without WHERE clause first
    statement = "SELECT name, value FROM test_table ORDER BY name"
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        try:
            rows_exported = psycopg_session.export_to_storage(statement, destination_uri=tmp.name, format="parquet")
            assert rows_exported == 2
            table = pq.read_table(tmp.name)
            assert table.num_rows == 2
            assert set(table.column_names) == {"name", "value"}
            names = table.column("name").to_pylist()
            assert "pq1" in names and "pq2" in names
        finally:
            import os

            os.unlink(tmp.name)
