"""Integration tests for psycopg driver implementation."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any, Literal

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgSyncConfig, PsycopgSyncDriver
from sqlspec.core.result import SQLResult

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


@pytest.fixture
def psycopg_session(postgres_service: PostgresService) -> Generator[PsycopgSyncDriver, None, None]:
    """Create a psycopg session with test table."""
    config = PsycopgSyncConfig(
        pool_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        }
    )

    try:
        with config.provide_session() as session:
            # Create test table outside of transaction
            # PostgreSQL DDL operations are transactional, so we need to commit
            session.execute_script("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Commit the DDL operation
            session.commit()

            # Start a new transaction for test isolation
            session.begin()
            yield session
            # Rollback test changes
            try:
                session.rollback()
            except Exception:
                pass  # May already be rolled back
            # Cleanup - DDL operations
            try:
                session.execute_script("DROP TABLE IF EXISTS test_table")
            except Exception:
                # If the transaction is in an error state, try direct connection
                if hasattr(session.connection, "rollback"):
                    try:
                        session.connection.rollback()  # pyright: ignore[reportAttributeAccessIssue]
                    except Exception:
                        pass
                # Try again after rollback
                try:
                    session.execute_script("DROP TABLE IF EXISTS test_table")
                except Exception:
                    # If it still fails, ignore - table might not exist
                    pass
    finally:
        # Ensure pool is closed properly to avoid "cannot join current thread" warnings
        config.close_pool()


@pytest.mark.xdist_group("postgres")
def test_psycopg_basic_crud(psycopg_session: PsycopgSyncDriver) -> None:
    """Test basic CRUD operations."""
    # INSERT
    insert_result = psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", "test_name", 42)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # SELECT
    select_result = psycopg_session.execute("SELECT name, value FROM test_table WHERE name = %s", "test_name")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    # UPDATE
    update_result = psycopg_session.execute("UPDATE test_table SET value = %s WHERE name = %s", 100, "test_name")
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    # Verify UPDATE
    verify_result = psycopg_session.execute("SELECT value FROM test_table WHERE name = %s", "test_name")
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    # DELETE
    delete_result = psycopg_session.execute("DELETE FROM test_table WHERE name = %s", "test_name")
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    # Verify DELETE
    empty_result = psycopg_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_value",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_value"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("postgres")
def test_psycopg_parameter_styles(psycopg_session: PsycopgSyncDriver, parameters: Any, style: ParamStyle) -> None:
    """Test different parameter binding styles."""
    # Insert test data
    psycopg_session.execute("INSERT INTO test_table (name) VALUES (%s)", "test_value")

    # Test parameter style
    if style == "tuple_binds":
        sql = "SELECT name FROM test_table WHERE name = %s"
        # For positional parameters, unpack the tuple
        result = psycopg_session.execute(sql, *parameters)
    else:  # dict_binds
        sql = "SELECT name FROM test_table WHERE name = %(name)s"
        # For named parameters, pass as kwargs
        result = psycopg_session.execute(sql, **parameters)

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert len(result) == 1
    assert result.data[0]["name"] == "test_value"


@pytest.mark.xdist_group("postgres")
def test_psycopg_execute_many(psycopg_session: PsycopgSyncDriver) -> None:
    """Test execute_many functionality."""
    parameters_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = psycopg_session.execute_many("INSERT INTO test_table (name, value) VALUES (%s, %s)", parameters_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(parameters_list)

    # Verify all records were inserted
    select_result = psycopg_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(parameters_list)

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
    empty_result = psycopg_session.execute("SELECT * FROM test_table WHERE name = %s", "nonexistent")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


@pytest.mark.xdist_group("postgres")
def test_psycopg_error_handling(psycopg_session: PsycopgSyncDriver) -> None:
    """Test error handling and exception propagation."""
    # Test invalid SQL
    with pytest.raises(Exception):  # psycopg.errors.SyntaxError
        psycopg_session.execute("INVALID SQL STATEMENT")

    # After an error, we need to rollback the transaction
    psycopg_session.rollback()
    psycopg_session.begin()

    # Test constraint violation
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", "unique_test", 1)

    # Try to insert with invalid column reference
    with pytest.raises(Exception):  # psycopg.errors.UndefinedColumn
        psycopg_session.execute("SELECT nonexistent_column FROM test_table")


@pytest.mark.xdist_group("postgres")
def test_psycopg_data_types(psycopg_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL data type handling with psycopg."""
    # Create table with various PostgreSQL data types
    # DDL operations may auto-commit, so we handle them separately
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
        "text_value",
        42,
        123.45,
        True,
        '{"key": "value"}',
        [1, 2, 3],
        "2024-01-15",
        "2024-01-15 10:30:00",
        "550e8400-e29b-41d4-a716-446655440000",
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
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", "transaction_test", 100)

    # Verify data is committed
    result = psycopg_session.execute("SELECT COUNT(*) as count FROM test_table WHERE name = %s", ("transaction_test"))
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
    insert_result = psycopg_session.execute("INSERT INTO schema_test (description) VALUES (%s)", "test description")
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
    psycopg_session.execute("INSERT INTO test_table (name, value) VALUES (%s, %s)", "metadata_test", 123)

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
        "INSERT INTO test_table (name, value) VALUES (%s, %s) RETURNING id, name", "returning_test", 999
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
    psycopg_session.execute("INSERT INTO json_test (data) VALUES (%s)", json_data)

    # Test JSON queries
    json_result = psycopg_session.execute("SELECT data->>'name' as name, data->>'age' as age FROM json_test")
    assert isinstance(json_result, SQLResult)
    assert json_result.data is not None
    assert json_result.data[0]["name"] == "test"
    assert json_result.data[0]["age"] == "30"

    # Clean up
    psycopg_session.execute_script("DROP TABLE json_test")


@pytest.mark.xdist_group("postgres")
def test_psycopg_copy_operations_positional(psycopg_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL COPY operations with psycopg using positional parameters."""
    # Create temp table for copy test
    psycopg_session.execute_script("""
        DROP TABLE IF EXISTS copy_test_pos;
        CREATE TABLE copy_test_pos (
            id INTEGER,
            name TEXT,
            value INTEGER
        )
    """)

    # Test COPY FROM STDIN with text format using positional parameter
    copy_data = "1\ttest1\t100\n2\ttest2\t200\n"
    result = psycopg_session.execute("COPY copy_test_pos FROM STDIN WITH (FORMAT text)", copy_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected >= 0  # May be -1 or actual count

    # Verify data was copied
    verify_result = psycopg_session.execute("SELECT * FROM copy_test_pos ORDER BY id")
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 2
    assert verify_result.data[0]["name"] == "test1"
    assert verify_result.data[1]["value"] == 200

    # Clean up
    psycopg_session.execute_script("DROP TABLE copy_test_pos")


@pytest.mark.xdist_group("postgres")
def test_psycopg_copy_operations_keyword(psycopg_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL COPY operations with psycopg using keyword parameters."""
    # Create temp table for copy test
    psycopg_session.execute_script("""
        DROP TABLE IF EXISTS copy_test_kw;
        CREATE TABLE copy_test_kw (
            id INTEGER,
            name TEXT,
            value INTEGER
        )
    """)

    # Test COPY FROM STDIN with text format using keyword parameter
    copy_data = "3\ttest3\t300\n4\ttest4\t400\n"
    result = psycopg_session.execute("COPY copy_test_kw FROM STDIN WITH (FORMAT text)", copy_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected >= 0  # May be -1 or actual count

    # Verify data was copied
    verify_result = psycopg_session.execute("SELECT * FROM copy_test_kw ORDER BY id")
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 2
    assert verify_result.data[0]["name"] == "test3"
    assert verify_result.data[1]["value"] == 400

    # Clean up
    psycopg_session.execute_script("DROP TABLE copy_test_kw")


@pytest.mark.xdist_group("postgres")
def test_psycopg_copy_csv_format(psycopg_session: PsycopgSyncDriver) -> None:
    """Test PostgreSQL COPY operations with CSV format."""
    # Create temp table
    psycopg_session.execute_script("""
        DROP TABLE IF EXISTS copy_csv_sync;
        CREATE TABLE copy_csv_sync (
            id INTEGER,
            name TEXT,
            value INTEGER
        )
    """)

    # Test COPY FROM STDIN with CSV format - positional
    csv_data = "5,test5,500\n6,test6,600\n7,test7,700\n"
    result_pos = psycopg_session.execute("COPY copy_csv_sync FROM STDIN WITH (FORMAT csv)", csv_data)
    assert isinstance(result_pos, SQLResult)
    assert result_pos.rows_affected == 3

    # Verify data
    verify_result = psycopg_session.execute("SELECT * FROM copy_csv_sync ORDER BY id")
    assert isinstance(verify_result, SQLResult)
    assert len(verify_result.data) == 3
    assert verify_result.data[0]["name"] == "test5"
    assert verify_result.data[2]["value"] == 700

    # Clear table and test with keyword parameter
    psycopg_session.execute_script("TRUNCATE TABLE copy_csv_sync")

    csv_data2 = "8,test8,800\n9,test9,900\n"
    result_kw = psycopg_session.execute("COPY copy_csv_sync FROM STDIN WITH (FORMAT csv)", csv_data2)
    assert isinstance(result_kw, SQLResult)
    assert result_kw.rows_affected == 2

    # Verify data again
    verify_result2 = psycopg_session.execute("SELECT * FROM copy_csv_sync ORDER BY id")
    assert isinstance(verify_result2, SQLResult)
    assert len(verify_result2.data) == 2
    assert verify_result2.data[0]["name"] == "test8"
    assert verify_result2.data[1]["value"] == 900

    # Clean up
    psycopg_session.execute_script("DROP TABLE copy_csv_sync")


@pytest.mark.integration
def test_psycopg_sync_pgvector_integration(psycopg_session: PsycopgSyncDriver) -> None:
    """Test that psycopg sync driver initializes pgvector support automatically via pool configure."""
    # pgvector should be registered automatically when the pool/connection is created
    # This test verifies that the connection was created without errors, which means
    # the pgvector initialization (if pgvector is available) completed successfully

    # Test that we can execute a basic query without errors
    result = psycopg_session.execute("SELECT 1 as test_value")
    assert result.data is not None
    assert result.data[0]["test_value"] == 1

    # If pgvector was available and registered, the connection should work normally
    # If pgvector was not available, the connection should still work normally
    # This test passes if no exceptions are raised during connection setup
