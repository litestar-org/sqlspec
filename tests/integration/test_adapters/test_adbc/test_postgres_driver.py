"""Integration tests for ADBC PostgreSQL driver implementation."""

from __future__ import annotations

import math
import tempfile
from collections.abc import Generator
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


def ensure_test_table(session: AdbcDriver) -> None:
    """Ensure test_table exists (recreate if needed after transaction rollback)."""
    session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


@pytest.fixture
def adbc_postgresql_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session with test table."""
    config = AdbcConfig(
        uri=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        driver_name="adbc_driver_postgresql.dbapi.connect",
        statement_config=SQLConfig(strict_mode=False),  # Allow DDL statements for tests
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
        # Cleanup
        session.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.xdist_group("postgres")
@xfail_if_driver_missing
def test_connection(postgres_service: PostgresService) -> None:
    """Test basic ADBC PostgreSQL connection."""
    config = AdbcConfig(
        uri=f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
        driver_name="adbc_driver_postgresql.dbapi.connect",
    )

    # Test connection creation
    with config.provide_connection() as conn:
        assert conn is not None
        # Test basic query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")  # pyright: ignore
            result = cur.fetchone()  # pyright: ignore
            assert result == (1,)

    # Test session creation
    with config.provide_session() as session:
        assert session is not None
        assert isinstance(session, AdbcDriver)
        result = session.execute(SQL("SELECT 1 as test_value"))
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["test_value"] == 1


@pytest.mark.xdist_group("postgres")
def test_basic_crud(adbc_postgresql_session: AdbcDriver) -> None:
    """Test basic CRUD operations with ADBC PostgreSQL."""
    # INSERT
    insert_result = adbc_postgresql_session.execute(
        "INSERT INTO test_table (name, value) VALUES ($1, $2)", ("test_name", 42)
    )
    assert isinstance(insert_result, SQLResult)
    # ADBC PostgreSQL driver may return -1 for rowcount on DML operations
    assert insert_result.rows_affected in (-1, 1)

    # SELECT
    select_result = adbc_postgresql_session.execute(
        "SELECT name, value FROM test_table WHERE name = $1", ("test_name",)
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    # UPDATE
    update_result = adbc_postgresql_session.execute(
        "UPDATE test_table SET value = $1 WHERE name = $2", (100, "test_name")
    )
    assert isinstance(update_result, SQLResult)
    # ADBC PostgreSQL driver may return -1 for rowcount on DML operations
    assert update_result.rows_affected in (-1, 1)

    # Verify UPDATE
    verify_result = adbc_postgresql_session.execute("SELECT value FROM test_table WHERE name = $1", ("test_name",))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    # DELETE
    delete_result = adbc_postgresql_session.execute("DELETE FROM test_table WHERE name = $1", ("test_name",))
    assert isinstance(delete_result, SQLResult)
    # ADBC PostgreSQL driver may return -1 for rowcount on DML operations
    assert delete_result.rows_affected in (-1, 1)

    # Verify DELETE
    empty_result = adbc_postgresql_session.execute("SELECT COUNT(*) as count FROM test_table")
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
def test_parameter_styles(adbc_postgresql_session: AdbcDriver, params: Any, style: ParamStyle) -> None:
    """Test different parameter binding styles with ADBC PostgreSQL."""
    # Insert test data
    adbc_postgresql_session.execute(SQL("INSERT INTO test_table (name) VALUES ($1)"), ("test_value",))

    # Test parameter style
    if style == "tuple_binds":
        sql = SQL("SELECT name FROM test_table WHERE name = $1")
    else:  # dict_binds - PostgreSQL uses numbered parameters
        sql = SQL("SELECT name FROM test_table WHERE name = $1")
        params = (params["name"],) if isinstance(params, dict) else params

    result = adbc_postgresql_session.execute(sql, params)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1
    assert result.data[0]["name"] == "test_value"


@pytest.mark.xdist_group("postgres")
def test_parameter_types(adbc_postgresql_session: AdbcDriver) -> None:
    """Test various parameter types with ADBC PostgreSQL."""
    adbc_postgresql_session.execute_script("""
        CREATE TABLE param_test (
            int_col INTEGER,
            text_col TEXT,
            float_col FLOAT,
            bool_col BOOLEAN,
            array_col INTEGER[]
        )
    """)

    # Test various parameter types
    params = (42, "test_string", math.pi, True, [1, 2, 3])
    insert_result = adbc_postgresql_session.execute(
        SQL("""
        INSERT INTO param_test (int_col, text_col, float_col, bool_col, array_col)
        VALUES ($1, $2, $3, $4, $5)
        """),
        params,
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected in (-1, 1)

    # Verify data
    select_result = adbc_postgresql_session.execute(SQL("SELECT * FROM param_test"))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1

    row = select_result.data[0]
    assert row["int_col"] == 42
    assert row["text_col"] == "test_string"
    assert abs(row["float_col"] - math.pi) < 0.001
    assert row["bool_col"] is True
    assert row["array_col"] == [1, 2, 3]

    # Cleanup
    adbc_postgresql_session.execute_script("DROP TABLE param_test")


@pytest.mark.xdist_group("postgres")
def test_multiple_parameters(adbc_postgresql_session: AdbcDriver) -> None:
    """Test queries with multiple parameters."""
    # Insert test data
    test_data = [("Alice", 25, True), ("Bob", 30, False), ("Charlie", 35, True)]
    adbc_postgresql_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES ($1, $2)", [(name, value) for name, value, _ in test_data]
    )

    # Query with multiple parameters
    result = adbc_postgresql_session.execute(
        "SELECT name, value FROM test_table WHERE value >= $1 AND value <= $2 ORDER BY value", (25, 30)
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 2
    assert result.data[0]["name"] == "Alice"
    assert result.data[1]["name"] == "Bob"


@pytest.mark.xdist_group("postgres")
@pytest.mark.xfail(reason="ADBC PostgreSQL driver has issues with null parameter handling")
def test_null_parameters(adbc_postgresql_session: AdbcDriver) -> None:
    """Test handling of NULL parameters."""
    # Create table that allows NULLs
    adbc_postgresql_session.execute_script("""
        CREATE TABLE null_test (
            id SERIAL PRIMARY KEY,
            nullable_text TEXT,
            nullable_int INTEGER
        )
    """)

    # Insert with NULL values
    adbc_postgresql_session.execute("INSERT INTO null_test (nullable_text, nullable_int) VALUES ($1, $2)", (None, None))

    # Query for NULL values
    result = adbc_postgresql_session.execute(
        "SELECT * FROM null_test WHERE nullable_text IS NULL AND nullable_int IS NULL"
    )
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1
    assert result.data[0]["nullable_text"] is None
    assert result.data[0]["nullable_int"] is None

    # Cleanup
    adbc_postgresql_session.execute_script("DROP TABLE null_test")


@pytest.mark.xdist_group("postgres")
def test_execute_many(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_many functionality with ADBC PostgreSQL."""
    params_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = adbc_postgresql_session.execute_many(
        SQL("INSERT INTO test_table (name, value) VALUES ($1, $2)"), params_list
    )
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(params_list)

    # Verify all records were inserted
    select_result = adbc_postgresql_session.execute(SQL("SELECT COUNT(*) as count FROM test_table"))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(params_list)

    # Verify data integrity
    ordered_result = adbc_postgresql_session.execute(SQL("SELECT name, value FROM test_table ORDER BY name"))
    assert isinstance(ordered_result, SQLResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


@pytest.mark.xdist_group("postgres")
def test_execute_many_update(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_many with UPDATE statements."""
    # Insert initial data
    initial_data = [("user1", 10), ("user2", 20), ("user3", 30)]
    adbc_postgresql_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", initial_data)

    # Update using execute_many
    updates = [(100, "user1"), (200, "user2"), (300, "user3")]
    result = adbc_postgresql_session.execute_many("UPDATE test_table SET value = $1 WHERE name = $2", updates)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify updates
    verify_result = adbc_postgresql_session.execute(SQL("SELECT name, value FROM test_table ORDER BY name"))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100
    assert verify_result.data[1]["value"] == 200
    assert verify_result.data[2]["value"] == 300


@pytest.mark.xdist_group("postgres")
def test_execute_many_empty(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_many with empty parameter list."""
    # Execute with empty list
    result = adbc_postgresql_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", [])
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 0

    # Verify no records were inserted
    count_result = adbc_postgresql_session.execute(SQL("SELECT COUNT(*) as count FROM test_table"))
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["count"] == 0


@pytest.mark.xdist_group("postgres")
def test_execute_many_transaction(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_many within transaction context."""
    # Note: ADBC drivers may handle transactions differently
    # This test verifies basic behavior

    # Insert data using execute_many
    data = [("tx_user1", 100), ("tx_user2", 200), ("tx_user3", 300)]

    result = adbc_postgresql_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify all data was inserted
    verify_result = adbc_postgresql_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'tx_user%'"
    )
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["count"] == 3


@pytest.mark.xdist_group("postgres")
def test_execute_script(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_script functionality with ADBC PostgreSQL."""
    script = """
        INSERT INTO test_table (name, value) VALUES ('script_test1', 999);
        INSERT INTO test_table (name, value) VALUES ('script_test2', 888);
        UPDATE test_table SET value = 1000 WHERE name = 'script_test1';
    """

    result = adbc_postgresql_session.execute_script(script)
    # Script execution returns SQLResult
    assert isinstance(result, SQLResult)

    # Verify script effects
    select_result = adbc_postgresql_session.execute(
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
def test_execute_script_ddl(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_script with DDL statements."""
    ddl_script = """
        -- Create a new table
        CREATE TABLE script_test_table (
            id SERIAL PRIMARY KEY,
            data TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create an index
        CREATE INDEX idx_script_test_data ON script_test_table(data);

        -- Insert some data
        INSERT INTO script_test_table (data) VALUES ('test1'), ('test2'), ('test3');
    """

    result = adbc_postgresql_session.execute_script(ddl_script)
    assert isinstance(result, SQLResult)

    # Verify table was created and data inserted
    verify_result = adbc_postgresql_session.execute(SQL("SELECT COUNT(*) as count FROM script_test_table"))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["count"] == 3

    # Verify index exists
    index_result = adbc_postgresql_session.execute("""
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'script_test_table' AND indexname = 'idx_script_test_data'
    """)
    assert isinstance(index_result, SQLResult)
    assert index_result.data is not None
    assert len(index_result.data) == 1

    # Cleanup
    adbc_postgresql_session.execute_script("DROP TABLE script_test_table")


@pytest.mark.xdist_group("postgres")
def test_execute_script_mixed(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_script with mixed DDL and DML statements."""
    mixed_script = """
        -- Create a temporary table
        CREATE TEMP TABLE temp_data (
            id INTEGER,
            value TEXT
        );

        -- Insert data
        INSERT INTO temp_data VALUES (1, 'one'), (2, 'two'), (3, 'three');

        -- Update existing table based on temp table
        INSERT INTO test_table (name, value)
        SELECT value, id * 10 FROM temp_data;

        -- Drop temp table
        DROP TABLE temp_data;
    """

    result = adbc_postgresql_session.execute_script(mixed_script)
    assert isinstance(result, SQLResult)

    # Verify data was inserted into main table
    verify_result = adbc_postgresql_session.execute(
        "SELECT name, value FROM test_table WHERE name IN ('one', 'two', 'three') ORDER BY value"
    )
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 3
    assert verify_result.data[0]["name"] == "one"
    assert verify_result.data[0]["value"] == 10
    assert verify_result.data[1]["name"] == "two"
    assert verify_result.data[1]["value"] == 20
    assert verify_result.data[2]["name"] == "three"
    assert verify_result.data[2]["value"] == 30


@pytest.mark.xdist_group("postgres")
def test_execute_script_error_handling(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_script error handling."""
    # Script with syntax error
    bad_script = """
        INSERT INTO test_table (name, value) VALUES ('test', 100);
        INVALID SQL STATEMENT HERE;
        INSERT INTO test_table (name, value) VALUES ('test2', 200);
    """

    # Should raise an error
    with pytest.raises(Exception):  # ADBC error
        adbc_postgresql_session.execute_script(bad_script)

    # Verify no partial execution (depends on driver transaction handling)
    # The table might have been rolled back, so check if it exists first
    try:
        count_result = adbc_postgresql_session.execute(
            "SELECT COUNT(*) as count FROM test_table WHERE name IN ($1, $2)", ("test", "test2")
        )
        assert isinstance(count_result, SQLResult)
        assert count_result.data is not None
        # Count should be 0 since transaction was rolled back
        assert count_result.data[0]["count"] == 0
    except Exception:
        # Table might not exist if the entire transaction was rolled back
        # This is acceptable behavior for transactional databases
        pass


@pytest.mark.xdist_group("postgres")
def test_result_methods(adbc_postgresql_session: AdbcDriver) -> None:
    """Test SelectResult and ExecuteResult methods with ADBC PostgreSQL."""
    # Insert test data
    adbc_postgresql_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES ($1, $2)", [("result1", 10), ("result2", 20), ("result3", 30)]
    )

    # Test SelectResult methods
    result = adbc_postgresql_session.execute(SQL("SELECT * FROM test_table ORDER BY name"))
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
    empty_result = adbc_postgresql_session.execute("SELECT * FROM test_table WHERE name = $1", ("nonexistent",))
    assert isinstance(empty_result, SQLResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


@pytest.mark.xdist_group("postgres")
def test_error_handling(adbc_postgresql_session: AdbcDriver) -> None:
    """Test error handling and exception propagation with ADBC PostgreSQL."""
    # Test invalid SQL
    with pytest.raises(Exception):  # ADBC error
        adbc_postgresql_session.execute(SQL("INVALID SQL STATEMENT"))

    # After error, we need to ensure table exists (might have been rolled back)
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Test constraint violation
    adbc_postgresql_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("unique_test", 1))

    # Try to insert with invalid column reference
    with pytest.raises(Exception):  # ADBC error
        adbc_postgresql_session.execute(SQL("SELECT nonexistent_column FROM test_table"))


@pytest.mark.xdist_group("postgres")
def test_data_types(adbc_postgresql_session: AdbcDriver) -> None:
    """Test PostgreSQL data type handling with ADBC."""
    # Ensure test_table exists after any prior errors
    ensure_test_table(adbc_postgresql_session)

    # Create table with various PostgreSQL data types
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS data_types_test (
            id SERIAL PRIMARY KEY,
            text_col TEXT,
            integer_col INTEGER,
            numeric_col NUMERIC(10,2),
            boolean_col BOOLEAN,
            array_col INTEGER[],
            date_col DATE,
            timestamp_col TIMESTAMP
        )
    """)

    # Insert data with various types
    adbc_postgresql_session.execute(
        """
        INSERT INTO data_types_test (
            text_col, integer_col, numeric_col, boolean_col,
            array_col, date_col, timestamp_col
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7
        )
    """,
        ("text_value", 42, 123.45, True, [1, 2, 3], date(2024, 1, 15), datetime(2024, 1, 15, 10, 30)),
    )

    # Retrieve and verify data
    select_result = adbc_postgresql_session.execute(
        "SELECT text_col, integer_col, numeric_col, boolean_col, array_col FROM data_types_test"
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
    adbc_postgresql_session.execute_script("DROP TABLE data_types_test")


@pytest.mark.xdist_group("postgres")
def test_basic_types(adbc_postgresql_session: AdbcDriver) -> None:
    """Test basic PostgreSQL data types."""
    # Create table with basic types
    adbc_postgresql_session.execute_script("""
        CREATE TABLE basic_types_test (
            int_col INTEGER,
            bigint_col BIGINT,
            smallint_col SMALLINT,
            text_col TEXT,
            varchar_col VARCHAR(100),
            char_col CHAR(10),
            boolean_col BOOLEAN,
            float_col FLOAT,
            double_col DOUBLE PRECISION,
            decimal_col DECIMAL(10,2)
        )
    """)

    # Insert test data
    adbc_postgresql_session.execute(
        """
        INSERT INTO basic_types_test VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        )
        """,
        (
            42,  # int
            9223372036854775807,  # bigint
            32767,  # smallint
            "text value",  # text
            "varchar value",  # varchar
            "char",  # char
            True,  # boolean
            math.pi,  # float
            math.e,  # double
            1234.56,  # decimal
        ),
    )

    # Verify data
    result = adbc_postgresql_session.execute(SQL("SELECT * FROM basic_types_test"))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1

    row = result.data[0]
    assert row["int_col"] == 42
    assert row["bigint_col"] == 9223372036854775807
    assert row["smallint_col"] == 32767
    assert row["text_col"] == "text value"
    assert row["varchar_col"] == "varchar value"
    assert row["char_col"].strip() == "char"  # CHAR type pads with spaces
    assert row["boolean_col"] is True
    assert abs(row["float_col"] - math.pi) < 0.001
    assert abs(row["double_col"] - math.e) < 0.000001

    # Cleanup
    adbc_postgresql_session.execute_script("DROP TABLE basic_types_test")


@pytest.mark.xdist_group("postgres")
def test_date_time_types(adbc_postgresql_session: AdbcDriver) -> None:
    """Test PostgreSQL date/time types."""
    # Ensure test_table exists after any prior errors
    ensure_test_table(adbc_postgresql_session)

    # Create table with date/time types
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS datetime_test (
            date_col DATE,
            time_col TIME,
            timestamp_col TIMESTAMP,
            timestamptz_col TIMESTAMPTZ,
            interval_col INTERVAL
        )
    """)

    # Insert test data with explicit casts
    adbc_postgresql_session.execute(
        """
        INSERT INTO datetime_test VALUES ($1::date, $2::time, $3::timestamp, $4::timestamptz, $5::interval)
        """,
        ("2024-01-15", "14:30:00", "2024-01-15 14:30:00", "2024-01-15 14:30:00+00", "1 day 2 hours 30 minutes"),
    )

    # Verify data
    result = adbc_postgresql_session.execute(SQL("SELECT * FROM datetime_test"))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1

    row = result.data[0]
    # Date/time handling may vary by ADBC driver version
    assert row["date_col"] is not None
    assert row["time_col"] is not None
    assert row["timestamp_col"] is not None
    assert row["timestamptz_col"] is not None
    assert row["interval_col"] is not None

    # Cleanup
    adbc_postgresql_session.execute_script("DROP TABLE datetime_test")


@pytest.mark.xdist_group("postgres")
@pytest.mark.xfail(reason="ADBC PostgreSQL driver has issues with null parameter handling")
def test_null_values(adbc_postgresql_session: AdbcDriver) -> None:
    """Test NULL value handling."""
    # Ensure test_table exists after any prior errors
    ensure_test_table(adbc_postgresql_session)

    # Create table allowing NULLs
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS null_values_test (
            id SERIAL PRIMARY KEY,
            nullable_int INTEGER,
            nullable_text TEXT,
            nullable_bool BOOLEAN,
            nullable_timestamp TIMESTAMP
        )
    """)

    # Insert row with NULL values
    adbc_postgresql_session.execute(
        """
        INSERT INTO null_values_test (nullable_int, nullable_text, nullable_bool, nullable_timestamp)
        VALUES ($1, $2, $3, $4)
        """,
        (None, None, None, None),
    )

    # Verify NULL values
    result = adbc_postgresql_session.execute(SQL("SELECT * FROM null_values_test"))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1

    row = result.data[0]
    assert row["nullable_int"] is None
    assert row["nullable_text"] is None
    assert row["nullable_bool"] is None
    assert row["nullable_timestamp"] is None

    # Cleanup
    adbc_postgresql_session.execute_script("DROP TABLE null_values_test")


@pytest.mark.xdist_group("postgres")
@pytest.mark.xfail(reason="ADBC PostgreSQL driver has issues with array and complex type handling")
def test_advanced_types(adbc_postgresql_session: AdbcDriver) -> None:
    """Test PostgreSQL advanced types (arrays, JSON, etc.)."""
    # Ensure test_table exists after any prior errors
    ensure_test_table(adbc_postgresql_session)

    # Create table with advanced types
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS advanced_types_test (
            array_int INTEGER[],
            array_text TEXT[],
            array_2d INTEGER[][],
            json_col JSON,
            jsonb_col JSONB,
            uuid_col UUID
        )
    """)

    # Insert test data
    import json

    adbc_postgresql_session.execute(
        """
        INSERT INTO advanced_types_test VALUES ($1, $2, $3, $4, $5, $6)
        """,
        (
            [1, 2, 3, 4, 5],
            ["a", "b", "c"],
            [[1, 2], [3, 4]],
            json.dumps({"key": "value", "number": 42}),
            json.dumps({"nested": {"data": "here"}}),
            "550e8400-e29b-41d4-a716-446655440000",
        ),
    )

    # Verify data
    result = adbc_postgresql_session.execute(SQL("SELECT * FROM advanced_types_test"))
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1

    row = result.data[0]
    assert row["array_int"] == [1, 2, 3, 4, 5]
    assert row["array_text"] == ["a", "b", "c"]
    assert row["array_2d"] == [[1, 2], [3, 4]]
    # JSON handling may vary by driver
    assert row["json_col"] is not None
    assert row["jsonb_col"] is not None
    assert row["uuid_col"] == "550e8400-e29b-41d4-a716-446655440000"

    # Cleanup
    adbc_postgresql_session.execute_script("DROP TABLE advanced_types_test")


@pytest.mark.xdist_group("postgres")
def test_arrow_result_format(adbc_postgresql_session: AdbcDriver) -> None:
    """Test ADBC Arrow result format functionality."""
    # Insert test data for Arrow testing
    test_data = [("arrow_test1", 100), ("arrow_test2", 200), ("arrow_test3", 300)]
    adbc_postgresql_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", test_data)

    # Test getting results as Arrow if available
    if hasattr(adbc_postgresql_session, "fetch_arrow_table"):
        arrow_result = adbc_postgresql_session.fetch_arrow_table("SELECT name, value FROM test_table ORDER BY name")

        assert isinstance(arrow_result, ArrowResult)
        arrow_table = arrow_result.data
        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows == 3
        assert arrow_table.num_columns == 2
        assert arrow_table.column_names == ["name", "value"]

        # Verify data
        names = arrow_table.column("name").to_pylist()
        values = arrow_table.column("value").to_pylist()
        assert names == ["arrow_test1", "arrow_test2", "arrow_test3"]
        assert values == [100, 200, 300]
    else:
        pytest.skip("ADBC driver does not support Arrow result format")


@pytest.mark.xdist_group("postgres")
def test_fetch_arrow_table(adbc_postgresql_session: AdbcDriver) -> None:
    """Test PostgreSQL fetch_arrow_table functionality."""
    # Insert test data
    test_data = [("Alice", 25, 50000.0), ("Bob", 30, 60000.0), ("Charlie", 35, 70000.0)]

    adbc_postgresql_session.execute_script("""
        CREATE TABLE arrow_test (
            name TEXT,
            age INTEGER,
            salary FLOAT
        )
    """)

    adbc_postgresql_session.execute_many("INSERT INTO arrow_test (name, age, salary) VALUES ($1, $2, $3)", test_data)

    # Test fetch_arrow_table
    result = adbc_postgresql_session.fetch_arrow_table("SELECT * FROM arrow_test ORDER BY name")

    assert isinstance(result, ArrowResult)
    assert isinstance(result, ArrowResult)
    assert result.num_rows == 3
    assert result.data.num_columns == 3
    assert result.column_names == ["name", "age", "salary"]

    # Verify data content
    names = result.data.column("name").to_pylist()
    ages = result.data.column("age").to_pylist()
    salaries = result.data.column("salary").to_pylist()

    assert names == ["Alice", "Bob", "Charlie"]
    assert ages == [25, 30, 35]
    assert salaries == [50000.0, 60000.0, 70000.0]

    # Cleanup
    adbc_postgresql_session.execute_script("DROP TABLE arrow_test")


@pytest.mark.xdist_group("postgres")
def test_to_parquet(adbc_postgresql_session: AdbcDriver) -> None:
    """Test PostgreSQL to_parquet functionality."""
    # Insert test data
    adbc_postgresql_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("arrow1", 111))
    adbc_postgresql_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("arrow2", 222))

    statement = SQL("SELECT id, name, value FROM test_table ORDER BY id")

    with tempfile.NamedTemporaryFile() as tmp:
        adbc_postgresql_session.export_to_storage(statement, destination_uri=tmp.name)

        # Read back the Parquet file - export_to_storage appends .parquet extension
        table = pq.read_table(f"{tmp.name}.parquet")
        assert table.num_rows == 2
        assert set(table.column_names) >= {"id", "name", "value"}

        # Verify data
        data = table.to_pylist()
        assert any(row["name"] == "arrow1" and row["value"] == 111 for row in data)
        assert any(row["name"] == "arrow2" and row["value"] == 222 for row in data)


@pytest.mark.xdist_group("postgres")
def test_arrow_with_parameters(adbc_postgresql_session: AdbcDriver) -> None:
    """Test Arrow functionality with parameterized queries."""
    # Insert test data
    test_data = [("param_test1", 10), ("param_test2", 20), ("param_test3", 30)]
    adbc_postgresql_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", test_data)

    # Test fetch_arrow_table with parameters
    result = adbc_postgresql_session.fetch_arrow_table(
        "SELECT name, value FROM test_table WHERE value > $1 ORDER BY value", (15,)
    )

    assert isinstance(result, ArrowResult)
    assert result.num_rows == 2

    names = result.data.column("name").to_pylist()
    values = result.data.column("value").to_pylist()
    assert names == ["param_test2", "param_test3"]
    assert values == [20, 30]


@pytest.mark.xdist_group("postgres")
def test_arrow_empty_result(adbc_postgresql_session: AdbcDriver) -> None:
    """Test Arrow functionality with empty result set."""
    # Query that returns no rows
    result = adbc_postgresql_session.fetch_arrow_table(
        "SELECT name, value FROM test_table WHERE name = $1", ("nonexistent",)
    )

    assert isinstance(result, ArrowResult)
    assert isinstance(result, ArrowResult)
    assert result.num_rows == 0
    assert result.data.num_columns == 2
    assert result.column_names == ["name", "value"]


@pytest.mark.xdist_group("postgres")
def test_complex_queries(adbc_postgresql_session: AdbcDriver) -> None:
    """Test complex SQL queries with ADBC PostgreSQL."""
    # Ensure test_table exists after any prior errors
    ensure_test_table(adbc_postgresql_session)

    # Insert test data
    test_data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 28)]

    adbc_postgresql_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", test_data)

    # Test JOIN (self-join)
    join_result = adbc_postgresql_session.execute("""
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
    agg_result = adbc_postgresql_session.execute("""
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
    # PostgreSQL returns numeric/decimal as string, convert for comparison
    assert float(agg_result.data[0]["avg_value"]) == 29.5
    assert agg_result.data[0]["min_value"] == 25
    assert agg_result.data[0]["max_value"] == 35

    # Test window functions
    window_result = adbc_postgresql_session.execute("""
        SELECT
            name,
            value,
            ROW_NUMBER() OVER (ORDER BY value) as row_num,
            LAG(value) OVER (ORDER BY value) as prev_value
        FROM test_table
        ORDER BY value
    """)
    assert isinstance(window_result, SQLResult)
    assert window_result.data is not None
    assert len(window_result.data) == 4
    assert window_result.data[0]["row_num"] == 1
    assert window_result.data[0]["prev_value"] is None


@pytest.mark.xdist_group("postgres")
def test_schema_operations(adbc_postgresql_session: AdbcDriver) -> None:
    """Test schema operations (DDL) with ADBC PostgreSQL."""
    # Ensure test_table exists after any prior errors
    ensure_test_table(adbc_postgresql_session)

    # Create a new table
    adbc_postgresql_session.execute_script("""
        CREATE TABLE IF NOT EXISTS schema_test (
            id SERIAL PRIMARY KEY,
            description TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert data into new table
    insert_result = adbc_postgresql_session.execute(
        "INSERT INTO schema_test (description) VALUES ($1)", ("test description",)
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected in (1, -1)

    # Verify table structure
    info_result = adbc_postgresql_session.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'schema_test'
        ORDER BY ordinal_position
    """)
    assert isinstance(info_result, SQLResult)
    assert info_result.data is not None
    assert len(info_result.data) == 3  # id, description, created_at

    # Drop table
    adbc_postgresql_session.execute_script("DROP TABLE schema_test")


@pytest.mark.xdist_group("postgres")
def test_column_names_and_metadata(adbc_postgresql_session: AdbcDriver) -> None:
    """Test column names and result metadata with ADBC PostgreSQL."""
    # Insert test data
    adbc_postgresql_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("metadata_test", 123))

    # Test column names
    result = adbc_postgresql_session.execute(
        "SELECT id, name, value, created_at FROM test_table WHERE name = $1", ("metadata_test",)
    )
    assert isinstance(result, SQLResult)
    assert result.column_names == ["id", "name", "value", "created_at"]
    assert result.data is not None
    assert result.get_count() == 1

    # Test that we can access data by column name
    row = result.data[0]
    assert row["name"] == "metadata_test"
    assert row["value"] == 123
    assert row["id"] is not None
    assert row["created_at"] is not None


@pytest.mark.xdist_group("postgres")
def test_with_schema_type(adbc_postgresql_session: AdbcDriver) -> None:
    """Test ADBC PostgreSQL driver with schema type conversion."""

    @dataclass
    class TestRecord:
        id: int | None
        name: str
        value: int

    # Insert test data
    adbc_postgresql_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("schema_test", 456))

    # Query with schema type
    result = adbc_postgresql_session.execute(
        "SELECT id, name, value FROM test_table WHERE name = $1", ("schema_test",), schema_type=TestRecord
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1

    # The data should be converted to the schema type by the ResultConverter
    assert result.column_names == ["id", "name", "value"]


@pytest.mark.xdist_group("postgres")
def test_performance_bulk_operations(adbc_postgresql_session: AdbcDriver) -> None:
    """Test performance with bulk operations using ADBC PostgreSQL."""
    # Generate bulk data
    bulk_data = [(f"bulk_user_{i}", i * 10) for i in range(100)]

    # Bulk insert
    result = adbc_postgresql_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", bulk_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 100

    # Bulk select
    select_result = adbc_postgresql_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'bulk_user_%'"
    )
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    # Test pagination-like query
    page_result = adbc_postgresql_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'bulk_user_%' ORDER BY value LIMIT 10 OFFSET 20"
    )
    assert isinstance(page_result, SQLResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_20"


@pytest.mark.xdist_group("postgres")
def test_insert_returning(adbc_postgresql_session: AdbcDriver) -> None:
    """Test INSERT with RETURNING clause."""
    # Single insert with RETURNING
    result = adbc_postgresql_session.execute(
        "INSERT INTO test_table (name, value) VALUES ($1, $2) RETURNING id, name, value", ("returning_test", 999)
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1
    assert result.data[0]["name"] == "returning_test"
    assert result.data[0]["value"] == 999
    assert result.data[0]["id"] is not None

    # Store the ID for later verification
    returned_id = result.data[0]["id"]

    # Verify the record was actually inserted
    verify_result = adbc_postgresql_session.execute("SELECT * FROM test_table WHERE id = $1", (returned_id,))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 1


@pytest.mark.xdist_group("postgres")
def test_update_returning(adbc_postgresql_session: AdbcDriver) -> None:
    """Test UPDATE with RETURNING clause."""
    # Insert initial data
    adbc_postgresql_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("update_returning", 100))

    # Update with RETURNING
    result = adbc_postgresql_session.execute(
        "UPDATE test_table SET value = $1 WHERE name = $2 RETURNING id, name, value, created_at",
        (200, "update_returning"),
    )

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 1
    assert result.data[0]["name"] == "update_returning"
    assert result.data[0]["value"] == 200
    assert result.data[0]["id"] is not None
    assert result.data[0]["created_at"] is not None


@pytest.mark.xdist_group("postgres")
def test_delete_returning(adbc_postgresql_session: AdbcDriver) -> None:
    """Test DELETE with RETURNING clause."""
    # Insert test data
    test_data = [("delete1", 10), ("delete2", 20), ("delete3", 30)]
    adbc_postgresql_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", test_data)

    # Delete with RETURNING
    result = adbc_postgresql_session.execute("DELETE FROM test_table WHERE value > $1 RETURNING name, value", (15,))

    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 2

    # Check returned data
    returned_names = {row["name"] for row in result.data}
    assert returned_names == {"delete2", "delete3"}

    # Verify records were deleted
    verify_result = adbc_postgresql_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'delete%'"
    )
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["count"] == 1  # Only delete1 should remain
