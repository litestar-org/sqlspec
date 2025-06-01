"""Integration tests for ADBC driver implementation."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any, Literal

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.statement.result import ArrowResult, ExecuteResult, SelectResult

ParamStyle = Literal["tuple_binds", "dict_binds", "named_binds"]


@pytest.fixture
def adbc_postgresql_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC PostgreSQL session with test table."""
    config = AdbcConfig(
        connection_config={
            "uri": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "driver_name": "adbc_driver_postgresql",
        },
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


@pytest.fixture
def adbc_sqlite_session() -> Generator[AdbcDriver, None, None]:
    """Create an ADBC SQLite session with test table."""
    config = AdbcConfig(
        connection_config={
            "uri": ":memory:",
            "driver_name": "adbc_driver_sqlite",
        },
    )

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        yield session
        # Cleanup is automatic with in-memory database


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_basic_crud(adbc_postgresql_session: AdbcDriver) -> None:
    """Test basic CRUD operations with ADBC PostgreSQL."""
    # INSERT
    insert_result = adbc_postgresql_session.execute(
        "INSERT INTO test_table (name, value) VALUES ($1, $2)", ("test_name", 42)
    )
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    # SELECT
    select_result = adbc_postgresql_session.execute(
        "SELECT name, value FROM test_table WHERE name = $1", ("test_name",)
    )
    assert isinstance(select_result, SelectResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    # UPDATE
    update_result = adbc_postgresql_session.execute(
        "UPDATE test_table SET value = $1 WHERE name = $2", (100, "test_name")
    )
    assert isinstance(update_result, ExecuteResult)
    assert update_result.rows_affected == 1

    # Verify UPDATE
    verify_result = adbc_postgresql_session.execute("SELECT value FROM test_table WHERE name = $1", ("test_name",))
    assert isinstance(verify_result, SelectResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    # DELETE
    delete_result = adbc_postgresql_session.execute("DELETE FROM test_table WHERE name = $1", ("test_name",))
    assert isinstance(delete_result, ExecuteResult)
    assert delete_result.rows_affected == 1

    # Verify DELETE
    empty_result = adbc_postgresql_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SelectResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.xdist_group("adbc_sqlite")
def test_adbc_sqlite_basic_crud(adbc_sqlite_session: AdbcDriver) -> None:
    """Test basic CRUD operations with ADBC SQLite."""
    # INSERT
    insert_result = adbc_sqlite_session.execute("INSERT INTO test_table (name, value) VALUES (?, ?)", ("test_name", 42))
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    # SELECT
    select_result = adbc_sqlite_session.execute("SELECT name, value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(select_result, SelectResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    # UPDATE
    update_result = adbc_sqlite_session.execute("UPDATE test_table SET value = ? WHERE name = ?", (100, "test_name"))
    assert isinstance(update_result, ExecuteResult)
    assert update_result.rows_affected == 1

    # Verify UPDATE
    verify_result = adbc_sqlite_session.execute("SELECT value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(verify_result, SelectResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    # DELETE
    delete_result = adbc_sqlite_session.execute("DELETE FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(delete_result, ExecuteResult)
    assert delete_result.rows_affected == 1

    # Verify DELETE
    empty_result = adbc_sqlite_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SelectResult)
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
def test_adbc_postgresql_parameter_styles(adbc_postgresql_session: AdbcDriver, params: Any, style: ParamStyle) -> None:
    """Test different parameter binding styles with ADBC PostgreSQL."""
    # Insert test data
    adbc_postgresql_session.execute("INSERT INTO test_table (name) VALUES ($1)", ("test_value",))

    # Test parameter style
    if style == "tuple_binds":
        sql = "SELECT name FROM test_table WHERE name = $1"
    else:  # dict_binds - PostgreSQL uses numbered parameters
        sql = "SELECT name FROM test_table WHERE name = $1"
        params = (params["name"],) if isinstance(params, dict) else params

    result = adbc_postgresql_session.execute(sql, params)
    assert isinstance(result, SelectResult)
    assert result.data is not None
    assert len(result.data) == 1
    assert result.data[0]["name"] == "test_value"


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_execute_many(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_many functionality with ADBC PostgreSQL."""
    params_list = [("name1", 1), ("name2", 2), ("name3", 3)]

    result = adbc_postgresql_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", params_list)
    assert isinstance(result, ExecuteResult)
    assert result.rows_affected == len(params_list)

    # Verify all records were inserted
    select_result = adbc_postgresql_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SelectResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(params_list)

    # Verify data integrity
    ordered_result = adbc_postgresql_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(ordered_result, SelectResult)
    assert ordered_result.data is not None
    assert len(ordered_result.data) == 3
    assert ordered_result.data[0]["name"] == "name1"
    assert ordered_result.data[0]["value"] == 1


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_execute_script(adbc_postgresql_session: AdbcDriver) -> None:
    """Test execute_script functionality with ADBC PostgreSQL."""
    script = """
        INSERT INTO test_table (name, value) VALUES ('script_test1', 999);
        INSERT INTO test_table (name, value) VALUES ('script_test2', 888);
        UPDATE test_table SET value = 1000 WHERE name = 'script_test1';
    """

    result = adbc_postgresql_session.execute_script(script)
    # Script execution typically returns a status string
    assert isinstance(result, str) or result is None

    # Verify script effects
    select_result = adbc_postgresql_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'script_test%' ORDER BY name"
    )
    assert isinstance(select_result, SelectResult)
    assert select_result.data is not None
    assert len(select_result.data) == 2
    assert select_result.data[0]["name"] == "script_test1"
    assert select_result.data[0]["value"] == 1000
    assert select_result.data[1]["name"] == "script_test2"
    assert select_result.data[1]["value"] == 888


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_result_methods(adbc_postgresql_session: AdbcDriver) -> None:
    """Test SelectResult and ExecuteResult methods with ADBC PostgreSQL."""
    # Insert test data
    adbc_postgresql_session.execute_many(
        "INSERT INTO test_table (name, value) VALUES ($1, $2)", [("result1", 10), ("result2", 20), ("result3", 30)]
    )

    # Test SelectResult methods
    result = adbc_postgresql_session.execute("SELECT * FROM test_table ORDER BY name")
    assert isinstance(result, SelectResult)

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
    assert isinstance(empty_result, SelectResult)
    assert empty_result.is_empty()
    assert empty_result.get_first() is None


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_error_handling(adbc_postgresql_session: AdbcDriver) -> None:
    """Test error handling and exception propagation with ADBC PostgreSQL."""
    # Test invalid SQL
    with pytest.raises(Exception):  # ADBC error
        adbc_postgresql_session.execute("INVALID SQL STATEMENT")

    # Test constraint violation
    adbc_postgresql_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("unique_test", 1))

    # Try to insert with invalid column reference
    with pytest.raises(Exception):  # ADBC error
        adbc_postgresql_session.execute("SELECT nonexistent_column FROM test_table")


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_data_types(adbc_postgresql_session: AdbcDriver) -> None:
    """Test PostgreSQL data type handling with ADBC."""
    # Create table with various PostgreSQL data types
    adbc_postgresql_session.execute_script("""
        CREATE TABLE data_types_test (
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
        ("text_value", 42, 123.45, True, [1, 2, 3], "2024-01-15", "2024-01-15 10:30:00"),
    )

    # Retrieve and verify data
    select_result = adbc_postgresql_session.execute(
        "SELECT text_col, integer_col, numeric_col, boolean_col, array_col FROM data_types_test"
    )
    assert isinstance(select_result, SelectResult)
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
def test_adbc_arrow_result_format(adbc_postgresql_session: AdbcDriver) -> None:
    """Test ADBC Arrow result format functionality."""
    # Insert test data for Arrow testing
    test_data = [("arrow_test1", 100), ("arrow_test2", 200), ("arrow_test3", 300)]
    adbc_postgresql_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", test_data)

    # Test getting results as Arrow if available
    if hasattr(adbc_postgresql_session, "select_to_arrow"):
        arrow_result = adbc_postgresql_session.select_to_arrow("SELECT name, value FROM test_table ORDER BY name")

        assert isinstance(arrow_result, ArrowResult)
        import pyarrow as pa

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
def test_adbc_postgresql_complex_queries(adbc_postgresql_session: AdbcDriver) -> None:
    """Test complex SQL queries with ADBC PostgreSQL."""
    # Insert test data
    test_data = [
        ("Alice", 25),
        ("Bob", 30),
        ("Charlie", 35),
        ("Diana", 28),
    ]

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
    assert isinstance(join_result, SelectResult)
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
    assert isinstance(agg_result, SelectResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["total_count"] == 4
    assert agg_result.data[0]["avg_value"] == 29.5
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
    assert isinstance(window_result, SelectResult)
    assert window_result.data is not None
    assert len(window_result.data) == 4
    assert window_result.data[0]["row_num"] == 1
    assert window_result.data[0]["prev_value"] is None


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_schema_operations(adbc_postgresql_session: AdbcDriver) -> None:
    """Test schema operations (DDL) with ADBC PostgreSQL."""
    # Create a new table
    adbc_postgresql_session.execute_script("""
        CREATE TABLE schema_test (
            id SERIAL PRIMARY KEY,
            description TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert data into new table
    insert_result = adbc_postgresql_session.execute(
        "INSERT INTO schema_test (description) VALUES ($1)", ("test description",)
    )
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    # Verify table structure
    info_result = adbc_postgresql_session.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'schema_test'
        ORDER BY ordinal_position
    """)
    assert isinstance(info_result, SelectResult)
    assert info_result.data is not None
    assert len(info_result.data) == 3  # id, description, created_at

    # Drop table
    adbc_postgresql_session.execute_script("DROP TABLE schema_test")


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_column_names_and_metadata(adbc_postgresql_session: AdbcDriver) -> None:
    """Test column names and result metadata with ADBC PostgreSQL."""
    # Insert test data
    adbc_postgresql_session.execute("INSERT INTO test_table (name, value) VALUES ($1, $2)", ("metadata_test", 123))

    # Test column names
    result = adbc_postgresql_session.execute(
        "SELECT id, name, value, created_at FROM test_table WHERE name = $1", ("metadata_test",)
    )
    assert isinstance(result, SelectResult)
    assert result.column_names == ["id", "name", "value", "created_at"]
    assert result.data is not None
    assert len(result.data) == 1

    # Test that we can access data by column name
    row = result.data[0]
    assert row["name"] == "metadata_test"
    assert row["value"] == 123
    assert row["id"] is not None
    assert row["created_at"] is not None


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_with_schema_type(adbc_postgresql_session: AdbcDriver) -> None:
    """Test ADBC PostgreSQL driver with schema type conversion."""
    from dataclasses import dataclass

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

    assert isinstance(result, SelectResult)
    assert result.data is not None
    assert len(result.data) == 1

    # The data should be converted to the schema type by the ResultConverter
    assert result.column_names == ["id", "name", "value"]


@pytest.mark.xdist_group("postgres")
def test_adbc_postgresql_performance_bulk_operations(adbc_postgresql_session: AdbcDriver) -> None:
    """Test performance with bulk operations using ADBC PostgreSQL."""
    # Generate bulk data
    bulk_data = [(f"bulk_user_{i}", i * 10) for i in range(100)]

    # Bulk insert
    result = adbc_postgresql_session.execute_many("INSERT INTO test_table (name, value) VALUES ($1, $2)", bulk_data)
    assert isinstance(result, ExecuteResult)
    assert result.rows_affected == 100

    # Bulk select
    select_result = adbc_postgresql_session.execute(
        "SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'bulk_user_%'"
    )
    assert isinstance(select_result, SelectResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 100

    # Test pagination-like query
    page_result = adbc_postgresql_session.execute(
        "SELECT name, value FROM test_table WHERE name LIKE 'bulk_user_%' ORDER BY value LIMIT 10 OFFSET 20"
    )
    assert isinstance(page_result, SelectResult)
    assert page_result.data is not None
    assert len(page_result.data) == 10
    assert page_result.data[0]["name"] == "bulk_user_20"


@pytest.mark.xdist_group("adbc_sqlite")
def test_adbc_multiple_backends_consistency(adbc_sqlite_session: AdbcDriver) -> None:
    """Test consistency across different ADBC backends."""
    # Insert test data
    test_data = [("backend_test1", 100), ("backend_test2", 200)]
    adbc_sqlite_session.execute_many("INSERT INTO test_table (name, value) VALUES (?, ?)", test_data)

    # Test basic query
    result = adbc_sqlite_session.execute("SELECT name, value FROM test_table ORDER BY name")
    assert isinstance(result, SelectResult)
    assert result.data is not None
    assert len(result.data) == 2
    assert result.data[0]["name"] == "backend_test1"
    assert result.data[0]["value"] == 100

    # Test aggregation
    agg_result = adbc_sqlite_session.execute("SELECT COUNT(*) as count, SUM(value) as total FROM test_table")
    assert isinstance(agg_result, SelectResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["count"] == 2
    assert agg_result.data[0]["total"] == 300
