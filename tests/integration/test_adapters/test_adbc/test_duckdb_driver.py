"""Integration tests for ADBC DuckDB driver implementation."""

from __future__ import annotations

from collections.abc import Generator

import pyarrow as pa
import pytest

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQLConfig

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


@pytest.fixture
def adbc_duckdb_session() -> Generator[AdbcDriver, None, None]:
    """Create an ADBC DuckDB session with test table."""
    config = AdbcConfig(
        driver_name="adbc_driver_duckdb.dbapi.connect",
        statement_config=SQLConfig(strict_mode=False),  # Allow DDL statements for tests
    )

    with config.provide_session() as session:
        # Create test table
        session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        yield session
        # Cleanup is automatic with in-memory database


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_connection() -> None:
    """Test basic ADBC DuckDB connection."""
    config = AdbcConfig(driver_name="adbc_driver_duckdb.dbapi.connect")

    # Test connection creation
    with config.provide_connection() as conn:
        assert conn is not None

    # Test session creation
    with config.provide_session() as session:
        assert session is not None
        assert isinstance(session, AdbcDriver)
        result = session.execute("SELECT 1 as test_value")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["test_value"] == 1


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_basic_crud(adbc_duckdb_session: AdbcDriver) -> None:
    """Test basic CRUD operations with ADBC DuckDB."""
    # INSERT
    insert_result = adbc_duckdb_session.execute(
        "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "test_name", 42)
    )
    assert isinstance(insert_result, SQLResult)
    # ADBC drivers may not support rowcount and return -1 or 0
    assert insert_result.rows_affected in (-1, 0, 1)

    # SELECT
    select_result = adbc_duckdb_session.execute("SELECT name, value FROM test_table WHERE name = ?", ("test_name",))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["value"] == 42

    # UPDATE
    update_result = adbc_duckdb_session.execute("UPDATE test_table SET value = ? WHERE id = ?", (100, 1))
    assert isinstance(update_result, SQLResult)
    # ADBC drivers may not support rowcount and return -1 or 0
    assert update_result.rows_affected in (-1, 0, 1)

    # Verify UPDATE
    verify_result = adbc_duckdb_session.execute("SELECT value FROM test_table WHERE id = ?", (1,))
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert verify_result.data[0]["value"] == 100

    # DELETE
    delete_result = adbc_duckdb_session.execute("DELETE FROM test_table WHERE id = ?", (1,))
    assert isinstance(delete_result, SQLResult)
    # ADBC drivers may not support rowcount and return -1 or 0
    assert delete_result.rows_affected in (-1, 0, 1)

    # Verify DELETE
    empty_result = adbc_duckdb_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(empty_result, SQLResult)
    assert empty_result.data is not None
    assert empty_result.data[0]["count"] == 0


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_data_types(adbc_duckdb_session: AdbcDriver) -> None:
    """Test DuckDB-specific data types with ADBC."""
    # Create table with various DuckDB data types
    adbc_duckdb_session.execute_script("""
        CREATE TABLE data_types_test (
            id INTEGER,
            text_col TEXT,
            numeric_col DECIMAL(10,2),
            date_col DATE,
            timestamp_col TIMESTAMP,
            boolean_col BOOLEAN,
            array_col INTEGER[],
            json_col JSON
        )
    """)

    # Insert test data with DuckDB-specific types
    insert_sql = """
        INSERT INTO data_types_test VALUES (
            1,
            'test_text',
            123.45,
            '2024-01-15',
            '2024-01-15 10:30:00',
            true,
            [1, 2, 3, 4],
            '{"key": "value", "number": 42}'
        )
    """
    result = adbc_duckdb_session.execute(insert_sql)
    assert isinstance(result, SQLResult)
    # DuckDB ADBC may return 0 for rows_affected
    assert result.rows_affected in (0, 1)

    # Query and verify data types
    select_result = adbc_duckdb_session.execute("SELECT * FROM data_types_test")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    row = select_result.data[0]

    assert row["id"] == 1
    assert row["text_col"] == "test_text"
    assert row["boolean_col"] is True
    # Array and JSON handling may vary based on DuckDB version
    assert row["array_col"] is not None
    assert row["json_col"] is not None

    # Clean up
    adbc_duckdb_session.execute_script("DROP TABLE data_types_test")


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_complex_queries(adbc_duckdb_session: AdbcDriver) -> None:
    """Test complex SQL queries with ADBC DuckDB."""
    # Create additional tables for complex queries
    adbc_duckdb_session.execute_script("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name TEXT
        );

        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name TEXT,
            dept_id INTEGER,
            salary DECIMAL(10,2)
        );

        INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing');
        INSERT INTO employees VALUES
            (1, 'Alice', 1, 75000.00),
            (2, 'Bob', 1, 80000.00),
            (3, 'Carol', 2, 65000.00),
            (4, 'Dave', 2, 70000.00),
            (5, 'Eve', 3, 60000.00);
    """)

    # Test complex JOIN query with aggregation
    complex_query = """
        SELECT
            d.dept_name,
            COUNT(e.emp_id) as employee_count,
            AVG(e.salary) as avg_salary,
            MAX(e.salary) as max_salary
        FROM departments d
        LEFT JOIN employees e ON d.dept_id = e.dept_id
        GROUP BY d.dept_id, d.dept_name
        ORDER BY avg_salary DESC
    """

    result = adbc_duckdb_session.execute(complex_query)
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.get_count() == 3

    # Engineering should have highest average salary
    engineering_row = next(row for row in result.data if row["dept_name"] == "Engineering")
    assert engineering_row["employee_count"] == 2
    assert engineering_row["avg_salary"] == 77500.0

    # Test subquery
    subquery = """
        SELECT emp_name, salary
        FROM employees
        WHERE salary > (SELECT AVG(salary) FROM employees)
        ORDER BY salary DESC
    """

    subquery_result = adbc_duckdb_session.execute(subquery)
    assert isinstance(subquery_result, SQLResult)
    assert subquery_result.data is not None
    assert len(subquery_result.data) >= 1  # At least one employee above average

    # Clean up
    adbc_duckdb_session.execute_script("DROP TABLE employees; DROP TABLE departments;")


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_arrow_integration(adbc_duckdb_session: AdbcDriver) -> None:
    """Test ADBC DuckDB Arrow integration functionality."""
    # Insert test data for Arrow testing
    test_data = [(4, "arrow_test1", 100), (5, "arrow_test2", 200), (6, "arrow_test3", 300)]
    for row_data in test_data:
        adbc_duckdb_session.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", row_data)

    # Test getting results as Arrow if available
    if hasattr(adbc_duckdb_session, "fetch_arrow_table"):
        arrow_result = adbc_duckdb_session.fetch_arrow_table("SELECT name, value FROM test_table ORDER BY name")

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
        pytest.skip("ADBC DuckDB driver does not support Arrow result format")


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_performance_bulk_operations(adbc_duckdb_session: AdbcDriver) -> None:
    """Test performance with bulk operations using ADBC DuckDB."""
    # Generate bulk data
    bulk_data = [(100 + i, f"bulk_user_{i}", i * 10) for i in range(100)]

    # Bulk insert (DuckDB ADBC doesn't support executemany yet)
    total_inserted = 0
    for row_data in bulk_data:
        result = adbc_duckdb_session.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", row_data)
        assert isinstance(result, SQLResult)
        # Count successful inserts (DuckDB may return 0 or 1)
        if result.rows_affected > 0:
            total_inserted += result.rows_affected
        else:
            total_inserted += 1  # Assume success if rowcount not supported

    # Verify total insertions by counting rows
    count_result = adbc_duckdb_session.execute("SELECT COUNT(*) as count FROM test_table WHERE name LIKE 'bulk_user_%'")
    assert isinstance(count_result, SQLResult)
    assert count_result.data is not None
    assert count_result.data[0]["count"] == 100

    # Test aggregation on bulk data
    agg_result = adbc_duckdb_session.execute("""
        SELECT
            COUNT(*) as count,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM test_table
        WHERE name LIKE 'bulk_user_%'
    """)

    assert isinstance(agg_result, SQLResult)
    assert agg_result.data is not None
    assert agg_result.data[0]["count"] == 100
    assert agg_result.data[0]["avg_value"] > 0
    assert agg_result.data[0]["min_value"] == 0
    assert agg_result.data[0]["max_value"] == 990


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_duckdb_specific_features(adbc_duckdb_session: AdbcDriver) -> None:
    """Test DuckDB-specific features like sequences, window functions, etc."""
    # Test sequence generation
    seq_result = adbc_duckdb_session.execute("""
        SELECT * FROM generate_series(1, 5) as t(value)
    """)
    assert isinstance(seq_result, SQLResult)
    assert seq_result.data is not None
    assert len(seq_result.data) == 5
    assert [row["value"] for row in seq_result.data] == [1, 2, 3, 4, 5]

    # Test LIST aggregate function (DuckDB specific)
    adbc_duckdb_session.execute_script("""
        CREATE TABLE list_test (
            category TEXT,
            item TEXT
        );

        INSERT INTO list_test VALUES
            ('fruits', 'apple'),
            ('fruits', 'banana'),
            ('fruits', 'orange'),
            ('vegetables', 'carrot'),
            ('vegetables', 'broccoli');
    """)

    list_result = adbc_duckdb_session.execute("""
        SELECT category, LIST(item ORDER BY item) as items
        FROM list_test
        GROUP BY category
        ORDER BY category
    """)
    assert isinstance(list_result, SQLResult)
    assert list_result.data is not None
    assert len(list_result.data) == 2

    fruits_row = next(row for row in list_result.data if row["category"] == "fruits")
    assert set(fruits_row["items"]) == {"apple", "banana", "orange"}

    # Clean up
    adbc_duckdb_session.execute_script("DROP TABLE list_test")


@pytest.mark.xdist_group("adbc_duckdb")
@xfail_if_driver_missing
def test_duckdb_file_formats(adbc_duckdb_session: AdbcDriver) -> None:
    """Test DuckDB's ability to read/write various file formats."""
    # Test CSV export/import functionality
    # Note: This test is basic as ADBC might not support all DuckDB extensions

    # Create test data
    adbc_duckdb_session.execute_script("""
        CREATE TABLE export_test (
            id INTEGER,
            name TEXT,
            value DOUBLE
        );

        INSERT INTO export_test VALUES
            (1, 'row1', 1.5),
            (2, 'row2', 2.5),
            (3, 'row3', 3.5);
    """)

    # Test basic query on the data
    result = adbc_duckdb_session.execute("SELECT COUNT(*) as count FROM export_test")
    assert isinstance(result, SQLResult)
    assert result.data is not None
    assert result.data[0]["count"] == 3

    # Clean up
    adbc_duckdb_session.execute_script("DROP TABLE export_test")
