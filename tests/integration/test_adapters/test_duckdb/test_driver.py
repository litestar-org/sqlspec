"""Test DuckDB driver implementation."""

from __future__ import annotations

import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture
def duckdb_session() -> Generator[DuckDBDriver, None, None]:
    """Create a DuckDB session with a test table.

    Returns:
        A DuckDB session with a test table.
    """
    adapter = DuckDBConfig()
    with adapter.provide_session() as session:
        session.execute_script("CREATE SEQUENCE IF NOT EXISTS test_id_seq START 1")
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY DEFAULT nextval('test_id_seq'),
                name TEXT NOT NULL
            )
        """
        session.execute_script(create_table_sql)
        yield session
        # Clean up
        session.execute_script("DROP TABLE IF EXISTS test_table")
        session.execute_script("DROP SEQUENCE IF EXISTS test_id_seq")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name", 1), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name", "id": 1}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("duckdb")
def test_insert(duckdb_session: DuckDBDriver, params: Any, style: ParamStyle) -> None:
    """Test inserting data with different parameter styles."""
    if style == "tuple_binds":
        sql = "INSERT INTO test_table (name, id) VALUES (?, ?)"
    else:
        sql = "INSERT INTO test_table (name, id) VALUES (:name, :id)"

    result = duckdb_session.execute(sql, params)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    # Verify insertion
    select_result = duckdb_session.execute("SELECT name, id FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["id"] == 1

    duckdb_session.execute_script("DELETE FROM test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name", 1), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name", "id": 1}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("duckdb")
def test_select(duckdb_session: DuckDBDriver, params: Any, style: ParamStyle) -> None:
    """Test selecting data with different parameter styles."""
    # Insert test record
    if style == "tuple_binds":
        insert_sql = "INSERT INTO test_table (name, id) VALUES (?, ?)"
    else:
        insert_sql = "INSERT INTO test_table (name, id) VALUES (:name, :id)"

    insert_result = duckdb_session.execute(insert_sql, params)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Test select
    select_result = duckdb_session.execute("SELECT name, id FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["id"] == 1

    # Test select with a WHERE clause
    if style == "tuple_binds":
        select_where_sql = "SELECT id FROM test_table WHERE name = ?"
        where_params = "test_name"
    else:
        select_where_sql = "SELECT id FROM test_table WHERE name = :name"
        where_params = {"name": "test_name"}

    where_result = duckdb_session.execute(select_where_sql, where_params)
    assert isinstance(where_result, SQLResult)
    assert where_result.data is not None
    assert len(where_result.data) == 1
    assert where_result.data[0]["id"] == 1

    duckdb_session.execute_script("DELETE FROM test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name", 1), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name", "id": 1}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("duckdb")
def test_select_value(duckdb_session: DuckDBDriver, params: Any, style: ParamStyle) -> None:
    """Test select value with different parameter styles."""
    # Insert test record
    if style == "tuple_binds":
        insert_sql = "INSERT INTO test_table (name, id) VALUES (?, ?)"
    else:
        insert_sql = "INSERT INTO test_table (name, id) VALUES (:name, :id)"

    insert_result = duckdb_session.execute(insert_sql, params)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Test select value
    if style == "tuple_binds":
        value_sql = "SELECT name FROM test_table WHERE id = ?"
        value_params = 1
    else:
        value_sql = "SELECT name FROM test_table WHERE id = :id"
        value_params = {"id": 1}

    value_result = duckdb_session.execute(value_sql, value_params)
    assert isinstance(value_result, SQLResult)
    assert value_result.data is not None
    assert len(value_result.data) == 1
    assert value_result.column_names is not None

    # Extract single value using column name
    value = value_result.data[0][value_result.column_names[0]]
    assert value == "test_name"

    duckdb_session.execute_script("DELETE FROM test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("arrow_name", 1), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "arrow_name", "id": 1}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("duckdb")
def test_select_arrow(duckdb_session: DuckDBDriver, params: Any, style: ParamStyle) -> None:
    """Test selecting data as an Arrow Table."""
    # Insert test record
    if style == "tuple_binds":
        insert_sql = "INSERT INTO test_table (name, id) VALUES (?, ?)"
    else:
        insert_sql = "INSERT INTO test_table (name, id) VALUES (:name, :id)"

    insert_result = duckdb_session.execute(insert_sql, params)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Test select_arrow using mixins
    if hasattr(duckdb_session, "fetch_arrow_table"):
        select_sql = "SELECT name, id FROM test_table WHERE id = 1"
        arrow_result = duckdb_session.fetch_arrow_table(select_sql)

        assert isinstance(arrow_result, ArrowResult)
        arrow_table = arrow_result.data
        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows == 1
        assert arrow_table.num_columns == 2
        assert arrow_table.column_names == ["name", "id"]
        assert arrow_table.column("name").to_pylist() == ["arrow_name"]
        assert arrow_table.column("id").to_pylist() == [1]
    else:
        pytest.skip("DuckDB driver does not support Arrow operations")

    duckdb_session.execute_script("DELETE FROM test_table")


@pytest.mark.xdist_group("duckdb")
def test_execute_many_insert(duckdb_session: DuckDBDriver) -> None:
    """Test execute_many functionality for batch inserts."""
    insert_sql = "INSERT INTO test_table (name, id) VALUES (?, ?)"
    params_list = [("name1", 10), ("name2", 20), ("name3", 30)]

    result = duckdb_session.execute_many(insert_sql, params_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(params_list)

    # Verify all records were inserted
    select_result = duckdb_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(params_list)


@pytest.mark.xdist_group("duckdb")
def test_execute_script(duckdb_session: DuckDBDriver) -> None:
    """Test execute_script functionality for multi-statement scripts."""
    script = """
    INSERT INTO test_table (name, id) VALUES ('script_name1', 100);
    INSERT INTO test_table (name, id) VALUES ('script_name2', 200);
    """

    result = duckdb_session.execute_script(script)
    assert isinstance(result, SQLResult)

    # Verify script executed successfully
    select_result = duckdb_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 2


@pytest.mark.xdist_group("duckdb")
def test_update_operation(duckdb_session: DuckDBDriver) -> None:
    """Test UPDATE operations."""
    # Insert a record first
    insert_result = duckdb_session.execute("INSERT INTO test_table (name, id) VALUES (?, ?)", ("original_name", 42))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Update the record
    update_result = duckdb_session.execute("UPDATE test_table SET name = ? WHERE id = ?", ("updated_name", 42))
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    # Verify the update
    select_result = duckdb_session.execute("SELECT name FROM test_table WHERE id = ?", (42))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["name"] == "updated_name"


@pytest.mark.xdist_group("duckdb")
def test_delete_operation(duckdb_session: DuckDBDriver) -> None:
    """Test DELETE operations."""
    # Insert a record first
    insert_result = duckdb_session.execute("INSERT INTO test_table (name, id) VALUES (?, ?)", ("to_delete", 99))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    # Delete the record
    delete_result = duckdb_session.execute("DELETE FROM test_table WHERE id = ?", (99))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    # Verify the deletion
    select_result = duckdb_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 0


@pytest.mark.xdist_group("duckdb")
def test_duckdb_data_types(duckdb_session: DuckDBDriver) -> None:
    """Test DuckDB-specific data types and functionality."""
    # Create table with various DuckDB data types
    duckdb_session.execute_script("""
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
    result = duckdb_session.execute(insert_sql)
    assert result.rows_affected == 1

    # Query and verify data types
    select_result = duckdb_session.execute("SELECT * FROM data_types_test")
    assert len(select_result.data) == 1
    row = select_result.data[0]

    assert row["id"] == 1
    assert row["text_col"] == "test_text"
    assert row["boolean_col"] is True
    # Array and JSON handling may vary based on DuckDB version
    assert row["array_col"] is not None
    assert row["json_col"] is not None

    # Clean up
    duckdb_session.execute_script("DROP TABLE data_types_test")


@pytest.mark.xdist_group("duckdb")
def test_duckdb_complex_queries(duckdb_session: DuckDBDriver) -> None:
    """Test complex SQL queries with DuckDB."""
    # Create additional tables for complex queries
    duckdb_session.execute_script("""
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

    result = duckdb_session.execute(complex_query)
    assert result.total_count == 3

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

    subquery_result = duckdb_session.execute(subquery)
    assert len(subquery_result.data) >= 1  # At least one employee above average

    # Clean up
    duckdb_session.execute_script("DROP TABLE employees; DROP TABLE departments;")


@pytest.mark.xdist_group("duckdb")
def test_duckdb_window_functions(duckdb_session: DuckDBDriver) -> None:
    """Test DuckDB window functions."""
    # Create test data for window functions
    duckdb_session.execute_script("""
        CREATE TABLE sales_data (
            id INTEGER,
            product TEXT,
            sales_amount DECIMAL(10,2),
            sale_date DATE
        );

        INSERT INTO sales_data VALUES
            (1, 'Product A', 1000.00, '2024-01-01'),
            (2, 'Product B', 1500.00, '2024-01-02'),
            (3, 'Product A', 1200.00, '2024-01-03'),
            (4, 'Product C', 800.00, '2024-01-04'),
            (5, 'Product B', 1800.00, '2024-01-05');
    """)

    # Test window function with ranking
    window_query = """
        SELECT
            product,
            sales_amount,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY sales_amount DESC) as rank_in_product,
            SUM(sales_amount) OVER (PARTITION BY product) as total_product_sales,
            LAG(sales_amount) OVER (ORDER BY sale_date) as previous_sale
        FROM sales_data
        ORDER BY product, sales_amount DESC
    """

    result = duckdb_session.execute(window_query)
    assert result.total_count == 5

    # Verify window function results
    product_a_rows = [row for row in result.data if row["product"] == "Product A"]
    assert len(product_a_rows) == 2
    assert product_a_rows[0]["rank_in_product"] == 1  # Highest sales amount ranked 1

    # Clean up
    duckdb_session.execute_script("DROP TABLE sales_data")


@pytest.mark.xdist_group("duckdb")
def test_duckdb_schema_operations(duckdb_session: DuckDBDriver) -> None:
    """Test DuckDB schema operations (DDL)."""
    # Test CREATE TABLE
    create_result = duckdb_session.execute("""
        CREATE TABLE schema_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    assert isinstance(create_result, SQLResult)

    # Test ALTER TABLE
    alter_result = duckdb_session.execute("ALTER TABLE schema_test ADD COLUMN email TEXT")
    assert isinstance(alter_result, SQLResult)

    # Test CREATE INDEX
    index_result = duckdb_session.execute("CREATE INDEX idx_schema_test_name ON schema_test(name)")
    assert isinstance(index_result, SQLResult)

    # Verify table structure by inserting and querying
    insert_result = duckdb_session.execute(
        "INSERT INTO schema_test (id, name, email) VALUES (?, ?, ?)", [1, "Test User", "test@example.com"]
    )
    assert insert_result.rows_affected == 1

    select_result = duckdb_session.execute("SELECT id, name, email FROM schema_test")
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "Test User"
    assert select_result.data[0]["email"] == "test@example.com"

    # Test DROP operations
    duckdb_session.execute("DROP INDEX idx_schema_test_name")
    duckdb_session.execute("DROP TABLE schema_test")


@pytest.mark.xdist_group("duckdb")
def test_duckdb_performance_bulk_operations(duckdb_session: DuckDBDriver) -> None:
    """Test DuckDB performance with bulk operations."""
    # Create table for bulk testing
    duckdb_session.execute_script("""
        CREATE TABLE bulk_test (
            id INTEGER,
            value TEXT,
            number DECIMAL(10,2)
        )
    """)

    # Generate bulk data (100 records)
    bulk_data = [(i, f"value_{i}", float(i * 10.5)) for i in range(1, 101)]

    # Test bulk insert
    bulk_insert_sql = "INSERT INTO bulk_test (id, value, number) VALUES (?, ?, ?)"
    bulk_result = duckdb_session.execute_many(bulk_insert_sql, bulk_data)
    assert bulk_result.rows_affected == 100

    # Test bulk query performance
    bulk_select_result = duckdb_session.execute("SELECT COUNT(*) as total FROM bulk_test")
    assert bulk_select_result.data[0]["total"] == 100

    # Test aggregation on bulk data
    agg_result = duckdb_session.execute("""
        SELECT
            COUNT(*) as count,
            AVG(number) as avg_number,
            MIN(number) as min_number,
            MAX(number) as max_number
        FROM bulk_test
    """)

    assert agg_result.data[0]["count"] == 100
    assert agg_result.data[0]["avg_number"] > 0
    assert agg_result.data[0]["min_number"] == 10.5
    assert agg_result.data[0]["max_number"] == 1050.0

    # Clean up
    duckdb_session.execute_script("DROP TABLE bulk_test")


@pytest.mark.xdist_group("duckdb")
def test_duckdb_arrow_integration_comprehensive(duckdb_session: DuckDBDriver) -> None:
    """Test comprehensive Arrow integration with DuckDB."""
    if not hasattr(duckdb_session, "fetch_arrow_table"):
        pytest.skip("DuckDB driver does not support Arrow operations")

    # Create table with various data types for Arrow testing
    duckdb_session.execute_script("""
        CREATE TABLE arrow_test (
            id INTEGER,
            name TEXT,
            value DOUBLE,
            active BOOLEAN,
            created_date DATE
        );

        INSERT INTO arrow_test VALUES
            (1, 'Alice', 123.45, true, '2024-01-01'),
            (2, 'Bob', 234.56, false, '2024-01-02'),
            (3, 'Carol', 345.67, true, '2024-01-03'),
            (4, 'Dave', 456.78, false, '2024-01-04'),
            (5, 'Eve', 567.89, true, '2024-01-05');
    """)

    # Test Arrow result with filtering
    arrow_result = duckdb_session.fetch_arrow_table(
        "SELECT id, name, value FROM arrow_test WHERE active = ? ORDER BY id", parameters=[True]
    )

    assert isinstance(arrow_result, ArrowResult)
    arrow_table = arrow_result.data
    assert isinstance(arrow_table, pa.Table)
    assert arrow_table.num_rows == 3  # 3 active records
    assert arrow_table.num_columns == 3
    assert arrow_table.column_names == ["id", "name", "value"]

    # Verify Arrow data
    ids = arrow_table.column("id").to_pylist()
    names = arrow_table.column("name").to_pylist()
    values = arrow_table.column("value").to_pylist()

    assert ids == [1, 3, 5]
    assert names == ["Alice", "Carol", "Eve"]
    assert values == [123.45, 345.67, 567.89]

    # Test Arrow with aggregation
    agg_arrow_result = duckdb_session.fetch_arrow_table("""
        SELECT
            active,
            COUNT(*) as count,
            AVG(value) as avg_value
        FROM arrow_test
        GROUP BY active
        ORDER BY active
    """)

    agg_table = agg_arrow_result.data
    assert agg_table.num_rows == 2  # true and false groups
    assert agg_table.num_columns == 3

    # Clean up
    duckdb_session.execute_script("DROP TABLE arrow_test")


@pytest.mark.xdist_group("duckdb")
def test_duckdb_error_handling_and_edge_cases(duckdb_session: DuckDBDriver) -> None:
    """Test DuckDB error handling and edge cases."""
    # Test invalid SQL
    with pytest.raises(Exception):
        duckdb_session.execute("INVALID SQL STATEMENT")

    # Test constraint violation
    duckdb_session.execute_script("""
        CREATE TABLE constraint_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)

    # Test NOT NULL constraint violation
    with pytest.raises(Exception):
        duckdb_session.execute("INSERT INTO constraint_test (id) VALUES (1)")

    # Test valid insert after constraint error
    valid_result = duckdb_session.execute("INSERT INTO constraint_test (id, name) VALUES (?, ?)", [1, "Valid Name"])
    assert valid_result.rows_affected == 1

    # Test duplicate primary key
    with pytest.raises(Exception):
        duckdb_session.execute("INSERT INTO constraint_test (id, name) VALUES (?, ?)", [1, "Duplicate ID"])

    # Clean up
    duckdb_session.execute_script("DROP TABLE constraint_test")


@pytest.mark.xdist_group("duckdb")
def test_duckdb_with_schema_type_conversion(duckdb_session: DuckDBDriver) -> None:
    """Test DuckDB driver with schema type conversion."""
    from dataclasses import dataclass

    @dataclass
    class TestRecord:
        id: int
        name: str
        value: float | None = None

    # Create test data
    duckdb_session.execute_script("""
        CREATE TABLE schema_conversion_test (
            id INTEGER,
            name TEXT,
            value DOUBLE
        );

        INSERT INTO schema_conversion_test VALUES
            (1, 'Record 1', 100.5),
            (2, 'Record 2', 200.75),
            (3, 'Record 3', NULL);
    """)

    # Test schema type conversion
    result = duckdb_session.execute(
        "SELECT id, name, value FROM schema_conversion_test ORDER BY id", schema_type=TestRecord
    )

    assert isinstance(result, SQLResult)
    assert result.total_count == 3

    # Verify converted data types
    for i, record in enumerate(result.data, 1):
        assert isinstance(record, TestRecord)
        assert record.id == i
        assert record.name == f"Record {i}"
        if i < 3:
            assert record.value is not None
        else:
            assert record.value is None

    # Clean up
    duckdb_session.execute_script("DROP TABLE schema_conversion_test")


@pytest.mark.xdist_group("duckdb")
def test_duckdb_result_methods_comprehensive(duckdb_session: DuckDBDriver) -> None:
    """Test comprehensive SelectResult and ExecuteResult methods."""
    # Test SelectResult methods
    duckdb_session.execute_script("""
        CREATE TABLE result_methods_test (
            id INTEGER,
            category TEXT,
            value INTEGER
        );

        INSERT INTO result_methods_test VALUES
            (1, 'A', 10),
            (2, 'B', 20),
            (3, 'A', 30),
            (4, 'C', 40);
    """)

    # Test SelectResult methods
    select_result = duckdb_session.execute("SELECT * FROM result_methods_test ORDER BY id")

    # Test get_count()
    assert select_result.get_count() == 4

    # Test get_first()
    first_row = select_result.get_first()
    assert first_row is not None
    assert first_row["id"] == 1

    # Test is_empty()
    assert not select_result.is_empty()

    # Test empty result
    empty_result = duckdb_session.execute("SELECT * FROM result_methods_test WHERE id > 100")
    assert empty_result.is_empty()
    assert empty_result.get_count() == 0
    assert empty_result.get_first() is None

    # Test ExecuteResult methods
    update_result = duckdb_session.execute("UPDATE result_methods_test SET value = value * 2 WHERE category = 'A'")

    # Test ExecuteResult methods
    assert isinstance(update_result, SQLResult)
    assert update_result.get_affected_count() == 2
    assert update_result.was_updated()
    assert not update_result.was_inserted()
    assert not update_result.was_deleted()

    # Test INSERT result
    insert_result = duckdb_session.execute(
        "INSERT INTO result_methods_test (id, category, value) VALUES (?, ?, ?)", [5, "D", 50]
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.was_inserted()
    assert insert_result.get_affected_count() == 1

    # Test DELETE result
    delete_result = duckdb_session.execute("DELETE FROM result_methods_test WHERE category = 'C'")
    assert isinstance(delete_result, SQLResult)
    assert delete_result.was_deleted()
    assert delete_result.get_affected_count() == 1

    # Clean up
    duckdb_session.execute_script("DROP TABLE result_methods_test")


@pytest.mark.xdist_group("duckdb")
def test_duckdb_to_parquet(duckdb_session: DuckDBDriver) -> None:
    """Integration test: to_parquet writes correct data to a Parquet file using DuckDB native API."""
    duckdb_session.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR)")
    duckdb_session.execute("INSERT INTO test_table (id, name) VALUES (?, ?)", (1, "arrow1"))
    duckdb_session.execute("INSERT INTO test_table (id, name) VALUES (?, ?)", (2, "arrow2"))
    statement = SQL("SELECT id, name FROM test_table ORDER BY id")
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "partitioned_data"
        try:
            duckdb_session.export_to_storage(statement, destination_uri=str(output_path))  # type: ignore[attr-defined]
            table = pq.read_table(f"{output_path}.parquet")
            assert table.num_rows == 2
            assert table.column_names == ["id", "name"]
            data = table.to_pylist()
            assert data[0]["id"] == 1 and data[0]["name"] == "arrow1"
            assert data[1]["id"] == 2 and data[1]["name"] == "arrow2"
        except Exception as e:
            pytest.fail(f"Failed to export to storage: {e}")
