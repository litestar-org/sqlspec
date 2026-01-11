"""Test DuckDB driver implementation."""

from collections.abc import Generator
from typing import Any, Literal

import pytest

from sqlspec import SQLResult, StatementStack, sql
from sqlspec.adapters.duckdb import DuckDBDriver
from tests.conftest import requires_interpreted

pytestmark = pytest.mark.xdist_group("duckdb")


ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture
def duckdb_session(duckdb_basic_session: DuckDBDriver) -> Generator[DuckDBDriver, None, None]:
    """Create a DuckDB session with a test table."""

    duckdb_basic_session.execute_script("CREATE SEQUENCE IF NOT EXISTS test_id_seq START 1")
    duckdb_basic_session.execute_script(
        """
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY DEFAULT nextval('test_id_seq'),
                name TEXT NOT NULL
            )
        """
    )

    try:
        yield duckdb_basic_session
    finally:
        duckdb_basic_session.execute_script("DROP TABLE IF EXISTS test_table")
        duckdb_basic_session.execute_script("DROP SEQUENCE IF EXISTS test_id_seq")


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_name", 1), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name", "id": 1}, "dict_binds", id="dict_binds"),
    ],
)
def test_insert(duckdb_session: DuckDBDriver, parameters: Any, style: ParamStyle) -> None:
    """Test inserting data with different parameter styles."""
    if style == "tuple_binds":
        sql = "INSERT INTO test_table (name, id) VALUES (?, ?)"
    else:
        sql = "INSERT INTO test_table (name, id) VALUES (:name, :id)"

    result = duckdb_session.execute(sql, parameters)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 1

    select_result = duckdb_session.execute("SELECT name, id FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["id"] == 1

    duckdb_session.execute_script("DELETE FROM test_table")


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_name", 1), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name", "id": 1}, "dict_binds", id="dict_binds"),
    ],
)
def test_select(duckdb_session: DuckDBDriver, parameters: Any, style: ParamStyle) -> None:
    """Test selecting data with different parameter styles."""

    if style == "tuple_binds":
        insert_sql = "INSERT INTO test_table (name, id) VALUES (?, ?)"
    else:
        insert_sql = "INSERT INTO test_table (name, id) VALUES (:name, :id)"

    insert_result = duckdb_session.execute(insert_sql, parameters)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    select_result = duckdb_session.execute("SELECT name, id FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "test_name"
    assert select_result.data[0]["id"] == 1

    if style == "tuple_binds":
        select_where_sql = "SELECT id FROM test_table WHERE name = ?"
        where_parameters = "test_name"
    else:
        select_where_sql = "SELECT id FROM test_table WHERE name = :name"
        where_parameters = {"name": "test_name"}

    where_result = duckdb_session.execute(select_where_sql, where_parameters)
    assert isinstance(where_result, SQLResult)
    assert where_result.data is not None
    assert len(where_result.data) == 1
    assert where_result.data[0]["id"] == 1

    duckdb_session.execute_script("DELETE FROM test_table")


@pytest.mark.parametrize(
    ("parameters", "style"),
    [
        pytest.param(("test_name", 1), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name", "id": 1}, "dict_binds", id="dict_binds"),
    ],
)
def test_select_value(duckdb_session: DuckDBDriver, parameters: Any, style: ParamStyle) -> None:
    """Test select value with different parameter styles."""

    if style == "tuple_binds":
        insert_sql = "INSERT INTO test_table (name, id) VALUES (?, ?)"
    else:
        insert_sql = "INSERT INTO test_table (name, id) VALUES (:name, :id)"

    insert_result = duckdb_session.execute(insert_sql, parameters)
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    if style == "tuple_binds":
        value_sql = "SELECT name FROM test_table WHERE id = ?"
        value_parameters = 1
    else:
        value_sql = "SELECT name FROM test_table WHERE id = :id"
        value_parameters = {"id": 1}

    value_result = duckdb_session.execute(value_sql, value_parameters)
    assert isinstance(value_result, SQLResult)
    assert value_result.data is not None
    assert len(value_result.data) == 1
    assert value_result.column_names is not None

    value = value_result.data[0][value_result.column_names[0]]
    assert value == "test_name"

    duckdb_session.execute_script("DELETE FROM test_table")


def test_execute_many_insert(duckdb_session: DuckDBDriver) -> None:
    """Test execute_many functionality for batch inserts."""
    insert_sql = "INSERT INTO test_table (name, id) VALUES (?, ?)"
    parameters_list = [("name1", 10), ("name2", 20), ("name3", 30)]

    result = duckdb_session.execute_many(insert_sql, parameters_list)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == len(parameters_list)

    select_result = duckdb_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == len(parameters_list)


def test_execute_script(duckdb_session: DuckDBDriver) -> None:
    """Test execute_script functionality for multi-statement scripts."""
    script = """
    INSERT INTO test_table (name, id) VALUES ('script_name1', 100);
    INSERT INTO test_table (name, id) VALUES ('script_name2', 200);
    """

    result = duckdb_session.execute_script(script)
    assert isinstance(result, SQLResult)

    select_result = duckdb_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 2


def test_update_operation(duckdb_session: DuckDBDriver) -> None:
    """Test UPDATE operations."""

    insert_result = duckdb_session.execute("INSERT INTO test_table (name, id) VALUES (?, ?)", ("original_name", 42))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    update_result = duckdb_session.execute("UPDATE test_table SET name = ? WHERE id = ?", ("updated_name", 42))
    assert isinstance(update_result, SQLResult)
    assert update_result.rows_affected == 1

    select_result = duckdb_session.execute("SELECT name FROM test_table WHERE id = ?", (42))
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["name"] == "updated_name"


def test_delete_operation(duckdb_session: DuckDBDriver) -> None:
    """Test DELETE operations."""

    insert_result = duckdb_session.execute("INSERT INTO test_table (name, id) VALUES (?, ?)", ("to_delete", 99))
    assert isinstance(insert_result, SQLResult)
    assert insert_result.rows_affected == 1

    delete_result = duckdb_session.execute("DELETE FROM test_table WHERE id = ?", (99))
    assert isinstance(delete_result, SQLResult)
    assert delete_result.rows_affected == 1

    select_result = duckdb_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert select_result.data[0]["count"] == 0


def test_duckdb_data_types(duckdb_session: DuckDBDriver) -> None:
    """Test DuckDB-specific data types and functionality."""

    duckdb_session.execute_script("""
        CREATE TABLE duckdb_data_types_test (
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

    insert_sql = """
        INSERT INTO duckdb_data_types_test VALUES (
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

    select_result = duckdb_session.execute("SELECT * FROM duckdb_data_types_test")
    assert len(select_result.data) == 1
    row = select_result.data[0]

    assert row["id"] == 1
    assert row["text_col"] == "test_text"
    assert row["boolean_col"] is True

    assert row["array_col"] is not None
    assert row["json_col"] is not None

    duckdb_session.execute_script("DROP TABLE duckdb_data_types_test")


def test_duckdb_complex_queries(duckdb_session: DuckDBDriver) -> None:
    """Test complex SQL queries with DuckDB."""

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

    engineering_row = next(row for row in result.data if row["dept_name"] == "Engineering")
    assert engineering_row["employee_count"] == 2
    assert engineering_row["avg_salary"] == 77500.0

    subquery = """
        SELECT emp_name, salary
        FROM employees
        WHERE salary > (SELECT AVG(salary) FROM employees)
        ORDER BY salary DESC
    """

    subquery_result = duckdb_session.execute(subquery)
    assert len(subquery_result.data) >= 1

    duckdb_session.execute_script("DROP TABLE employees; DROP TABLE departments;")


def test_duckdb_window_functions(duckdb_session: DuckDBDriver) -> None:
    """Test DuckDB window functions."""

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

    product_a_rows = [row for row in result.data if row["product"] == "Product A"]
    assert len(product_a_rows) == 2
    assert product_a_rows[0]["rank_in_product"] == 1

    duckdb_session.execute_script("DROP TABLE sales_data")


def test_duckdb_schema_operations(duckdb_session: DuckDBDriver) -> None:
    """Test DuckDB schema operations (DDL)."""

    create_result = duckdb_session.execute("""
        CREATE TABLE schema_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    assert isinstance(create_result, SQLResult)

    alter_result = duckdb_session.execute("ALTER TABLE schema_test ADD COLUMN email TEXT")
    assert isinstance(alter_result, SQLResult)

    index_result = duckdb_session.execute("CREATE INDEX idx_schema_test_name ON schema_test(name)")
    assert isinstance(index_result, SQLResult)

    insert_result = duckdb_session.execute(
        "INSERT INTO schema_test (id, name, email) VALUES (?, ?, ?)", [1, "Test User", "test@example.com"]
    )
    assert insert_result.rows_affected == 1

    select_result = duckdb_session.execute("SELECT id, name, email FROM schema_test")
    assert len(select_result.data) == 1
    assert select_result.data[0]["name"] == "Test User"
    assert select_result.data[0]["email"] == "test@example.com"

    duckdb_session.execute("DROP INDEX idx_schema_test_name")
    duckdb_session.execute("DROP TABLE schema_test")


def test_duckdb_performance_bulk_operations(duckdb_session: DuckDBDriver) -> None:
    """Test DuckDB performance with bulk operations."""

    duckdb_session.execute_script("""
        CREATE TABLE bulk_test (
            id INTEGER,
            value TEXT,
            number DECIMAL(10,2)
        )
    """)

    bulk_data = [(i, f"value_{i}", float(i * 10.5)) for i in range(1, 101)]

    bulk_insert_sql = "INSERT INTO bulk_test (id, value, number) VALUES (?, ?, ?)"
    bulk_result = duckdb_session.execute_many(bulk_insert_sql, bulk_data)
    assert bulk_result.rows_affected == 100

    bulk_select_result = duckdb_session.execute("SELECT COUNT(*) as total FROM bulk_test")
    assert bulk_select_result.data[0]["total"] == 100

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

    duckdb_session.execute_script("DROP TABLE bulk_test")


def test_duckdb_error_handling_and_edge_cases(duckdb_session: DuckDBDriver) -> None:
    """Test DuckDB error handling and edge cases."""

    with pytest.raises(Exception):
        duckdb_session.execute("INVALID SQL STATEMENT")

    duckdb_session.execute_script("""
        CREATE TABLE constraint_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)

    with pytest.raises(Exception):
        duckdb_session.execute("INSERT INTO constraint_test (id) VALUES (1)")

    valid_result = duckdb_session.execute("INSERT INTO constraint_test (id, name) VALUES (?, ?)", [1, "Valid Name"])
    assert valid_result.rows_affected == 1

    with pytest.raises(Exception):
        duckdb_session.execute("INSERT INTO constraint_test (id, name) VALUES (?, ?)", [1, "Duplicate ID"])

    duckdb_session.execute_script("DROP TABLE constraint_test")


def test_duckdb_result_methods_comprehensive(duckdb_session: DuckDBDriver) -> None:
    """Test comprehensive SQLResult methods."""

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

    select_result = duckdb_session.execute("SELECT * FROM result_methods_test ORDER BY id")

    assert select_result.get_count() == 4

    first_row = select_result.get_first()
    assert first_row is not None
    assert first_row["id"] == 1

    assert not select_result.is_empty()

    empty_result = duckdb_session.execute("SELECT * FROM result_methods_test WHERE id > 100")
    assert empty_result.is_empty()
    assert empty_result.get_count() == 0
    assert empty_result.get_first() is None

    update_result = duckdb_session.execute("UPDATE result_methods_test SET value = value * 2 WHERE category = 'A'")

    assert isinstance(update_result, SQLResult)
    assert update_result.get_affected_count() == 2
    assert update_result.was_updated()
    assert not update_result.was_inserted()
    assert not update_result.was_deleted()

    insert_result = duckdb_session.execute(
        "INSERT INTO result_methods_test (id, category, value) VALUES (?, ?, ?)", [5, "D", 50]
    )
    assert isinstance(insert_result, SQLResult)
    assert insert_result.was_inserted()
    assert insert_result.get_affected_count() == 1

    delete_result = duckdb_session.execute("DELETE FROM result_methods_test WHERE category = 'C'")
    assert isinstance(delete_result, SQLResult)
    assert delete_result.was_deleted()
    assert delete_result.get_affected_count() == 1

    duckdb_session.execute_script("DROP TABLE result_methods_test")


def test_duckdb_for_update_locking(duckdb_session: DuckDBDriver) -> None:
    """Test FOR UPDATE row locking with DuckDB (may have limited support)."""

    # Setup test table
    duckdb_session.execute_script("DROP TABLE IF EXISTS test_table")
    duckdb_session.execute_script("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            value INTEGER
        )
    """)

    # Insert test data
    duckdb_session.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "duckdb_lock", 100))

    try:
        duckdb_session.begin()

        # Test basic FOR UPDATE (DuckDB may have limited or no support)
        result = duckdb_session.select_one(
            sql.select("id", "name", "value").from_("test_table").where_eq("name", "duckdb_lock").for_update()
        )
        assert result is not None
        assert result["name"] == "duckdb_lock"
        assert result["value"] == 100

        duckdb_session.commit()
    except Exception:
        duckdb_session.rollback()
        raise
    finally:
        duckdb_session.execute_script("DROP TABLE IF EXISTS test_table")


def test_duckdb_for_update_nowait(duckdb_session: DuckDBDriver) -> None:
    """Test FOR UPDATE NOWAIT with DuckDB."""

    # Setup test table
    duckdb_session.execute_script("DROP TABLE IF EXISTS test_table")
    duckdb_session.execute_script("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            value INTEGER
        )
    """)

    # Insert test data
    duckdb_session.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "duckdb_nowait", 200))

    try:
        duckdb_session.begin()

        # Test FOR UPDATE NOWAIT
        result = duckdb_session.select_one(
            sql.select("*").from_("test_table").where_eq("name", "duckdb_nowait").for_update(nowait=True)
        )
        assert result is not None
        assert result["name"] == "duckdb_nowait"

        duckdb_session.commit()
    except Exception:
        duckdb_session.rollback()
        raise
    finally:
        duckdb_session.execute_script("DROP TABLE IF EXISTS test_table")


def test_duckdb_for_share_locking(duckdb_session: DuckDBDriver) -> None:
    """Test FOR SHARE row locking with DuckDB."""

    # Setup test table
    duckdb_session.execute_script("DROP TABLE IF EXISTS test_table")
    duckdb_session.execute_script("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            value INTEGER
        )
    """)

    # Insert test data
    duckdb_session.execute("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)", (1, "duckdb_share", 300))

    try:
        duckdb_session.begin()

        # Test FOR SHARE (DuckDB support may vary)
        result = duckdb_session.select_one(
            sql.select("id", "name", "value").from_("test_table").where_eq("name", "duckdb_share").for_share()
        )
        assert result is not None
        assert result["name"] == "duckdb_share"
        assert result["value"] == 300

        duckdb_session.commit()
    except Exception:
        duckdb_session.rollback()
        raise
    finally:
        duckdb_session.execute_script("DROP TABLE IF EXISTS test_table")


def test_duckdb_statement_stack_sequential(duckdb_session: DuckDBDriver) -> None:
    """DuckDB drivers should use sequential stack execution."""

    duckdb_session.execute("DELETE FROM test_table")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name) VALUES (?, ?)", (1, "duckdb-stack-one"))
        .push_execute("INSERT INTO test_table (id, name) VALUES (?, ?)", (2, "duckdb-stack-two"))
        .push_execute("SELECT COUNT(*) AS total FROM test_table WHERE name LIKE ?", ("duckdb-stack-%",))
    )

    results = duckdb_session.execute_stack(stack)

    assert len(results) == 3
    assert results[0].rows_affected == 1
    assert results[1].rows_affected == 1
    assert results[2].result is not None
    assert results[2].result.data is not None
    assert results[2].result.data[0]["total"] == 2


@requires_interpreted
def test_duckdb_statement_stack_continue_on_error(duckdb_session: DuckDBDriver) -> None:
    """DuckDB sequential stack execution should honor continue-on-error."""

    duckdb_session.execute("DELETE FROM test_table")

    stack = (
        StatementStack()
        .push_execute("INSERT INTO test_table (id, name) VALUES (?, ?)", (1, "duckdb-initial"))
        .push_execute("INSERT INTO test_table (id, name) VALUES (?, ?)", (1, "duckdb-duplicate"))
        .push_execute("INSERT INTO test_table (id, name) VALUES (?, ?)", (2, "duckdb-final"))
    )

    results = duckdb_session.execute_stack(stack, continue_on_error=True)

    assert len(results) == 3
    assert results[0].rows_affected == 1
    assert results[1].error is not None
    assert results[2].rows_affected == 1

    verify = duckdb_session.execute("SELECT COUNT(*) AS total FROM test_table")
    assert verify.data is not None
    assert verify.data[0]["total"] == 2
