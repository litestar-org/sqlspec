"""Test DuckDB driver implementation."""

from collections.abc import Generator

import pytest

from sqlspec import SQLResult, StatementStack, sql
from sqlspec.adapters.duckdb import DuckDBDriver
from tests.conftest import requires_interpreted

pytestmark = pytest.mark.xdist_group("duckdb")


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
    row = select_result.get_data()[0]

    assert row["id"] == 1
    assert row["text_col"] == "test_text"
    assert row["boolean_col"] is True

    assert row["array_col"] is not None
    assert row["json_col"] is not None

    duckdb_session.execute_script("DROP TABLE duckdb_data_types_test")


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

    product_a_rows = [row for row in result.get_data() if row["product"] == "Product A"]
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
    assert select_result.get_data()[0]["name"] == "Test User"
    assert select_result.get_data()[0]["email"] == "test@example.com"

    duckdb_session.execute("DROP INDEX idx_schema_test_name")
    duckdb_session.execute("DROP TABLE schema_test")


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
    assert verify.get_data()[0]["total"] == 2
