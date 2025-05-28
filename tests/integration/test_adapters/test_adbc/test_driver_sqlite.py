"""Test ADBC driver with PostgreSQL."""

from __future__ import annotations

from typing import Any, Literal

import pyarrow as pa
import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.sql.result import ExecuteResult, SelectResult

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture
def adbc_session() -> AdbcConfig:
    """Create an ADBC session for SQLite using URI."""
    return AdbcConfig(
        uri="sqlite://:memory:",
    )


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xdist_group("sqlite")
def test_driver_insert_returning(adbc_session: AdbcConfig, params: Any, style: ParamStyle) -> None:
    """Test insert returning functionality with different parameter styles."""
    with adbc_session.provide_session() as driver:
        sql_create = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql_create)

        if style == "tuple_binds":
            sql_insert = """
            INSERT INTO test_table (name)
            VALUES (?)
            RETURNING *
            """
        elif style == "dict_binds":
            sql_insert = """
            INSERT INTO test_table (name)
            VALUES (:name)
            RETURNING *
            """
        else:
            raise ValueError(f"Unsupported style: {style}")

        result = driver.execute(sql_insert, params)
        assert isinstance(result, SelectResult)
        assert result.rows is not None
        assert len(result.rows) == 1
        returned_data = result.rows[0]
        assert returned_data["name"] == "test_name"
        assert returned_data["id"] is not None

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_select(adbc_session: AdbcConfig) -> None:
    """Test select functionality with simple tuple parameters."""
    params = ("test_name",)
    with adbc_session.provide_session() as driver:
        # Create test table
        sql_create = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql_create)
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        insert_result = driver.execute(insert_sql, params)
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1 or insert_result.rows_affected == -1

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE name = ?"
        results = driver.execute(select_sql, params)
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test_name"

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_select_value(adbc_session: AdbcConfig) -> None:
    """Test select_value functionality with simple tuple parameters."""
    params = ("test_name",)
    with adbc_session.provide_session() as driver:
        # Create test table
        sql_create = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql_create)

        # Insert test record
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        insert_result = driver.execute(insert_sql, params)
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1 or insert_result.rows_affected == -1

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE name = ?"
        result = driver.execute(select_sql, params)
        assert isinstance(result, SelectResult)
        assert result.rows is not None
        assert len(result.rows) == 1
        assert result.column_names is not None
        assert len(result.column_names) == 1
        value = result.rows[0][result.column_names[0]]
        assert value == "test_name"

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_insert(adbc_session: AdbcConfig) -> None:
    """Test insert functionality."""
    with adbc_session.provide_session() as driver:
        # Create test table
        sql_create = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql_create)

        # Insert test record
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (?)
        """
        result = driver.execute(insert_sql, ("test_name",))
        assert isinstance(result, ExecuteResult)
        assert result.rows_affected == 1 or result.rows_affected == -1

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_select_normal(adbc_session: AdbcConfig) -> None:
    """Test select functionality."""
    with adbc_session.provide_session() as driver:
        # Create test table
        sql_create = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql_create)

        # Insert test record
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (?)
        """
        insert_result = driver.execute(insert_sql, ("test_name",))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1 or insert_result.rows_affected == -1
        select_sql = "SELECT name FROM test_table WHERE name = ?"
        results = driver.execute(select_sql, ("test_name",))
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test_name"

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("param_style_sql", "params"),
    [
        ("SELECT name FROM test_table WHERE name = ?", ("test_name",)),
        ("SELECT name FROM test_table WHERE name = :name", {"name": "test_name"}),
        # Pyformat (%s) is not directly supported by Query object's default parsing for qmark/named.
        # SQLGlot might transpile it, but it's safer to test styles sqlglot explicitly uses for placeholders.
        # If Query object is made to handle pyformat to qmark/named, this can be added.
        # For now, testing qmark and named (:param) which are common.
    ],
)
@xfail_if_driver_missing
def test_param_styles(adbc_session: AdbcConfig, param_style_sql: str, params: Any) -> None:
    """Test different parameter styles that Query object can handle."""
    with adbc_session.provide_session() as driver:
        # Create test table
        sql_create = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql_create)

        # Insert test record (using qmark for simplicity, as insertion isn't the focus here)
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        insert_result = driver.execute(insert_sql, ("test_name",))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1 or insert_result.rows_affected == -1

        # Select and verify using the parameterized SQL and params
        results = driver.execute(param_style_sql, params)
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test_name"

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_select_to_arrow(adbc_session: AdbcConfig) -> None:  # Changed name from select_arrow
    """Test select_to_arrow functionality."""
    with adbc_session.provide_session() as driver:
        # Create test table
        sql_create = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql_create)

        # Insert test record
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (?)
        """
        insert_result = driver.execute(insert_sql, ("arrow_name",))  # Changed to execute
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1 or insert_result.rows_affected == -1

        # Select and verify with select_to_arrow
        select_sql = "SELECT name, id FROM test_table WHERE name = ?"
        arrow_table = driver.select_to_arrow(select_sql, ("arrow_name",))  # Corrected method name

        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows == 1
        assert arrow_table.num_columns == 2
        # Note: Column order might vary depending on DB/driver, adjust if needed
        # Sorting column names for consistent check
        assert sorted(arrow_table.column_names) == sorted(["name", "id"])
        # Check data irrespective of column order
        assert arrow_table.column("name").to_pylist() == ["arrow_name"]
        # Assuming id is 1 for the inserted record, but it's auto-increment. Fetch it to be sure.
        # For simplicity here, we'll assume it's the first record and gets id 1 with SQLite.
        # A more robust test would select the id or not assert its specific value if not controlled.
        # For now, let's assume id is 1 for "arrow_name" if it's the only one.
        # To make it more robust, we could select MAX(id) or query by name and get id.
        # Simplified for this test:
        name_col_data = arrow_table.column("name").to_pylist()
        id_col_data = arrow_table.column("id").to_pylist()

        found_idx = -1
        for i, name_val in enumerate(name_col_data):
            if name_val == "arrow_name":
                found_idx = i
                break
        assert found_idx != -1
        # We don't know the exact ID, just that it exists for this row.
        assert id_col_data[found_idx] is not None

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_named_params_with_scalar(adbc_session: AdbcConfig) -> None:
    """Test that scalar parameters work with named parameters in SQL."""
    with adbc_session.provide_session() as driver:
        sql_create = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_table (name) VALUES (:name)"
        insert_result = driver.execute(insert_sql, "test_name")
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1 or insert_result.rows_affected == -1

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE name = :name"
        results = driver.execute(select_sql, "test_name")
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test_name"
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_named_params_with_tuple(adbc_session: AdbcConfig) -> None:
    """Test that tuple parameters work with named parameters in SQL."""
    with adbc_session.provide_session() as driver:
        sql_create = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50),
            age INTEGER
        );
        """
        driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_table (name, age) VALUES (:name, :age)"
        insert_result = driver.execute(insert_sql, ("test_name", 30))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1 or insert_result.rows_affected == -1

        # Select and verify
        select_sql = "SELECT name, age FROM test_table WHERE name = :name AND age = :age"
        results = driver.execute(select_sql, ("test_name", 30))
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test_name"
        assert results.rows[0]["age"] == 30
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xdist_group("sqlite")
def test_driver_execute_many_insert_sqlite(adbc_session: AdbcConfig) -> None:
    """Test execute_many for batch inserts with ADBC SQLite."""
    with adbc_session.provide_session() as driver:
        sql_create = """
        CREATE TABLE test_many_sqlite_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_many_sqlite_table (name) VALUES (?)"
        params_list = [("sqlite_name1",), ("sqlite_name2",), ("sqlite_name3",)]

        result = driver.execute_many(insert_sql, params_list)
        assert isinstance(result, ExecuteResult)
        # SQLite ADBC driver should report rows_affected accurately for executemany.
        assert result.rows_affected == len(params_list)

        select_sql = "SELECT COUNT(*) as count FROM test_many_sqlite_table"
        count_result = driver.execute(select_sql)
        assert isinstance(count_result, SelectResult)
        assert count_result.rows is not None
        assert count_result.rows[0]["count"] == len(params_list)

        driver.execute_script("DROP TABLE IF EXISTS test_many_sqlite_table")


@xfail_if_driver_missing
@pytest.mark.xdist_group("sqlite")
def test_driver_execute_many_empty_params_sqlite(adbc_session: AdbcConfig) -> None:
    """Test execute_many with an empty list of parameters for ADBC SQLite."""
    with adbc_session.provide_session() as driver:
        sql_create = """
        CREATE TABLE test_empty_many_sqlite_table (
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_empty_many_sqlite_table (name) VALUES (?)"
        params_list: list[tuple[str]] = []

        result = driver.execute_many(insert_sql, params_list)
        assert isinstance(result, ExecuteResult)
        assert result.rows_affected == 0

        select_sql = "SELECT COUNT(*) as count FROM test_empty_many_sqlite_table"
        count_result = driver.execute(select_sql)
        assert isinstance(count_result, SelectResult)
        assert count_result.rows is not None
        assert count_result.rows[0]["count"] == 0

        driver.execute_script("DROP TABLE IF EXISTS test_empty_many_sqlite_table")
