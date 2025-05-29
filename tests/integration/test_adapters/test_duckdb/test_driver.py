"""Test DuckDB driver implementation."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any, Literal

import pyarrow as pa
import pytest

from sqlspec.adapters.duckdb import DuckDBConfig, DuckDBDriver
from sqlspec.sql.result import ExecuteResult, SelectResult

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
    assert isinstance(result, ExecuteResult)
    assert result.rows_affected == 1

    # Verify insertion
    select_result = duckdb_session.execute("SELECT name, id FROM test_table")
    assert isinstance(select_result, SelectResult)
    assert select_result.rows is not None
    assert len(select_result.rows) == 1
    assert select_result.rows[0]["name"] == "test_name"
    assert select_result.rows[0]["id"] == 1

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
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    # Test select
    select_result = duckdb_session.execute("SELECT name, id FROM test_table")
    assert isinstance(select_result, SelectResult)
    assert select_result.rows is not None
    assert len(select_result.rows) == 1
    assert select_result.rows[0]["name"] == "test_name"
    assert select_result.rows[0]["id"] == 1

    # Test select with a WHERE clause
    if style == "tuple_binds":
        select_where_sql = "SELECT id FROM test_table WHERE name = ?"
        where_params = ("test_name",)
    else:
        select_where_sql = "SELECT id FROM test_table WHERE name = :name"
        where_params = {"name": "test_name"}

    where_result = duckdb_session.execute(select_where_sql, where_params)
    assert isinstance(where_result, SelectResult)
    assert where_result.rows is not None
    assert len(where_result.rows) == 1
    assert where_result.rows[0]["id"] == 1

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
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    # Test select value
    if style == "tuple_binds":
        value_sql = "SELECT name FROM test_table WHERE id = ?"
        value_params = (1,)
    else:
        value_sql = "SELECT name FROM test_table WHERE id = :id"
        value_params = {"id": 1}

    value_result = duckdb_session.execute(value_sql, value_params)
    assert isinstance(value_result, SelectResult)
    assert value_result.rows is not None
    assert len(value_result.rows) == 1
    assert value_result.column_names is not None

    # Extract single value using column name
    value = value_result.rows[0][value_result.column_names[0]]
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
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    # Test select_arrow using mixins
    if hasattr(duckdb_session, "select_to_arrow"):
        select_sql = "SELECT name, id FROM test_table WHERE id = 1"
        arrow_result = duckdb_session.select_to_arrow(select_sql)

        assert hasattr(arrow_result, "arrow_table")
        arrow_table = arrow_result.arrow_table
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
    assert isinstance(result, ExecuteResult)
    assert result.rows_affected == len(params_list)

    # Verify all records were inserted
    select_result = duckdb_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SelectResult)
    assert select_result.rows is not None
    assert select_result.rows[0]["count"] == len(params_list)


@pytest.mark.xdist_group("duckdb")
def test_execute_script(duckdb_session: DuckDBDriver) -> None:
    """Test execute_script functionality for multi-statement scripts."""
    script = """
    INSERT INTO test_table (name, id) VALUES ('script_name1', 100);
    INSERT INTO test_table (name, id) VALUES ('script_name2', 200);
    """

    result = duckdb_session.execute_script(script)
    assert isinstance(result, str)

    # Verify script executed successfully
    select_result = duckdb_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SelectResult)
    assert select_result.rows is not None
    assert select_result.rows[0]["count"] == 2


@pytest.mark.xdist_group("duckdb")
def test_update_operation(duckdb_session: DuckDBDriver) -> None:
    """Test UPDATE operations."""
    # Insert a record first
    insert_result = duckdb_session.execute("INSERT INTO test_table (name, id) VALUES (?, ?)", ("original_name", 42))
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    # Update the record
    update_result = duckdb_session.execute("UPDATE test_table SET name = ? WHERE id = ?", ("updated_name", 42))
    assert isinstance(update_result, ExecuteResult)
    assert update_result.rows_affected == 1

    # Verify the update
    select_result = duckdb_session.execute("SELECT name FROM test_table WHERE id = ?", (42,))
    assert isinstance(select_result, SelectResult)
    assert select_result.rows is not None
    assert select_result.rows[0]["name"] == "updated_name"


@pytest.mark.xdist_group("duckdb")
def test_delete_operation(duckdb_session: DuckDBDriver) -> None:
    """Test DELETE operations."""
    # Insert a record first
    insert_result = duckdb_session.execute("INSERT INTO test_table (name, id) VALUES (?, ?)", ("to_delete", 99))
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    # Delete the record
    delete_result = duckdb_session.execute("DELETE FROM test_table WHERE id = ?", (99,))
    assert isinstance(delete_result, ExecuteResult)
    assert delete_result.rows_affected == 1

    # Verify the deletion
    select_result = duckdb_session.execute("SELECT COUNT(*) as count FROM test_table")
    assert isinstance(select_result, SelectResult)
    assert select_result.rows is not None
    assert select_result.rows[0]["count"] == 0
