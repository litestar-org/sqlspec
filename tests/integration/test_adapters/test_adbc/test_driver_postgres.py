"""Test ADBC postgres driver implementation."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import pyarrow as pa
import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.sql.result import ExecuteResult, SelectResult


@pytest.fixture
def adbc_postgres_session(postgres_service: PostgresService) -> Generator[AdbcDriver, None, None]:
    """Create an ADBC postgres session with a test table.

    Returns:
        A configured ADBC postgres session with a test table.
    """
    adapter = AdbcConfig(
        uri=f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
    )
    try:
        with adapter.provide_session() as session:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            )
            """
            session.execute_script(create_table_sql)
            yield session
            session.execute_script("DROP TABLE IF EXISTS test_table")
    except Exception as e:
        if "cannot open shared object file" in str(e):
            pytest.xfail(f"ADBC driver shared object file not found during session setup: {e}")
        raise e


@pytest.mark.parametrize(
    ("params", "sql_placeholder_style"),
    [
        pytest.param(("test_name",), "qmark", id="qmark_params"),
        pytest.param({"name": "test_name"}, "named_colon", id="named_colon_params"),
    ],
)
@pytest.mark.xdist_group("postgres")
def test_insert_returning(adbc_postgres_session: AdbcDriver, params: Any, sql_placeholder_style: str) -> None:
    """Test insert returning functionality with different parameter styles."""
    adbc_postgres_session.execute_script("DELETE FROM test_table")

    if sql_placeholder_style == "qmark":
        sql_insert = "INSERT INTO test_table (name) VALUES (?) RETURNING id, name"
    elif sql_placeholder_style == "named_colon":
        sql_insert = "INSERT INTO test_table (name) VALUES (:name) RETURNING id, name"
    else:
        raise ValueError(f"Unsupported placeholder style: {sql_placeholder_style}")

    result = adbc_postgres_session.execute(sql_insert, params)
    assert isinstance(result, SelectResult)
    assert result.rows is not None
    assert len(result.rows) == 1
    returned_data = result.rows[0]
    assert returned_data["name"] == params[0] if isinstance(params, tuple) else params["name"]
    assert "id" in returned_data
    assert isinstance(returned_data["id"], int)


@pytest.mark.parametrize(
    ("params", "sql_placeholder_style"),
    [
        pytest.param(("test_name",), "qmark", id="qmark_params"),
        pytest.param({"name": "test_name"}, "named_colon", id="named_colon_params"),
    ],
)
@pytest.mark.xdist_group("postgres")
def test_select(adbc_postgres_session: AdbcDriver, params: Any, sql_placeholder_style: str) -> None:
    """Test select functionality with different parameter styles."""
    adbc_postgres_session.execute_script("DELETE FROM test_table")

    insert_params_value = params[0] if isinstance(params, tuple) else params["name"]

    if sql_placeholder_style == "qmark":
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        select_sql = "SELECT id, name FROM test_table WHERE name = ?"
    elif sql_placeholder_style == "named_colon":
        insert_sql = "INSERT INTO test_table (name) VALUES (:name)"
        select_sql = "SELECT id, name FROM test_table WHERE name = :name"
    else:
        raise ValueError(f"Unsupported placeholder style: {sql_placeholder_style}")

    insert_op_params = (insert_params_value,) if sql_placeholder_style == "qmark" else {"name": insert_params_value}
    insert_result = adbc_postgres_session.execute(insert_sql, insert_op_params)
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    select_op_params = (insert_params_value,) if sql_placeholder_style == "qmark" else {"name": insert_params_value}
    results = adbc_postgres_session.execute(select_sql, select_op_params)
    assert isinstance(results, SelectResult)
    assert len(results.rows) == 1
    assert results.rows[0]["name"] == insert_params_value


@pytest.mark.parametrize(
    ("params", "sql_placeholder_style"),
    [
        pytest.param(("test_name",), "qmark", id="qmark_params"),
        pytest.param({"name": "test_name"}, "named_colon", id="named_colon_params"),
    ],
)
@pytest.mark.xdist_group("postgres")
def test_select_one(adbc_postgres_session: AdbcDriver, params: Any, sql_placeholder_style: str) -> None:
    """Test select_one (now execute + get_first) functionality."""
    adbc_postgres_session.execute_script("DELETE FROM test_table")
    insert_params_value = params[0] if isinstance(params, tuple) else params["name"]

    if sql_placeholder_style == "qmark":
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        select_sql = "SELECT id, name FROM test_table WHERE name = ?"
    elif sql_placeholder_style == "named_colon":
        insert_sql = "INSERT INTO test_table (name) VALUES (:name)"
        select_sql = "SELECT id, name FROM test_table WHERE name = :name"
    else:
        raise ValueError(f"Unsupported placeholder style: {sql_placeholder_style}")

    insert_op_params = (insert_params_value,) if sql_placeholder_style == "qmark" else {"name": insert_params_value}
    insert_result = adbc_postgres_session.execute(insert_sql, insert_op_params)
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    select_op_params = (insert_params_value,) if sql_placeholder_style == "qmark" else {"name": insert_params_value}
    result = adbc_postgres_session.execute(select_sql, select_op_params)
    assert isinstance(result, SelectResult)
    assert result.rows is not None
    assert len(result.rows) == 1
    first_row = result.get_first()
    assert first_row is not None
    assert first_row["name"] == insert_params_value


@pytest.mark.parametrize(
    ("params", "sql_placeholder_style"),
    [
        pytest.param(("test_name",), "qmark", id="qmark_params"),
        pytest.param({"name": "test_name"}, "named_colon", id="named_colon_params"),
    ],
)
@pytest.mark.xdist_group("postgres")
def test_select_value(adbc_postgres_session: AdbcDriver, params: Any, sql_placeholder_style: str) -> None:
    """Test select_value (now execute + extract) functionality."""
    adbc_postgres_session.execute_script("DELETE FROM test_table")
    insert_params_value = params[0] if isinstance(params, tuple) else params["name"]

    if sql_placeholder_style == "qmark":
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        select_sql = "SELECT name FROM test_table WHERE name = ?"
    elif sql_placeholder_style == "named_colon":
        insert_sql = "INSERT INTO test_table (name) VALUES (:name)"
        select_sql = "SELECT name FROM test_table WHERE name = :name"
    else:
        raise ValueError(f"Unsupported placeholder style: {sql_placeholder_style}")

    insert_op_params = (insert_params_value,) if sql_placeholder_style == "qmark" else {"name": insert_params_value}
    insert_result = adbc_postgres_session.execute(insert_sql, insert_op_params)
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    select_op_params = (insert_params_value,) if sql_placeholder_style == "qmark" else {"name": insert_params_value}
    result = adbc_postgres_session.execute(select_sql, select_op_params)
    assert isinstance(result, SelectResult)
    assert result.rows is not None
    assert len(result.rows) == 1
    assert result.column_names is not None
    assert len(result.column_names) == 1
    value = result.rows[0][result.column_names[0]]
    assert value == insert_params_value


@pytest.mark.xdist_group("postgres")
def test_select_to_arrow(adbc_postgres_session: AdbcDriver) -> None:
    """Test select_to_arrow functionality for ADBC Postgres."""
    adbc_postgres_session.execute_script("DELETE FROM test_table")

    insert_sql = "INSERT INTO test_table (name) VALUES (?)"
    insert_result = adbc_postgres_session.execute(insert_sql, ("arrow_name",))
    assert isinstance(insert_result, ExecuteResult)
    assert insert_result.rows_affected == 1

    select_sql = "SELECT name, id FROM test_table WHERE name = ?"
    arrow_table = adbc_postgres_session.select_to_arrow(select_sql, ("arrow_name",))

    assert isinstance(arrow_table, pa.Table)
    assert arrow_table.num_rows == 1
    assert arrow_table.num_columns == 2
    assert sorted(arrow_table.column_names) == sorted(["name", "id"])
    assert arrow_table.column("name").to_pylist() == ["arrow_name"]
    id_val = arrow_table.column("id").to_pylist()[0]
    assert id_val is not None
    assert isinstance(id_val, int)


@pytest.mark.xdist_group("postgres")
def test_execute_many_insert(adbc_postgres_session: AdbcDriver) -> None:
    """Test execute_many for batch inserts with ADBC Postgres."""
    adbc_postgres_session.execute_script("DELETE FROM test_table")  # Clear table before test

    insert_sql = "INSERT INTO test_table (name) VALUES (?)"  # Using qmark as it's commonly supported
    params_list = [("pg_name1",), ("pg_name2",), ("pg_name3",)]

    result = adbc_postgres_session.execute_many(insert_sql, params_list)
    assert isinstance(result, ExecuteResult)
    # For ADBC Postgres, rows_affected should be the sum of rows affected by each statement.
    assert result.rows_affected == len(params_list)

    select_sql = "SELECT COUNT(*) as count FROM test_table"
    count_result = adbc_postgres_session.execute(select_sql)
    assert isinstance(count_result, SelectResult)
    assert count_result.rows is not None
    assert count_result.rows[0]["count"] == len(params_list)


@pytest.mark.xdist_group("postgres")
def test_execute_many_empty_params(adbc_postgres_session: AdbcDriver) -> None:
    """Test execute_many with an empty list of parameters for ADBC Postgres."""
    adbc_postgres_session.execute_script("DELETE FROM test_table")  # Clear table

    insert_sql = "INSERT INTO test_table (name) VALUES (?)"
    params_list: list[tuple[str]] = []

    result = adbc_postgres_session.execute_many(insert_sql, params_list)
    assert isinstance(result, ExecuteResult)
    assert result.rows_affected == 0

    select_sql = "SELECT COUNT(*) as count FROM test_table"
    count_result = adbc_postgres_session.execute(select_sql)
    assert isinstance(count_result, SelectResult)
    assert count_result.rows is not None
    assert count_result.rows[0]["count"] == 0
