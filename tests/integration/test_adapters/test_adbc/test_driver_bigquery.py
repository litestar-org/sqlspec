"""Test ADBC driver with BigQuery."""

from __future__ import annotations

from typing import Any, Literal

import pyarrow as pa
import pytest
from adbc_driver_bigquery import DatabaseOptions
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.sql.result import ExecuteResult, SelectResult
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture
def adbc_session(bigquery_service: BigQueryService) -> AdbcConfig:
    """Create an ADBC session for BigQuery."""
    db_kwargs = {
        DatabaseOptions.PROJECT_ID.value: bigquery_service.project,
        DatabaseOptions.DATASET_ID.value: bigquery_service.dataset,
        DatabaseOptions.AUTH_TYPE.value: DatabaseOptions.AUTH_VALUE_BIGQUERY.value,
    }

    return AdbcConfig(driver_name="adbc_driver_bigquery", db_kwargs=db_kwargs)


@pytest.mark.parametrize(
    ("params", "sql_placeholder_style", "insert_id"),
    [
        pytest.param((1, "test_tuple"), "qmark", 1, id="qmark_params"),
        pytest.param({"id": 2, "name": "test_dict"}, "bigquery_named", 2, id="bq_named_params"),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
@pytest.mark.xdist_group("bigquery")
def test_driver_select(adbc_session: AdbcConfig, params: Any, sql_placeholder_style: str, insert_id: int) -> None:
    """Test select functionality with different parameter styles."""
    with adbc_session.provide_session() as driver:
        sql_create = "CREATE TABLE test_table (id INT64, name STRING);"
        driver.execute_script(sql_create)

        if sql_placeholder_style == "qmark":
            insert_sql = "INSERT INTO test_table (id, name) VALUES (?, ?)"
            select_sql = "SELECT name FROM test_table WHERE name = ?"
            insert_op_params = params
            select_op_params = (params[1],)
            expected_name = "test_tuple"
        elif sql_placeholder_style == "bigquery_named":
            insert_sql = "INSERT INTO test_table (id, name) VALUES (@id, @name)"
            select_sql = "SELECT name FROM test_table WHERE name = @name"
            insert_op_params = params
            select_op_params = {"name": params["name"]}
            expected_name = "test_dict"
        else:
            raise ValueError(f"Unsupported placeholder style for test: {sql_placeholder_style}")

        insert_result = driver.execute(insert_sql, insert_op_params)
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected in (0, 1, -1)

        results = driver.execute(select_sql, select_op_params)
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == expected_name
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("params", "sql_placeholder_style", "insert_id"),
    [
        pytest.param((1, "test_tuple"), "qmark", 1, id="qmark_params"),
        pytest.param({"id": 2, "name": "test_dict"}, "bigquery_named", 2, id="bq_named_params"),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
@pytest.mark.xdist_group("bigquery")
def test_driver_select_value(adbc_session: AdbcConfig, params: Any, sql_placeholder_style: str, insert_id: int) -> None:
    """Test select_value functionality with different parameter styles."""
    with adbc_session.provide_session() as driver:
        sql_create = "CREATE TABLE test_table (id INT64, name STRING);"
        driver.execute_script(sql_create)

        if sql_placeholder_style == "qmark":
            insert_sql = "INSERT INTO test_table (id, name) VALUES (?, ?)"
            select_sql = "SELECT name FROM test_table WHERE name = ?"
            insert_op_params = params
            select_op_params = (params[1],)
            expected_name = "test_tuple"
        elif sql_placeholder_style == "bigquery_named":
            insert_sql = "INSERT INTO test_table (id, name) VALUES (@id, @name)"
            select_sql = "SELECT name FROM test_table WHERE name = @name"
            insert_op_params = params
            select_op_params = {"name": params["name"]}
            expected_name = "test_dict"
        else:
            raise ValueError(f"Unsupported placeholder style for test: {sql_placeholder_style}")

        insert_result = driver.execute(insert_sql, insert_op_params)
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected in (0, 1, -1)

        result = driver.execute(select_sql, select_op_params)
        assert isinstance(result, SelectResult)
        assert result.rows is not None
        assert len(result.rows) == 1
        assert result.column_names is not None
        assert len(result.column_names) == 1
        value = result.rows[0][result.column_names[0]]
        assert value == expected_name
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
@pytest.mark.xdist_group("bigquery")
def test_driver_insert(adbc_session: AdbcConfig) -> None:
    """Test insert functionality using qmark parameters."""
    with adbc_session.provide_session() as driver:
        sql_create = "CREATE TABLE test_table (id INT64, name STRING);"
        driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_table (id, name) VALUES (?, ?)"
        insert_result = driver.execute(insert_sql, (1, "test_insert"))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected in (0, 1, -1)

        results = driver.execute("SELECT name FROM test_table WHERE id = ?", (1,))
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test_insert"
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
@pytest.mark.xdist_group("bigquery")
def test_driver_select_normal(adbc_session: AdbcConfig) -> None:
    """Test select functionality using qmark parameters."""
    with adbc_session.provide_session() as driver:
        sql_create = "CREATE TABLE test_table (id INT64, name STRING);"
        driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_table (id, name) VALUES (?, ?)"
        insert_result = driver.execute(insert_sql, (10, "test_select_normal"))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected in (0, 1, -1)

        select_sql = "SELECT name FROM test_table WHERE id = ?"
        results = driver.execute(select_sql, (10,))
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test_select_normal"
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
@pytest.mark.xdist_group("bigquery")
def test_execute_script_multiple_statements(adbc_session: AdbcConfig) -> None:
    """Test execute_script with multiple statements."""
    with adbc_session.provide_session() as driver:
        script = """
        CREATE TABLE test_table (id INT64, name STRING);
        INSERT INTO test_table (id, name) VALUES (1, 'script_test');
        INSERT INTO test_table (id, name) VALUES (2, 'script_test_2');
        """
        driver.execute_script(script)

        results = driver.execute("SELECT COUNT(*) AS count FROM test_table WHERE name LIKE 'script_test%'")
        assert isinstance(results, SelectResult)
        assert results.rows[0]["count"] == 2

        select_val_result = driver.execute("SELECT name FROM test_table WHERE id = ?", (1,))
        assert isinstance(select_val_result, SelectResult)
        assert select_val_result.rows is not None
        assert len(select_val_result.rows) == 1
        assert select_val_result.column_names is not None
        assert len(select_val_result.column_names) == 1
        value = select_val_result.rows[0][select_val_result.column_names[0]]
        assert value == "script_test"
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
@pytest.mark.xdist_group("bigquery")
def test_driver_select_to_arrow(adbc_session: AdbcConfig) -> None:
    """Test select_to_arrow functionality for ADBC BigQuery."""
    with adbc_session.provide_session() as driver:
        sql_create = "CREATE TABLE test_table (id INT64, name STRING);"
        driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_table (id, name) VALUES (?, ?)"
        insert_result = driver.execute(insert_sql, (100, "arrow_name"))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected in (0, 1, -1)

        select_sql = "SELECT name, id FROM test_table WHERE name = ?"
        arrow_table = driver.select_to_arrow(select_sql, ("arrow_name",))

        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows == 1
        assert arrow_table.num_columns == 2
        assert sorted(arrow_table.column_names) == sorted(["name", "id"])
        assert arrow_table.column("name").to_pylist() == ["arrow_name"]
        assert arrow_table.column("id").to_pylist() == [100]
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures or not support all features like real BQ")
@pytest.mark.xdist_group("bigquery")
def test_driver_execute_many_insert_bigquery(adbc_session: AdbcConfig) -> None:
    """Test execute_many for batch inserts with ADBC BigQuery."""
    with adbc_session.provide_session() as driver:
        table_name = "test_many_bq_table"
        sql_create = f"CREATE TABLE {table_name} (id INT64, name STRING);"
        driver.execute_script(sql_create)

        # BigQuery typically uses @param for named parameters or ? for positional
        insert_sql = f"INSERT INTO {table_name} (id, name) VALUES (?, ?)"
        params_list = [(1, "bq_name1"), (2, "bq_name2"), (3, "bq_name3")]

        result = driver.execute_many(insert_sql, params_list)
        assert isinstance(result, ExecuteResult)
        # BigQuery ADBC driver might return 0 or -1 for rows_affected with execute_many.
        # The key is that the operation succeeds and data is queryable.
        assert result.rows_affected in (0, -1, len(params_list))

        select_sql = f"SELECT COUNT(*) as count FROM {table_name}"
        count_result = driver.execute(select_sql)
        assert isinstance(count_result, SelectResult)
        assert count_result.rows is not None
        assert count_result.rows[0]["count"] == len(params_list)

        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")


@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
@pytest.mark.xdist_group("bigquery")
def test_driver_execute_many_empty_params_bigquery(adbc_session: AdbcConfig) -> None:
    """Test execute_many with an empty list of parameters for ADBC BigQuery."""
    with adbc_session.provide_session() as driver:
        table_name = "test_empty_many_bq_table"
        sql_create = f"CREATE TABLE {table_name} (id INT64, name STRING);"
        driver.execute_script(sql_create)

        insert_sql = f"INSERT INTO {table_name} (id, name) VALUES (?, ?)"
        params_list: list[tuple[int, str]] = []

        result = driver.execute_many(insert_sql, params_list)
        assert isinstance(result, ExecuteResult)
        assert result.rows_affected == 0  # No operations means 0 rows affected.

        select_sql = f"SELECT COUNT(*) as count FROM {table_name}"
        count_result = driver.execute(select_sql)
        assert isinstance(count_result, SelectResult)
        assert count_result.rows is not None
        assert count_result.rows[0]["count"] == 0

        driver.execute_script(f"DROP TABLE IF EXISTS {table_name}")
