"""Test ADBC driver with BigQuery."""

from __future__ import annotations

from typing import Any, Literal

import pyarrow as pa
import pytest
from adbc_driver_bigquery import DatabaseOptions
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec.adapters.adbc import AdbcConfig
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
    ("params", "style", "insert_id"),
    [
        pytest.param((1, "test_tuple"), "tuple_binds", 1, id="tuple_binds"),
        pytest.param({"id": 2, "name": "test_dict"}, "dict_binds", 2, id="dict_binds"),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
@pytest.mark.xdist_group("bigquery")
def test_driver_select(adbc_session: AdbcConfig, params: Any, style: ParamStyle, insert_id: int) -> None:
    """Test select functionality with different parameter styles."""
    with adbc_session.provide_session() as driver:
        # Create test table (Use BigQuery compatible types)
        sql = """
        CREATE TABLE test_table (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(sql)

        # Insert test record
        if style == "tuple_binds":
            insert_sql = "INSERT INTO test_table (id, name) VALUES (?, ?)"
            select_params = (params[1],)  # Select by name using positional param
            select_sql = "SELECT name FROM test_table WHERE name = ?"
            expected_name = "test_tuple"
        else:  # dict_binds
            insert_sql = "INSERT INTO test_table (id, name) VALUES (@id, @name)"
            select_params = {"name": params["name"]}  # type: ignore[assignment]
            select_sql = "SELECT name FROM test_table WHERE name = @name"
            expected_name = "test_dict"

        driver.insert_update_delete(insert_sql, params)

        # Select and verify
        results = driver.select(select_sql, select_params)
        assert len(results) == 1
        assert results[0]["name"] == expected_name
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("params", "style", "insert_id"),
    [
        pytest.param((1, "test_tuple"), "tuple_binds", 1, id="tuple_binds"),
        pytest.param({"id": 2, "name": "test_dict"}, "dict_binds", 2, id="dict_binds"),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
@pytest.mark.xdist_group("bigquery")
def test_driver_select_value(adbc_session: AdbcConfig, params: Any, style: ParamStyle, insert_id: int) -> None:
    """Test select_value functionality with different parameter styles."""
    with adbc_session.provide_session() as driver:
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(sql)

        # Insert test record
        if style == "tuple_binds":
            insert_sql = "INSERT INTO test_table (id, name) VALUES (?, ?)"
            select_params = (params[1],)  # Select by name using positional param
            select_sql = "SELECT name FROM test_table WHERE name = ?"
            expected_name = "test_tuple"
        else:  # dict_binds
            insert_sql = "INSERT INTO test_table (id, name) VALUES (@id, @name)"
            select_params = {"name": params["name"]}  # type: ignore[assignment]
            select_sql = "SELECT name FROM test_table WHERE name = @name"
            expected_name = "test_dict"

        driver.insert_update_delete(insert_sql, params)

        # Select and verify
        value = driver.select_value(select_sql, select_params)
        assert value == expected_name
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
@pytest.mark.xdist_group("bigquery")
def test_driver_insert(adbc_session: AdbcConfig) -> None:
    """Test insert functionality using positional parameters."""
    with adbc_session.provide_session() as driver:
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(sql)

        # Insert test record using positional parameters (?)
        insert_sql = "INSERT INTO test_table (id, name) VALUES (?, ?)"
        driver.insert_update_delete(insert_sql, (1, "test_insert"))
        # Note: ADBC insert_update_delete often returns -1 if row count is unknown/unavailable
        # BigQuery might not report row count for INSERT. Check driver behavior.
        # For now, we check execution without error. We'll verify with select.

        # Verify insertion
        results = driver.select("SELECT name FROM test_table WHERE id = ?", (1,))
        assert len(results) == 1
        assert results[0]["name"] == "test_insert"
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
@pytest.mark.xdist_group("bigquery")
def test_driver_select_normal(adbc_session: AdbcConfig) -> None:
    """Test select functionality using positional parameters."""
    with adbc_session.provide_session() as driver:
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = "INSERT INTO test_table (id, name) VALUES (?, ?)"
        driver.insert_update_delete(insert_sql, (10, "test_select_normal"))

        # Select and verify using positional parameters (?)
        select_sql = "SELECT name FROM test_table WHERE id = ?"
        results = driver.select(select_sql, (10,))
        assert len(results) == 1
        assert results[0]["name"] == "test_select_normal"
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
        # Note: BigQuery might require statements separated by semicolons,
        # and driver/adapter needs to handle splitting if the backend doesn't support multistatement scripts directly.
        # Assuming the ADBC driver handles this.
        driver.execute_script(script)

        # Verify execution
        results = driver.select("SELECT COUNT(*) AS count FROM test_table WHERE name LIKE 'script_test%'")
        assert results[0]["count"] == 2

        value = driver.select_value("SELECT name FROM test_table WHERE id = ?", (1,))
        assert value == "script_test"
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
@pytest.mark.xdist_group("bigquery")
def test_driver_select_arrow(adbc_session: AdbcConfig) -> None:
    """Test select_arrow functionality for ADBC BigQuery."""
    with adbc_session.provide_session() as driver:
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id INT64,
            name STRING
        );
        """
        driver.execute_script(sql)

        # Insert test record using positional parameters (?)
        insert_sql = "INSERT INTO test_table (id, name) VALUES (?, ?)"
        driver.insert_update_delete(insert_sql, (100, "arrow_name"))

        # Select and verify with select_arrow using positional parameters (?)
        select_sql = "SELECT name, id FROM test_table WHERE name = ?"
        arrow_table = driver.select_arrow(select_sql, ("arrow_name",))

        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows == 1
        assert arrow_table.num_columns == 2
        # BigQuery might not guarantee column order, sort for check
        assert sorted(arrow_table.column_names) == sorted(["name", "id"])
        # Check data irrespective of column order
        assert arrow_table.column("name").to_pylist() == ["arrow_name"]
        assert arrow_table.column("id").to_pylist() == [100]
        driver.execute_script("DROP TABLE IF EXISTS test_table")
