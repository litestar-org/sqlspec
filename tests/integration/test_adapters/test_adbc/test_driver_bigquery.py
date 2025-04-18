"""Test ADBC driver with BigQuery."""

from __future__ import annotations

from typing import Any, Literal

import pytest
from adbc_driver_bigquery import DatabaseOptions
from pytest_databases.docker.bigquery import BigQueryService

from sqlspec.adapters.adbc import Adbc
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture(scope="session")
def adbc_session(bigquery_service: BigQueryService) -> Adbc:
    """Create an ADBC session for BigQuery."""
    db_kwargs = {
        DatabaseOptions.PROJECT_ID.value: bigquery_service.project,
        DatabaseOptions.DATASET_ID.value: bigquery_service.dataset,
        DatabaseOptions.AUTH_TYPE.value: DatabaseOptions.AUTH_VALUE_BIGQUERY.value,
    }

    return Adbc(driver_name="adbc_driver_bigquery", db_kwargs=db_kwargs)


@pytest.fixture(autouse=True)
def cleanup_test_table(adbc_session: Adbc) -> None:
    """Clean up the test table before each test."""
    with adbc_session.provide_session() as driver:
        # Using IF EXISTS is generally safer for cleanup
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
def test_driver_select(adbc_session: Adbc, params: Any, style: ParamStyle, insert_id: int) -> None:
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


@pytest.mark.parametrize(
    ("params", "style", "insert_id"),
    [
        pytest.param((1, "test_tuple"), "tuple_binds", 1, id="tuple_binds"),
        pytest.param({"id": 2, "name": "test_dict"}, "dict_binds", 2, id="dict_binds"),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
def test_driver_select_value(adbc_session: Adbc, params: Any, style: ParamStyle, insert_id: int) -> None:
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


@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
def test_driver_insert(adbc_session: Adbc) -> None:
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


@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
def test_driver_select_normal(adbc_session: Adbc) -> None:
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


@xfail_if_driver_missing
@pytest.mark.xfail(reason="BigQuery emulator may cause failures")
def test_execute_script_multiple_statements(adbc_session: Adbc) -> None:
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
