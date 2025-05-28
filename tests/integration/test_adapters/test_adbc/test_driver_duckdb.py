"""Test ADBC driver with DuckDB."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.sql.result import ExecuteResult, SelectResult
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing


def get_duckdb_create_table_sql() -> str:
    """Get DuckDB-compatible CREATE TABLE SQL with auto-incrementing PK."""
    return """
    CREATE SEQUENCE IF NOT EXISTS test_table_id_seq START 1;
    CREATE TABLE test_table (
        id BIGINT PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
        name VARCHAR(50)
    );
    """


def get_duckdb_create_table_with_age_sql() -> str:
    """Get DuckDB-compatible CREATE TABLE SQL with age column and auto-incrementing PK."""
    return """
    CREATE SEQUENCE IF NOT EXISTS test_table_id_seq START 1;
    CREATE TABLE test_table (
        id BIGINT PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
        name VARCHAR(50),
        age INTEGER
    );
    """


def get_duckdb_create_many_table_sql() -> str:
    """Get DuckDB-compatible CREATE TABLE SQL for execute_many tests with auto-incrementing PK."""
    return """
    CREATE SEQUENCE IF NOT EXISTS test_many_table_id_seq START 1;
    CREATE TABLE test_many_table (
        id BIGINT PRIMARY KEY DEFAULT nextval('test_many_table_id_seq'),
        name VARCHAR(50)
    );
    """


def get_duckdb_create_empty_many_table_sql() -> str:
    """Get DuckDB-compatible CREATE TABLE SQL for empty execute_many tests."""
    return """
    CREATE TABLE test_empty_many_table (
        name VARCHAR(50)
    );
    """


@pytest.fixture
def adbc_session() -> AdbcConfig:
    """Create an ADBC session for DuckDB using URI."""
    return AdbcConfig(
        uri="duckdb://:memory:",
    )


@pytest.mark.parametrize(
    ("params", "sql_placeholder_style"),
    [
        pytest.param(("test_name",), "qmark", id="qmark_params"),
        pytest.param({"name": "test_name"}, "named_colon", id="named_colon_params"),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_insert_returning(adbc_session: AdbcConfig, params: Any, sql_placeholder_style: str) -> None:
    """Test insert returning functionality with different parameter styles."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_table_sql())

        if sql_placeholder_style == "qmark":
            sql_insert = "INSERT INTO test_table (name) VALUES (?) RETURNING *"
        elif sql_placeholder_style == "named_colon":
            sql_insert = "INSERT INTO test_table (name) VALUES (:name) RETURNING *"
        else:
            raise ValueError(f"Unsupported placeholder style for test: {sql_placeholder_style}")

        result = driver.execute(sql_insert, params)
        assert isinstance(result, SelectResult)
        assert result.rows is not None
        assert len(result.rows) == 1
        returned_data = result.rows[0]
        assert returned_data["name"] == "test_name"
        # For DuckDB without explicit AUTOINCREMENT, id might be null initially
        # We'll just check that we got the data we expect

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("params", "sql_placeholder_style"),
    [
        pytest.param(("test_name",), "qmark", id="qmark_params"),
        pytest.param({"name": "test_name"}, "named_colon", id="named_colon_params"),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_select(adbc_session: AdbcConfig, params: Any, sql_placeholder_style: str) -> None:
    """Test select functionality with different parameter styles."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_table_sql())

        if sql_placeholder_style == "qmark":
            insert_sql = "INSERT INTO test_table (name) VALUES (?)"
            select_sql = "SELECT name FROM test_table WHERE name = ?"
        elif sql_placeholder_style == "named_colon":
            insert_sql = "INSERT INTO test_table (name) VALUES (:name)"
            select_sql = "SELECT name FROM test_table WHERE name = :name"
        else:
            raise ValueError(f"Unsupported placeholder style for test: {sql_placeholder_style}")

        insert_result = driver.execute(insert_sql, params)
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        results = driver.execute(select_sql, params)
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test_name"

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("params", "sql_placeholder_style"),
    [
        pytest.param(("test_name",), "qmark", id="qmark_params"),
        pytest.param({"name": "test_name"}, "named_colon", id="named_colon_params"),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_select_value(adbc_session: AdbcConfig, params: Any, sql_placeholder_style: str) -> None:
    """Test select_value functionality with different parameter styles."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_table_sql())

        if sql_placeholder_style == "qmark":
            insert_sql = "INSERT INTO test_table (name) VALUES (?)"
            select_sql = "SELECT name FROM test_table WHERE name = ?"
        elif sql_placeholder_style == "named_colon":
            insert_sql = "INSERT INTO test_table (name) VALUES (:name)"
            select_sql = "SELECT name FROM test_table WHERE name = :name"
        else:
            raise ValueError(f"Unsupported placeholder style for test: {sql_placeholder_style}")

        insert_result = driver.execute(insert_sql, params)
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

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
@pytest.mark.xdist_group("duckdb")
def test_driver_insert(adbc_session: AdbcConfig) -> None:
    """Test insert functionality."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_table_sql())

        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        result = driver.execute(insert_sql, ("test_name",))
        assert isinstance(result, ExecuteResult)
        assert result.rows_affected == 1

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_select_normal(adbc_session: AdbcConfig) -> None:
    """Test select functionality."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_table_sql())

        insert_sql = """
        INSERT INTO test_table (name)
        VALUES ($1)
        """
        driver.execute(insert_sql, ("test_name",))

        select_sql = "SELECT name FROM test_table WHERE name = :name"
        results = driver.execute(select_sql, {"name": "test_name"})
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test_name"
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("sql_to_test", "params_to_test"),
    [
        ("SELECT name FROM test_table WHERE name = ?", ("test_name",)),
        ("SELECT name FROM test_table WHERE name = :name", {"name": "test_name"}),
        ("SELECT name FROM test_table WHERE name = $1", ("test_name",)),
        ("SELECT name FROM test_table WHERE name = $name", {"name": "test_name"}),
        ("SELECT name FROM test_table WHERE name = %s", ("test_name",)),
        ("SELECT name FROM test_table WHERE name = %(name)s", {"name": "test_name"}),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_input_param_styles_for_query(adbc_session: AdbcConfig, sql_to_test: str, params_to_test: Any) -> None:
    """Test that Query object correctly parses various input SQL placeholder styles."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_table_sql())

        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        insert_result = driver.execute(insert_sql, ("test_name",))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        results = driver.execute(sql_to_test, params_to_test)
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test_name"

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_select_to_arrow(adbc_session: AdbcConfig) -> None:
    """Test select_to_arrow functionality for ADBC DuckDB."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_table_sql())

        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        insert_result = driver.execute(insert_sql, ("arrow_name",))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        select_sql = "SELECT name, id FROM test_table WHERE name = ?"
        arrow_table = driver.select_to_arrow(select_sql, ("arrow_name",))

        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows == 1
        assert arrow_table.num_columns == 2
        assert sorted(arrow_table.column_names) == sorted(["name", "id"])
        assert arrow_table.column("name").to_pylist() == ["arrow_name"]
        id_val = arrow_table.column("id").to_pylist()[0]
        assert isinstance(id_val, int)
        assert id_val >= 1

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_named_params_with_scalar(adbc_session: AdbcConfig) -> None:
    """Test that scalar parameters work with named parameters in SQL."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_table_sql())

        insert_sql = "INSERT INTO test_table (name) VALUES (:name)"
        insert_result = driver.execute(insert_sql, "test_name")
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        select_sql = "SELECT name FROM test_table WHERE name = :name"
        results = driver.execute(select_sql, "test_name")
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test_name"

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_named_params_with_tuple(adbc_session: AdbcConfig) -> None:
    """Test that tuple parameters work with named parameters in SQL (mapped by Query)."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_table_with_age_sql())

        insert_sql = "INSERT INTO test_table (name, age) VALUES (:name, :age)"
        insert_result = driver.execute(insert_sql, ("test_name", 30))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        select_sql = "SELECT name, age FROM test_table WHERE name = :name AND age = :age"
        results = driver.execute(select_sql, ("test_name", 30))
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test_name"
        assert results.rows[0]["age"] == 30

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_duckdb_native_named_params(adbc_session: AdbcConfig) -> None:
    """Test DuckDB's native named parameter style ($name)."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_table_sql())

        insert_sql = "INSERT INTO test_table (name) VALUES ($name)"
        insert_result = driver.execute(insert_sql, {"name": "native_name"})
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        select_sql = "SELECT name FROM test_table WHERE name = $name"
        results = driver.execute(select_sql, {"name": "native_name"})
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "native_name"
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_duckdb_native_positional_params(adbc_session: AdbcConfig) -> None:
    """Test DuckDB's native positional parameter style ($1, $2, etc.)."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_table_with_age_sql())

        insert_sql = "INSERT INTO test_table (name, age) VALUES ($1, $2)"
        insert_result = driver.execute(insert_sql, ("native_pos", 30))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        select_sql = "SELECT name, age FROM test_table WHERE name = $1 AND age = $2"
        results = driver.execute(select_sql, ("native_pos", 30))
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "native_pos"
        assert results.rows[0]["age"] == 30
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_duckdb_native_qmark_params(adbc_session: AdbcConfig) -> None:
    """Test DuckDB's native qmark parameter style (?)."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_table_with_age_sql())

        insert_sql = "INSERT INTO test_table (name, age) VALUES (?, ?)"
        insert_result = driver.execute(insert_sql, ("native_qmark", 35))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        select_sql = "SELECT name, age FROM test_table WHERE name = ? AND age = ?"
        results = driver.execute(select_sql, ("native_qmark", 35))
        assert isinstance(results, SelectResult)
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "native_qmark"
        assert results.rows[0]["age"] == 35
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_execute_many_insert(adbc_session: AdbcConfig) -> None:
    """Test execute_many for batch inserts."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_many_table_sql())

        insert_sql = "INSERT INTO test_many_table (name) VALUES (?)"
        params_list = [("name1",), ("name2",), ("name3",)]

        result = driver.execute_many(insert_sql, params_list)
        assert isinstance(result, ExecuteResult)

        # For DuckDB ADBC, rows_affected should be accurate.
        # For other drivers, it might be None or -1 if not supported/reported.
        if driver.dialect == "duckdb":
            assert result.rows_affected == len(params_list)
        else:
            # General assertion for other drivers: rows_affected could be None or a non-negative int
            assert result.rows_affected is None or result.rows_affected >= 0, (
                f"Expected rows_affected to be None or >= 0, got {result.rows_affected}"
            )

        select_sql = "SELECT COUNT(*) as count FROM test_many_table"
        count_result = driver.execute(select_sql)
        assert isinstance(count_result, SelectResult)
        assert count_result.rows is not None
        assert count_result.rows[0]["count"] == len(params_list)

        driver.execute_script("DROP TABLE IF EXISTS test_many_table")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_execute_many_empty_params(adbc_session: AdbcConfig) -> None:
    """Test execute_many with an empty list of parameters."""
    with adbc_session.provide_session() as driver:
        driver.execute_script(get_duckdb_create_empty_many_table_sql())

        insert_sql = "INSERT INTO test_empty_many_table (name) VALUES (?)"
        params_list: list[tuple[str]] = []

        result = driver.execute_many(insert_sql, params_list)
        assert isinstance(result, ExecuteResult)
        assert result.rows_affected == 0

        select_sql = "SELECT COUNT(*) as count FROM test_empty_many_table"
        count_result = driver.execute(select_sql)
        assert isinstance(count_result, SelectResult)
        assert count_result.rows is not None
        assert count_result.rows[0]["count"] == 0

        driver.execute_script("DROP TABLE IF EXISTS test_empty_many_table")
