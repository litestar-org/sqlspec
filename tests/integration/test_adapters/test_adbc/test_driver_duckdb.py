"""Test ADBC driver with PostgreSQL."""

from __future__ import annotations

from typing import Any, Literal

import pyarrow as pa
import pytest

from sqlspec.adapters.adbc import AdbcConfig

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture
def adbc_session() -> AdbcConfig:
    """Create an ADBC session for DuckDB using URI."""
    return AdbcConfig(
        uri="duckdb://:memory:",
    )


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_insert_returning(adbc_session: AdbcConfig, params: Any, style: ParamStyle) -> None:
    """Test insert returning functionality with different parameter styles."""
    with adbc_session.provide_session() as driver:
        create_sequence_sql = "CREATE SEQUENCE test_table_id_seq START 1;"
        driver.execute_script(create_sequence_sql)
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        RETURNING *
        """ % ("$1" if style == "tuple_binds" else ":name")

        result = driver.insert_update_delete_returning(sql, params)
        assert result is not None
        assert result["name"] == "test_name"
        assert result["id"] is not None
        driver.execute_script("DROP TABLE IF EXISTS test_table")
        driver.execute_script("DROP SEQUENCE IF EXISTS test_table_id_seq")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_select(adbc_session: AdbcConfig, params: Any, style: ParamStyle) -> None:
    """Test select functionality with different parameter styles."""
    with adbc_session.provide_session() as driver:
        # Create test table
        create_sequence_sql = "CREATE SEQUENCE test_table_id_seq START 1;"
        driver.execute_script(create_sequence_sql)
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        """ % ("$1" if style == "tuple_binds" else ":name")
        driver.insert_update_delete(insert_sql, params)

        # Select and verify
        select_sql = """
        SELECT name FROM test_table WHERE name = %s
        """ % ("$1" if style == "tuple_binds" else ":name")
        results = driver.select(select_sql, params)
        assert len(results) == 1
        assert results[0]["name"] == "test_name"
        driver.execute_script("DROP TABLE IF EXISTS test_table")
        driver.execute_script("DROP SEQUENCE IF EXISTS test_table_id_seq")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_select_value(adbc_session: AdbcConfig, params: Any, style: ParamStyle) -> None:
    """Test select_value functionality with different parameter styles."""
    with adbc_session.provide_session() as driver:
        # Create test table
        create_sequence_sql = "CREATE SEQUENCE test_table_id_seq START 1;"
        driver.execute_script(create_sequence_sql)
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        """ % ("$1" if style == "tuple_binds" else ":name")
        driver.insert_update_delete(insert_sql, params)

        # Select and verify
        select_sql = """
        SELECT name FROM test_table WHERE name = %s
        """ % ("$1" if style == "tuple_binds" else ":name")
        value = driver.select_value(select_sql, params)
        assert value == "test_name"
        driver.execute_script("DROP TABLE IF EXISTS test_table")
        driver.execute_script("DROP SEQUENCE IF EXISTS test_table_id_seq")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_insert(adbc_session: AdbcConfig) -> None:
    """Test insert functionality."""
    with adbc_session.provide_session() as driver:
        # Create test table
        create_sequence_sql = "CREATE SEQUENCE test_table_id_seq START 1;"
        driver.execute_script(create_sequence_sql)
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES ($1)
        """
        row_count = driver.insert_update_delete(insert_sql, ("test_name",))
        assert row_count in (0, 1, -1)
        driver.execute_script("DROP TABLE IF EXISTS test_table")
        driver.execute_script("DROP SEQUENCE IF EXISTS test_table_id_seq")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_select_normal(adbc_session: AdbcConfig) -> None:
    """Test select functionality."""
    with adbc_session.provide_session() as driver:
        # Create test table
        create_sequence_sql = "CREATE SEQUENCE test_table_id_seq START 1;"
        driver.execute_script(create_sequence_sql)
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES ($1)
        """
        driver.insert_update_delete(insert_sql, ("test_name",))

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE name = :name"
        results = driver.select(select_sql, {"name": "test_name"})
        assert len(results) == 1
        assert results[0]["name"] == "test_name"
        driver.execute_script("DROP TABLE IF EXISTS test_table")
        driver.execute_script("DROP SEQUENCE IF EXISTS test_table_id_seq")


@pytest.mark.parametrize(
    "param_style",
    [
        "qmark",
        "format",
        "pyformat",
    ],
)
@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_param_styles(adbc_session: AdbcConfig, param_style: str) -> None:
    """Test different parameter styles."""
    with adbc_session.provide_session() as driver:
        # Create test table
        create_sequence_sql = "CREATE SEQUENCE test_table_id_seq START 1;"
        driver.execute_script(create_sequence_sql)
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES ($1)
        """
        driver.insert_update_delete(insert_sql, ("test_name",))

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE name = $1"
        results = driver.select(select_sql, ("test_name",))
        assert len(results) == 1
        assert results[0]["name"] == "test_name"
        driver.execute_script("DROP TABLE IF EXISTS test_table")
        driver.execute_script("DROP SEQUENCE IF EXISTS test_table_id_seq")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_select_arrow(adbc_session: AdbcConfig) -> None:
    """Test select_arrow functionality for ADBC DuckDB."""
    with adbc_session.provide_session() as driver:
        # Create test table
        create_sequence_sql = "CREATE SEQUENCE test_table_id_seq START 1;"
        driver.execute_script(create_sequence_sql)
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record using a known param style ($1 for duckdb)
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES ($1)
        """
        driver.insert_update_delete(insert_sql, ("arrow_name",))

        # Select and verify with select_arrow using a known param style
        select_sql = "SELECT name, id FROM test_table WHERE name = $1"
        arrow_table = driver.select_arrow(select_sql, ("arrow_name",))

        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows == 1
        assert arrow_table.num_columns == 2
        # DuckDB should return columns in selected order
        assert arrow_table.column_names == ["name", "id"]
        assert arrow_table.column("name").to_pylist() == ["arrow_name"]
        # Assuming id is 1 for the inserted record
        assert arrow_table.column("id").to_pylist() == [1]
        driver.execute_script("DROP TABLE IF EXISTS test_table")
        driver.execute_script("DROP SEQUENCE IF EXISTS test_table_id_seq")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_named_params_with_scalar(adbc_session: AdbcConfig) -> None:
    """Test that scalar parameters work with named parameters in SQL."""
    with adbc_session.provide_session() as driver:
        # Create test table
        create_sequence_sql = "CREATE SEQUENCE test_table_id_seq START 1;"
        driver.execute_script(create_sequence_sql)
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record using positional parameter with scalar value
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (?)
        """
        driver.insert_update_delete(insert_sql, "test_name")

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE name = ?"
        results = driver.select(select_sql, "test_name")
        assert len(results) == 1
        assert results[0]["name"] == "test_name"
        driver.execute_script("DROP TABLE IF EXISTS test_table")
        driver.execute_script("DROP SEQUENCE IF EXISTS test_table_id_seq")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_named_params_with_tuple(adbc_session: AdbcConfig) -> None:
    """Test that tuple parameters work with named parameters in SQL."""
    with adbc_session.provide_session() as driver:
        # Create test table
        create_sequence_sql = "CREATE SEQUENCE test_table_id_seq START 1;"
        driver.execute_script(create_sequence_sql)
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
            name VARCHAR(50),
            age INTEGER
        );
        """
        driver.execute_script(sql)

        # Insert test record using positional parameters with tuple values
        insert_sql = """
        INSERT INTO test_table (name, age)
        VALUES (?, ?)
        """
        driver.insert_update_delete(insert_sql, ("test_name", 30))

        # Select and verify
        select_sql = "SELECT name, age FROM test_table WHERE name = ? AND age = ?"
        results = driver.select(select_sql, ("test_name", 30))
        assert len(results) == 1
        assert results[0]["name"] == "test_name"
        assert results[0]["age"] == 30
        driver.execute_script("DROP TABLE IF EXISTS test_table")
        driver.execute_script("DROP SEQUENCE IF EXISTS test_table_id_seq")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_native_named_params(adbc_session: AdbcConfig) -> None:
    """Test DuckDB's native named parameter style ($name)."""
    with adbc_session.provide_session() as driver:
        # Create test table
        create_sequence_sql = "CREATE SEQUENCE test_table_id_seq START 1;"
        driver.execute_script(create_sequence_sql)
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record using native $name style
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES ($name)
        """
        driver.insert_update_delete(insert_sql, {"name": "native_name"})

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE name = $name"
        results = driver.select(select_sql, {"name": "native_name"})
        assert len(results) == 1
        assert results[0]["name"] == "native_name"
        driver.execute_script("DROP TABLE IF EXISTS test_table")
        driver.execute_script("DROP SEQUENCE IF EXISTS test_table_id_seq")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_native_positional_params(adbc_session: AdbcConfig) -> None:
    """Test DuckDB's native positional parameter style ($1, $2, etc.)."""
    with adbc_session.provide_session() as driver:
        # Create test table
        create_sequence_sql = "CREATE SEQUENCE test_table_id_seq START 1;"
        driver.execute_script(create_sequence_sql)
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
            name VARCHAR(50),
            age INTEGER
        );
        """
        driver.execute_script(sql)

        # Insert test record using native $1 style
        insert_sql = """
        INSERT INTO test_table (name, age)
        VALUES ($1, $2)
        """
        driver.insert_update_delete(insert_sql, ("native_pos", 30))

        # Select and verify
        select_sql = "SELECT name, age FROM test_table WHERE name = $1 AND age = $2"
        results = driver.select(select_sql, ("native_pos", 30))
        assert len(results) == 1
        assert results[0]["name"] == "native_pos"
        assert results[0]["age"] == 30
        driver.execute_script("DROP TABLE IF EXISTS test_table")
        driver.execute_script("DROP SEQUENCE IF EXISTS test_table_id_seq")


@xfail_if_driver_missing
@pytest.mark.xdist_group("duckdb")
def test_driver_native_auto_incremented_params(adbc_session: AdbcConfig) -> None:
    """Test DuckDB's native auto-incremented parameter style (?)."""
    with adbc_session.provide_session() as driver:
        # Create test table
        create_sequence_sql = "CREATE SEQUENCE test_table_id_seq START 1;"
        driver.execute_script(create_sequence_sql)
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY DEFAULT nextval('test_table_id_seq'),
            name VARCHAR(50),
            age INTEGER
        );
        """
        driver.execute_script(sql)

        # Insert test record using native ? style
        insert_sql = """
        INSERT INTO test_table (name, age)
        VALUES (?, ?)
        """
        driver.insert_update_delete(insert_sql, ("native_auto", 35))

        # Select and verify
        select_sql = "SELECT name, age FROM test_table WHERE name = ? AND age = ?"
        results = driver.select(select_sql, ("native_auto", 35))
        assert len(results) == 1
        assert results[0]["name"] == "native_auto"
        assert results[0]["age"] == 35
        driver.execute_script("DROP TABLE IF EXISTS test_table")
        driver.execute_script("DROP SEQUENCE IF EXISTS test_table_id_seq")
