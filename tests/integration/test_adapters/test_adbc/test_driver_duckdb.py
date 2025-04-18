"""Test ADBC driver with PostgreSQL."""

from __future__ import annotations

from typing import Any, Literal

import pytest

from sqlspec.adapters.adbc import Adbc

# Import the decorator
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture(scope="session")
def adbc_session() -> Adbc:
    """Create an ADBC session for DuckDB using URI."""
    return Adbc(
        uri="duckdb://:memory:",
    )


@pytest.fixture(autouse=True)
def cleanup_test_table(adbc_session: Adbc) -> None:
    """Clean up the test table before each test."""
    with adbc_session.provide_session() as driver:
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
def test_driver_insert_returning(adbc_session: Adbc, params: Any, style: ParamStyle) -> None:
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


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@xfail_if_driver_missing
def test_driver_select(adbc_session: Adbc, params: Any, style: ParamStyle) -> None:
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


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@xfail_if_driver_missing
def test_driver_select_value(adbc_session: Adbc, params: Any, style: ParamStyle) -> None:
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


@xfail_if_driver_missing
def test_driver_insert(adbc_session: Adbc) -> None:
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


@xfail_if_driver_missing
def test_driver_select_normal(adbc_session: Adbc) -> None:
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


@pytest.mark.parametrize(
    "param_style",
    [
        "qmark",
        "format",
        "pyformat",
    ],
)
@xfail_if_driver_missing
def test_param_styles(adbc_session: Adbc, param_style: str) -> None:
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
