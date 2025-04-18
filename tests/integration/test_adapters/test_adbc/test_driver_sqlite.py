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
    """Create an ADBC session for SQLite using URI."""
    return Adbc(
        uri="sqlite://:memory:",
    )


@pytest.fixture(autouse=True)
def cleanup_test_table(adbc_session: Adbc) -> None:
    """Clean up the test table before each test."""
    with adbc_session.provide_session() as driver:
        driver.execute_script("DROP TABLE IF EXISTS test_table")


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
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        if style == "tuple_binds":
            sql = """
            INSERT INTO test_table (name)
            VALUES (?)
            RETURNING *
            """
        elif style == "dict_binds":
            sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            RETURNING *
            """
        else:
            raise ValueError(f"Unsupported style: {style}")

        result = driver.insert_update_delete_returning(sql, params)
        assert result is not None
        assert result["name"] == "test_name"
        assert result["id"] is not None


@xfail_if_driver_missing
def test_driver_select(adbc_session: Adbc) -> None:
    """Test select functionality with simple tuple parameters."""
    params = ("test_name",)
    with adbc_session.provide_session() as driver:
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        driver.insert_update_delete(insert_sql, params)

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE name = ?"
        results = driver.select(select_sql, params)
        assert len(results) == 1
        assert results[0]["name"] == "test_name"


@xfail_if_driver_missing
def test_driver_select_value(adbc_session: Adbc) -> None:
    """Test select_value functionality with simple tuple parameters."""
    params = ("test_name",)
    with adbc_session.provide_session() as driver:
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        driver.insert_update_delete(insert_sql, params)

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE name = ?"
        value = driver.select_value(select_sql, params)
        assert value == "test_name"


@xfail_if_driver_missing
def test_driver_insert(adbc_session: Adbc) -> None:
    """Test insert functionality."""
    with adbc_session.provide_session() as driver:
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (?)
        """
        row_count = driver.insert_update_delete(insert_sql, ("test_name",))
        assert row_count == 1 or row_count == -1


@xfail_if_driver_missing
def test_driver_select_normal(adbc_session: Adbc) -> None:
    """Test select functionality."""
    with adbc_session.provide_session() as driver:
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (?)
        """
        driver.insert_update_delete(insert_sql, ("test_name",))

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE name = ?"
        results = driver.select(select_sql, ("test_name",))
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
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (?)
        """
        driver.insert_update_delete(insert_sql, ("test_name",))

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE name = ?"
        results = driver.select(select_sql, ("test_name",))
        assert len(results) == 1
        assert results[0]["name"] == "test_name"
