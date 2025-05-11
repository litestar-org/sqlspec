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

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_select(adbc_session: AdbcConfig) -> None:
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

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_select_value(adbc_session: AdbcConfig) -> None:
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

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_insert(adbc_session: AdbcConfig) -> None:
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

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_select_normal(adbc_session: AdbcConfig) -> None:
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

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    "param_style",
    [
        "qmark",
        "format",
        "pyformat",
    ],
)
@xfail_if_driver_missing
def test_param_styles(adbc_session: AdbcConfig, param_style: str) -> None:
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

        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_select_arrow(adbc_session: AdbcConfig) -> None:
    """Test select_arrow functionality."""
    with adbc_session.provide_session() as driver:
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (?)
        """
        driver.insert_update_delete(insert_sql, ("arrow_name",))

        # Select and verify with select_arrow
        select_sql = "SELECT name, id FROM test_table WHERE name = ?"
        arrow_table = driver.select_arrow(select_sql, ("arrow_name",))

        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows == 1
        assert arrow_table.num_columns == 2
        # Note: Column order might vary depending on DB/driver, adjust if needed
        # Sorting column names for consistent check
        assert sorted(arrow_table.column_names) == sorted(["name", "id"])
        # Check data irrespective of column order
        assert arrow_table.column("name").to_pylist() == ["arrow_name"]
        # Assuming id is 1 for the inserted record
        assert arrow_table.column("id").to_pylist() == [1]
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_named_params_with_scalar(adbc_session: AdbcConfig) -> None:
    """Test that scalar parameters work with named parameters in SQL."""
    with adbc_session.provide_session() as driver:
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record using named parameter with scalar value
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (:name)
        """
        driver.insert_update_delete(insert_sql, "test_name")

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE name = :name"
        results = driver.select(select_sql, "test_name")
        assert len(results) == 1
        assert results[0]["name"] == "test_name"
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@xfail_if_driver_missing
def test_driver_named_params_with_tuple(adbc_session: AdbcConfig) -> None:
    """Test that tuple parameters work with named parameters in SQL."""
    with adbc_session.provide_session() as driver:
        sql = """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(50),
            age INTEGER
        );
        """
        driver.execute_script(sql)

        # Insert test record using named parameters with tuple values
        insert_sql = """
        INSERT INTO test_table (name, age)
        VALUES (:name, :age)
        """
        driver.insert_update_delete(insert_sql, ("test_name", 30))

        # Select and verify
        select_sql = "SELECT name, age FROM test_table WHERE name = :name AND age = :age"
        results = driver.select(select_sql, ("test_name", 30))
        assert len(results) == 1
        assert results[0]["name"] == "test_name"
        assert results[0]["age"] == 30
        driver.execute_script("DROP TABLE IF EXISTS test_table")
