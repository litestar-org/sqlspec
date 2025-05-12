"""Test Psqlpy driver implementation."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any, Literal

import pytest

from sqlspec.adapters.psqlpy.config import PsqlpyConfig, PsqlpyPoolConfig

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService

# Define supported parameter styles for testing
ParamStyle = Literal["tuple_binds", "dict_binds"]

pytestmark = [pytest.mark.psqlpy, pytest.mark.postgres, pytest.mark.integration]


@pytest.fixture
def psqlpy_config(postgres_service: PostgresService) -> PsqlpyConfig:
    """Fixture for PsqlpyConfig using the postgres service."""
    dsn = f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    return PsqlpyConfig(
        pool_config=PsqlpyPoolConfig(
            dsn=dsn,
            max_db_pool_size=5,  # Adjust pool size as needed for tests
        )
    )


@pytest.fixture(autouse=True)
async def _manage_table(psqlpy_config: PsqlpyConfig) -> AsyncGenerator[None, None]:  # pyright: ignore[reportUnusedFunction]
    """Fixture to create and drop the test table for each test."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50)
    );
    """
    drop_sql = "DROP TABLE IF EXISTS test_table;"
    async with psqlpy_config.provide_session() as driver:
        await driver.execute_script(create_sql)
    yield
    async with psqlpy_config.provide_session() as driver:
        await driver.execute_script(drop_sql)


# --- Test Parameter Styles --- #


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.asyncio
async def test_insert_returning_param_styles(psqlpy_config: PsqlpyConfig, params: Any, style: ParamStyle) -> None:
    """Test insert returning with different parameter styles."""
    if style == "tuple_binds":
        sql = "INSERT INTO test_table (name) VALUES (?) RETURNING *"
    else:  # dict_binds
        sql = "INSERT INTO test_table (name) VALUES (:name) RETURNING *"

    async with psqlpy_config.provide_session() as driver:
        result = await driver.insert_update_delete_returning(sql, params)
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
async def test_select_param_styles(psqlpy_config: PsqlpyConfig, params: Any, style: ParamStyle) -> None:
    """Test select with different parameter styles."""
    # Insert test data first (using tuple style for simplicity here)
    insert_sql = "INSERT INTO test_table (name) VALUES (?)"
    async with psqlpy_config.provide_session() as driver:
        await driver.insert_update_delete(insert_sql, ("test_name",))

        # Prepare select SQL based on style
        if style == "tuple_binds":
            select_sql = "SELECT id, name FROM test_table WHERE name = ?"
        else:  # dict_binds
            select_sql = "SELECT id, name FROM test_table WHERE name = :name"

        results = await driver.select(select_sql, params)
        assert len(results) == 1
        assert results[0]["name"] == "test_name"


# --- Test Core Driver Methods --- #


async def test_insert_update_delete(psqlpy_config: PsqlpyConfig) -> None:
    """Test basic insert, update, delete operations."""
    async with psqlpy_config.provide_session() as driver:
        # Insert
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        row_count = await driver.insert_update_delete(insert_sql, ("initial_name",))
        assert row_count == 1

        # Verify Insert
        select_sql = "SELECT name FROM test_table WHERE name = ?"
        result = await driver.select_one(select_sql, ("initial_name",))
        assert result["name"] == "initial_name"

        # Update
        update_sql = "UPDATE test_table SET name = ? WHERE name = ?"
        row_count = await driver.insert_update_delete(update_sql, ("updated_name", "initial_name"))
        assert row_count == 1

        # Verify Update
        result_or_none = await driver.select_one_or_none(select_sql, ("updated_name",))
        assert result_or_none is not None
        assert result_or_none["name"] == "updated_name"
        result_or_none = await driver.select_one_or_none(select_sql, "initial_name")
        assert result_or_none is None

        # Delete
        delete_sql = "DELETE FROM test_table WHERE name = ?"
        row_count = await driver.insert_update_delete(delete_sql, ("updated_name",))
        assert row_count == 1

        # Verify Delete
        result_or_none = await driver.select_one_or_none(select_sql, ("updated_name",))
        assert result_or_none is None


async def test_select_methods(psqlpy_config: PsqlpyConfig) -> None:
    """Test various select methods (select, select_one, select_one_or_none, select_value)."""
    async with psqlpy_config.provide_session() as driver:
        # Insert multiple records
        await driver.insert_update_delete("INSERT INTO test_table (name) VALUES (?), (?)", ("name1", "name2"))

        # Test select (multiple results)
        results = await driver.select("SELECT name FROM test_table ORDER BY name")
        assert len(results) == 2
        assert results[0]["name"] == "name1"
        assert results[1]["name"] == "name2"

        # Test select_one
        result_one = await driver.select_one("SELECT name FROM test_table WHERE name = ?", ("name1",))
        assert result_one["name"] == "name1"

        # Test select_one_or_none (found)
        result_one_none = await driver.select_one_or_none("SELECT name FROM test_table WHERE name = ?", ("name2",))
        assert result_one_none is not None
        assert result_one_none["name"] == "name2"

        # Test select_one_or_none (not found)
        result_one_none_missing = await driver.select_one_or_none(
            "SELECT name FROM test_table WHERE name = ?", ("missing",)
        )
        assert result_one_none_missing is None

        # Test select_value
        value = await driver.select_value("SELECT id FROM test_table WHERE name = ?", ("name1",))
        assert isinstance(value, int)

        # Test select_value_or_none (found)
        value_or_none = await driver.select_value_or_none("SELECT id FROM test_table WHERE name = ?", ("name2",))
        assert isinstance(value_or_none, int)

        # Test select_value_or_none (not found)
        value_or_none_missing = await driver.select_value_or_none(
            "SELECT id FROM test_table WHERE name = ?", ("missing",)
        )
        assert value_or_none_missing is None


async def test_execute_script(psqlpy_config: PsqlpyConfig) -> None:
    """Test execute_script method for non-query operations."""
    sql = "SELECT 1;"  # Simple script
    async with psqlpy_config.provide_session() as driver:
        status = await driver.execute_script(sql)
        # psqlpy execute returns a status string, exact content might vary
        assert isinstance(status, str)
        # We don't assert exact status content as it might change, just that it runs


async def test_multiple_positional_parameters(psqlpy_config: PsqlpyConfig) -> None:
    """Test handling multiple positional parameters in a single SQL statement."""
    async with psqlpy_config.provide_session() as driver:
        # Insert multiple records
        await driver.insert_update_delete("INSERT INTO test_table (name) VALUES (?), (?)", ("param1", "param2"))

        # Query with multiple parameters
        results = await driver.select("SELECT * FROM test_table WHERE name = ? OR name = ?", ("param1", "param2"))
        assert len(results) == 2

        # Test with IN clause
        results = await driver.select("SELECT * FROM test_table WHERE name IN (?, ?)", ("param1", "param2"))
        assert len(results) == 2

        # Test with a mixture of parameter styles
        results = await driver.select("SELECT * FROM test_table WHERE name = ? AND id > ?", ("param1", 0))
        assert len(results) == 1


async def test_scalar_parameter_handling(psqlpy_config: PsqlpyConfig) -> None:
    """Test handling of scalar parameters in various contexts."""
    async with psqlpy_config.provide_session() as driver:
        # Insert a record
        await driver.insert_update_delete("INSERT INTO test_table (name) VALUES (?)", "single_param")

        # Verify the record exists with scalar parameter
        result1 = await driver.select_one("SELECT * FROM test_table WHERE name = ?", "single_param")
        assert result1["name"] == "single_param"

        # Test select_value with scalar parameter
        value = await driver.select_value("SELECT id FROM test_table WHERE name = ?", "single_param")
        assert isinstance(value, int)

        # Test select_one_or_none with scalar parameter that doesn't exist
        result2 = await driver.select_one_or_none("SELECT * FROM test_table WHERE name = ?", "non_existent_param")  #
        assert result2 is None


async def test_question_mark_in_edge_cases(psqlpy_config: PsqlpyConfig) -> None:
    """Test that question marks in comments, strings, and other contexts aren't mistaken for parameters."""
    async with psqlpy_config.provide_session() as driver:
        # Insert a record
        await driver.insert_update_delete("INSERT INTO test_table (name) VALUES (?)", "edge_case_test")

        # Test question mark in a string literal - should not be treated as a parameter
        result = await driver.select_one("SELECT * FROM test_table WHERE name = ? AND '?' = '?'", "edge_case_test")
        assert result["name"] == "edge_case_test"

        # Test question mark in a comment - should not be treated as a parameter
        result = await driver.select_one(
            "SELECT * FROM test_table WHERE name = ? -- Does this work with a ? in a comment?", "edge_case_test"
        )
        assert result["name"] == "edge_case_test"

        # Test question mark in a block comment - should not be treated as a parameter
        result = await driver.select_one(
            "SELECT * FROM test_table WHERE name = ? /* Does this work with a ? in a block comment? */",
            "edge_case_test",
        )
        assert result["name"] == "edge_case_test"

        # Test with mixed parameter styles and multiple question marks
        result = await driver.select_one(
            "SELECT * FROM test_table WHERE name = ? AND '?' = '?' -- Another ? here", "edge_case_test"
        )
        assert result["name"] == "edge_case_test"

        # Test a complex query with multiple question marks in different contexts
        result = await driver.select_one(
            """
            SELECT * FROM test_table
            WHERE name = ? -- A ? in a comment
            AND '?' = '?' -- Another ? here
            AND 'String with a ? in it' = 'String with a ? in it'
            AND /* Block comment with a ? */ id > 0
            """,
            "edge_case_test",
        )
        assert result["name"] == "edge_case_test"


async def test_regex_parameter_binding_complex_case(psqlpy_config: PsqlpyConfig) -> None:
    """Test handling of complex SQL with question mark parameters in various positions."""
    async with psqlpy_config.provide_session() as driver:
        # Insert test records
        await driver.insert_update_delete(
            "INSERT INTO test_table (name) VALUES (?), (?), (?)", ("complex1", "complex2", "complex3")
        )

        # Complex query with parameters at various positions
        results = await driver.select(
            """
            SELECT t1.*
            FROM test_table t1
            JOIN test_table t2 ON t2.id <> t1.id
            WHERE
                t1.name = ? OR
                t1.name = ? OR
                t1.name = ?
                -- Let's add a comment with ? here
                /* And a block comment with ? here */
            ORDER BY t1.id
            """,
            ("complex1", "complex2", "complex3"),
        )

        # With a self-join where id <> id, each of the 3 rows joins with the other 2,
        # resulting in 6 total rows (3 names * 2 matches each)
        assert len(results) == 6

        # Verify that all three names are present in results
        names = {row["name"] for row in results}
        assert names == {"complex1", "complex2", "complex3"}

        # Verify that question marks escaped in strings don't count as parameters
        # This passes 2 parameters and has one ? in a string literal
        result = await driver.select_one(
            """
            SELECT * FROM test_table
            WHERE name = ? AND id IN (
                SELECT id FROM test_table WHERE name = ? AND '?' = '?'
            )
            """,
            ("complex1", "complex1"),
        )
        assert result["name"] == "complex1"
