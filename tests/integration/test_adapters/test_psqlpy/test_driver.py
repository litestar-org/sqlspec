"""Test Psqlpy driver implementation."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any, Literal

import pytest

from sqlspec.adapters.psqlpy.config import PsqlpyConfig, PsqlpyPoolConfig
from sqlspec.statement.result import ExecuteResult, SelectResult

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
        result = await driver.execute(sql, params)
        assert isinstance(result, SelectResult)  # RETURNING makes this a SELECT result
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test_name"
        assert result.data[0]["id"] is not None


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
        insert_result = await driver.execute(insert_sql, ("test_name",))
        assert isinstance(insert_result, ExecuteResult)  # type: ignore[unreachable]
        assert insert_result.rows_affected == 1  # type: ignore[unreachable]

        # Prepare select SQL based on style
        if style == "tuple_binds":
            select_sql = "SELECT id, name FROM test_table WHERE name = ?"
        else:  # dict_binds
            select_sql = "SELECT id, name FROM test_table WHERE name = :name"

        select_result = await driver.execute(select_sql, params)
        assert isinstance(select_result, SelectResult)
        assert select_result.data is not None
        assert len(select_result.data) == 1
        assert select_result.data[0]["name"] == "test_name"


# --- Test Core Driver Methods --- #


async def test_insert_update_delete(psqlpy_config: PsqlpyConfig) -> None:
    """Test basic insert, update, delete operations."""
    async with psqlpy_config.provide_session() as driver:
        # Insert
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        insert_result = await driver.execute(insert_sql, ("initial_name",))
        assert isinstance(insert_result, ExecuteResult)  # type: ignore[unreachable]
        assert insert_result.rows_affected == 1  # type: ignore[unreachable]

        # Verify Insert
        select_sql = "SELECT name FROM test_table WHERE name = ?"
        select_result = await driver.execute(select_sql, ("initial_name",))
        assert isinstance(select_result, SelectResult)
        assert select_result.data is not None
        assert len(select_result.data) == 1
        assert select_result.data[0]["name"] == "initial_name"

        # Update
        update_sql = "UPDATE test_table SET name = ? WHERE name = ?"
        update_result = await driver.execute(update_sql, ("updated_name", "initial_name"))
        assert isinstance(update_result, ExecuteResult)
        assert update_result.rows_affected == 1

        # Verify Update
        updated_result = await driver.execute(select_sql, ("updated_name",))
        assert isinstance(updated_result, SelectResult)
        assert updated_result.data is not None
        assert len(updated_result.data) == 1
        assert updated_result.data[0]["name"] == "updated_name"

        # Verify old name no longer exists
        old_result = await driver.execute(select_sql, ("initial_name",))
        assert isinstance(old_result, SelectResult)
        assert old_result.data is not None
        assert len(old_result.data) == 0

        # Delete
        delete_sql = "DELETE FROM test_table WHERE name = ?"
        delete_result = await driver.execute(delete_sql, ("updated_name",))
        assert isinstance(delete_result, ExecuteResult)
        assert delete_result.rows_affected == 1

        # Verify Delete
        final_result = await driver.execute(select_sql, ("updated_name",))
        assert isinstance(final_result, SelectResult)
        assert final_result.data is not None
        assert len(final_result.data) == 0


async def test_select_methods(psqlpy_config: PsqlpyConfig) -> None:
    """Test various select methods and result handling."""
    async with psqlpy_config.provide_session() as driver:
        # Insert multiple records using execute_many
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        params_list = [("name1",), ("name2",)]
        many_result = await driver.execute_many(insert_sql, params_list)
        assert isinstance(many_result, ExecuteResult)
        assert many_result.rows_affected == 2

        # Test select (multiple results)
        select_result = await driver.execute("SELECT name FROM test_table ORDER BY name")
        assert isinstance(select_result, SelectResult)
        assert select_result.data is not None
        assert len(select_result.data) == 2
        assert select_result.data[0]["name"] == "name1"
        assert select_result.data[1]["name"] == "name2"

        # Test select one (using get_first helper)
        single_result = await driver.execute("SELECT name FROM test_table WHERE name = ?", ("name1",))
        assert isinstance(single_result, SelectResult)
        assert single_result.data is not None
        assert len(single_result.data) == 1
        first_row = single_result.get_first()
        assert first_row is not None
        assert first_row["name"] == "name1"

        # Test select one or none (found)
        found_result = await driver.execute("SELECT name FROM test_table WHERE name = ?", ("name2",))
        assert isinstance(found_result, SelectResult)
        assert found_result.data is not None
        assert len(found_result.data) == 1
        found_first = found_result.get_first()
        assert found_first is not None
        assert found_first["name"] == "name2"

        # Test select one or none (not found)
        missing_result = await driver.execute("SELECT name FROM test_table WHERE name = ?", ("missing",))
        assert isinstance(missing_result, SelectResult)
        assert missing_result.data is not None
        assert len(missing_result.data) == 0
        assert missing_result.get_first() is None

        # Test select value
        value_result = await driver.execute("SELECT id FROM test_table WHERE name = ?", ("name1",))
        assert isinstance(value_result, SelectResult)
        assert value_result.data is not None
        assert len(value_result.data) == 1
        assert value_result.column_names is not None
        value = value_result.data[0][value_result.column_names[0]]
        assert isinstance(value, int)


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
        # Insert multiple records using execute_many
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        params_list = [("param1",), ("param2",)]
        many_result = await driver.execute_many(insert_sql, params_list)
        assert isinstance(many_result, ExecuteResult)
        assert many_result.rows_affected == 2

        # Query with multiple parameters
        select_result = await driver.execute(
            "SELECT * FROM test_table WHERE name = ? OR name = ?", ("param1", "param2")
        )
        assert isinstance(select_result, SelectResult)
        assert select_result.data is not None
        assert len(select_result.data) == 2

        # Test with IN clause
        in_result = await driver.execute("SELECT * FROM test_table WHERE name IN (?, ?)", ("param1", "param2"))
        assert isinstance(in_result, SelectResult)
        assert in_result.data is not None
        assert len(in_result.data) == 2

        # Test with a mixture of parameter styles
        mixed_result = await driver.execute("SELECT * FROM test_table WHERE name = ? AND id > ?", ("param1", 0))
        assert isinstance(mixed_result, SelectResult)
        assert mixed_result.data is not None
        assert len(mixed_result.data) == 1


async def test_scalar_parameter_handling(psqlpy_config: PsqlpyConfig) -> None:
    """Test handling of scalar parameters in various contexts."""
    async with psqlpy_config.provide_session() as driver:
        # Insert a record
        insert_result = await driver.execute("INSERT INTO test_table (name) VALUES (?)", "single_param")
        assert isinstance(insert_result, ExecuteResult)  # type: ignore[unreachable]
        assert insert_result.rows_affected == 1  # type: ignore[unreachable]

        # Verify the record exists with scalar parameter
        select_result = await driver.execute("SELECT * FROM test_table WHERE name = ?", "single_param")
        assert isinstance(select_result, SelectResult)
        assert select_result.data is not None
        assert len(select_result.data) == 1
        assert select_result.data[0]["name"] == "single_param"

        # Test select_value with scalar parameter
        value_result = await driver.execute("SELECT id FROM test_table WHERE name = ?", "single_param")
        assert isinstance(value_result, SelectResult)
        assert value_result.data is not None
        assert len(value_result.data) == 1
        assert value_result.column_names is not None
        value = value_result.data[0][value_result.column_names[0]]
        assert isinstance(value, int)

        # Test select_one_or_none with scalar parameter that doesn't exist
        missing_result = await driver.execute("SELECT * FROM test_table WHERE name = ?", "non_existent_param")
        assert isinstance(missing_result, SelectResult)
        assert missing_result.data is not None
        assert len(missing_result.data) == 0


async def test_question_mark_in_edge_cases(psqlpy_config: PsqlpyConfig) -> None:
    """Test that question marks in comments, strings, and other contexts aren't mistaken for parameters."""
    async with psqlpy_config.provide_session() as driver:
        # Insert a record
        insert_result = await driver.execute("INSERT INTO test_table (name) VALUES (?)", "edge_case_test")
        assert isinstance(insert_result, ExecuteResult)  # type: ignore[unreachable]
        assert insert_result.rows_affected == 1  # type: ignore[unreachable]

        # Test question mark in a string literal - should not be treated as a parameter
        result = await driver.execute("SELECT * FROM test_table WHERE name = ? AND '?' = '?'", "edge_case_test")
        assert isinstance(result, SelectResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["name"] == "edge_case_test"

        # Test question mark in a comment - should not be treated as a parameter
        result = await driver.execute(
            "SELECT * FROM test_table WHERE name = ? -- Does this work with a ? in a comment?", "edge_case_test"
        )
        assert isinstance(result, SelectResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["name"] == "edge_case_test"

        # Test question mark in a block comment - should not be treated as a parameter
        result = await driver.execute(
            "SELECT * FROM test_table WHERE name = ? /* Does this work with a ? in a block comment? */",
            "edge_case_test",
        )
        assert isinstance(result, SelectResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["name"] == "edge_case_test"

        # Test with mixed parameter styles and multiple question marks
        result = await driver.execute(
            "SELECT * FROM test_table WHERE name = ? AND '?' = '?' -- Another ? here", "edge_case_test"
        )
        assert isinstance(result, SelectResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["name"] == "edge_case_test"

        # Test a complex query with multiple question marks in different contexts
        result = await driver.execute(
            """
            SELECT * FROM test_table
            WHERE name = ? -- A ? in a comment
            AND '?' = '?' -- Another ? here
            AND 'String with a ? in it' = 'String with a ? in it'
            AND /* Block comment with a ? */ id > 0
            """,
            "edge_case_test",
        )
        assert isinstance(result, SelectResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["name"] == "edge_case_test"


async def test_regex_parameter_binding_complex_case(psqlpy_config: PsqlpyConfig) -> None:
    """Test handling of complex SQL with question mark parameters in various positions."""
    async with psqlpy_config.provide_session() as driver:
        # Insert test records using execute_many
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        params_list = [("complex1",), ("complex2",), ("complex3",)]
        many_result = await driver.execute_many(insert_sql, params_list)
        assert isinstance(many_result, ExecuteResult)
        assert many_result.rows_affected == 3

        # Complex query with parameters at various positions
        select_result = await driver.execute(
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
        assert isinstance(select_result, SelectResult)
        assert select_result.data is not None

        # With a self-join where id <> id, each of the 3 rows joins with the other 2,
        # resulting in 6 total rows (3 names * 2 matches each)
        assert len(select_result.data) == 6

        # Verify that all three names are present in results
        names = {row["name"] for row in select_result.data}
        assert names == {"complex1", "complex2", "complex3"}

        # Verify that question marks escaped in strings don't count as parameters
        # This passes 2 parameters and has one ? in a string literal
        subquery_result = await driver.execute(
            """
            SELECT * FROM test_table
            WHERE name = ? AND id IN (
                SELECT id FROM test_table WHERE name = ? AND '?' = '?'
            )
            """,
            ("complex1", "complex1"),
        )
        assert isinstance(subquery_result, SelectResult)
        assert subquery_result.data is not None
        assert len(subquery_result.data) == 1
        assert subquery_result.data[0]["name"] == "complex1"


async def test_execute_many_insert(psqlpy_config: PsqlpyConfig) -> None:
    """Test execute_many functionality for batch inserts."""
    async with psqlpy_config.provide_session() as driver:
        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        params_list = [("many_name1",), ("many_name2",), ("many_name3",)]

        result = await driver.execute_many(insert_sql, params_list)
        assert isinstance(result, ExecuteResult)
        assert result.rows_affected == len(params_list)

        # Verify all records were inserted
        select_result = await driver.execute("SELECT COUNT(*) as count FROM test_table")
        assert isinstance(select_result, SelectResult)
        assert select_result.data is not None
        assert select_result.data[0]["count"] == len(params_list)


async def test_update_operation(psqlpy_config: PsqlpyConfig) -> None:
    """Test UPDATE operations."""
    async with psqlpy_config.provide_session() as driver:
        # Insert a record first
        insert_result = await driver.execute("INSERT INTO test_table (name) VALUES (?)", ("original_name",))
        assert isinstance(insert_result, ExecuteResult)  # type: ignore[unreachable]
        assert insert_result.rows_affected == 1  # type: ignore[unreachable]

        # Update the record
        update_result = await driver.execute("UPDATE test_table SET name = ? WHERE id = ?", ("updated_name", 1))
        assert isinstance(update_result, ExecuteResult)
        assert update_result.rows_affected == 1

        # Verify the update
        select_result = await driver.execute("SELECT name FROM test_table WHERE id = ?", (1,))
        assert isinstance(select_result, SelectResult)
        assert select_result.data is not None
        assert select_result.data[0]["name"] == "updated_name"


async def test_delete_operation(psqlpy_config: PsqlpyConfig) -> None:
    """Test DELETE operations."""
    async with psqlpy_config.provide_session() as driver:
        # Insert a record first
        insert_result = await driver.execute("INSERT INTO test_table (name) VALUES (?)", ("to_delete",))
        assert isinstance(insert_result, ExecuteResult)  # type: ignore[unreachable]
        assert insert_result.rows_affected == 1  # type: ignore[unreachable]

        # Delete the record
        delete_result = await driver.execute("DELETE FROM test_table WHERE id = ?", (1,))
        assert isinstance(delete_result, ExecuteResult)  # type: ignore[unreachable]
        assert delete_result.rows_affected == 1  # type: ignore[unreachable]

        # Verify the deletion
        select_result = await driver.execute("SELECT COUNT(*) as count FROM test_table")
        assert isinstance(select_result, SelectResult)
        assert select_result.data is not None
        assert select_result.data[0]["count"] == 0
