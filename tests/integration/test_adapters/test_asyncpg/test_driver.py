"""Test Asyncpg driver implementation."""

from __future__ import annotations

from typing import Any, Literal

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgPoolConfig
from sqlspec.sql.result import ExecuteResult, SelectResult

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture
def asyncpg_config(postgres_service: PostgresService) -> AsyncpgConfig:
    """Create an Asyncpg configuration.

    Args:
        postgres_service: PostgreSQL service fixture.

    Returns:
        Configured Asyncpg session config.
    """
    return AsyncpgConfig(
        pool_config=AsyncpgPoolConfig(
            dsn=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            min_size=1,  # Add min_size to avoid pool deadlock issues in tests
            max_size=5,
        )
    )


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_async_insert_returning(asyncpg_config: AsyncpgConfig, params: Any, style: ParamStyle) -> None:
    """Test async insert returning functionality with different parameter styles."""
    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_table")  # Ensure clean state
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql)

        # Use appropriate SQL for each style (sqlspec driver handles conversion to $1, $2...)
        if style == "tuple_binds":
            sql = """
            INSERT INTO test_table (name)
            VALUES (?)
            RETURNING *
            """
        else:  # dict_binds
            sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            RETURNING *
            """

        try:
            result = await driver.execute(sql, params)
            assert isinstance(result, SelectResult)  # RETURNING makes this a SELECT result
            assert result.rows is not None
            assert len(result.rows) == 1
            assert result.rows[0]["name"] == "test_name"
            assert result.rows[0]["id"] is not None
        finally:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_async_select(asyncpg_config: AsyncpgConfig, params: Any, style: ParamStyle) -> None:
    """Test async select functionality with different parameter styles."""
    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_table")  # Ensure clean state
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql)

        # Insert test record
        if style == "tuple_binds":
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (?)
            """
        else:  # dict_binds
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            """
        insert_result = await driver.execute(insert_sql, params)
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        # Select and verify
        if style == "tuple_binds":
            select_sql = """
            SELECT name FROM test_table WHERE name = ?
            """
        else:  # dict_binds
            select_sql = """
            SELECT name FROM test_table WHERE name = :name
            """
        try:
            select_result = await driver.execute(select_sql, params)
            assert isinstance(select_result, SelectResult)
            assert select_result.rows is not None
            assert len(select_result.rows) == 1
            assert select_result.rows[0]["name"] == "test_name"
        finally:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_async_select_value(asyncpg_config: AsyncpgConfig, params: Any, style: ParamStyle) -> None:
    """Test async select value functionality with different parameter styles."""
    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_table")  # Ensure clean state
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql)

        # Insert test record
        if style == "tuple_binds":
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (?)
            """
        else:  # dict_binds
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            """
        insert_result = await driver.execute(insert_sql, params)
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        # Get literal string to test with select value
        # Use a literal query to test select_value
        select_sql = "SELECT 'test_name' AS test_name"

        try:
            # Don't pass parameters with a literal query that has no placeholders
            value_result = await driver.execute(select_sql)
            assert isinstance(value_result, SelectResult)
            assert value_result.rows is not None
            assert len(value_result.rows) == 1
            assert value_result.column_names is not None

            # Extract single value using column name
            value = value_result.rows[0][value_result.column_names[0]]
            assert value == "test_name"
        finally:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_insert(asyncpg_config: AsyncpgConfig) -> None:
    """Test inserting data."""
    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_table")  # Ensure clean state
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql)

        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        try:
            insert_result = await driver.execute(insert_sql, ("test",))
            assert isinstance(insert_result, ExecuteResult)
            assert insert_result.rows_affected == 1

            # Verify insertion
            select_sql = "SELECT COUNT(*) as count FROM test_table WHERE name = ?"
            count_result = await driver.execute(select_sql, ("test",))
            assert isinstance(count_result, SelectResult)
            assert count_result.rows is not None
            assert count_result.rows[0]["count"] == 1
        finally:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_select(asyncpg_config: AsyncpgConfig) -> None:
    """Test selecting data."""
    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_table")  # Ensure clean state
        # Create and populate test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql)

        insert_sql = "INSERT INTO test_table (name) VALUES (?)"
        insert_result = await driver.execute(insert_sql, ("test",))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE id = ?"
        try:
            select_result = await driver.execute(select_sql, (1,))
            assert isinstance(select_result, SelectResult)
            assert select_result.rows is not None
            assert len(select_result.rows) == 1
            assert select_result.rows[0]["name"] == "test"
        finally:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")


# Asyncpg uses positional ($n) parameters internally.
# The sqlspec driver converts '?' (tuple) and ':name' (dict) styles.
# We test these two styles as they are what the user interacts with via sqlspec.
@pytest.mark.parametrize(
    "param_style",
    [
        "tuple_binds",  # Corresponds to '?' in SQL passed to sqlspec
        "dict_binds",  # Corresponds to ':name' in SQL passed to sqlspec
    ],
)
@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_param_styles(asyncpg_config: AsyncpgConfig, param_style: str) -> None:
    """Test different parameter styles expected by sqlspec."""
    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_table")  # Ensure clean state
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql)

        # Insert test record based on param style
        if param_style == "tuple_binds":
            insert_sql = "INSERT INTO test_table (name) VALUES (?)"
            params: Any = ("test",)
        else:  # dict_binds
            insert_sql = "INSERT INTO test_table (name) VALUES (:name)"
            params = {"name": "test"}

        try:
            insert_result = await driver.execute(insert_sql, params)
            assert isinstance(insert_result, ExecuteResult)
            assert insert_result.rows_affected == 1

            # Select and verify
            select_sql = "SELECT name FROM test_table WHERE id = ?"
            select_result = await driver.execute(select_sql, (1,))
            assert isinstance(select_result, SelectResult)
            assert select_result.rows is not None
            assert len(select_result.rows) == 1
            assert select_result.rows[0]["name"] == "test"
        finally:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_question_mark_in_edge_cases(asyncpg_config: AsyncpgConfig) -> None:
    """Test that question marks in comments, strings, and other contexts aren't mistaken for parameters."""
    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_table")  # Ensure clean state
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql)

        # Insert a record
        insert_result = await driver.execute("INSERT INTO test_table (name) VALUES (?)", "edge_case_test")
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        try:
            # Test question mark in a string literal - should not be treated as a parameter
            result = await driver.execute("SELECT * FROM test_table WHERE name = ? AND '?' = '?'", "edge_case_test")
            assert isinstance(result, SelectResult)
            assert result.rows is not None
            assert len(result.rows) == 1
            assert result.rows[0]["name"] == "edge_case_test"

            # Test question mark in a comment - should not be treated as a parameter
            result = await driver.execute(
                "SELECT * FROM test_table WHERE name = ? -- Does this work with a ? in a comment?", "edge_case_test"
            )
            assert isinstance(result, SelectResult)
            assert result.rows is not None
            assert len(result.rows) == 1
            assert result.rows[0]["name"] == "edge_case_test"

            # Test question mark in a block comment - should not be treated as a parameter
            result = await driver.execute(
                "SELECT * FROM test_table WHERE name = ? /* Does this work with a ? in a block comment? */",
                "edge_case_test",
            )
            assert isinstance(result, SelectResult)
            assert result.rows is not None
            assert len(result.rows) == 1
            assert result.rows[0]["name"] == "edge_case_test"

            # Test with mixed parameter styles and multiple question marks
            result = await driver.execute(
                "SELECT * FROM test_table WHERE name = ? AND '?' = '?' -- Another ? here", "edge_case_test"
            )
            assert isinstance(result, SelectResult)
            assert result.rows is not None
            assert len(result.rows) == 1
            assert result.rows[0]["name"] == "edge_case_test"

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
            assert result.rows is not None
            assert len(result.rows) == 1
            assert result.rows[0]["name"] == "edge_case_test"
        finally:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_regex_parameter_binding_complex_case(asyncpg_config: AsyncpgConfig) -> None:
    """Test handling of complex SQL with question mark parameters in various positions."""
    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_table")  # Ensure clean state
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql)

        try:
            # Insert test records
            insert_result = await driver.execute(
                "INSERT INTO test_table (name) VALUES (?), (?), (?)", ("complex1", "complex2", "complex3")
            )
            assert isinstance(insert_result, ExecuteResult)
            assert insert_result.rows_affected == 3

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
            assert select_result.rows is not None

            # With a self-join where id <> id, each of the 3 rows joins with the other 2,
            # resulting in 6 total rows (3 names X 2 matches each)
            assert len(select_result.rows) == 6

            # Verify that all three names are present in results
            names = {row["name"] for row in select_result.rows}
            assert names == {"complex1", "complex2", "complex3"}

            # Verify that question marks escaped in strings don't count as parameters
            # This passes 2 parameters and has one ? in a string literal
            result = await driver.execute(
                """
                SELECT * FROM test_table
                WHERE name = ? AND id IN (
                    SELECT id FROM test_table WHERE name = ? AND '?' = '?'
                )
                """,
                ("complex1", "complex1"),
            )
            assert isinstance(result, SelectResult)
            assert result.rows is not None
            assert len(result.rows) == 1
            assert result.rows[0]["name"] == "complex1"
        finally:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_execute_many_insert(asyncpg_config: AsyncpgConfig) -> None:
    """Test execute_many functionality for batch inserts."""
    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_many_table")
        sql_create = """
        CREATE TABLE test_many_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_many_table (name) VALUES (?)"
        params_list = [("name1",), ("name2",), ("name3",)]

        try:
            result = await driver.execute_many(insert_sql, params_list)
            assert isinstance(result, ExecuteResult)
            assert result.rows_affected == len(params_list)

            select_sql = "SELECT COUNT(*) as count FROM test_many_table"
            count_result = await driver.execute(select_sql)
            assert isinstance(count_result, SelectResult)
            assert count_result.rows is not None
            assert count_result.rows[0]["count"] == len(params_list)
        finally:
            await driver.execute_script("DROP TABLE IF EXISTS test_many_table")


@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_update_operation(asyncpg_config: AsyncpgConfig) -> None:
    """Test UPDATE operations."""
    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_table")
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql)

        try:
            # Insert a record first
            insert_result = await driver.execute("INSERT INTO test_table (name) VALUES (?)", ("original_name",))
            assert isinstance(insert_result, ExecuteResult)
            assert insert_result.rows_affected == 1

            # Update the record
            update_result = await driver.execute("UPDATE test_table SET name = ? WHERE id = ?", ("updated_name", 1))
            assert isinstance(update_result, ExecuteResult)
            assert update_result.rows_affected == 1

            # Verify the update
            select_result = await driver.execute("SELECT name FROM test_table WHERE id = ?", (1,))
            assert isinstance(select_result, SelectResult)
            assert select_result.rows is not None
            assert select_result.rows[0]["name"] == "updated_name"
        finally:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_delete_operation(asyncpg_config: AsyncpgConfig) -> None:
    """Test DELETE operations."""
    async with asyncpg_config.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_table")
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql)

        try:
            # Insert a record first
            insert_result = await driver.execute("INSERT INTO test_table (name) VALUES (?)", ("to_delete",))
            assert isinstance(insert_result, ExecuteResult)
            assert insert_result.rows_affected == 1

            # Delete the record
            delete_result = await driver.execute("DELETE FROM test_table WHERE id = ?", (1,))
            assert isinstance(delete_result, ExecuteResult)
            assert delete_result.rows_affected == 1

            # Verify the deletion
            select_result = await driver.execute("SELECT COUNT(*) as count FROM test_table")
            assert isinstance(select_result, SelectResult)
            assert select_result.rows is not None
            assert select_result.rows[0]["count"] == 0
        finally:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")
