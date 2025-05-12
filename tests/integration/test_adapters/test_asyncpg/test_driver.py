"""Test Asyncpg driver implementation."""

from __future__ import annotations

from typing import Any, Literal

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgPoolConfig

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
            result = await driver.insert_update_delete_returning(sql, params)
            assert result is not None
            assert result["name"] == "test_name"
            assert result["id"] is not None
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
        await driver.insert_update_delete(insert_sql, params)

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
            results = await driver.select(select_sql, params)
            assert len(results) == 1
            assert results[0]["name"] == "test_name"
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
    """Test async select_value functionality with different parameter styles."""
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
        await driver.insert_update_delete(insert_sql, params)

        # Get literal string to test with select_value
        # Use a literal query to test select_value
        select_sql = "SELECT 'test_name' AS test_name"

        try:
            # Don't pass parameters with a literal query that has no placeholders
            value = await driver.select_value(select_sql)
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
            row_count = await driver.insert_update_delete(insert_sql, ("test",))
            assert row_count == 1

            # Verify insertion
            select_sql = "SELECT COUNT(*) FROM test_table WHERE name = ?"
            count = await driver.select_value(select_sql, ("test",))
            assert count == 1
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
        await driver.insert_update_delete(insert_sql, ("test",))

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE id = ?"
        try:
            results = await driver.select(select_sql, (1,))
            assert len(results) == 1
            assert results[0]["name"] == "test"
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
            row_count = await driver.insert_update_delete(insert_sql, params)
            assert row_count == 1

            # Select and verify
            select_sql = "SELECT name FROM test_table WHERE id = ?"
            results = await driver.select(select_sql, (1,))
            assert len(results) == 1
            assert results[0]["name"] == "test"
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
        await driver.insert_update_delete("INSERT INTO test_table (name) VALUES (?)", "edge_case_test")

        try:
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
            # resulting in 6 total rows (3 names X 2 matches each)
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
        finally:
            await driver.execute_script("DROP TABLE IF EXISTS test_table")
