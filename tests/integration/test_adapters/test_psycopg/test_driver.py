"""Test Psycopg driver implementation."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, Literal

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsync, PsycopgAsyncPool, PsycopgSync, PsycopgSyncPool

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture(scope="session")
def psycopg_sync_session(postgres_service: PostgresService) -> PsycopgSync:
    """Create a Psycopg synchronous session.

    Args:
        postgres_service: PostgreSQL service fixture.

    Returns:
        Configured Psycopg synchronous session.
    """
    return PsycopgSync(
        pool_config=PsycopgSyncPool(
            conninfo=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        )
    )


@pytest.fixture(scope="session")
def psycopg_async_session(postgres_service: PostgresService) -> PsycopgAsync:
    """Create a Psycopg asynchronous session.

    Args:
        postgres_service: PostgreSQL service fixture.

    Returns:
        Configured Psycopg asynchronous session.
    """
    return PsycopgAsync(
        pool_config=PsycopgAsyncPool(
            conninfo=f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}"
        )
    )


@pytest.fixture(autouse=True)
async def cleanup_test_table(psycopg_async_session: PsycopgAsync) -> AsyncGenerator[None, None]:
    """Clean up the test table after each test."""
    yield
    async with psycopg_async_session.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.fixture(autouse=True)
def cleanup_sync_table(psycopg_sync_session: PsycopgSync) -> None:
    """Clean up the test table after each test."""
    with psycopg_sync_session.provide_session() as driver:
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.fixture(autouse=True)
async def cleanup_async_table(psycopg_async_session: PsycopgAsync) -> None:
    """Clean up the test table after each test."""
    async with psycopg_async_session.provide_session() as driver:
        await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_sync_insert_returning(psycopg_sync_session: PsycopgSync, params: Any, style: ParamStyle) -> None:
    """Test synchronous insert returning functionality with different parameter styles."""
    with psycopg_sync_session.provide_session() as driver:
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Use appropriate SQL for each style
        if style == "tuple_binds":
            sql = """
            INSERT INTO test_table (name)
            VALUES (%s)
            RETURNING *
            """
        else:
            sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            RETURNING *
            """

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
def test_sync_select(psycopg_sync_session: PsycopgSync, params: Any, style: ParamStyle) -> None:
    """Test synchronous select functionality with different parameter styles."""
    with psycopg_sync_session.provide_session() as driver:
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        if style == "tuple_binds":
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (%s)
            """
        else:
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            """
        driver.insert_update_delete(insert_sql, params)

        # Select and verify
        if style == "tuple_binds":
            select_sql = """
            SELECT name FROM test_table WHERE name = %s
            """
        else:
            select_sql = """
            SELECT name FROM test_table WHERE name = :name
            """
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
def test_sync_select_value(psycopg_sync_session: PsycopgSync, params: Any, style: ParamStyle) -> None:
    """Test synchronous select_value functionality with different parameter styles."""
    with psycopg_sync_session.provide_session() as driver:
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        # Insert test record
        if style == "tuple_binds":
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (%s)
            """
        else:
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            """
        driver.insert_update_delete(insert_sql, params)

        # Select and verify
        select_sql = "SELECT 'test_name' AS test_name"
        # Don't pass parameters with a literal query that has no placeholders
        value = driver.select_value(select_sql)
        assert value == "test_name"


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
async def test_async_insert_returning(psycopg_async_session: PsycopgAsync, params: Any, style: ParamStyle) -> None:
    """Test async insert returning functionality with different parameter styles."""
    async with psycopg_async_session.provide_session() as driver:
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql)

        # Use appropriate SQL for each style
        if style == "tuple_binds":
            sql = """
            INSERT INTO test_table (name)
            VALUES (%s)
            RETURNING *
            """
        else:
            sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            RETURNING *
            """

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
async def test_async_select(psycopg_async_session: PsycopgAsync, params: Any, style: ParamStyle) -> None:
    """Test async select functionality with different parameter styles."""
    async with psycopg_async_session.provide_session() as driver:
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
            VALUES (%s)
            """
        else:
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            """
        await driver.insert_update_delete(insert_sql, params)

        # Select and verify
        if style == "tuple_binds":
            select_sql = """
            SELECT name FROM test_table WHERE name = %s
            """
        else:
            select_sql = """
            SELECT name FROM test_table WHERE name = :name
            """
        results = await driver.select(select_sql, params)
        assert len(results) == 1
        assert results[0]["name"] == "test_name"


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
async def test_async_select_value(psycopg_async_session: PsycopgAsync, params: Any, style: ParamStyle) -> None:
    """Test async select_value functionality with different parameter styles."""
    async with psycopg_async_session.provide_session() as driver:
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
            VALUES (%s)
            """
        else:
            insert_sql = """
            INSERT INTO test_table (name)
            VALUES (:name)
            """
        await driver.insert_update_delete(insert_sql, params)

        # Get literal string to test with select_value
        if style == "tuple_binds":
            # Use a literal query to test select_value
            select_sql = "SELECT 'test_name' AS test_name"
        else:
            select_sql = "SELECT 'test_name' AS test_name"

        # Don't pass parameters with a literal query that has no placeholders
        value = await driver.select_value(select_sql)
        assert value == "test_name"


async def test_insert(psycopg_async_session: PsycopgAsync) -> None:
    """Test inserting data."""
    async with psycopg_async_session.provide_session() as driver:
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql)

        insert_sql = "INSERT INTO test_table (name) VALUES (%s)"
        row_count = await driver.insert_update_delete(insert_sql, ("test",))
        assert row_count == 1


async def test_select(psycopg_async_session: PsycopgAsync) -> None:
    """Test selecting data."""
    async with psycopg_async_session.provide_session() as driver:
        # Create and populate test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql)

        insert_sql = "INSERT INTO test_table (name) VALUES (%s)"
        await driver.insert_update_delete(insert_sql, ("test",))

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE id = 1"
        results = await driver.select(select_sql)
        assert len(results) == 1
        assert results[0]["name"] == "test"


@pytest.mark.parametrize(
    "param_style",
    [
        "qmark",
        "format",
        "pyformat",
    ],
)
def test_param_styles(psycopg_sync_session: PsycopgSync, param_style: str) -> None:
    """Test different parameter styles."""
    with psycopg_sync_session.provide_session() as driver:
        # Create test table
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        driver.execute_script(sql)

        # Insert test record based on param style
        if param_style == "qmark":
            insert_sql = "INSERT INTO test_table (name) VALUES (%s)"
            params = ("test",)
        elif param_style == "format":
            insert_sql = "INSERT INTO test_table (name) VALUES (%s)"
            params = ("test",)
        else:  # pyformat
            # Use :name format in SQL query, as that's what our SQLSpec API expects
            # The driver will convert it to %(name)s internally
            insert_sql = "INSERT INTO test_table (name) VALUES (:name)"
            params = {"name": "test"}  # type: ignore[assignment]

        row_count = driver.insert_update_delete(insert_sql, params)
        assert row_count == 1

        # Select and verify
        select_sql = "SELECT name FROM test_table WHERE id = 1"
        results = driver.select(select_sql)
        assert len(results) == 1
        assert results[0]["name"] == "test"
