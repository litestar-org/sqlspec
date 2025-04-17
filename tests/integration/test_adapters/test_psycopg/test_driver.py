"""Test psycopg driver implementation."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, Literal

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import (
    PsycopgAsync,
    PsycopgAsyncPool,
    PsycopgSync,
    PsycopgSyncPool,
)

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture(scope="session")
def psycopg_sync_session(postgres_service: PostgresService) -> PsycopgSync:
    """Create a sync psycopg session."""
    return PsycopgSync(
        pool_config=PsycopgSyncPool(
            conninfo=f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}",
        ),
    )


@pytest.fixture(scope="session")
def psycopg_async_session(postgres_service: PostgresService) -> PsycopgAsync:
    """Create an async psycopg session."""
    return PsycopgAsync(
        pool_config=PsycopgAsyncPool(
            conninfo=f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}",
        ),
    )


@pytest.fixture(autouse=True)
async def cleanup_test_table(psycopg_async_session: PsycopgAsync) -> AsyncGenerator[None, None]:
    """Clean up the test table after each test."""
    yield
    async with await psycopg_async_session.create_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DROP TABLE IF EXISTS test_table")


@pytest.fixture(autouse=True)
def cleanup_sync_table(psycopg_sync_session: PsycopgSync) -> None:
    """Clean up the test table before each sync test."""
    with psycopg_sync_session.create_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM test_table")


@pytest.fixture(autouse=True)
async def cleanup_async_table(psycopg_async_session: PsycopgAsync) -> None:
    """Clean up the test table before each async test."""
    async with await psycopg_async_session.create_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
def test_sync_insert_returning(psycopg_sync_session: PsycopgSync, params: Any, style: ParamStyle) -> None:
    """Test sync insert returning functionality with different parameter styles."""
    with psycopg_sync_session.provide_session() as driver:
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        RETURNING *
        """ % ("%s" if style == "tuple_binds" else "%(name)s")

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
    """Test sync select functionality with different parameter styles."""
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
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        """ % ("%s" if style == "tuple_binds" else "%(name)s")
        driver.insert_update_delete(insert_sql, params)

        # Select and verify
        select_sql = """
        SELECT name FROM test_table WHERE name = %s
        """ % ("%s" if style == "tuple_binds" else "%(name)s")
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
    """Test sync select_value functionality with different parameter styles."""
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
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        """ % ("%s" if style == "tuple_binds" else "%(name)s")
        driver.insert_update_delete(insert_sql, params)

        # Select and verify
        select_sql = """
        SELECT name FROM test_table WHERE name = %s
        """ % ("%s" if style == "tuple_binds" else "%(name)s")
        value = driver.select_value(select_sql, params)
        assert value == "test_name"


@pytest.mark.asyncio
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

        sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        RETURNING *
        """ % ("%s" if style == "tuple_binds" else "%(name)s")

        result = await driver.insert_update_delete_returning(sql, params)
        assert result is not None
        assert result["name"] == "test_name"
        assert result["id"] is not None


@pytest.mark.asyncio
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
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        """ % ("%s" if style == "tuple_binds" else "%(name)s")
        await driver.insert_update_delete(insert_sql, params)

        # Select and verify
        select_sql = """
        SELECT name FROM test_table WHERE name = %s
        """ % ("%s" if style == "tuple_binds" else "%(name)s")
        results = await driver.select(select_sql, params)
        assert len(results) == 1
        assert results[0]["name"] == "test_name"


@pytest.mark.asyncio
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
        insert_sql = """
        INSERT INTO test_table (name)
        VALUES (%s)
        """ % ("%s" if style == "tuple_binds" else "%(name)s")
        await driver.insert_update_delete(insert_sql, params)

        # Select and verify
        select_sql = """
        SELECT name FROM test_table WHERE name = %s
        """ % ("%s" if style == "tuple_binds" else "%(name)s")
        value = await driver.select_value(select_sql, params)
        assert value == "test_name"


@pytest.mark.asyncio
async def test_insert(psycopg_async_session: PsycopgAsync) -> None:
    """Test inserting data."""
    async with await psycopg_async_session.create_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50)
                )
                """
            )
            await cur.execute(
                "INSERT INTO test_table (name) VALUES (%s)",
                ("test",),
            )
            await conn.commit()


@pytest.mark.asyncio
async def test_select(psycopg_async_session: PsycopgAsync) -> None:
    """Test selecting data."""
    async with await psycopg_async_session.create_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT name FROM test_table WHERE id = 1")
            result = await cur.fetchone()
            assert result == ("test",)


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
    with psycopg_sync_session.create_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50)
                )
                """
            )
            if param_style == "qmark":
                cur.execute(
                    "INSERT INTO test_table (name) VALUES (?)",
                    ("test",),
                )
            elif param_style == "format":
                cur.execute(
                    "INSERT INTO test_table (name) VALUES (%s)",
                    ("test",),
                )
            elif param_style == "pyformat":
                cur.execute(
                    "INSERT INTO test_table (name) VALUES (%(name)s)",
                    {"name": "test"},
                )
            conn.commit()
            cur.execute("SELECT name FROM test_table WHERE id = 1")
            result = cur.fetchone()
            assert result == ("test",)
