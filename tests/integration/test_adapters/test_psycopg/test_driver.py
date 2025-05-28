"""Test Psycopg driver implementation."""

from __future__ import annotations

from typing import Any, Literal

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import (
    PsycopgAsyncConfig,
    PsycopgAsyncPoolConfig,
    PsycopgSyncConfig,
    PsycopgSyncPoolConfig,
)
from sqlspec.sql.result import ExecuteResult, SelectResult

ParamStyle = Literal["tuple_binds", "dict_binds"]


@pytest.fixture
def psycopg_sync_session(postgres_service: PostgresService) -> PsycopgSyncConfig:
    """Create a Psycopg synchronous session.

    Args:
        postgres_service: PostgreSQL service fixture.

    Returns:
        Configured Psycopg synchronous session.
    """
    return PsycopgSyncConfig(
        pool_config=PsycopgSyncPoolConfig(
            conninfo=f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        )
    )


@pytest.fixture
def psycopg_async_session(postgres_service: PostgresService) -> PsycopgAsyncConfig:
    """Create a Psycopg asynchronous session.

    Args:
        postgres_service: PostgreSQL service fixture.

    Returns:
        Configured Psycopg asynchronous session.
    """
    return PsycopgAsyncConfig(
        pool_config=PsycopgAsyncPoolConfig(
            conninfo=f"host={postgres_service.host} port={postgres_service.port} user={postgres_service.user} password={postgres_service.password} dbname={postgres_service.database}"
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
def test_sync_insert_returning(psycopg_sync_session: PsycopgSyncConfig, params: Any, style: ParamStyle) -> None:
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

        result = driver.execute(sql, params)
        assert isinstance(result, SelectResult)
        assert result.rows is not None
        assert len(result.rows) == 1
        returned_data = result.rows[0]
        assert returned_data["name"] == "test_name"
        assert returned_data["id"] is not None
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("postgres")
def test_sync_select(psycopg_sync_session: PsycopgSyncConfig, params: Any, style: ParamStyle) -> None:
    """Test synchronous select functionality with different parameter styles."""
    with psycopg_sync_session.provide_session() as driver:
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        if style == "tuple_binds":
            insert_sql = "INSERT INTO test_table (name) VALUES (%s)"
        else:
            insert_sql = "INSERT INTO test_table (name) VALUES (:name)"

        insert_op_result = driver.execute(insert_sql, params)
        assert isinstance(insert_op_result, ExecuteResult)
        assert insert_op_result.rows_affected == 1

        if style == "tuple_binds":
            select_sql = "SELECT name FROM test_table WHERE name = %s"
        else:
            select_sql = "SELECT name FROM test_table WHERE name = :name"

        select_op_result = driver.execute(select_sql, params)
        assert isinstance(select_op_result, SelectResult)
        assert select_op_result.rows is not None
        assert len(select_op_result.rows) == 1
        assert select_op_result.rows[0]["name"] == "test_name"
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("postgres")
def test_sync_select_value(psycopg_sync_session: PsycopgSyncConfig, params: Any, style: ParamStyle) -> None:
    """Test synchronous select_value functionality with different parameter styles."""
    with psycopg_sync_session.provide_session() as driver:
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql)

        if style == "tuple_binds":
            insert_sql = "INSERT INTO test_table (name) VALUES (%s)"
        else:
            insert_sql = "INSERT INTO test_table (name) VALUES (:name)"

        insert_op_result = driver.execute(insert_sql, params)
        assert isinstance(insert_op_result, ExecuteResult)
        assert insert_op_result.rows_affected == 1

        # For select_value test, we are selecting a literal, not from the inserted data directly with params.
        # The original test selected a literal 'test_name'.
        select_sql_literal = "SELECT 'test_name' AS test_column"

        value_result = driver.execute(select_sql_literal)  # No params for literal select
        assert isinstance(value_result, SelectResult)
        assert value_result.rows is not None
        assert len(value_result.rows) == 1
        assert value_result.column_names is not None
        assert len(value_result.column_names) == 1
        # Extract the value using the first column name from SelectResult
        extracted_value = value_result.rows[0][value_result.column_names[0]]
        assert extracted_value == "test_name"
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("params", "style"),
    [
        pytest.param(("test_name",), "tuple_binds", id="tuple_binds"),
        pytest.param({"name": "test_name"}, "dict_binds", id="dict_binds"),
    ],
)
@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_async_insert_returning(
    psycopg_async_session: PsycopgAsyncConfig, params: Any, style: ParamStyle
) -> None:
    """Test async insert returning functionality with different parameter styles."""
    async with psycopg_async_session.provide_session() as driver:
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql)

        if style == "tuple_binds":
            sql_insert = "INSERT INTO test_table (name) VALUES (%s) RETURNING *"
        else:
            sql_insert = "INSERT INTO test_table (name) VALUES (:name) RETURNING *"

        result = await driver.execute(sql_insert, params)
        assert isinstance(result, SelectResult)  # RETURNING implies SelectResult
        assert result.rows is not None
        assert len(result.rows) == 1
        returned_data = result.rows[0]
        assert returned_data["name"] == "test_name"
        assert returned_data["id"] is not None
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
async def test_async_select(psycopg_async_session: PsycopgAsyncConfig, params: Any, style: ParamStyle) -> None:
    """Test async select functionality with different parameter styles."""
    async with psycopg_async_session.provide_session() as driver:
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql)

        if style == "tuple_binds":
            insert_sql = "INSERT INTO test_table (name) VALUES (%s)"
        else:
            insert_sql = "INSERT INTO test_table (name) VALUES (:name)"

        insert_op_result = await driver.execute(insert_sql, params)
        assert isinstance(insert_op_result, ExecuteResult)
        assert insert_op_result.rows_affected == 1

        if style == "tuple_binds":
            select_sql = "SELECT name FROM test_table WHERE name = %s"
        else:
            select_sql = "SELECT name FROM test_table WHERE name = :name"

        select_op_result = await driver.execute(select_sql, params)
        assert isinstance(select_op_result, SelectResult)
        assert select_op_result.rows is not None
        assert len(select_op_result.rows) == 1
        assert select_op_result.rows[0]["name"] == "test_name"
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
async def test_async_select_value(psycopg_async_session: PsycopgAsyncConfig, params: Any, style: ParamStyle) -> None:
    """Test async select_value functionality with different parameter styles."""
    async with psycopg_async_session.provide_session() as driver:
        sql = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql)

        # Insert a record first (though not directly used by the literal select_value part)
        # This setup was part of the original test, retaining it for structural similarity.
        if style == "tuple_binds":
            insert_sql_setup = "INSERT INTO test_table (name) VALUES (%s)"
        else:
            insert_sql_setup = "INSERT INTO test_table (name) VALUES (:name)"
        setup_params = ("setup_value",) if style == "tuple_binds" else {"name": "setup_value"}
        insert_setup_result = await driver.execute(insert_sql_setup, setup_params)
        assert isinstance(insert_setup_result, ExecuteResult)
        assert insert_setup_result.rows_affected == 1

        select_sql_literal = "SELECT 'test_name' AS test_column"

        value_result = await driver.execute(select_sql_literal)
        assert isinstance(value_result, SelectResult)
        assert value_result.rows is not None
        assert len(value_result.rows) == 1
        assert value_result.column_names is not None
        assert len(value_result.column_names) == 1
        extracted_value = value_result.rows[0][value_result.column_names[0]]
        assert extracted_value == "test_name"
        await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_insert(psycopg_async_session: PsycopgAsyncConfig) -> None:
    """Test inserting data (async)."""
    async with psycopg_async_session.provide_session() as driver:
        sql_create = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_table (name) VALUES (%s)"
        result = await driver.execute(insert_sql, ("test",))
        assert isinstance(result, ExecuteResult)
        assert result.rows_affected == 1
        await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_select(psycopg_async_session: PsycopgAsyncConfig) -> None:
    """Test selecting data (async)."""
    async with psycopg_async_session.provide_session() as driver:
        sql_create = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        await driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_table (name) VALUES (%s)"
        insert_result = await driver.execute(insert_sql, ("test",))
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        select_sql = "SELECT name FROM test_table WHERE id = 1"
        results = await driver.execute(select_sql)
        assert isinstance(results, SelectResult)
        assert results.rows is not None
        assert len(results.rows) == 1
        assert results.rows[0]["name"] == "test"
        await driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.parametrize(
    ("input_sql_style", "sql_template", "params_data"),
    [
        pytest.param(
            "qmark_style_input_expect_tuple",
            "INSERT INTO test_table (name) VALUES (%s)",
            ("test",),
            id="qmark_tuple_to_psycopg_percent_s",
        ),
        pytest.param(
            "format_style_input_expect_tuple",
            "INSERT INTO test_table (name) VALUES (%s)",
            ("test",),
            id="format_tuple_to_psycopg_percent_s",
        ),
        pytest.param(
            "named_colon_style_input_expect_dict",
            "INSERT INTO test_table (name) VALUES (:name)",
            {"name": "test"},
            id="named_colon_to_psycopg_pyformat",
        ),
        # Add a direct pyformat style to ensure it passes through if already correct for psycopg
        pytest.param(
            "pyformat_style_input_expect_dict",
            "INSERT INTO test_table (name) VALUES (%(name)s)",
            {"name": "test"},
            id="direct_psycopg_pyformat_dict",
        ),
    ],
)
@pytest.mark.xdist_group("postgres")
def test_param_styles(
    psycopg_sync_session: PsycopgSyncConfig, input_sql_style: str, sql_template: str, params_data: Any
) -> None:
    """Test different input parameter styles are correctly processed and executed (sync)."""
    with psycopg_sync_session.provide_session() as driver:
        sql_create = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
        """
        driver.execute_script(sql_create)

        # The sql_template here represents the SQL string AS IT IS PASSED TO THE DRIVER'S EXECUTE METHOD.
        # The _process_sql_params method in the driver is responsible for converting this to
        # what psycopg actually needs (e.g., :name to %(name)s).

        insert_result = driver.execute(sql_template, params_data)
        assert isinstance(insert_result, ExecuteResult)
        assert insert_result.rows_affected == 1

        # Verification: Select the inserted data (independently of the insert param style)
        # We use a consistent select style here for verification to simplify.
        verify_select_sql = "SELECT name FROM test_table WHERE name = %s"
        verify_params = ("test",)

        select_result = driver.execute(verify_select_sql, verify_params)
        assert isinstance(select_result, SelectResult)
        assert select_result.rows is not None
        assert len(select_result.rows) == 1
        assert select_result.rows[0]["name"] == "test"
        driver.execute_script("DROP TABLE IF EXISTS test_table")


@pytest.mark.xdist_group("postgres")
def test_sync_execute_many_insert(psycopg_sync_session: PsycopgSyncConfig) -> None:
    """Test synchronous execute_many for batch inserts."""
    with psycopg_sync_session.provide_session() as driver:
        sql_create = """
        CREATE TABLE test_many_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_many_table (name) VALUES (%s)"  # Psycopg uses %s for positional
        params_list = [("sync_name1",), ("sync_name2",), ("sync_name3",)]

        result = driver.execute_many(insert_sql, params_list)
        assert isinstance(result, ExecuteResult)
        assert result.rows_affected == len(params_list)

        select_sql = "SELECT COUNT(*) as count FROM test_many_table"
        count_result = driver.execute(select_sql)
        assert isinstance(count_result, SelectResult)
        assert count_result.rows is not None
        assert count_result.rows[0]["count"] == len(params_list)

        driver.execute_script("DROP TABLE IF EXISTS test_many_table")


@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_async_execute_many_insert(psycopg_async_session: PsycopgAsyncConfig) -> None:
    """Test asynchronous execute_many for batch inserts."""
    async with psycopg_async_session.provide_session() as driver:
        sql_create = """
        CREATE TABLE test_async_many_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        );
        """
        await driver.execute_script(sql_create)

        insert_sql = "INSERT INTO test_async_many_table (name) VALUES (%s)"  # Psycopg uses %s for positional
        params_list = [("async_name1",), ("async_name2",), ("async_name3",)]

        result = await driver.execute_many(insert_sql, params_list)
        assert isinstance(result, ExecuteResult)
        assert result.rows_affected == len(params_list)

        select_sql = "SELECT COUNT(*) as count FROM test_async_many_table"
        count_result = await driver.execute(select_sql)
        assert isinstance(count_result, SelectResult)
        assert count_result.rows is not None
        assert count_result.rows[0]["count"] == len(params_list)

        await driver.execute_script("DROP TABLE IF EXISTS test_async_many_table")
