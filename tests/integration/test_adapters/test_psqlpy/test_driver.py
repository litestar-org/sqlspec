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
@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_execute_script(psqlpy_config: PsqlpyConfig) -> None:
    """Test execute_script method for non-query operations."""
    sql = "SELECT 1;"  # Simple script
    async with psqlpy_config.provide_session() as driver:
        status = await driver.execute_script(sql)
        # psqlpy execute returns a status string, exact content might vary
        assert isinstance(status, str)
        # We don't assert exact status content as it might change, just that it runs
