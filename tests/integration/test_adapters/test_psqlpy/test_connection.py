"""Test PSQLPy connection functionality."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.core.result import SQLResult

if TYPE_CHECKING:
    pass

pytestmark = [pytest.mark.psqlpy, pytest.mark.postgres, pytest.mark.integration]


@pytest.mark.asyncio
async def test_connect_via_pool(psqlpy_config: PsqlpyConfig) -> None:
    """Test establishing a connection via the pool."""
    pool = await psqlpy_config.create_pool()
    async with pool.acquire() as conn:
        assert conn is not None
        # Perform a simple query to confirm connection
        # For psqlpy, we need to use execute() for simple queries
        result = await conn.execute("SELECT 1")
        # The result should be a QueryResult object with result() method
        rows = result.result()
        assert len(rows) == 1
        assert rows[0]["?column?"] == 1  # PostgreSQL default column name for SELECT 1


@pytest.mark.asyncio
async def test_connect_direct(psqlpy_config: PsqlpyConfig) -> None:
    """Test establishing a connection via the provide_connection context manager."""
    # This test now uses provide_connection for a cleaner approach
    # to getting a managed connection.
    async with psqlpy_config.provide_connection() as conn:
        assert conn is not None
        # Perform a simple query
        result = await conn.execute("SELECT 1")
        rows = result.result()
        assert len(rows) == 1
        assert rows[0]["?column?"] == 1
    # Connection is automatically released by the context manager


@pytest.mark.asyncio
async def test_provide_session_context_manager(psqlpy_config: PsqlpyConfig) -> None:
    """Test the provide_session context manager."""
    async with psqlpy_config.provide_session() as driver:
        assert driver is not None
        assert driver.connection is not None
        # Test a simple query within the session
        result = await driver.execute("SELECT 'test'")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.column_names is not None
        val = result.data[0][result.column_names[0]]
        assert val == "test"

    # After exiting context, connection should be released/closed (handled by config)


@pytest.mark.asyncio
async def test_connection_error_handling(psqlpy_config: PsqlpyConfig) -> None:
    """Test connection error handling."""
    async with psqlpy_config.provide_session() as driver:
        # Test invalid SQL
        with pytest.raises(Exception):
            await driver.execute("INVALID SQL SYNTAX")

        # Connection should still be usable
        result = await driver.execute("SELECT 'still_working' as status")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data[0]["status"] == "still_working"


@pytest.mark.asyncio
async def test_connection_with_core_round_3(psqlpy_config: PsqlpyConfig) -> None:
    """Test connection integration with CORE_ROUND_3 architecture."""
    from sqlspec.core.statement import SQL

    # Create SQL object
    test_sql = SQL("SELECT $1::text as test_value")

    async with psqlpy_config.provide_session() as driver:
        result = await driver.execute(test_sql, ("core_test",))
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert len(result.data) == 1
        assert result.data[0]["test_value"] == "core_test"


@pytest.mark.asyncio
async def test_multiple_connections_sequential(psqlpy_config: PsqlpyConfig) -> None:
    """Test multiple sequential connections."""
    # First connection
    async with psqlpy_config.provide_session() as driver1:
        result1 = await driver1.execute("SELECT 'connection1' as conn_id")
        assert isinstance(result1, SQLResult)
        assert result1.data is not None
        assert result1.data[0]["conn_id"] == "connection1"

    # Second connection should work independently
    async with psqlpy_config.provide_session() as driver2:
        result2 = await driver2.execute("SELECT 'connection2' as conn_id")
        assert isinstance(result2, SQLResult)
        assert result2.data is not None
        assert result2.data[0]["conn_id"] == "connection2"


@pytest.mark.asyncio
async def test_connection_concurrent_access(psqlpy_config: PsqlpyConfig) -> None:
    """Test concurrent connection access."""
    import asyncio

    async def query_task(task_id: int) -> str:
        """Execute a query in a separate connection."""
        async with psqlpy_config.provide_session() as driver:
            result = await driver.execute("SELECT $1::text as task_id", (f"task_{task_id}",))
            assert isinstance(result, SQLResult)
            assert result.data is not None
            from typing import cast

            return cast(str, result.data[0]["task_id"])

    # Run multiple concurrent queries
    tasks = [query_task(i) for i in range(3)]
    results = await asyncio.gather(*tasks)

    # Verify all tasks completed successfully
    assert len(results) == 3
    assert all(result.startswith("task_") for result in results)
    assert sorted(results) == ["task_0", "task_1", "task_2"]
