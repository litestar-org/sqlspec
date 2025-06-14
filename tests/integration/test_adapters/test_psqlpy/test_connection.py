"""Test Psqlpy connection functionality."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.statement.result import SQLResult

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService

pytestmark = [pytest.mark.psqlpy, pytest.mark.postgres, pytest.mark.integration]


@pytest.fixture
def psqlpy_config(postgres_service: PostgresService) -> PsqlpyConfig:
    """Fixture for PsqlpyConfig using the postgres service."""
    # Construct DSN manually like in asyncpg tests
    dsn = f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    return PsqlpyConfig(dsn=dsn, max_db_pool_size=2)


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
    # Verification depends on pool implementation, might be hard to check directly
