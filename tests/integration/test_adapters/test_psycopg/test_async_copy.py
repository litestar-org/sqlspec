"""Integration tests for psycopg async driver COPY operations."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgAsyncDriver
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQLConfig


@pytest.fixture
async def psycopg_async_session(postgres_service: PostgresService) -> AsyncGenerator[PsycopgAsyncDriver, None]:
    """Create a psycopg async session with test table."""
    config = PsycopgAsyncConfig(
        host=postgres_service.host,
        port=postgres_service.port,
        user=postgres_service.user,
        password=postgres_service.password,
        dbname=postgres_service.database,
        autocommit=True,  # Enable autocommit for tests
        statement_config=SQLConfig(enable_transformations=False, enable_validation=False, enable_parsing=False),
    )

    async with config.provide_session() as session:
        # Create test table
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_table_async (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER DEFAULT 0
            )
        """)
        yield session
        # Cleanup
        try:
            await session.execute_script("DROP TABLE IF EXISTS test_table_async")
        except Exception:
            # Ignore errors during cleanup
            pass


@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_psycopg_async_copy_operations(psycopg_async_session: PsycopgAsyncDriver) -> None:
    """Test PostgreSQL COPY operations with async psycopg driver."""
    # Create temp table for copy test
    await psycopg_async_session.execute_script("""
        DROP TABLE IF EXISTS copy_test_async;
        CREATE TABLE copy_test_async (
            id INTEGER,
            name TEXT,
            value INTEGER
        )
    """)

    # Test COPY FROM STDIN with text format
    copy_data = "1\ttest1\t100\n2\ttest2\t200\n"
    result = await psycopg_async_session.execute("COPY copy_test_async FROM STDIN WITH (FORMAT text)", copy_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected >= 0  # May be -1 or actual count

    # Verify data was copied - use a simple select first
    verify_result = await psycopg_async_session.execute("SELECT * FROM copy_test_async ORDER BY id")
    assert isinstance(verify_result, SQLResult)
    assert verify_result.data is not None
    assert len(verify_result.data) == 2
    assert verify_result.data[0]["name"] == "test1"
    assert verify_result.data[1]["value"] == 200

    # Clean up
    await psycopg_async_session.execute_script("DROP TABLE copy_test_async")


@pytest.mark.xdist_group("postgres")
@pytest.mark.asyncio
async def test_psycopg_async_copy_csv_format(psycopg_async_session: PsycopgAsyncDriver) -> None:
    """Test PostgreSQL COPY operations with CSV format using async driver."""
    # Create temp table
    await psycopg_async_session.execute_script("""
        CREATE TABLE copy_csv_async (
            id INTEGER,
            name TEXT,
            value INTEGER
        )
    """)

    # Test COPY FROM STDIN with CSV format
    csv_data = "3,test3,300\n4,test4,400\n5,test5,500\n"
    result = await psycopg_async_session.execute("COPY copy_csv_async FROM STDIN WITH (FORMAT csv)", csv_data)
    assert isinstance(result, SQLResult)
    assert result.rows_affected == 3

    # Verify data
    select_result = await psycopg_async_session.execute("SELECT * FROM copy_csv_async ORDER BY id")
    assert isinstance(select_result, SQLResult)
    assert select_result.data is not None
    assert len(select_result.data) == 3
    assert select_result.data[0]["name"] == "test3"
    assert select_result.data[2]["value"] == 500

    # Clean up
    await psycopg_async_session.execute_script("DROP TABLE copy_csv_async")
