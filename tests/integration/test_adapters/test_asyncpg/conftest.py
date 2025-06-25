from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig, AsyncpgDriver
from sqlspec.statement import SQLConfig


@pytest.fixture(scope="function")
async def asyncpg_arrow_session(postgres_service: PostgresService) -> "AsyncGenerator[AsyncpgDriver, None]":
    """Create an AsyncPG session for Arrow testing."""
    config = AsyncpgConfig(
        host=postgres_service.host,
        port=postgres_service.port,
        user=postgres_service.user,
        password=postgres_service.password,
        database=postgres_service.database,
        statement_config=SQLConfig(enable_transformations=False),
    )

    async with config.provide_session() as session:
        # Create test table with various data types
        await session.execute_script("""
            CREATE TABLE IF NOT EXISTS test_arrow (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER,
                price DECIMAL(10, 2),
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Clear any existing data
        await session.execute_script("TRUNCATE TABLE test_arrow RESTART IDENTITY")

        # Insert test data
        await session.execute_many(
            "INSERT INTO test_arrow (name, value, price, is_active) VALUES ($1, $2, $3, $4)",
            [
                ("Product A", 100, 19.99, True),
                ("Product B", 200, 29.99, True),
                ("Product C", 300, 39.99, False),
                ("Product D", 400, 49.99, True),
                ("Product E", 500, 59.99, False),
            ],
        )
        yield session
        # Cleanup
        await session.execute_script("DROP TABLE IF EXISTS test_arrow")
