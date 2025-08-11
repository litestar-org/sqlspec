"""Fixtures and configuration for PSQLPy integration tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService


@pytest.fixture
def psqlpy_config(postgres_service: PostgresService) -> PsqlpyConfig:
    """Fixture for PsqlpyConfig using the postgres service."""
    dsn = f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    return PsqlpyConfig(pool_config={"dsn": dsn, "max_db_pool_size": 5})


@pytest.fixture
async def psqlpy_session(psqlpy_config: PsqlpyConfig) -> AsyncGenerator[PsqlpyDriver, None]:
    """Create a PSQLPy session with test table setup and cleanup."""
    try:
        async with psqlpy_config.provide_session() as session:
            # Create test table
            await session.execute_script("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50)
                );
            """)

            try:
                yield session
            finally:
                # Clean up test table
                try:
                    await session.execute_script("DROP TABLE IF EXISTS test_table;")
                except Exception:
                    # Ignore cleanup errors
                    pass
    finally:
        # Ensure pool is closed properly
        await psqlpy_config.close_pool()
