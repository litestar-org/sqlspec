"""Fixtures and configuration for PSQLPy integration tests."""

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.psqlpy import PsqlpyConfig, PsqlpyDriver

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService


@pytest.fixture(scope="function")
async def psqlpy_config(postgres_service: "PostgresService") -> "AsyncGenerator[PsqlpyConfig, None]":
    """Fixture for PsqlpyConfig using the postgres service."""
    dsn = (
        f"postgres://{postgres_service.user}:{postgres_service.password}@"
        f"{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )
    config = PsqlpyConfig(connection_config={"dsn": dsn, "max_db_pool_size": 5})
    try:
        yield config
    finally:
        if config.connection_instance:
            config.connection_instance.close()
        config.connection_instance = None


@pytest.fixture
async def psqlpy_driver(psqlpy_config: "PsqlpyConfig") -> "AsyncGenerator[PsqlpyDriver, None]":
    """Yield a raw PSQLPy driver session."""

    async with psqlpy_config.provide_session() as session:
        yield session


@pytest.fixture
async def psqlpy_session(psqlpy_config: "PsqlpyConfig") -> "AsyncGenerator[PsqlpyDriver, None]":
    """Create a PSQLPy session with test table setup and cleanup."""
    async with psqlpy_config.provide_session() as session:
        await session.execute_script(
            """
                CREATE TABLE IF NOT EXISTS test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50)
                );
            """
        )

        try:
            yield session
        finally:
            try:
                await session.execute_script("DROP TABLE IF EXISTS test_table;")
            except Exception:
                pass
