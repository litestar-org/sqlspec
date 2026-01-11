"""AsyncPG ADK test fixtures."""

from collections.abc import AsyncGenerator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore


@pytest.fixture
async def asyncpg_adk_store(postgres_service: "PostgresService") -> "AsyncGenerator[AsyncpgADKStore, None]":
    """Create AsyncPG ADK store with test database.

    Args:
        postgres_service: Pytest fixture providing PostgreSQL connection config.

    Yields:
        Configured AsyncPG ADK store instance.

    Notes:
        Uses pytest-databases PostgreSQL container for testing.
        Tables are created before test and cleaned up after.
        Pool is properly closed to avoid threading issues.
    """
    config = AsyncpgConfig(
        connection_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
            "max_size": 20,
            "min_size": 5,
        }
    )

    try:
        store = AsyncpgADKStore(config)
        await store.create_tables()

        yield store

        async with config.provide_connection() as conn:
            await conn.execute("DROP TABLE IF EXISTS adk_events CASCADE")
            await conn.execute("DROP TABLE IF EXISTS adk_sessions CASCADE")
    finally:
        if config.connection_instance:
            await config.close_pool()
        config.connection_instance = None


@pytest.fixture
async def session_fixture(asyncpg_adk_store: AsyncpgADKStore) -> dict[str, str]:
    """Create a test session.

    Args:
        asyncpg_adk_store: AsyncPG ADK store fixture.

    Returns:
        Dictionary with session metadata.
    """
    session_id = "test-session"
    app_name = "test-app"
    user_id = "user-123"
    state = {"test": True}
    await asyncpg_adk_store.create_session(session_id, app_name, user_id, state)
    return {"session_id": session_id, "app_name": app_name, "user_id": user_id}
