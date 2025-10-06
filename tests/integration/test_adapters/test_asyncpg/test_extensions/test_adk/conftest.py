"""AsyncPG ADK test fixtures."""

import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.adk import AsyncpgADKStore


@pytest.fixture
async def asyncpg_adk_store(postgres_service):
    """Create AsyncPG ADK store with test database."""
    config = AsyncpgConfig(
        pool_config={
            "host": postgres_service.host,
            "port": postgres_service.port,
            "user": postgres_service.user,
            "password": postgres_service.password,
            "database": postgres_service.database,
        }
    )
    store = AsyncpgADKStore(config)
    await store.create_tables()

    yield store

    async with config.provide_connection() as conn:
        await conn.execute("DROP TABLE IF EXISTS adk_events CASCADE")
        await conn.execute("DROP TABLE IF EXISTS adk_sessions CASCADE")


@pytest.fixture
async def session_fixture(asyncpg_adk_store):
    """Create a test session."""
    session_id = "test-session"
    app_name = "test-app"
    user_id = "user-123"
    state = {"test": True}
    await asyncpg_adk_store.create_session(session_id, app_name, user_id, state)
    return {"session_id": session_id, "app_name": app_name, "user_id": user_id}
