"""Integration tests for aiosqlite session backend with store integration."""

import asyncio
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.testing import AsyncTestClient

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend, SQLSpecSessionConfig, SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = [pytest.mark.aiosqlite, pytest.mark.integration, pytest.mark.asyncio, pytest.mark.xdist_group("aiosqlite")]


async def test_aiosqlite_migration_creates_default_table(aiosqlite_migration_config: AiosqliteConfig) -> None:
    """Test that Litestar migration creates the correct table structure with default name."""
    # Apply migrations
    commands = AsyncMigrationCommands(aiosqlite_migration_config)
    await commands.init(aiosqlite_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Verify table was created with correct SQLite-specific types
    async with aiosqlite_migration_config.provide_session() as driver:
        result = await driver.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='litestar_sessions'")
        assert len(result.data) == 1
        create_sql = result.data[0]["sql"]

        # SQLite should use TEXT for data column (not JSONB or JSON)
        assert "TEXT" in create_sql
        assert "DATETIME" in create_sql or "TIMESTAMP" in create_sql
        assert "litestar_sessions" in create_sql

        # Verify columns exist
        result = await driver.execute("PRAGMA table_info(litestar_sessions)")
        columns = {row["name"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns


async def test_aiosqlite_migration_creates_custom_table(aiosqlite_migration_config_with_dict: AiosqliteConfig) -> None:
    """Test that Litestar migration creates table with custom name from dict config."""
    # Apply migrations
    commands = AsyncMigrationCommands(aiosqlite_migration_config_with_dict)
    await commands.init(aiosqlite_migration_config_with_dict.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Verify table was created with custom name
    async with aiosqlite_migration_config_with_dict.provide_session() as driver:
        result = await driver.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='custom_sessions'")
        assert len(result.data) == 1
        create_sql = result.data[0]["sql"]

        # SQLite should use TEXT for data column (not JSONB or JSON)
        assert "TEXT" in create_sql
        assert "DATETIME" in create_sql or "TIMESTAMP" in create_sql
        assert "custom_sessions" in create_sql

        # Verify columns exist
        result = await driver.execute("PRAGMA table_info(custom_sessions)")
        columns = {row["name"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns

        # Verify default table doesn't exist
        result = await driver.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='litestar_sessions'")
        assert len(result.data) == 0


async def test_aiosqlite_session_basic_operations(
    session_backend_default: SQLSpecSessionBackend, session_store_default: SQLSpecSessionStore
) -> None:
    """Test basic session operations with aiosqlite backend."""

    @get("/set-session")
    async def set_session(request: Any) -> dict:
        request.session["user_id"] = 12345
        request.session["username"] = "testuser"
        request.session["preferences"] = {"theme": "dark", "lang": "en"}
        return {"status": "session set"}

    @get("/get-session")
    async def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "preferences": request.session.get("preferences"),
        }

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = SQLSpecSessionConfig(backend=session_backend_default, key="aiosqlite-session", max_age=3600)

    app = Litestar(
        route_handlers=[set_session, get_session, clear_session],
        middleware=[session_config.middleware],
        stores={"sessions": session_store_default},
    )

    async with AsyncTestClient(app=app) as client:
        # Set session data
        response = await client.get("/set-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session set"}

        # Get session data
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["user_id"] == 12345
        assert data["username"] == "testuser"
        assert data["preferences"] == {"theme": "dark", "lang": "en"}

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"user_id": None, "username": None, "preferences": None}


async def test_aiosqlite_session_persistence(
    session_backend_default: SQLSpecSessionBackend, session_store_default: SQLSpecSessionStore
) -> None:
    """Test that sessions persist across requests."""

    @get("/counter")
    async def increment_counter(request: Any) -> dict:
        count = request.session.get("count", 0)
        count += 1
        request.session["count"] = count
        return {"count": count}

    session_config = SQLSpecSessionConfig(backend=session_backend_default, key="aiosqlite-persistence", max_age=3600)

    app = Litestar(
        route_handlers=[increment_counter],
        middleware=[session_config.middleware],
        stores={"sessions": session_store_default},
    )

    async with AsyncTestClient(app=app) as client:
        # Multiple increments should persist
        for expected in range(1, 6):
            response = await client.get("/counter")
            assert response.json() == {"count": expected}


async def test_aiosqlite_session_expiration(session_store_default: SQLSpecSessionStore) -> None:
    """Test session expiration handling."""
    # Create backend with very short lifetime
    config = SQLSpecSessionConfig(
        key="aiosqlite-expiration",
        max_age=1,  # 1 second
        table_name="litestar_sessions",
    )
    backend = SQLSpecSessionBackend(config=config)

    @get("/set-data")
    async def set_data(request: Any) -> dict:
        request.session["test"] = "data"
        return {"status": "set"}

    @get("/get-data")
    async def get_data(request: Any) -> dict:
        return {"test": request.session.get("test")}

    session_config = ServerSideSessionConfig(backend=backend, key="aiosqlite-expiration", max_age=1)

    app = Litestar(
        route_handlers=[set_data, get_data],
        middleware=[session_config.middleware],
        stores={"sessions": session_store_default},
    )

    async with AsyncTestClient(app=app) as client:
        # Set data
        response = await client.get("/set-data")
        assert response.json() == {"status": "set"}

        # Data should be available immediately
        response = await client.get("/get-data")
        assert response.json() == {"test": "data"}

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        response = await client.get("/get-data")
        assert response.json() == {"test": None}


async def test_aiosqlite_concurrent_sessions(
    session_backend_default: SQLSpecSessionBackend, session_store_default: SQLSpecSessionStore
) -> None:
    """Test handling of concurrent sessions."""

    @get("/user/{user_id:int}")
    async def set_user(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        return {"user_id": user_id}

    @get("/whoami")
    async def get_user(request: Any) -> dict:
        return {"user_id": request.session.get("user_id")}

    session_config = ServerSideSessionConfig(backend=session_backend_default, key="aiosqlite-concurrent", max_age=3600)

    app = Litestar(
        route_handlers=[set_user, get_user],
        middleware=[session_config.middleware],
        stores={"sessions": session_store_default},
    )

    async with AsyncTestClient(app=app) as client1, AsyncTestClient(app=app) as client2:
        # Set different users in different clients
        response1 = await client1.get("/user/1")
        assert response1.json() == {"user_id": 1}

        response2 = await client2.get("/user/2")
        assert response2.json() == {"user_id": 2}

        # Each client should maintain its own session
        response1 = await client1.get("/whoami")
        assert response1.json() == {"user_id": 1}

        response2 = await client2.get("/whoami")
        assert response2.json() == {"user_id": 2}


async def test_aiosqlite_session_cleanup(session_store_default: SQLSpecSessionStore) -> None:
    """Test expired session cleanup."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(5):
        session_id = f"cleanup-test-{i}"
        session_ids.append(session_id)
        await session_store_default.set(session_id, {"data": i}, expires_in=1)

    # Create one long-lived session
    await session_store_default.set("persistent", {"data": "keep"}, expires_in=3600)

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await session_store_default.delete_expired()

    # Check that expired sessions are gone
    for session_id in session_ids:
        result = await session_store_default.get(session_id)
        assert result is None

    # Long-lived session should still exist
    result = await session_store_default.get("persistent")
    assert result == {"data": "keep"}


async def test_aiosqlite_store_operations(session_store_default: SQLSpecSessionStore) -> None:
    """Test aiosqlite store operations directly."""
    # Test basic store operations
    session_id = "test-session-aiosqlite"
    test_data = {"user_id": 456, "preferences": {"theme": "light", "lang": "fr"}}

    # Set data
    await session_store_default.set(session_id, test_data, expires_in=3600)

    # Get data
    result = await session_store_default.get(session_id)
    assert result == test_data

    # Check exists
    assert await session_store_default.exists(session_id) is True

    # Delete data
    await session_store_default.delete(session_id)

    # Verify deleted
    result = await session_store_default.get(session_id)
    assert result is None
    assert await session_store_default.exists(session_id) is False
