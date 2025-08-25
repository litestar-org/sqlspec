"""Integration tests for aiosqlite session backend with store integration."""

import asyncio
import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.testing import AsyncTestClient

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = [pytest.mark.aiosqlite, pytest.mark.integration, pytest.mark.asyncio, pytest.mark.xdist_group("aiosqlite")]


@pytest.fixture
async def aiosqlite_migration_config(request: pytest.FixtureRequest) -> AsyncGenerator[AiosqliteConfig, None]:
    """Create AioSQLite configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)
        db_path = Path(temp_dir) / "test.db"

        # Create unique names for test isolation (based on advanced-alchemy pattern)
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_aiosqlite_{table_suffix}"
        session_table = f"litestar_sessions_aiosqlite_{table_suffix}"

        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": migration_table,
                "include_extensions": [{"name": "litestar", "session_table": session_table}],
            },
        )
        yield config
        # Cleanup: close pool
        try:
            if config.pool_instance:
                await config.close_pool()
        except Exception:
            pass  # Ignore cleanup errors


@pytest.fixture
async def aiosqlite_migration_config_with_dict(request: pytest.FixtureRequest) -> AsyncGenerator[AiosqliteConfig, None]:
    """Create AioSQLite configuration with dict-based config and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)
        db_path = Path(temp_dir) / "test.db"

        # Create unique names for test isolation (based on advanced-alchemy pattern)
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_aiosqlite_{table_suffix}"
        custom_session_table = f"custom_sessions_aiosqlite_{table_suffix}"

        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": migration_table,
                "include_extensions": [{"name": "litestar", "session_table": custom_session_table}],
            },
        )
        yield config
        # Cleanup: close pool
        try:
            if config.pool_instance:
                await config.close_pool()
        except Exception:
            pass  # Ignore cleanup errors


@pytest.fixture
async def session_store_default(aiosqlite_migration_config: AiosqliteConfig) -> SQLSpecSessionStore:
    """Create a session store with migrations applied using unique table names."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(aiosqlite_migration_config)
    await commands.init(aiosqlite_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Extract the unique session table name from config context
    session_table_name = aiosqlite_migration_config.migration_config.get("context", {}).get(
        "session_table_name", "litestar_sessions"
    )
    return SQLSpecSessionStore(aiosqlite_migration_config, table_name=session_table_name)


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


async def test_aiosqlite_session_basic_operations(session_store_default: SQLSpecSessionStore) -> None:
    """Test basic session operations with aiosqlite backend."""

    @get("/set-session")
    async def set_session(request: Any) -> dict:
        request.session["user_id"] = 12345
        request.session["username"] = "testuser"
        request.session["preferences"] = {"theme": "dark", "lang": "en"}
        request.session["tags"] = ["user", "sqlite", "async"]
        return {"status": "session set"}

    @get("/get-session")
    async def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "preferences": request.session.get("preferences"),
            "tags": request.session.get("tags"),
        }

    @post("/update-session")
    async def update_session(request: Any) -> dict:
        request.session["last_access"] = "2024-01-01T12:00:00"
        request.session["preferences"]["notifications"] = True
        return {"status": "session updated"}

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(store="sessions", key="aiosqlite-session", max_age=3600)

    app = Litestar(
        route_handlers=[set_session, get_session, update_session, clear_session],
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
        assert data["tags"] == ["user", "sqlite", "async"]

        # Update session
        response = await client.post("/update-session")
        assert response.status_code == HTTP_201_CREATED

        # Verify update
        response = await client.get("/get-session")
        data = response.json()
        assert data["preferences"]["notifications"] is True

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"user_id": None, "username": None, "preferences": None, "tags": None}


async def test_aiosqlite_session_persistence(session_store_default: SQLSpecSessionStore) -> None:
    """Test that sessions persist across requests."""

    @get("/counter")
    async def increment_counter(request: Any) -> dict:
        count = request.session.get("count", 0)
        count += 1
        request.session["count"] = count
        return {"count": count}

    session_config = ServerSideSessionConfig(store="sessions", key="aiosqlite-persistence", max_age=3600)

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
    # Use the store with short expiration

    @get("/set-data")
    async def set_data(request: Any) -> dict:
        request.session["test"] = "data"
        return {"status": "set"}

    @get("/get-data")
    async def get_data(request: Any) -> dict:
        return {"test": request.session.get("test")}

    session_config = ServerSideSessionConfig(store="sessions", key="aiosqlite-expiration", max_age=1)

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


async def test_aiosqlite_concurrent_sessions(session_store_default: SQLSpecSessionStore) -> None:
    """Test handling of concurrent sessions."""

    @get("/user/{user_id:int}")
    async def set_user(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        return {"user_id": user_id}

    @get("/whoami")
    async def get_user(request: Any) -> dict:
        return {"user_id": request.session.get("user_id")}

    session_config = ServerSideSessionConfig(store="sessions", key="aiosqlite-concurrent", max_age=3600)

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


async def test_aiosqlite_session_complex_data(session_store_default: SQLSpecSessionStore) -> None:
    """Test storing complex data structures in AioSQLite sessions."""

    @post("/save-complex")
    async def save_complex(request: Any) -> dict:
        # Store various complex data types
        request.session["nested"] = {
            "level1": {"level2": {"level3": ["deep", "nested", "list"], "number": 42.5, "boolean": True}}
        }
        request.session["mixed_list"] = [1, "two", 3.0, {"four": 4}, [5, 6]]
        request.session["unicode"] = "AioSQLite: 🗃️ база данных données 数据库"
        request.session["null_value"] = None
        request.session["empty_dict"] = {}
        request.session["empty_list"] = []
        return {"status": "complex data saved"}

    @get("/load-complex")
    async def load_complex(request: Any) -> dict:
        return {
            "nested": request.session.get("nested"),
            "mixed_list": request.session.get("mixed_list"),
            "unicode": request.session.get("unicode"),
            "null_value": request.session.get("null_value"),
            "empty_dict": request.session.get("empty_dict"),
            "empty_list": request.session.get("empty_list"),
        }

    session_config = ServerSideSessionConfig(store="sessions", key="aiosqlite-complex", max_age=3600)

    app = Litestar(
        route_handlers=[save_complex, load_complex],
        middleware=[session_config.middleware],
        stores={"sessions": session_store_default},
    )

    async with AsyncTestClient(app=app) as client:
        # Save complex data
        response = await client.post("/save-complex")
        assert response.json() == {"status": "complex data saved"}

        # Load and verify complex data
        response = await client.get("/load-complex")
        data = response.json()

        # Verify nested structure
        assert data["nested"]["level1"]["level2"]["level3"] == ["deep", "nested", "list"]
        assert data["nested"]["level1"]["level2"]["number"] == 42.5
        assert data["nested"]["level1"]["level2"]["boolean"] is True

        # Verify mixed list
        assert data["mixed_list"] == [1, "two", 3.0, {"four": 4}, [5, 6]]

        # Verify unicode
        assert data["unicode"] == "AioSQLite: 🗃️ база данных données 数据库"

        # Verify null and empty values
        assert data["null_value"] is None
        assert data["empty_dict"] == {}
        assert data["empty_list"] == []


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

    # Update with renewal
    updated_data = {**test_data, "last_login": "2024-01-01"}
    await session_store_default.set(session_id, updated_data, expires_in=7200)

    # Get updated data
    result = await session_store_default.get(session_id)
    assert result == updated_data

    # Delete data
    await session_store_default.delete(session_id)

    # Verify deleted
    result = await session_store_default.get(session_id)
    assert result is None
    assert await session_store_default.exists(session_id) is False
