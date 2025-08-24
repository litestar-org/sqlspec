"""Integration tests for AsyncPG session backend with store integration."""

import asyncio
import tempfile
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK
from litestar.testing import AsyncTestClient

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.extensions.litestar.session import SQLSpecSessionBackend, SQLSpecSessionConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = [pytest.mark.asyncpg, pytest.mark.postgres, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
async def asyncpg_config(postgres_service, request: pytest.FixtureRequest) -> AsyncpgConfig:
    """Create AsyncPG configuration with migration support."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_asyncpg_test_{abs(hash(request.node.nodeid)) % 1000000}"

        config = AsyncpgConfig(
            pool_config={
                "host": postgres_service.host,
                "port": postgres_service.port,
                "user": postgres_service.user,
                "password": postgres_service.password,
                "database": postgres_service.database,
                "min_size": 2,
                "max_size": 10,
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": ["litestar"],  # Include Litestar migrations
            },
        )
        yield config
        # Cleanup
        await config.close_pool()


@pytest.fixture
async def session_store(asyncpg_config: AsyncpgConfig) -> SQLSpecSessionStore:
    """Create a session store with migrations applied."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(asyncpg_config)
    await commands.init(asyncpg_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    return SQLSpecSessionStore(asyncpg_config, table_name="litestar_sessions")


@pytest.fixture
def session_backend_config() -> SQLSpecSessionConfig:
    """Create session backend configuration."""
    return SQLSpecSessionConfig(key="asyncpg-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def session_backend(session_backend_config: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend instance."""
    return SQLSpecSessionBackend(config=session_backend_config)


async def test_asyncpg_migration_creates_correct_table(asyncpg_config: AsyncpgConfig) -> None:
    """Test that Litestar migration creates the correct table structure for PostgreSQL."""
    # Apply migrations
    commands = AsyncMigrationCommands(asyncpg_config)
    await commands.init(asyncpg_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Verify table was created with correct PostgreSQL-specific types
    async with asyncpg_config.provide_session() as driver:
        result = await driver.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'litestar_sessions'
            AND column_name IN ('data', 'expires_at')
        """)

        columns = {row["column_name"]: row["data_type"] for row in result.data}

        # PostgreSQL should use JSONB for data column (not JSON or TEXT)
        assert columns.get("data") == "jsonb"
        assert "timestamp" in columns.get("expires_at", "").lower()

        # Verify all expected columns exist
        result = await driver.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'litestar_sessions'
        """)
        columns = {row["column_name"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns


async def test_asyncpg_session_basic_operations(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test basic session operations with AsyncPG backend."""

    @get("/set-session")
    async def set_session(request: Any) -> dict:
        request.session["user_id"] = 54321
        request.session["username"] = "pguser"
        request.session["preferences"] = {"theme": "light", "lang": "fr"}
        request.session["tags"] = ["admin", "moderator", "user"]
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

    session_config = ServerSideSessionConfig(store=session_store, key="asyncpg-session", max_age=3600)

    app = Litestar(
        route_handlers=[set_session, get_session, update_session, clear_session],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
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
        assert data["user_id"] == 54321
        assert data["username"] == "pguser"
        assert data["preferences"] == {"theme": "light", "lang": "fr"}
        assert data["tags"] == ["admin", "moderator", "user"]

        # Update session
        response = await client.post("/update-session")
        assert response.status_code == HTTP_200_OK

        # Verify update
        response = await client.get("/get-session")
        data = response.json()
        assert data["preferences"]["notifications"] is True

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"user_id": None, "username": None, "preferences": None, "tags": None}


async def test_asyncpg_session_persistence(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test that sessions persist across requests with AsyncPG."""

    @get("/counter")
    async def increment_counter(request: Any) -> dict:
        count = request.session.get("count", 0)
        history = request.session.get("history", [])
        count += 1
        history.append(count)
        request.session["count"] = count
        request.session["history"] = history
        return {"count": count, "history": history}

    session_config = ServerSideSessionConfig(store=session_store, key="asyncpg-counter", max_age=3600)

    app = Litestar(
        route_handlers=[increment_counter], middleware=[session_config.middleware], stores={"sessions": session_store}
    )

    async with AsyncTestClient(app=app) as client:
        # Multiple increments should persist with history
        for expected in range(1, 6):
            response = await client.get("/counter")
            data = response.json()
            assert data["count"] == expected
            assert data["history"] == list(range(1, expected + 1))


async def test_asyncpg_session_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test session expiration handling with AsyncPG."""
    # No need to create a custom backend - just use the store with short expiration

    @get("/set-data")
    async def set_data(request: Any) -> dict:
        request.session["test"] = "postgres_data"
        request.session["timestamp"] = "2024-01-01"
        return {"status": "set"}

    @get("/get-data")
    async def get_data(request: Any) -> dict:
        return {"test": request.session.get("test"), "timestamp": request.session.get("timestamp")}

    session_config = ServerSideSessionConfig(
        store="sessions",  # Use the string name for the store
        key="asyncpg-expiring",
        max_age=1,  # 1 second expiration
    )

    app = Litestar(
        route_handlers=[set_data, get_data], middleware=[session_config.middleware], stores={"sessions": session_store}
    )

    async with AsyncTestClient(app=app) as client:
        # Set data
        response = await client.get("/set-data")
        assert response.json() == {"status": "set"}

        # Data should be available immediately
        response = await client.get("/get-data")
        assert response.json() == {"test": "postgres_data", "timestamp": "2024-01-01"}

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        response = await client.get("/get-data")
        assert response.json() == {"test": None, "timestamp": None}


async def test_asyncpg_concurrent_sessions(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test handling of concurrent sessions with AsyncPG."""

    @get("/user/{user_id:int}")
    async def set_user(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        request.session["db"] = "postgres"
        return {"user_id": user_id}

    @get("/whoami")
    async def get_user(request: Any) -> dict:
        return {"user_id": request.session.get("user_id"), "db": request.session.get("db")}

    session_config = ServerSideSessionConfig(store=session_store, key="asyncpg-concurrent", max_age=3600)

    app = Litestar(
        route_handlers=[set_user, get_user], middleware=[session_config.middleware], stores={"sessions": session_store}
    )

    # Test with multiple concurrent clients
    async with (
        AsyncTestClient(app=app) as client1,
        AsyncTestClient(app=app) as client2,
        AsyncTestClient(app=app) as client3,
    ):
        # Set different users in different clients
        response1 = await client1.get("/user/101")
        assert response1.json() == {"user_id": 101}

        response2 = await client2.get("/user/202")
        assert response2.json() == {"user_id": 202}

        response3 = await client3.get("/user/303")
        assert response3.json() == {"user_id": 303}

        # Each client should maintain its own session
        response1 = await client1.get("/whoami")
        assert response1.json() == {"user_id": 101, "db": "postgres"}

        response2 = await client2.get("/whoami")
        assert response2.json() == {"user_id": 202, "db": "postgres"}

        response3 = await client3.get("/whoami")
        assert response3.json() == {"user_id": 303, "db": "postgres"}


async def test_asyncpg_session_cleanup(session_store: SQLSpecSessionStore) -> None:
    """Test expired session cleanup with AsyncPG."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(10):
        session_id = f"asyncpg-cleanup-{i}"
        session_ids.append(session_id)
        await session_store.set(session_id, {"data": i, "type": "temporary"}, expires_in=1)

    # Create long-lived sessions
    persistent_ids = []
    for i in range(3):
        session_id = f"asyncpg-persistent-{i}"
        persistent_ids.append(session_id)
        await session_store.set(session_id, {"data": f"keep-{i}", "type": "persistent"}, expires_in=3600)

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await session_store.delete_expired()

    # Check that expired sessions are gone
    for session_id in session_ids:
        result = await session_store.get(session_id)
        assert result is None

    # Long-lived sessions should still exist
    for session_id in persistent_ids:
        result = await session_store.get(session_id)
        assert result is not None
        assert result["type"] == "persistent"


async def test_asyncpg_session_complex_data(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test storing complex data structures in AsyncPG sessions."""

    @post("/save-complex")
    async def save_complex(request: Any) -> dict:
        # Store various complex data types
        request.session["nested"] = {
            "level1": {"level2": {"level3": ["deep", "nested", "list"], "number": 42.5, "boolean": True}}
        }
        request.session["mixed_list"] = [1, "two", 3.0, {"four": 4}, [5, 6]]
        request.session["unicode"] = "PostgreSQL: 🐘 Слон éléphant 象"
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

    session_config = ServerSideSessionConfig(store=session_store, key="asyncpg-complex", max_age=3600)

    app = Litestar(
        route_handlers=[save_complex, load_complex],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
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
        assert data["unicode"] == "PostgreSQL: 🐘 Слон éléphant 象"

        # Verify null and empty values
        assert data["null_value"] is None
        assert data["empty_dict"] == {}
        assert data["empty_list"] == []


async def test_asyncpg_store_operations(session_store: SQLSpecSessionStore) -> None:
    """Test AsyncPG store operations directly."""
    # Test basic store operations
    session_id = "test-session-asyncpg"
    test_data = {"user_id": 789, "preferences": {"theme": "blue", "lang": "es"}, "tags": ["admin", "user"]}

    # Set data
    await session_store.set(session_id, test_data, expires_in=3600)

    # Get data
    result = await session_store.get(session_id)
    assert result == test_data

    # Check exists
    assert await session_store.exists(session_id) is True

    # Update with renewal
    updated_data = {**test_data, "last_login": "2024-01-01"}
    await session_store.set(session_id, updated_data, expires_in=7200)

    # Get updated data
    result = await session_store.get(session_id)
    assert result == updated_data

    # Delete data
    await session_store.delete(session_id)

    # Verify deleted
    result = await session_store.get(session_id)
    assert result is None
    assert await session_store.exists(session_id) is False
