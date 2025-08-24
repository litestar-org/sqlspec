"""Integration tests for SQLite session backend with store integration."""

import asyncio
import tempfile
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.testing import AsyncTestClient

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.extensions.litestar import SQLSpec
from sqlspec.extensions.litestar.session import SQLSpecSessionBackend, SQLSpecSessionConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.sync_tools import async_

pytestmark = [pytest.mark.sqlite, pytest.mark.integration, pytest.mark.xdist_group("sqlite")]


@pytest.fixture
def sqlite_config() -> SqliteConfig:
    """Create SQLite configuration with migration support."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sessions.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        return SqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],  # Include Litestar migrations
            },
        )


@pytest.fixture
async def session_store(sqlite_config: SqliteConfig) -> SQLSpecSessionStore:
    """Create a session store with migrations applied."""
    # Apply migrations synchronously (SQLite uses sync commands)
    @async_
    def apply_migrations():
        commands = SyncMigrationCommands(sqlite_config)
        commands.init(sqlite_config.migration_config["script_location"], package=False)
        commands.upgrade()

    # Run migrations
    await apply_migrations()

    return SQLSpecSessionStore(sqlite_config, table_name="litestar_sessions")


@pytest.fixture
def session_backend_config() -> SQLSpecSessionConfig:
    """Create session backend configuration."""
    return SQLSpecSessionConfig(
        key="test-session",
        max_age=3600,  # 1 hour
        table_name="litestar_sessions",
    )


@pytest.fixture
def session_backend(session_backend_config: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend instance."""
    return SQLSpecSessionBackend(config=session_backend_config)


async def test_sqlite_migration_creates_correct_table(sqlite_config: SqliteConfig) -> None:
    """Test that Litestar migration creates the correct table structure for SQLite."""

    # Apply migrations synchronously (SQLite uses sync commands)
    @async_
    def apply_migrations():
        commands = SyncMigrationCommands(sqlite_config)
        commands.init(sqlite_config.migration_config["script_location"], package=False)
        commands.upgrade()

    # Run migrations
    await apply_migrations()

    # Verify table was created with correct SQLite-specific types
    with sqlite_config.provide_session() as driver:
        result = driver.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='litestar_sessions'")
        assert len(result.data) == 1
        create_sql = result.data[0]["sql"]

        # SQLite should use TEXT for data column (not JSONB or JSON)
        assert "TEXT" in create_sql
        assert "DATETIME" in create_sql or "TIMESTAMP" in create_sql
        assert "litestar_sessions" in create_sql

        # Verify columns exist
        result = driver.execute("PRAGMA table_info(litestar_sessions)")
        columns = {row["name"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns


async def test_sqlite_session_basic_operations(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test basic session operations with SQLite backend."""

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

    @post("/update-session")
    async def update_session(request: Any) -> dict:
        request.session["last_access"] = "2024-01-01T12:00:00"
        request.session["preferences"]["notifications"] = True
        return {"status": "session updated"}

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(
        store=session_store,
        key="sqlite-session",
        max_age=3600,
    )

    # Create app with session store registered
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
        assert data["user_id"] == 12345
        assert data["username"] == "testuser"
        assert data["preferences"] == {"theme": "dark", "lang": "en"}

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
        assert response.json() == {"user_id": None, "username": None, "preferences": None}


async def test_sqlite_session_persistence(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test that sessions persist across requests."""

    @get("/counter")
    async def increment_counter(request: Any) -> dict:
        count = request.session.get("count", 0)
        history = request.session.get("history", [])
        count += 1
        history.append(count)
        request.session["count"] = count
        request.session["history"] = history
        return {"count": count, "history": history}

    session_config = ServerSideSessionConfig(
        store=session_store,
        key="sqlite-persistence",
        max_age=3600,
    )

    app = Litestar(
        route_handlers=[increment_counter],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Multiple increments should persist with history
        for expected in range(1, 6):
            response = await client.get("/counter")
            data = response.json()
            assert data["count"] == expected
            assert data["history"] == list(range(1, expected + 1))


async def test_sqlite_session_expiration() -> None:
    """Test session expiration handling."""
    # Create a separate database for this test to avoid locking issues
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "expiration_test.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create configuration
        config = SqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],
            },
        )

        # Apply migrations synchronously and ensure proper cleanup
        @async_
        def apply_migrations():
            migration_config = SqliteConfig(
                pool_config={"database": str(db_path)},
                migration_config={
                    "script_location": str(migration_dir),
                    "version_table_name": "sqlspec_migrations",
                    "include_extensions": ["litestar"],
                },
            )
            commands = SyncMigrationCommands(migration_config)
            commands.init(migration_config.migration_config["script_location"], package=False)
            commands.upgrade()
            # Explicitly close the config's pool to release database locks
            if migration_config.pool_instance:
                migration_config.close_pool()

        await apply_migrations()
        
        # Give a small delay to ensure the file lock is released
        await asyncio.sleep(0.1)

        # Create a fresh store configuration
        store_config = SqliteConfig(pool_config={"database": str(db_path)})
        session_store = SQLSpecSessionStore(store_config, table_name="litestar_sessions")

        # Test expiration
        session_id = "expiration-test-session"
        test_data = {"test": "sqlite_data", "timestamp": "2024-01-01"}

        # Set data with 1 second expiration
        await session_store.set(session_id, test_data, expires_in=1)

        # Data should be available immediately
        result = await session_store.get(session_id)
        assert result == test_data

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        result = await session_store.get(session_id)
        assert result is None

        # Close pool to avoid issues
        if store_config.pool_instance:
            store_config.close_pool()


async def test_sqlite_concurrent_sessions(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test handling of concurrent sessions."""

    @get("/user/{user_id:int}")
    async def set_user(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        request.session["db"] = "sqlite"
        return {"user_id": user_id}

    @get("/whoami")
    async def get_user(request: Any) -> dict:
        return {"user_id": request.session.get("user_id"), "db": request.session.get("db")}

    session_config = ServerSideSessionConfig(
        store=session_store,
        key="sqlite-concurrent",
        max_age=3600,
    )

    app = Litestar(
        route_handlers=[set_user, get_user],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
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
        assert response1.json() == {"user_id": 101, "db": "sqlite"}

        response2 = await client2.get("/whoami")
        assert response2.json() == {"user_id": 202, "db": "sqlite"}

        response3 = await client3.get("/whoami")
        assert response3.json() == {"user_id": 303, "db": "sqlite"}


async def test_sqlite_session_cleanup() -> None:
    """Test expired session cleanup with SQLite."""
    # Create a separate database for this test to avoid locking issues
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "cleanup_test.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Apply migrations and create store
        @async_
        def setup_database():
            migration_config = SqliteConfig(
                pool_config={"database": str(db_path)},
                migration_config={
                    "script_location": str(migration_dir),
                    "version_table_name": "sqlspec_migrations",
                    "include_extensions": ["litestar"],
                },
            )
            commands = SyncMigrationCommands(migration_config)
            commands.init(migration_config.migration_config["script_location"], package=False)
            commands.upgrade()
            if migration_config.pool_instance:
                migration_config.close_pool()

        await setup_database()
        await asyncio.sleep(0.1)

        # Create fresh store
        store_config = SqliteConfig(pool_config={"database": str(db_path)})
        session_store = SQLSpecSessionStore(store_config, table_name="litestar_sessions")

        # Create multiple sessions with short expiration
        session_ids = []
        for i in range(10):
            session_id = f"sqlite-cleanup-{i}"
            session_ids.append(session_id)
            await session_store.set(session_id, {"data": i, "type": "temporary"}, expires_in=1)

        # Create long-lived sessions
        persistent_ids = []
        for i in range(3):
            session_id = f"sqlite-persistent-{i}"
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

        # Clean up
        if store_config.pool_instance:
            store_config.close_pool()


async def test_sqlite_session_complex_data(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test storing complex data structures in SQLite sessions."""

    @post("/save-complex")
    async def save_complex(request: Any) -> dict:
        # Store various complex data types
        request.session["nested"] = {
            "level1": {"level2": {"level3": ["deep", "nested", "list"], "number": 42.5, "boolean": True}}
        }
        request.session["mixed_list"] = [1, "two", 3.0, {"four": 4}, [5, 6]]
        request.session["unicode"] = "SQLite: 💾 база данных données 数据库"
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

    session_config = ServerSideSessionConfig(
        store=session_store,
        key="sqlite-complex",
        max_age=3600,
    )

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
        assert data["unicode"] == "SQLite: 💾 база данных données 数据库"

        # Verify null and empty values
        assert data["null_value"] is None
        assert data["empty_dict"] == {}
        assert data["empty_list"] == []


async def test_sqlite_store_operations() -> None:
    """Test SQLite store operations directly."""
    # Create a separate database for this test to avoid locking issues
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "store_ops_test.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Apply migrations and create store
        @async_
        def setup_database():
            migration_config = SqliteConfig(
                pool_config={"database": str(db_path)},
                migration_config={
                    "script_location": str(migration_dir),
                    "version_table_name": "sqlspec_migrations",
                    "include_extensions": ["litestar"],
                },
            )
            commands = SyncMigrationCommands(migration_config)
            commands.init(migration_config.migration_config["script_location"], package=False)
            commands.upgrade()
            if migration_config.pool_instance:
                migration_config.close_pool()

        await setup_database()
        await asyncio.sleep(0.1)

        # Create fresh store
        store_config = SqliteConfig(pool_config={"database": str(db_path)})
        session_store = SQLSpecSessionStore(store_config, table_name="litestar_sessions")

        # Test basic store operations
        session_id = "test-session-sqlite"
        test_data = {
            "user_id": 789,
            "preferences": {"theme": "blue", "lang": "es"},
            "tags": ["admin", "user"],
        }

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

        # Clean up
        if store_config.pool_instance:
            store_config.close_pool()
