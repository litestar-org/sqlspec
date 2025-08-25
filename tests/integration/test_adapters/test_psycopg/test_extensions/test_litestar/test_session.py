"""Integration tests for Psycopg session backend with store integration."""

import asyncio
import tempfile
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.testing import AsyncTestClient, TestClient
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.utils.sync_tools import run_

pytestmark = [pytest.mark.psycopg, pytest.mark.postgres, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
def psycopg_sync_config(
    postgres_service: PostgresService, request: pytest.FixtureRequest
) -> Generator[PsycopgSyncConfig, None, None]:
    """Create Psycopg sync configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique names for test isolation (based on advanced-alchemy pattern)
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_psycopg_sync_{table_suffix}"
        session_table = f"litestar_sessions_psycopg_sync_{table_suffix}"

        config = PsycopgSyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": migration_table,
                "include_extensions": [{"name": "litestar", "session_table": session_table}],
            },
        )
        yield config
        # Cleanup: drop test tables and close pool
        try:
            with config.provide_session() as driver:
                driver.execute(f"DROP TABLE IF EXISTS {session_table}")
                driver.execute(f"DROP TABLE IF EXISTS {migration_table}")
        except Exception:
            pass  # Ignore cleanup errors
        if config.pool_instance:
            config.close_pool()


@pytest.fixture
async def psycopg_async_config(
    postgres_service: PostgresService, request: pytest.FixtureRequest
) -> AsyncGenerator[PsycopgAsyncConfig, None]:
    """Create Psycopg async configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique names for test isolation (based on advanced-alchemy pattern)
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_psycopg_async_{table_suffix}"
        session_table = f"litestar_sessions_psycopg_async_{table_suffix}"

        config = PsycopgAsyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": migration_table,
                "include_extensions": [{"name": "litestar", "session_table": session_table}],
            },
        )
        yield config
        # Cleanup: drop test tables and close pool
        try:
            async with config.provide_session() as driver:
                await driver.execute(f"DROP TABLE IF EXISTS {session_table}")
                await driver.execute(f"DROP TABLE IF EXISTS {migration_table}")
        except Exception:
            pass  # Ignore cleanup errors
        await config.close_pool()


@pytest.fixture
def sync_session_store(psycopg_sync_config: PsycopgSyncConfig) -> SQLSpecSessionStore:
    """Create a sync session store with migrations applied using unique table names."""
    # Apply migrations to create the session table
    commands = SyncMigrationCommands(psycopg_sync_config)
    commands.init(psycopg_sync_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Extract the unique session table name from extensions config
    extensions = psycopg_sync_config.migration_config.get("include_extensions", [])
    session_table_name = "litestar_sessions"  # default
    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions")
            break

    return SQLSpecSessionStore(psycopg_sync_config, table_name=session_table_name)


@pytest.fixture
async def async_session_store(psycopg_async_config: PsycopgAsyncConfig) -> SQLSpecSessionStore:
    """Create an async session store with migrations applied using unique table names."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(psycopg_async_config)
    await commands.init(psycopg_async_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Extract the unique session table name from extensions config
    extensions = psycopg_async_config.migration_config.get("include_extensions", [])
    session_table_name = "litestar_sessions"  # default
    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions")
            break

    return SQLSpecSessionStore(psycopg_async_config, table_name=session_table_name)


def test_psycopg_sync_migration_creates_correct_table(psycopg_sync_config: PsycopgSyncConfig) -> None:
    """Test that Litestar migration creates the correct table structure for PostgreSQL with sync driver."""
    # Apply migrations
    commands = SyncMigrationCommands(psycopg_sync_config)
    commands.init(psycopg_sync_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Verify table was created with correct PostgreSQL-specific types
    with psycopg_sync_config.provide_session() as driver:
        # Get the actual table name from the migration context or extensions config
        extensions = psycopg_sync_config.migration_config.get("include_extensions", [])
        table_name = "litestar_sessions"  # default
        for ext in extensions:
            if isinstance(ext, dict) and ext.get("name") == "litestar":
                table_name = ext.get("session_table", "litestar_sessions")
                break

        result = driver.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            AND column_name IN ('data', 'expires_at')
        """,
            (table_name,),
        )

        columns = {row["column_name"]: row["data_type"] for row in result.data}

        # PostgreSQL should use JSONB for data column (not JSON or TEXT)
        assert columns.get("data") == "jsonb"
        assert "timestamp" in columns.get("expires_at", "").lower()

        # Verify all expected columns exist
        result = driver.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """,
            (table_name,),
        )
        columns = {row["column_name"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns


async def test_psycopg_async_migration_creates_correct_table(psycopg_async_config: PsycopgAsyncConfig) -> None:
    """Test that Litestar migration creates the correct table structure for PostgreSQL with async driver."""
    # Apply migrations
    commands = AsyncMigrationCommands(psycopg_async_config)
    await commands.init(psycopg_async_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Verify table was created with correct PostgreSQL-specific types
    async with psycopg_async_config.provide_session() as driver:
        # Get the actual table name from the migration context or extensions config
        extensions = psycopg_async_config.migration_config.get("include_extensions", [])
        table_name = "litestar_sessions"  # default
        for ext in extensions:
            if isinstance(ext, dict) and ext.get("name") == "litestar":
                table_name = ext.get("session_table", "litestar_sessions")
                break

        result = await driver.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            AND column_name IN ('data', 'expires_at')
        """,
            (table_name,),
        )

        columns = {row["column_name"]: row["data_type"] for row in result.data}

        # PostgreSQL should use JSONB for data column (not JSON or TEXT)
        assert columns.get("data") == "jsonb"
        assert "timestamp" in columns.get("expires_at", "").lower()

        # Verify all expected columns exist
        result = await driver.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """,
            (table_name,),
        )
        columns = {row["column_name"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns


def test_psycopg_sync_session_basic_operations(sync_session_store: SQLSpecSessionStore) -> None:
    """Test basic session operations with Psycopg sync backend."""

    @get("/set-session")
    def set_session(request: Any) -> dict:
        request.session["user_id"] = 54321
        request.session["username"] = "psycopg_sync_user"
        request.session["preferences"] = {"theme": "light", "lang": "fr", "postgres": True}
        request.session["tags"] = ["admin", "moderator", "user", "psycopg"]
        return {"status": "session set"}

    @get("/get-session")
    def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "preferences": request.session.get("preferences"),
            "tags": request.session.get("tags"),
        }

    @post("/update-session")
    def update_session(request: Any) -> dict:
        request.session["last_access"] = "2024-01-01T12:00:00"
        request.session["preferences"]["notifications"] = True
        request.session["postgres_sync"] = "active"
        return {"status": "session updated"}

    @post("/clear-session")
    def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(store="sessions", key="psycopg-sync-session", max_age=3600)

    app = Litestar(
        route_handlers=[set_session, get_session, update_session, clear_session],
        middleware=[session_config.middleware],
        stores={"sessions": sync_session_store},
    )

    with TestClient(app=app) as client:
        # Set session data
        response = client.get("/set-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session set"}

        # Get session data
        response = client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["user_id"] == 54321
        assert data["username"] == "psycopg_sync_user"
        assert data["preferences"] == {"theme": "light", "lang": "fr", "postgres": True}
        assert data["tags"] == ["admin", "moderator", "user", "psycopg"]

        # Update session
        response = client.post("/update-session")
        assert response.status_code == HTTP_201_CREATED

        # Verify update
        response = client.get("/get-session")
        data = response.json()
        assert data["preferences"]["notifications"] is True

        # Clear session
        response = client.post("/clear-session")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"user_id": None, "username": None, "preferences": None, "tags": None}


async def test_psycopg_async_session_basic_operations(async_session_store: SQLSpecSessionStore) -> None:
    """Test basic session operations with Psycopg async backend."""

    @get("/set-session")
    async def set_session(request: Any) -> dict:
        request.session["user_id"] = 98765
        request.session["username"] = "psycopg_async_user"
        request.session["preferences"] = {"theme": "dark", "lang": "es", "postgres": True}
        request.session["tags"] = ["editor", "reviewer", "user", "psycopg_async"]
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
        request.session["last_access"] = "2024-01-01T15:30:00"
        request.session["preferences"]["notifications"] = False
        request.session["postgres_async"] = "active"
        return {"status": "session updated"}

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(store="sessions", key="psycopg-async-session", max_age=3600)

    app = Litestar(
        route_handlers=[set_session, get_session, update_session, clear_session],
        middleware=[session_config.middleware],
        stores={"sessions": async_session_store},
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
        assert data["user_id"] == 98765
        assert data["username"] == "psycopg_async_user"
        assert data["preferences"] == {"theme": "dark", "lang": "es", "postgres": True}
        assert data["tags"] == ["editor", "reviewer", "user", "psycopg_async"]

        # Update session
        response = await client.post("/update-session")
        assert response.status_code == HTTP_201_CREATED

        # Verify update
        response = await client.get("/get-session")
        data = response.json()
        assert data["preferences"]["notifications"] is False

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"user_id": None, "username": None, "preferences": None, "tags": None}


def test_psycopg_sync_session_persistence(sync_session_store: SQLSpecSessionStore) -> None:
    """Test that sessions persist across requests with Psycopg sync driver."""

    @get("/counter")
    def increment_counter(request: Any) -> dict:
        count = request.session.get("count", 0)
        history = request.session.get("history", [])
        count += 1
        history.append(count)
        request.session["count"] = count
        request.session["history"] = history
        request.session["postgres_type"] = "sync"
        return {"count": count, "history": history, "postgres_type": "sync"}

    session_config = ServerSideSessionConfig(store="sessions", key="psycopg-sync-counter", max_age=3600)

    app = Litestar(
        route_handlers=[increment_counter],
        middleware=[session_config.middleware],
        stores={"sessions": sync_session_store},
    )

    with TestClient(app=app) as client:
        # Multiple increments should persist with history
        for expected in range(1, 6):
            response = client.get("/counter")
            data = response.json()
            assert data["count"] == expected
            assert data["history"] == list(range(1, expected + 1))
            assert data["postgres_type"] == "sync"


async def test_psycopg_async_session_persistence(async_session_store: SQLSpecSessionStore) -> None:
    """Test that sessions persist across requests with Psycopg async driver."""

    @get("/counter")
    async def increment_counter(request: Any) -> dict:
        count = request.session.get("count", 0)
        history = request.session.get("history", [])
        count += 1
        history.append(count)
        request.session["count"] = count
        request.session["history"] = history
        request.session["postgres_type"] = "async"
        return {"count": count, "history": history, "postgres_type": "async"}

    session_config = ServerSideSessionConfig(store="sessions", key="psycopg-async-counter", max_age=3600)

    app = Litestar(
        route_handlers=[increment_counter],
        middleware=[session_config.middleware],
        stores={"sessions": async_session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Multiple increments should persist with history
        for expected in range(1, 6):
            response = await client.get("/counter")
            data = response.json()
            assert data["count"] == expected
            assert data["history"] == list(range(1, expected + 1))
            assert data["postgres_type"] == "async"


def test_psycopg_sync_session_expiration(sync_session_store: SQLSpecSessionStore) -> None:
    """Test session expiration handling with Psycopg sync driver."""

    @get("/set-data")
    def set_data(request: Any) -> dict:
        request.session["test"] = "psycopg_sync_data"
        request.session["timestamp"] = "2024-01-01"
        request.session["driver"] = "psycopg_sync"
        return {"status": "set"}

    @get("/get-data")
    def get_data(request: Any) -> dict:
        return {
            "test": request.session.get("test"),
            "timestamp": request.session.get("timestamp"),
            "driver": request.session.get("driver"),
        }

    session_config = ServerSideSessionConfig(
        store="sessions",
        key="psycopg-sync-expiring",
        max_age=1,  # 1 second expiration
    )

    app = Litestar(
        route_handlers=[set_data, get_data],
        middleware=[session_config.middleware],
        stores={"sessions": sync_session_store},
    )

    with TestClient(app=app) as client:
        # Set data
        response = client.get("/set-data")
        assert response.json() == {"status": "set"}

        # Data should be available immediately
        response = client.get("/get-data")
        assert response.json() == {"test": "psycopg_sync_data", "timestamp": "2024-01-01", "driver": "psycopg_sync"}

        # Wait for expiration
        import time

        time.sleep(2)

        # Data should be expired
        response = client.get("/get-data")
        assert response.json() == {"test": None, "timestamp": None, "driver": None}


async def test_psycopg_async_session_expiration(async_session_store: SQLSpecSessionStore) -> None:
    """Test session expiration handling with Psycopg async driver."""

    @get("/set-data")
    async def set_data(request: Any) -> dict:
        request.session["test"] = "psycopg_async_data"
        request.session["timestamp"] = "2024-01-01"
        request.session["driver"] = "psycopg_async"
        return {"status": "set"}

    @get("/get-data")
    async def get_data(request: Any) -> dict:
        return {
            "test": request.session.get("test"),
            "timestamp": request.session.get("timestamp"),
            "driver": request.session.get("driver"),
        }

    session_config = ServerSideSessionConfig(
        store="sessions",
        key="psycopg-async-expiring",
        max_age=1,  # 1 second expiration
    )

    app = Litestar(
        route_handlers=[set_data, get_data],
        middleware=[session_config.middleware],
        stores={"sessions": async_session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Set data
        response = await client.get("/set-data")
        assert response.json() == {"status": "set"}

        # Data should be available immediately
        response = await client.get("/get-data")
        assert response.json() == {"test": "psycopg_async_data", "timestamp": "2024-01-01", "driver": "psycopg_async"}

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        response = await client.get("/get-data")
        assert response.json() == {"test": None, "timestamp": None, "driver": None}


def test_psycopg_sync_concurrent_sessions(sync_session_store: SQLSpecSessionStore) -> None:
    """Test handling of concurrent sessions with Psycopg sync driver."""

    @get("/user/{user_id:int}")
    def set_user(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        request.session["db"] = "postgres_sync"
        request.session["driver"] = "psycopg"
        return {"user_id": user_id}

    @get("/whoami")
    def get_user(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "db": request.session.get("db"),
            "driver": request.session.get("driver"),
        }

    session_config = ServerSideSessionConfig(store="sessions", key="psycopg-sync-concurrent", max_age=3600)

    app = Litestar(
        route_handlers=[set_user, get_user],
        middleware=[session_config.middleware],
        stores={"sessions": sync_session_store},
    )

    # Test with multiple concurrent clients using sync test client
    with TestClient(app=app) as client1, TestClient(app=app) as client2, TestClient(app=app) as client3:
        # Set different users in different clients
        response1 = client1.get("/user/101")
        assert response1.json() == {"user_id": 101}

        response2 = client2.get("/user/202")
        assert response2.json() == {"user_id": 202}

        response3 = client3.get("/user/303")
        assert response3.json() == {"user_id": 303}

        # Each client should maintain its own session
        response1 = client1.get("/whoami")
        assert response1.json() == {"user_id": 101, "db": "postgres_sync", "driver": "psycopg"}

        response2 = client2.get("/whoami")
        assert response2.json() == {"user_id": 202, "db": "postgres_sync", "driver": "psycopg"}

        response3 = client3.get("/whoami")
        assert response3.json() == {"user_id": 303, "db": "postgres_sync", "driver": "psycopg"}


async def test_psycopg_async_concurrent_sessions(async_session_store: SQLSpecSessionStore) -> None:
    """Test handling of concurrent sessions with Psycopg async driver."""

    @get("/user/{user_id:int}")
    async def set_user(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        request.session["db"] = "postgres_async"
        request.session["driver"] = "psycopg"
        return {"user_id": user_id}

    @get("/whoami")
    async def get_user(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "db": request.session.get("db"),
            "driver": request.session.get("driver"),
        }

    session_config = ServerSideSessionConfig(store="sessions", key="psycopg-async-concurrent", max_age=3600)

    app = Litestar(
        route_handlers=[set_user, get_user],
        middleware=[session_config.middleware],
        stores={"sessions": async_session_store},
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
        assert response1.json() == {"user_id": 101, "db": "postgres_async", "driver": "psycopg"}

        response2 = await client2.get("/whoami")
        assert response2.json() == {"user_id": 202, "db": "postgres_async", "driver": "psycopg"}

        response3 = await client3.get("/whoami")
        assert response3.json() == {"user_id": 303, "db": "postgres_async", "driver": "psycopg"}


async def test_psycopg_sync_session_cleanup(sync_session_store: SQLSpecSessionStore) -> None:
    """Test expired session cleanup with Psycopg sync driver."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(10):
        session_id = f"psycopg-sync-cleanup-{i}"
        session_ids.append(session_id)
        run_(sync_session_store.set)(
            session_id, {"data": i, "type": "temporary", "driver": "psycopg_sync"}, expires_in=1
        )

    # Create long-lived sessions
    persistent_ids = []
    for i in range(3):
        session_id = f"psycopg-sync-persistent-{i}"
        persistent_ids.append(session_id)
        run_(sync_session_store.set)(
            session_id, {"data": f"keep-{i}", "type": "persistent", "driver": "psycopg_sync"}, expires_in=3600
        )

    # Wait for short sessions to expire
    import time

    time.sleep(2)

    # Clean up expired sessions
    run_(sync_session_store.delete_expired)()

    # Check that expired sessions are gone
    for session_id in session_ids:
        result = run_(sync_session_store.get)(session_id)
        assert result is None

    # Long-lived sessions should still exist
    for session_id in persistent_ids:
        result = run_(sync_session_store.get)(session_id)
        assert result is not None
        assert result["type"] == "persistent"
        assert result["driver"] == "psycopg_sync"


async def test_psycopg_async_session_cleanup(async_session_store: SQLSpecSessionStore) -> None:
    """Test expired session cleanup with Psycopg async driver."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(10):
        session_id = f"psycopg-async-cleanup-{i}"
        session_ids.append(session_id)
        await async_session_store.set(
            session_id, {"data": i, "type": "temporary", "driver": "psycopg_async"}, expires_in=1
        )

    # Create long-lived sessions
    persistent_ids = []
    for i in range(3):
        session_id = f"psycopg-async-persistent-{i}"
        persistent_ids.append(session_id)
        await async_session_store.set(
            session_id, {"data": f"keep-{i}", "type": "persistent", "driver": "psycopg_async"}, expires_in=3600
        )

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await async_session_store.delete_expired()

    # Check that expired sessions are gone
    for session_id in session_ids:
        result = await async_session_store.get(session_id)
        assert result is None

    # Long-lived sessions should still exist
    for session_id in persistent_ids:
        result = await async_session_store.get(session_id)
        assert result is not None
        assert result["type"] == "persistent"
        assert result["driver"] == "psycopg_async"


async def test_psycopg_sync_session_complex_data(sync_session_store: SQLSpecSessionStore) -> None:
    """Test storing complex data structures in Psycopg sync sessions."""

    @post("/save-complex")
    def save_complex(request: Any) -> dict:
        # Store various complex data types that PostgreSQL JSONB handles well
        request.session["nested"] = {
            "level1": {
                "level2": {
                    "level3": ["deep", "nested", "list", "postgres"],
                    "number": 42.5,
                    "boolean": True,
                    "postgres_feature": "JSONB",
                }
            }
        }
        request.session["mixed_list"] = [1, "two", 3.0, {"four": 4}, [5, 6], {"postgres": "rocks"}]
        request.session["unicode"] = "PostgreSQL: 🐘 Слон éléphant 象 with psycopg sync"
        request.session["null_value"] = None
        request.session["empty_dict"] = {}
        request.session["empty_list"] = []
        request.session["postgres_metadata"] = {
            "driver": "psycopg",
            "mode": "sync",
            "jsonb_support": True,
            "version": "3.x",
        }
        return {"status": "complex data saved"}

    @get("/load-complex")
    def load_complex(request: Any) -> dict:
        return {
            "nested": request.session.get("nested"),
            "mixed_list": request.session.get("mixed_list"),
            "unicode": request.session.get("unicode"),
            "null_value": request.session.get("null_value"),
            "empty_dict": request.session.get("empty_dict"),
            "empty_list": request.session.get("empty_list"),
            "postgres_metadata": request.session.get("postgres_metadata"),
        }

    session_config = ServerSideSessionConfig(store="sessions", key="psycopg-sync-complex", max_age=3600)

    app = Litestar(
        route_handlers=[save_complex, load_complex],
        middleware=[session_config.middleware],
        stores={"sessions": sync_session_store},
    )

    with TestClient(app=app) as client:
        # Save complex data
        response = client.post("/save-complex")
        assert response.json() == {"status": "complex data saved"}

        # Load and verify complex data
        response = client.get("/load-complex")
        data = response.json()

        # Verify nested structure
        assert data["nested"]["level1"]["level2"]["level3"] == ["deep", "nested", "list", "postgres"]
        assert data["nested"]["level1"]["level2"]["number"] == 42.5
        assert data["nested"]["level1"]["level2"]["boolean"] is True
        assert data["nested"]["level1"]["level2"]["postgres_feature"] == "JSONB"

        # Verify mixed list
        assert data["mixed_list"] == [1, "two", 3.0, {"four": 4}, [5, 6], {"postgres": "rocks"}]

        # Verify unicode
        assert data["unicode"] == "PostgreSQL: 🐘 Слон éléphant 象 with psycopg sync"

        # Verify null and empty values
        assert data["null_value"] is None
        assert data["empty_dict"] == {}
        assert data["empty_list"] == []

        # Verify PostgreSQL metadata
        assert data["postgres_metadata"]["driver"] == "psycopg"
        assert data["postgres_metadata"]["mode"] == "sync"
        assert data["postgres_metadata"]["jsonb_support"] is True


async def test_psycopg_async_session_complex_data(async_session_store: SQLSpecSessionStore) -> None:
    """Test storing complex data structures in Psycopg async sessions."""

    @post("/save-complex")
    async def save_complex(request: Any) -> dict:
        # Store various complex data types that PostgreSQL JSONB handles well
        request.session["nested"] = {
            "level1": {
                "level2": {
                    "level3": ["deep", "nested", "list", "postgres_async"],
                    "number": 84.7,
                    "boolean": False,
                    "postgres_feature": "JSONB_ASYNC",
                }
            }
        }
        request.session["mixed_list"] = [10, "twenty", 30.5, {"forty": 40}, [50, 60], {"postgres_async": "awesome"}]
        request.session["unicode"] = "PostgreSQL: 🐘 Слон éléphant 象 with psycopg async"
        request.session["null_value"] = None
        request.session["empty_dict"] = {}
        request.session["empty_list"] = []
        request.session["postgres_metadata"] = {
            "driver": "psycopg",
            "mode": "async",
            "jsonb_support": True,
            "version": "3.x",
            "connection_pool": True,
        }
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
            "postgres_metadata": request.session.get("postgres_metadata"),
        }

    session_config = ServerSideSessionConfig(store="sessions", key="psycopg-async-complex", max_age=3600)

    app = Litestar(
        route_handlers=[save_complex, load_complex],
        middleware=[session_config.middleware],
        stores={"sessions": async_session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Save complex data
        response = await client.post("/save-complex")
        assert response.json() == {"status": "complex data saved"}

        # Load and verify complex data
        response = await client.get("/load-complex")
        data = response.json()

        # Verify nested structure
        assert data["nested"]["level1"]["level2"]["level3"] == ["deep", "nested", "list", "postgres_async"]
        assert data["nested"]["level1"]["level2"]["number"] == 84.7
        assert data["nested"]["level1"]["level2"]["boolean"] is False
        assert data["nested"]["level1"]["level2"]["postgres_feature"] == "JSONB_ASYNC"

        # Verify mixed list
        assert data["mixed_list"] == [10, "twenty", 30.5, {"forty": 40}, [50, 60], {"postgres_async": "awesome"}]

        # Verify unicode
        assert data["unicode"] == "PostgreSQL: 🐘 Слон éléphant 象 with psycopg async"

        # Verify null and empty values
        assert data["null_value"] is None
        assert data["empty_dict"] == {}
        assert data["empty_list"] == []

        # Verify PostgreSQL metadata
        assert data["postgres_metadata"]["driver"] == "psycopg"
        assert data["postgres_metadata"]["mode"] == "async"
        assert data["postgres_metadata"]["jsonb_support"] is True
        assert data["postgres_metadata"]["connection_pool"] is True


def test_psycopg_sync_store_operations(sync_session_store: SQLSpecSessionStore) -> None:
    """Test Psycopg sync store operations directly."""
    # Test basic store operations
    session_id = "test-session-psycopg-sync"
    test_data = {
        "user_id": 789,
        "preferences": {"theme": "blue", "lang": "es", "postgres": "sync"},
        "tags": ["admin", "user", "psycopg"],
        "metadata": {"driver": "psycopg", "type": "sync", "jsonb": True},
    }

    # Set data
    run_(sync_session_store.set)(session_id, test_data, expires_in=3600)

    # Get data
    result = run_(sync_session_store.get)(session_id)
    assert result == test_data

    # Check exists
    assert run_(sync_session_store.exists)(session_id) is True

    # Update with renewal
    updated_data = {**test_data, "last_login": "2024-01-01", "postgres_updated": True}
    run_(sync_session_store.set)(session_id, updated_data, expires_in=7200)

    # Get updated data
    result = run_(sync_session_store.get)(session_id)
    assert result == updated_data

    # Delete data
    run_(sync_session_store.delete)(session_id)

    # Verify deleted
    result = run_(sync_session_store.get)(session_id)
    assert result is None
    assert run_(sync_session_store.exists)(session_id) is False


async def test_psycopg_async_store_operations(async_session_store: SQLSpecSessionStore) -> None:
    """Test Psycopg async store operations directly."""
    # Test basic store operations
    session_id = "test-session-psycopg-async"
    test_data = {
        "user_id": 456,
        "preferences": {"theme": "green", "lang": "pt", "postgres": "async"},
        "tags": ["editor", "reviewer", "psycopg_async"],
        "metadata": {"driver": "psycopg", "type": "async", "jsonb": True, "pool": True},
    }

    # Set data
    await async_session_store.set(session_id, test_data, expires_in=3600)

    # Get data
    result = await async_session_store.get(session_id)
    assert result == test_data

    # Check exists
    assert await async_session_store.exists(session_id) is True

    # Update with renewal
    updated_data = {**test_data, "last_login": "2024-01-01", "postgres_updated": True}
    await async_session_store.set(session_id, updated_data, expires_in=7200)

    # Get updated data
    result = await async_session_store.get(session_id)
    assert result == updated_data

    # Delete data
    await async_session_store.delete(session_id)

    # Verify deleted
    result = await async_session_store.get(session_id)
    assert result is None
    assert await async_session_store.exists(session_id) is False
