"""Comprehensive Litestar integration tests for SQLite adapter."""

import asyncio
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post, put
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.stores.registry import StoreRegistry
from litestar.testing import TestClient

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecSessionConfig, SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands

pytestmark = [pytest.mark.sqlite, pytest.mark.integration, pytest.mark.xdist_group("sqlite")]


@pytest.fixture
def migrated_config() -> SqliteConfig:
    """Apply migrations to the config."""
    tmpdir = tempfile.mkdtemp()
    db_path = Path(tmpdir) / "test.db"
    migration_dir = Path(tmpdir) / "migrations"

    # Create a separate config for migrations to avoid connection issues
    migration_config = SqliteConfig(
        pool_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": "test_migrations",
            "include_extensions": ["litestar"],  # Include litestar extension migrations
        },
    )

    commands = SyncMigrationCommands(migration_config)
    commands.init(str(migration_dir), package=False)
    commands.upgrade()

    # Close the migration pool to release the database lock
    if migration_config.pool_instance:
        migration_config.close_pool()

    # Return a fresh config for the tests
    return SqliteConfig(
        pool_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": "test_migrations",
            "include_extensions": ["litestar"],
        },
    )


@pytest.fixture
def session_store(migrated_config: SqliteConfig) -> SQLSpecSessionStore:
    """Create a session store using the migrated config."""
    return SQLSpecSessionStore(config=migrated_config, table_name="litestar_sessions")


@pytest.fixture
def session_config() -> SQLSpecSessionConfig:
    """Create a session config."""
    return SQLSpecSessionConfig(table_name="litestar_sessions", store="sessions", max_age=3600)


@pytest.fixture
def litestar_app(session_config: SQLSpecSessionConfig, session_store: SQLSpecSessionStore) -> Litestar:
    """Create a Litestar app with session middleware for testing."""

    @get("/session/set/{key:str}")
    async def set_session_value(request: Any, key: str) -> dict:
        """Set a session value."""
        value = request.query_params.get("value", "default")
        request.session[key] = value
        return {"status": "set", "key": key, "value": value}

    @get("/session/get/{key:str}")
    async def get_session_value(request: Any, key: str) -> dict:
        """Get a session value."""
        value = request.session.get(key)
        return {"key": key, "value": value}

    @post("/session/bulk")
    async def set_bulk_session(request: Any) -> dict:
        """Set multiple session values."""
        data = await request.json()
        for key, value in data.items():
            request.session[key] = value
        return {"status": "bulk set", "count": len(data)}

    @get("/session/all")
    async def get_all_session(request: Any) -> dict:
        """Get all session data."""
        return dict(request.session)

    @post("/session/clear")
    async def clear_session(request: Any) -> dict:
        """Clear all session data."""
        request.session.clear()
        return {"status": "cleared"}

    @post("/session/key/{key:str}/delete")
    async def delete_session_key(request: Any, key: str) -> dict:
        """Delete a specific session key."""
        if key in request.session:
            del request.session[key]
            return {"status": "deleted", "key": key}
        return {"status": "not found", "key": key}

    @get("/counter")
    async def counter(request: Any) -> dict:
        """Increment a counter in session."""
        count = request.session.get("count", 0)
        count += 1
        request.session["count"] = count
        return {"count": count}

    @put("/user/profile")
    async def set_user_profile(request: Any) -> dict:
        """Set user profile data."""
        profile = await request.json()
        request.session["profile"] = profile
        return {"status": "profile set", "profile": profile}

    @get("/user/profile")
    async def get_user_profile(request: Any) -> dict[str, Any]:
        """Get user profile data."""
        profile = request.session.get("profile")
        if not profile:
            return {"error": "No profile found"}
        return {"profile": profile}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    return Litestar(
        route_handlers=[
            set_session_value,
            get_session_value,
            set_bulk_session,
            get_all_session,
            clear_session,
            delete_session_key,
            counter,
            set_user_profile,
            get_user_profile,
        ],
        middleware=[session_config.middleware],
        stores=stores,
    )


def test_session_store_creation(session_store: SQLSpecSessionStore) -> None:
    """Test that session store is created properly."""
    assert session_store is not None
    assert session_store.table_name == "litestar_sessions"


def test_session_store_sqlite_table_structure(
    session_store: SQLSpecSessionStore, migrated_config: SqliteConfig
) -> None:
    """Test that session store table has correct SQLite-specific structure."""
    with migrated_config.provide_session() as driver:
        # Verify table exists
        result = driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='litestar_sessions'")
        assert len(result.data) == 1
        assert result.data[0]["name"] == "litestar_sessions"

        # Verify table structure with SQLite-specific types
        result = driver.execute("PRAGMA table_info(litestar_sessions)")
        columns = {row["name"]: row["type"] for row in result.data}

        # SQLite should use TEXT for data column (JSON stored as text)
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Check SQLite-specific column types
        assert "TEXT" in columns.get("data", "")
        assert any(dt in columns.get("expires_at", "") for dt in ["DATETIME", "TIMESTAMP"])

        # Verify indexes exist
        result = driver.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='litestar_sessions'")
        indexes = [row["name"] for row in result.data]
        # Should have some indexes for performance
        assert len(indexes) > 0


def test_basic_session_operations(litestar_app: Litestar) -> None:
    """Test basic session get/set/delete operations."""
    with TestClient(app=litestar_app) as client:
        # Set a simple value
        response = client.get("/session/set/username?value=testuser")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "set", "key": "username", "value": "testuser"}

        # Get the value back
        response = client.get("/session/get/username")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"key": "username", "value": "testuser"}

        # Set another value
        response = client.get("/session/set/user_id?value=12345")
        assert response.status_code == HTTP_200_OK

        # Get all session data
        response = client.get("/session/all")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["username"] == "testuser"
        assert data["user_id"] == "12345"

        # Delete a specific key
        response = client.post("/session/key/username/delete")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "deleted", "key": "username"}

        # Verify it's gone
        response = client.get("/session/get/username")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"key": "username", "value": None}

        # user_id should still exist
        response = client.get("/session/get/user_id")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"key": "user_id", "value": "12345"}


def test_bulk_session_operations(litestar_app: Litestar) -> None:
    """Test bulk session operations."""
    with TestClient(app=litestar_app) as client:
        # Set multiple values at once
        bulk_data = {
            "user_id": 42,
            "username": "alice",
            "email": "alice@example.com",
            "preferences": {"theme": "dark", "notifications": True, "language": "en"},
            "roles": ["user", "admin"],
            "last_login": "2024-01-15T10:30:00Z",
        }

        response = client.post("/session/bulk", json=bulk_data)
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "bulk set", "count": 6}

        # Verify all data was set
        response = client.get("/session/all")
        assert response.status_code == HTTP_200_OK
        data = response.json()

        for key, expected_value in bulk_data.items():
            assert data[key] == expected_value


def test_session_persistence_across_requests(litestar_app: Litestar) -> None:
    """Test that sessions persist across multiple requests."""
    with TestClient(app=litestar_app) as client:
        # Test counter functionality across multiple requests
        expected_counts = [1, 2, 3, 4, 5]

        for expected_count in expected_counts:
            response = client.get("/counter")
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"count": expected_count}

        # Verify count persists after setting other data
        response = client.get("/session/set/other_data?value=some_value")
        assert response.status_code == HTTP_200_OK

        response = client.get("/counter")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"count": 6}


async def test_sqlite_json_support(session_store: SQLSpecSessionStore, migrated_config: SqliteConfig) -> None:
    """Test SQLite JSON support for session data."""
    complex_json_data = {
        "user_profile": {
            "id": 12345,
            "preferences": {
                "theme": "dark",
                "notifications": {"email": True, "push": False, "sms": True},
                "language": "en-US",
            },
            "activity": {
                "login_count": 42,
                "last_login": "2024-01-15T10:30:00Z",
                "recent_actions": [
                    {"action": "login", "timestamp": "2024-01-15T10:30:00Z"},
                    {"action": "view_profile", "timestamp": "2024-01-15T10:31:00Z"},
                    {"action": "update_settings", "timestamp": "2024-01-15T10:32:00Z"},
                ],
            },
        },
        "session_metadata": {
            "created_at": "2024-01-15T10:30:00Z",
            "ip_address": "192.168.1.100",
            "user_agent": "Mozilla/5.0 (Test Browser)",
            "features": ["json_support", "session_storage", "sqlite_backend"],
        },
    }

    # Test storing and retrieving complex JSON data
    session_id = "json-test-session"
    await session_store.set(session_id, complex_json_data, expires_in=3600)

    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == complex_json_data

    # Verify nested structure access
    assert retrieved_data["user_profile"]["preferences"]["theme"] == "dark"
    assert retrieved_data["user_profile"]["activity"]["login_count"] == 42
    assert len(retrieved_data["session_metadata"]["features"]) == 3

    # Test JSON operations directly in SQLite
    with migrated_config.provide_session() as driver:
        # Verify the data is stored as JSON text in SQLite
        result = driver.execute("SELECT data FROM litestar_sessions WHERE session_id = ?", (session_id,))
        assert len(result.data) == 1
        stored_json = result.data[0]["data"]
        assert isinstance(stored_json, str)  # JSON is stored as text in SQLite

        # Parse and verify the JSON
        import json

        parsed_json = json.loads(stored_json)
        assert parsed_json == complex_json_data

    # Cleanup
    await session_store.delete(session_id)


async def test_concurrent_session_operations(session_store: SQLSpecSessionStore) -> None:
    """Test concurrent operations on sessions with SQLite."""
    import asyncio

    async def create_session(session_id: str) -> bool:
        """Create a session with unique data."""
        try:
            session_data = {
                "session_id": session_id,
                "timestamp": time.time(),
                "data": f"Session data for {session_id}",
            }
            await session_store.set(session_id, session_data, expires_in=3600)
            return True
        except Exception:
            return False

    async def read_session(session_id: str) -> dict:
        """Read a session."""
        return await session_store.get(session_id)

    # Test concurrent session creation
    session_ids = [f"concurrent-session-{i}" for i in range(10)]

    # Create sessions concurrently using asyncio
    create_tasks = [create_session(sid) for sid in session_ids]
    create_results = await asyncio.gather(*create_tasks)

    # All creates should succeed (SQLite handles concurrency)
    assert all(create_results)

    # Read sessions concurrently
    read_tasks = [read_session(sid) for sid in session_ids]
    read_results = await asyncio.gather(*read_tasks)

    # All reads should return valid data
    assert all(result is not None for result in read_results)
    assert all("session_id" in result for result in read_results)

    # Cleanup
    for session_id in session_ids:
        await session_store.delete(session_id)


async def test_session_expiration(migrated_config: SqliteConfig) -> None:
    """Test session expiration handling."""
    # Create store with very short lifetime
    session_store = SQLSpecSessionStore(config=migrated_config, table_name="litestar_sessions")

    session_config = SQLSpecSessionConfig(
        table_name="litestar_sessions",
        store="sessions",
        max_age=1,  # 1 second
    )

    @get("/set-temp")
    async def set_temp_data(request: Any) -> dict:
        request.session["temp_data"] = "will_expire"
        return {"status": "set"}

    @get("/get-temp")
    async def get_temp_data(request: Any) -> dict:
        return {"temp_data": request.session.get("temp_data")}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(route_handlers=[set_temp_data, get_temp_data], middleware=[session_config.middleware], stores=stores)

    with TestClient(app=app) as client:
        # Set temporary data
        response = client.get("/set-temp")
        assert response.json() == {"status": "set"}

        # Data should be available immediately
        response = client.get("/get-temp")
        assert response.json() == {"temp_data": "will_expire"}

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired (new session created)
        response = client.get("/get-temp")
        assert response.json() == {"temp_data": None}


async def test_transaction_handling(session_store: SQLSpecSessionStore, migrated_config: SqliteConfig) -> None:
    """Test transaction handling in SQLite store operations."""
    session_id = "transaction-test-session"

    # Test successful transaction
    test_data = {"counter": 0, "operations": []}
    await session_store.set(session_id, test_data, expires_in=3600)

    # SQLite handles transactions automatically in WAL mode
    with migrated_config.provide_session() as driver:
        # Start a transaction context
        driver.begin()
        try:
            # Read current data
            result = driver.execute("SELECT data FROM litestar_sessions WHERE session_id = ?", (session_id,))
            if result.data:
                import json

                current_data = json.loads(result.data[0]["data"])
                current_data["counter"] += 1
                current_data["operations"].append("increment")

                # Update in transaction
                updated_json = json.dumps(current_data)
                driver.execute("UPDATE litestar_sessions SET data = ? WHERE session_id = ?", (updated_json, session_id))
                driver.commit()
        except Exception:
            driver.rollback()
            raise

    # Verify the update succeeded
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data["counter"] == 1
    assert "increment" in retrieved_data["operations"]

    # Test rollback scenario
    with migrated_config.provide_session() as driver:
        driver.begin()
        try:
            # Make a change that we'll rollback
            driver.execute(
                "UPDATE litestar_sessions SET data = ? WHERE session_id = ?",
                ('{"counter": 999, "operations": ["rollback_test"]}', session_id),
            )
            # Force a rollback
            driver.rollback()
        except Exception:
            driver.rollback()

    # Verify the rollback worked - data should be unchanged
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data["counter"] == 1  # Should still be 1, not 999
    assert "rollback_test" not in retrieved_data["operations"]

    # Cleanup
    await session_store.delete(session_id)


def test_concurrent_sessions(session_config: SQLSpecSessionConfig, session_store: SQLSpecSessionStore) -> None:
    """Test handling of concurrent sessions with different clients."""

    @get("/user/login/{user_id:int}")
    async def login_user(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        request.session["login_time"] = time.time()
        return {"status": "logged in", "user_id": user_id}

    @get("/user/whoami")
    async def whoami(request: Any) -> dict:
        user_id = request.session.get("user_id")
        login_time = request.session.get("login_time")
        return {"user_id": user_id, "login_time": login_time}

    @post("/user/update-profile")
    async def update_profile(request: Any) -> dict:
        profile_data = await request.json()
        request.session["profile"] = profile_data
        return {"status": "profile updated"}

    @get("/session/all")
    async def get_all_session(request: Any) -> dict:
        """Get all session data."""
        return dict(request.session)

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(
        route_handlers=[login_user, whoami, update_profile, get_all_session],
        middleware=[session_config.middleware],
        stores=stores,
    )

    # Use separate clients to simulate different browsers/users
    with TestClient(app=app) as client1, TestClient(app=app) as client2, TestClient(app=app) as client3:
        # Each client logs in as different user
        response1 = client1.get("/user/login/100")
        assert response1.json()["user_id"] == 100

        response2 = client2.get("/user/login/200")
        assert response2.json()["user_id"] == 200

        response3 = client3.get("/user/login/300")
        assert response3.json()["user_id"] == 300

        # Each client should maintain separate session
        who1 = client1.get("/user/whoami")
        assert who1.json()["user_id"] == 100

        who2 = client2.get("/user/whoami")
        assert who2.json()["user_id"] == 200

        who3 = client3.get("/user/whoami")
        assert who3.json()["user_id"] == 300

        # Update profiles independently
        client1.post("/user/update-profile", json={"name": "User One", "age": 25})
        client2.post("/user/update-profile", json={"name": "User Two", "age": 30})

        # Verify isolation - get all session data
        response1 = client1.get("/session/all")
        data1 = response1.json()
        assert data1["user_id"] == 100
        assert data1["profile"]["name"] == "User One"

        response2 = client2.get("/session/all")
        data2 = response2.json()
        assert data2["user_id"] == 200
        assert data2["profile"]["name"] == "User Two"

        # Client3 should not have profile data
        response3 = client3.get("/session/all")
        data3 = response3.json()
        assert data3["user_id"] == 300
        assert "profile" not in data3


async def test_store_crud_operations(session_store: SQLSpecSessionStore) -> None:
    """Test direct store CRUD operations."""
    session_id = "test-session-crud"

    # Test data with various types
    test_data = {
        "user_id": 12345,
        "username": "testuser",
        "preferences": {"theme": "dark", "language": "en", "notifications": True},
        "tags": ["admin", "user", "premium"],
        "metadata": {"last_login": "2024-01-15T10:30:00Z", "login_count": 42, "is_verified": True},
    }

    # CREATE
    await session_store.set(session_id, test_data, expires_in=3600)

    # READ
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == test_data

    # UPDATE (overwrite)
    updated_data = {**test_data, "last_activity": "2024-01-15T11:00:00Z"}
    await session_store.set(session_id, updated_data, expires_in=3600)

    retrieved_updated = await session_store.get(session_id)
    assert retrieved_updated == updated_data
    assert "last_activity" in retrieved_updated

    # EXISTS
    assert await session_store.exists(session_id) is True
    assert await session_store.exists("nonexistent") is False

    # EXPIRES_IN
    expires_in = await session_store.expires_in(session_id)
    assert 3500 < expires_in <= 3600  # Should be close to 3600

    # DELETE
    await session_store.delete(session_id)

    # Verify deletion
    assert await session_store.get(session_id) is None
    assert await session_store.exists(session_id) is False


async def test_large_data_handling(session_store: SQLSpecSessionStore) -> None:
    """Test handling of large session data."""
    session_id = "test-large-data"

    # Create large data structure
    large_data = {
        "large_list": list(range(10000)),  # 10k integers
        "large_text": "x" * 50000,  # 50k character string
        "nested_structure": {
            f"key_{i}": {"value": f"data_{i}", "numbers": list(range(i, i + 100)), "text": f"{'content_' * 100}{i}"}
            for i in range(100)  # 100 nested objects
        },
        "metadata": {"size": "large", "created_at": "2024-01-15T10:30:00Z", "version": 1},
    }

    # Store large data
    await session_store.set(session_id, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == large_data
    assert len(retrieved_data["large_list"]) == 10000
    assert len(retrieved_data["large_text"]) == 50000
    assert len(retrieved_data["nested_structure"]) == 100

    # Cleanup
    await session_store.delete(session_id)


async def test_special_characters_handling(session_store: SQLSpecSessionStore) -> None:
    """Test handling of special characters in keys and values."""

    # Test data with various special characters
    test_cases = [
        ("unicode_🔑", {"message": "Hello 🌍 World! 你好世界"}),
        ("special-chars!@#$%", {"data": "Value with special chars: !@#$%^&*()"}),
        ("json_escape", {"quotes": '"double"', "single": "'single'", "backslash": "\\path\\to\\file"}),
        ("newlines_tabs", {"multi_line": "Line 1\nLine 2\tTabbed"}),
        ("empty_values", {"empty_string": "", "empty_list": [], "empty_dict": {}}),
        ("null_values", {"null_value": None, "false_value": False, "zero_value": 0}),
    ]

    for session_id, test_data in test_cases:
        # Store data with special characters
        await session_store.set(session_id, test_data, expires_in=3600)

        # Retrieve and verify
        retrieved_data = await session_store.get(session_id)
        assert retrieved_data == test_data, f"Failed for session_id: {session_id}"

        # Cleanup
        await session_store.delete(session_id)


async def test_session_cleanup_operations(session_store: SQLSpecSessionStore) -> None:
    """Test session cleanup and maintenance operations."""

    # Create multiple sessions with different expiration times
    sessions_data = [
        ("short_lived_1", {"data": "expires_soon_1"}, 1),  # 1 second
        ("short_lived_2", {"data": "expires_soon_2"}, 1),  # 1 second
        ("medium_lived", {"data": "expires_medium"}, 10),  # 10 seconds
        ("long_lived", {"data": "expires_long"}, 3600),  # 1 hour
    ]

    # Set all sessions
    for session_id, data, expires_in in sessions_data:
        await session_store.set(session_id, data, expires_in=expires_in)

    # Verify all sessions exist
    for session_id, _, _ in sessions_data:
        assert await session_store.exists(session_id), f"Session {session_id} should exist"

    # Wait for short-lived sessions to expire
    await asyncio.sleep(2)

    # Delete expired sessions
    await session_store.delete_expired()

    # Check which sessions remain
    assert await session_store.exists("short_lived_1") is False
    assert await session_store.exists("short_lived_2") is False
    assert await session_store.exists("medium_lived") is True
    assert await session_store.exists("long_lived") is True

    # Test get_all functionality
    all_sessions = []

    async def collect_sessions():
        async for session_id, session_data in session_store.get_all():
            all_sessions.append((session_id, session_data))

    await collect_sessions()

    # Should have 2 remaining sessions
    assert len(all_sessions) == 2
    session_ids = {session_id for session_id, _ in all_sessions}
    assert "medium_lived" in session_ids
    assert "long_lived" in session_ids

    # Test delete_all
    await session_store.delete_all()

    # Verify all sessions are gone
    for session_id, _, _ in sessions_data:
        assert await session_store.exists(session_id) is False


async def test_session_renewal(session_store: SQLSpecSessionStore) -> None:
    """Test session renewal functionality."""
    session_id = "renewal_test"
    test_data = {"user_id": 123, "activity": "browsing"}

    # Set session with short expiration
    await session_store.set(session_id, test_data, expires_in=5)

    # Get initial expiration time (allow some timing tolerance)
    initial_expires_in = await session_store.expires_in(session_id)
    assert 3 <= initial_expires_in <= 6  # More tolerant range

    # Get session data with renewal
    retrieved_data = await session_store.get(session_id, renew_for=timedelta(hours=1))
    assert retrieved_data == test_data

    # Check that expiration time was extended (more tolerant)
    new_expires_in = await session_store.expires_in(session_id)
    assert new_expires_in > initial_expires_in  # Just check it was renewed
    assert new_expires_in > 3400  # Should be close to 3600 (1 hour) with tolerance

    # Cleanup
    await session_store.delete(session_id)


async def test_error_handling_and_edge_cases(session_store: SQLSpecSessionStore) -> None:
    """Test error handling and edge cases."""

    # Test getting non-existent session
    result = await session_store.get("non_existent_session")
    assert result is None

    # Test deleting non-existent session (should not raise error)
    await session_store.delete("non_existent_session")

    # Test expires_in for non-existent session
    expires_in = await session_store.expires_in("non_existent_session")
    assert expires_in == 0

    # Test empty session data
    await session_store.set("empty_session", {}, expires_in=3600)
    empty_data = await session_store.get("empty_session")
    assert empty_data == {}

    # Test very large expiration time
    await session_store.set("long_expiry", {"data": "test"}, expires_in=365 * 24 * 60 * 60)  # 1 year
    long_expires_in = await session_store.expires_in("long_expiry")
    assert long_expires_in > 365 * 24 * 60 * 60 - 10  # Should be close to 1 year

    # Cleanup
    await session_store.delete("empty_session")
    await session_store.delete("long_expiry")


def test_complex_user_workflow(litestar_app: Litestar) -> None:
    """Test a complex user workflow combining multiple operations."""
    with TestClient(app=litestar_app) as client:
        # User registration workflow
        user_profile = {
            "user_id": 12345,
            "username": "complex_user",
            "email": "complex@example.com",
            "profile": {
                "first_name": "Complex",
                "last_name": "User",
                "age": 25,
                "preferences": {
                    "theme": "dark",
                    "language": "en",
                    "notifications": {"email": True, "push": False, "sms": True},
                },
            },
            "permissions": ["read", "write", "admin"],
            "last_login": "2024-01-15T10:30:00Z",
        }

        # Set user profile
        response = client.put("/user/profile", json=user_profile)
        assert response.status_code == HTTP_200_OK  # PUT returns 200 by default

        # Verify profile was set
        response = client.get("/user/profile")
        assert response.status_code == HTTP_200_OK
        assert response.json()["profile"] == user_profile

        # Update session with additional activity data
        activity_data = {
            "page_views": 15,
            "session_start": "2024-01-15T10:30:00Z",
            "cart_items": [
                {"id": 1, "name": "Product A", "price": 29.99},
                {"id": 2, "name": "Product B", "price": 19.99},
            ],
        }

        response = client.post("/session/bulk", json=activity_data)
        assert response.status_code == HTTP_201_CREATED

        # Test counter functionality within complex session
        for i in range(1, 6):
            response = client.get("/counter")
            assert response.json()["count"] == i

        # Get all session data to verify everything is maintained
        response = client.get("/session/all")
        all_data = response.json()

        # Verify all data components are present
        assert "profile" in all_data
        assert all_data["profile"] == user_profile
        assert all_data["page_views"] == 15
        assert len(all_data["cart_items"]) == 2
        assert all_data["count"] == 5

        # Test selective data removal
        response = client.post("/session/key/cart_items/delete")
        assert response.json()["status"] == "deleted"

        # Verify cart_items removed but other data persists
        response = client.get("/session/all")
        updated_data = response.json()
        assert "cart_items" not in updated_data
        assert "profile" in updated_data
        assert updated_data["count"] == 5

        # Final counter increment to ensure functionality still works
        response = client.get("/counter")
        assert response.json()["count"] == 6
