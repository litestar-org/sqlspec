"""Comprehensive Litestar integration tests for AsyncPG adapter.

This test suite validates the full integration between SQLSpec's AsyncPG adapter
and Litestar's session middleware, including PostgreSQL-specific features like JSONB.
"""

import asyncio
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from litestar import Litestar, get, post, put
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.stores.registry import StoreRegistry
from litestar.testing import AsyncTestClient

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.extensions.litestar import SQLSpecSessionConfig, SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = [pytest.mark.asyncpg, pytest.mark.postgres, pytest.mark.integration]


@pytest.fixture
async def migrated_config(asyncpg_migration_config: AsyncpgConfig) -> AsyncpgConfig:
    """Apply migrations once and return the config."""
    commands = AsyncMigrationCommands(asyncpg_migration_config)
    await commands.init(asyncpg_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()
    return asyncpg_migration_config


@pytest.fixture
async def litestar_app(session_config: SQLSpecSessionConfig, session_store: SQLSpecSessionStore) -> Litestar:
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


async def test_session_store_creation(session_store: SQLSpecSessionStore) -> None:
    """Test that SessionStore can be created with AsyncPG configuration."""
    assert session_store is not None
    assert session_store.table_name == "litestar_sessions_asyncpg"
    assert session_store.session_id_column == "session_id"
    assert session_store.data_column == "data"
    assert session_store.expires_at_column == "expires_at"
    assert session_store.created_at_column == "created_at"


async def test_session_store_postgres_table_structure(
    session_store: SQLSpecSessionStore, asyncpg_migration_config: AsyncpgConfig
) -> None:
    """Test that session table is created with proper PostgreSQL structure."""
    async with asyncpg_migration_config.provide_session() as driver:
        # Verify table exists
        result = await driver.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE tablename = $1
        """,
            "litestar_sessions_asyncpg",
        )
        assert len(result.data) == 1
        assert result.data[0]["tablename"] == "litestar_sessions_asyncpg"

        # Verify column structure
        result = await driver.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position
        """,
            "litestar_sessions_asyncpg",
        )

        columns = {row["column_name"]: row for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Check data types specific to PostgreSQL
        assert columns["data"]["data_type"] == "jsonb"  # PostgreSQL JSONB type
        assert columns["expires_at"]["data_type"] == "timestamp with time zone"
        assert columns["created_at"]["data_type"] == "timestamp with time zone"

        # Verify indexes exist
        result = await driver.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE tablename = $1
        """,
            "litestar_sessions_asyncpg",
        )
        index_names = [row["indexname"] for row in result.data]
        assert any("expires_at" in name for name in index_names)


async def test_basic_session_operations(litestar_app: Litestar) -> None:
    """Test basic session get/set/delete operations."""
    async with AsyncTestClient(app=litestar_app) as client:
        # Set a simple value
        response = await client.get("/session/set/username?value=testuser")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "set", "key": "username", "value": "testuser"}

        # Get the value back
        response = await client.get("/session/get/username")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"key": "username", "value": "testuser"}

        # Set another value
        response = await client.get("/session/set/user_id?value=12345")
        assert response.status_code == HTTP_200_OK

        # Get all session data
        response = await client.get("/session/all")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["username"] == "testuser"
        assert data["user_id"] == "12345"

        # Delete a specific key
        response = await client.post("/session/key/username/delete")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "deleted", "key": "username"}

        # Verify it's gone
        response = await client.get("/session/get/username")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"key": "username", "value": None}

        # user_id should still exist
        response = await client.get("/session/get/user_id")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"key": "user_id", "value": "12345"}


async def test_bulk_session_operations(litestar_app: Litestar) -> None:
    """Test bulk session operations."""
    async with AsyncTestClient(app=litestar_app) as client:
        # Set multiple values at once
        bulk_data = {
            "user_id": 42,
            "username": "alice",
            "email": "alice@example.com",
            "preferences": {"theme": "dark", "notifications": True, "language": "en"},
            "roles": ["user", "admin"],
            "last_login": "2024-01-15T10:30:00Z",
        }

        response = await client.post("/session/bulk", json=bulk_data)
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "bulk set", "count": 6}

        # Verify all data was set
        response = await client.get("/session/all")
        assert response.status_code == HTTP_200_OK
        data = response.json()

        for key, expected_value in bulk_data.items():
            assert data[key] == expected_value


async def test_session_persistence_across_requests(litestar_app: Litestar) -> None:
    """Test that sessions persist across multiple requests."""
    async with AsyncTestClient(app=litestar_app) as client:
        # Test counter functionality across multiple requests
        expected_counts = [1, 2, 3, 4, 5]

        for expected_count in expected_counts:
            response = await client.get("/counter")
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"count": expected_count}

        # Verify count persists after setting other data
        response = await client.get("/session/set/other_data?value=some_value")
        assert response.status_code == HTTP_200_OK

        response = await client.get("/counter")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"count": 6}


async def test_session_expiration(migrated_config: AsyncpgConfig) -> None:
    """Test session expiration handling."""
    # Create store with very short lifetime
    session_store = SQLSpecSessionStore(config=migrated_config, table_name="litestar_sessions_asyncpg")

    session_config = SQLSpecSessionConfig(
        table_name="litestar_sessions_asyncpg",
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

    async with AsyncTestClient(app=app) as client:
        # Set temporary data
        response = await client.get("/set-temp")
        assert response.json() == {"status": "set"}

        # Data should be available immediately
        response = await client.get("/get-temp")
        assert response.json() == {"temp_data": "will_expire"}

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired (new session created)
        response = await client.get("/get-temp")
        assert response.json() == {"temp_data": None}


async def test_jsonb_support(session_store: SQLSpecSessionStore, asyncpg_migration_config: AsyncpgConfig) -> None:
    """Test PostgreSQL JSONB support for complex data types."""
    session_id = f"jsonb-test-{uuid4()}"

    # Complex nested data that benefits from JSONB
    complex_data = {
        "user_profile": {
            "personal": {
                "name": "John Doe",
                "age": 30,
                "address": {
                    "street": "123 Main St",
                    "city": "Anytown",
                    "coordinates": {"lat": 40.7128, "lng": -74.0060},
                },
            },
            "preferences": {
                "notifications": {"email": True, "sms": False, "push": True},
                "privacy": {"public_profile": False, "show_email": False},
            },
        },
        "permissions": ["read", "write", "admin"],
        "metadata": {"created_at": "2024-01-01T00:00:00Z", "last_modified": "2024-01-02T10:30:00Z", "version": 2},
    }

    # Store complex data
    await session_store.set(session_id, complex_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == complex_data

    # Verify data is stored as JSONB in database
    async with asyncpg_migration_config.provide_session() as driver:
        result = await driver.execute(f"SELECT data FROM {session_store.table_name} WHERE session_id = $1", session_id)
        assert len(result.data) == 1
        stored_json = result.data[0]["data"]
        assert isinstance(stored_json, dict)  # Should be parsed as dict, not string


async def test_concurrent_session_operations(session_store: SQLSpecSessionStore) -> None:
    """Test concurrent session operations with AsyncPG."""

    async def create_session(session_num: int) -> None:
        """Create a session with unique data."""
        session_id = f"concurrent-{session_num}"
        session_data = {
            "session_number": session_num,
            "data": f"session_{session_num}_data",
            "timestamp": f"2024-01-01T12:{session_num:02d}:00Z",
        }
        await session_store.set(session_id, session_data, expires_in=3600)

    async def read_session(session_num: int) -> "dict[str, Any] | None":
        """Read a session by number."""
        session_id = f"concurrent-{session_num}"
        return await session_store.get(session_id, None)

    # Create multiple sessions concurrently
    create_tasks = [create_session(i) for i in range(10)]
    await asyncio.gather(*create_tasks)

    # Read all sessions concurrently
    read_tasks = [read_session(i) for i in range(10)]
    results = await asyncio.gather(*read_tasks)

    # Verify all sessions were created and can be read
    assert len(results) == 10
    for i, result in enumerate(results):
        assert result is not None
        assert result["session_number"] == i
        assert result["data"] == f"session_{i}_data"


async def test_large_session_data(session_store: SQLSpecSessionStore) -> None:
    """Test handling of large session data with AsyncPG."""
    session_id = f"large-data-{uuid4()}"

    # Create large session data
    large_data = {
        "user_id": 12345,
        "large_array": [{"id": i, "data": f"item_{i}" * 100} for i in range(1000)],
        "large_text": "x" * 50000,  # 50KB of text
        "nested_structure": {f"key_{i}": {"subkey": f"value_{i}", "data": ["item"] * 100} for i in range(100)},
    }

    # Store large data
    await session_store.set(session_id, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == large_data
    assert len(retrieved_data["large_array"]) == 1000
    assert len(retrieved_data["large_text"]) == 50000
    assert len(retrieved_data["nested_structure"]) == 100


async def test_session_cleanup_operations(session_store: SQLSpecSessionStore) -> None:
    """Test session cleanup and maintenance operations."""

    # Create sessions with different expiration times
    sessions_data = [
        (f"short-{i}", {"data": f"short_{i}"}, 1)
        for i in range(3)  # Will expire quickly
    ] + [
        (f"long-{i}", {"data": f"long_{i}"}, 3600)
        for i in range(3)  # Won't expire
    ]

    # Set all sessions
    for session_id, data, expires_in in sessions_data:
        await session_store.set(session_id, data, expires_in=expires_in)

    # Verify all sessions exist
    for session_id, expected_data, _ in sessions_data:
        result = await session_store.get(session_id)
        assert result == expected_data

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await session_store.delete_expired()

    # Verify short sessions are gone and long sessions remain
    for session_id, expected_data, expires_in in sessions_data:
        result = await session_store.get(session_id, None)
        if expires_in == 1:  # Short expiration
            assert result is None
        else:  # Long expiration
            assert result == expected_data


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


async def test_session_renewal(session_store: SQLSpecSessionStore) -> None:
    """Test session renewal functionality."""
    session_id = "renewal_test"
    test_data = {"user_id": 123, "activity": "browsing"}

    # Set session with short expiration
    await session_store.set(session_id, test_data, expires_in=5)

    # Get initial expiration time
    initial_expires_in = await session_store.expires_in(session_id)
    assert 4 <= initial_expires_in <= 5

    # Get session data with renewal
    retrieved_data = await session_store.get(session_id, renew_for=timedelta(hours=1))
    assert retrieved_data == test_data

    # Check that expiration time was extended
    new_expires_in = await session_store.expires_in(session_id)
    assert new_expires_in > 3500  # Should be close to 3600 (1 hour)

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
