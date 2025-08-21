"""Comprehensive Litestar integration tests for AsyncPG adapter."""

import asyncio
from typing import Any
from uuid import uuid4

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK
from litestar.testing import AsyncTestClient

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend, SQLSpecSessionStore

pytestmark = [pytest.mark.asyncpg, pytest.mark.postgres, pytest.mark.integration]


@pytest.fixture
async def asyncpg_config() -> AsyncpgConfig:
    """Create AsyncPG configuration for testing."""
    return AsyncpgConfig(
        pool_config={"dsn": "postgresql://postgres:postgres@localhost:5432/postgres", "min_size": 2, "max_size": 10}
    )


@pytest.fixture
async def session_store(asyncpg_config: AsyncpgConfig) -> SQLSpecSessionStore:
    """Create a session store instance."""
    store = SQLSpecSessionStore(
        config=asyncpg_config,
        table_name="test_litestar_sessions",
        session_id_column="session_id",
        data_column="session_data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )
    # Ensure table exists
    async with asyncpg_config.provide_session() as driver:
        await store._ensure_table_exists(driver)
    return store


@pytest.fixture
async def session_backend(asyncpg_config: AsyncpgConfig) -> SQLSpecSessionBackend:
    """Create a session backend instance."""
    backend = SQLSpecSessionBackend(config=asyncpg_config, table_name="test_litestar_backend", session_lifetime=3600)
    # Ensure table exists
    async with asyncpg_config.provide_session() as driver:
        await backend.store._ensure_table_exists(driver)
    return backend


async def test_session_store_basic_operations(session_store: SQLSpecSessionStore) -> None:
    """Test basic session store operations with AsyncPG."""
    session_id = f"test-session-{uuid4()}"
    session_data = {
        "user_id": 42,
        "username": "asyncpg_user",
        "preferences": {"theme": "dark", "language": "en"},
        "roles": ["user", "admin"],
    }

    # Set session data
    await session_store.set(session_id, session_data, expires_in=3600)

    # Get session data
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == session_data

    # Update session data
    updated_data = {**session_data, "last_login": "2024-01-01T12:00:00Z"}
    await session_store.set(session_id, updated_data, expires_in=3600)

    # Verify update
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == updated_data

    # Delete session
    await session_store.delete(session_id)

    # Verify deletion
    result = await session_store.get(session_id, None)
    assert result is None


async def test_session_store_jsonb_support(session_store: SQLSpecSessionStore, asyncpg_config: AsyncpgConfig) -> None:
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
    async with asyncpg_config.provide_session() as driver:
        result = await driver.execute(
            f"SELECT session_data FROM {session_store._table_name} WHERE session_id = $1", session_id
        )
        assert len(result.data) == 1
        stored_json = result.data[0]["session_data"]
        assert isinstance(stored_json, dict)  # Should be parsed as dict, not string


async def test_session_backend_litestar_integration(session_backend: SQLSpecSessionBackend) -> None:
    """Test SQLSpecSessionBackend integration with Litestar application."""

    @get("/set-user")
    async def set_user_session(request: Any) -> dict:
        request.session["user_id"] = 54321
        request.session["username"] = "asyncpg_user"
        request.session["roles"] = ["user", "moderator"]
        request.session["metadata"] = {"login_time": "2024-01-01T12:00:00Z"}
        return {"status": "user session set"}

    @get("/get-user")
    async def get_user_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "roles": request.session.get("roles"),
            "metadata": request.session.get("metadata"),
        }

    @post("/update-preferences")
    async def update_preferences(request: Any) -> dict:
        preferences = request.session.get("preferences", {})
        preferences.update({"theme": "dark", "notifications": True})
        request.session["preferences"] = preferences
        return {"status": "preferences updated"}

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(backend=session_backend, key="asyncpg-test-session", max_age=3600)

    app = Litestar(
        route_handlers=[set_user_session, get_user_session, update_preferences, clear_session],
        middleware=[session_config.middleware],
    )

    async with AsyncTestClient(app=app) as client:
        # Set user session
        response = await client.get("/set-user")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "user session set"}

        # Get user session
        response = await client.get("/get-user")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["user_id"] == 54321
        assert data["username"] == "asyncpg_user"
        assert data["roles"] == ["user", "moderator"]
        assert data["metadata"] == {"login_time": "2024-01-01T12:00:00Z"}

        # Update preferences
        response = await client.post("/update-preferences")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "preferences updated"}

        # Verify preferences were added
        response = await client.get("/get-user")
        data = response.json()
        assert "preferences" in data
        assert data["preferences"] == {"theme": "dark", "notifications": True}

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_200_OK

        # Verify session is cleared
        response = await client.get("/get-user")
        data = response.json()
        assert all(value is None for value in data.values())


async def test_session_persistence_across_requests(session_backend: SQLSpecSessionBackend) -> None:
    """Test session persistence across multiple requests."""

    @get("/counter")
    async def counter_endpoint(request: Any) -> dict:
        count = request.session.get("count", 0)
        visits = request.session.get("visits", [])

        count += 1
        visits.append(f"visit_{count}")

        request.session["count"] = count
        request.session["visits"] = visits
        request.session["last_visit"] = f"2024-01-01T12:{count:02d}:00Z"

        return {"count": count, "visits": visits, "last_visit": request.session["last_visit"]}

    session_config = ServerSideSessionConfig(backend=session_backend, key="persistence-test", max_age=3600)

    app = Litestar(route_handlers=[counter_endpoint], middleware=[session_config.middleware])

    async with AsyncTestClient(app=app) as client:
        # First request
        response = await client.get("/counter")
        data = response.json()
        assert data["count"] == 1
        assert data["visits"] == ["visit_1"]
        assert data["last_visit"] == "2024-01-01T12:01:00Z"

        # Second request
        response = await client.get("/counter")
        data = response.json()
        assert data["count"] == 2
        assert data["visits"] == ["visit_1", "visit_2"]
        assert data["last_visit"] == "2024-01-01T12:02:00Z"

        # Third request
        response = await client.get("/counter")
        data = response.json()
        assert data["count"] == 3
        assert data["visits"] == ["visit_1", "visit_2", "visit_3"]
        assert data["last_visit"] == "2024-01-01T12:03:00Z"


async def test_session_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test session expiration functionality."""
    session_id = f"expiration-test-{uuid4()}"
    session_data = {"user_id": 999, "test": "expiration"}

    # Set session with very short expiration
    await session_store.set(session_id, session_data, expires_in=1)

    # Should exist immediately
    result = await session_store.get(session_id)
    assert result == session_data

    # Wait for expiration
    await asyncio.sleep(2)

    # Should be expired now
    result = await session_store.get(session_id, None)
    assert result is None


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
    base_time = "2024-01-01T12:00:00Z"

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


async def test_transaction_handling(session_store: SQLSpecSessionStore, asyncpg_config: AsyncpgConfig) -> None:
    """Test transaction handling in session operations."""
    session_id = f"transaction-test-{uuid4()}"

    # Test that session operations work within transactions
    async with asyncpg_config.provide_session() as driver:
        async with driver.begin_transaction():
            # Set session data within transaction
            await session_store.set(session_id, {"test": "transaction"}, expires_in=3600)

            # Verify data is accessible within same transaction
            result = await session_store.get(session_id)
            assert result == {"test": "transaction"}

            # Update data within transaction
            await session_store.set(session_id, {"test": "updated"}, expires_in=3600)

        # Verify data persists after transaction commit
        result = await session_store.get(session_id)
        assert result == {"test": "updated"}


async def test_session_backend_error_handling(session_backend: SQLSpecSessionBackend) -> None:
    """Test error handling in session backend operations."""

    @get("/error-test")
    async def error_test_endpoint(request: Any) -> dict:
        # Try to access session normally
        try:
            request.session["valid_key"] = "valid_value"
            return {"status": "success", "value": request.session.get("valid_key")}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    session_config = ServerSideSessionConfig(backend=session_backend, key="error-test-session", max_age=3600)

    app = Litestar(route_handlers=[error_test_endpoint], middleware=[session_config.middleware])

    async with AsyncTestClient(app=app) as client:
        response = await client.get("/error-test")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["status"] == "success"
        assert data["value"] == "valid_value"


async def test_multiple_concurrent_apps(asyncpg_config: AsyncpgConfig) -> None:
    """Test multiple Litestar applications with separate session backends."""

    # Create separate backends for different apps
    backend1 = SQLSpecSessionBackend(config=asyncpg_config, table_name="app1_sessions", session_lifetime=3600)

    backend2 = SQLSpecSessionBackend(config=asyncpg_config, table_name="app2_sessions", session_lifetime=3600)

    # Ensure tables exist
    async with asyncpg_config.provide_session() as driver:
        await backend1.store._ensure_table_exists(driver)
        await backend2.store._ensure_table_exists(driver)

    @get("/app1-data")
    async def app1_endpoint(request: Any) -> dict:
        request.session["app"] = "app1"
        request.session["data"] = "app1_data"
        return {"app": "app1", "data": request.session["data"]}

    @get("/app2-data")
    async def app2_endpoint(request: Any) -> dict:
        request.session["app"] = "app2"
        request.session["data"] = "app2_data"
        return {"app": "app2", "data": request.session["data"]}

    # Create separate apps
    app1 = Litestar(
        route_handlers=[app1_endpoint], middleware=[ServerSideSessionConfig(backend=backend1, key="app1").middleware]
    )

    app2 = Litestar(
        route_handlers=[app2_endpoint], middleware=[ServerSideSessionConfig(backend=backend2, key="app2").middleware]
    )

    # Test both apps concurrently
    async with AsyncTestClient(app=app1) as client1, AsyncTestClient(app=app2) as client2:
        # Make requests to both apps
        response1 = await client1.get("/app1-data")
        response2 = await client2.get("/app2-data")

        # Verify responses
        assert response1.status_code == HTTP_200_OK
        assert response1.json() == {"app": "app1", "data": "app1_data"}

        assert response2.status_code == HTTP_200_OK
        assert response2.json() == {"app": "app2", "data": "app2_data"}

        # Verify session data is isolated between apps
        response1_second = await client1.get("/app1-data")
        response2_second = await client2.get("/app2-data")

        assert response1_second.json()["data"] == "app1_data"
        assert response2_second.json()["data"] == "app2_data"