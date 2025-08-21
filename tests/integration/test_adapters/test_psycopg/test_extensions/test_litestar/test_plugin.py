"""Litestar integration tests for Psycopg adapter."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK
from litestar.testing import AsyncTestClient

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend, SQLSpecSessionStore


@pytest.fixture
async def sync_session_store(psycopg_sync_config: PsycopgSyncConfig) -> SQLSpecSessionStore:
    """Create a session store instance with sync Psycopg configuration."""
    return SQLSpecSessionStore(
        config=psycopg_sync_config,
        table_name="psycopg_sync_sessions",
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


@pytest.fixture
async def async_session_store(psycopg_async_config: PsycopgAsyncConfig) -> SQLSpecSessionStore:
    """Create a session store instance with async Psycopg configuration."""
    return SQLSpecSessionStore(
        config=psycopg_async_config,
        table_name="psycopg_async_sessions",
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


async def test_sync_store_creation(sync_session_store: SQLSpecSessionStore) -> None:
    """Test that sync session store can be created."""
    assert sync_session_store is not None
    assert sync_session_store._table_name == "psycopg_sync_sessions"
    assert sync_session_store._session_id_column == "session_id"
    assert sync_session_store._data_column == "data"
    assert sync_session_store._expires_at_column == "expires_at"
    assert sync_session_store._created_at_column == "created_at"


async def test_async_store_creation(async_session_store: SQLSpecSessionStore) -> None:
    """Test that async session store can be created."""
    assert async_session_store is not None
    assert async_session_store._table_name == "psycopg_async_sessions"
    assert async_session_store._session_id_column == "session_id"
    assert async_session_store._data_column == "data"
    assert async_session_store._expires_at_column == "expires_at"
    assert async_session_store._created_at_column == "created_at"


async def test_sync_table_creation(
    sync_session_store: SQLSpecSessionStore, psycopg_sync_config: PsycopgSyncConfig
) -> None:
    """Test that session table is created automatically with sync driver."""
    async with psycopg_sync_config.provide_session() as driver:
        await sync_session_store._ensure_table_exists(driver)

        # Verify table exists with proper schema
        result = await driver.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'psycopg_sync_sessions' ORDER BY ordinal_position"
        )

        columns = {row["column_name"]: row["data_type"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Check PostgreSQL-specific types
        assert "jsonb" in columns["data"].lower()
        assert "timestamp" in columns["expires_at"].lower()


async def test_async_table_creation(
    async_session_store: SQLSpecSessionStore, psycopg_async_config: PsycopgAsyncConfig
) -> None:
    """Test that session table is created automatically with async driver."""
    async with psycopg_async_config.provide_session() as driver:
        await async_session_store._ensure_table_exists(driver)

        # Verify table exists with proper schema
        result = await driver.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'psycopg_async_sessions' ORDER BY ordinal_position"
        )

        columns = {row["column_name"]: row["data_type"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Check PostgreSQL-specific types
        assert "jsonb" in columns["data"].lower()
        assert "timestamp" in columns["expires_at"].lower()


async def test_sync_session_set_and_get(sync_session_store: SQLSpecSessionStore) -> None:
    """Test setting and getting session data with sync driver."""
    session_id = "test-sync-session-123"
    session_data = {
        "user_id": 42,
        "username": "testuser",
        "roles": ["user", "admin"],
        "metadata": {"login_time": "2023-01-01T00:00:00Z"},
    }

    # Set session data
    await sync_session_store.set(session_id, session_data, expires_in=3600)

    # Get session data
    retrieved_data = await sync_session_store.get(session_id)
    assert retrieved_data == session_data


async def test_async_session_set_and_get(async_session_store: SQLSpecSessionStore) -> None:
    """Test setting and getting session data with async driver."""
    session_id = "test-async-session-123"
    session_data = {
        "user_id": 42,
        "username": "testuser",
        "roles": ["user", "admin"],
        "metadata": {"login_time": "2023-01-01T00:00:00Z"},
    }

    # Set session data
    await async_session_store.set(session_id, session_data, expires_in=3600)

    # Get session data
    retrieved_data = await async_session_store.get(session_id)
    assert retrieved_data == session_data


async def test_postgresql_jsonb_features(
    async_session_store: SQLSpecSessionStore, psycopg_async_config: PsycopgAsyncConfig
) -> None:
    """Test PostgreSQL-specific JSONB features."""
    session_id = "test-jsonb-session"
    complex_data = {
        "user_profile": {
            "name": "John Doe",
            "age": 30,
            "settings": {"theme": "dark", "notifications": True, "preferences": ["email", "sms"]},
        },
        "permissions": {"admin": False, "modules": ["users", "reports"]},
        "arrays": [1, 2, 3, "test", {"nested": True}],
        "null_value": None,
        "boolean_value": True,
        "numeric_value": 123.45,
    }

    # Set complex JSONB data
    await async_session_store.set(session_id, complex_data, expires_in=3600)

    # Get and verify complex data
    retrieved_data = await async_session_store.get(session_id)
    assert retrieved_data == complex_data

    # Test direct JSONB queries
    async with psycopg_async_config.provide_session() as driver:
        # Query JSONB field directly
        result = await driver.execute(
            "SELECT data->>'user_profile' as profile FROM psycopg_async_sessions WHERE session_id = %s",
            parameters=[session_id],
        )
        assert len(result.data) == 1

        profile_data = json.loads(result.data[0]["profile"])
        assert profile_data["name"] == "John Doe"
        assert profile_data["age"] == 30


async def test_postgresql_array_handling(async_session_store: SQLSpecSessionStore) -> None:
    """Test PostgreSQL array handling in session data."""
    session_id = "test-array-session"
    array_data = {
        "string_array": ["apple", "banana", "cherry"],
        "int_array": [1, 2, 3, 4, 5],
        "mixed_array": [1, "test", True, None, {"obj": "value"}],
        "nested_arrays": [[1, 2], [3, 4], [5, 6]],
        "empty_array": [],
    }

    # Set array data
    await async_session_store.set(session_id, array_data, expires_in=3600)

    # Get and verify array data
    retrieved_data = await async_session_store.get(session_id)
    assert retrieved_data == array_data


async def test_session_expiration_sync(sync_session_store: SQLSpecSessionStore) -> None:
    """Test that expired sessions are not returned with sync driver."""
    session_id = "test-sync-expired"
    session_data = {"user_id": 123, "test": "data"}

    # Set session with very short expiration (1 second)
    await sync_session_store.set(session_id, session_data, expires_in=1)

    # Should exist immediately
    result = await sync_session_store.get(session_id)
    assert result == session_data

    # Wait for expiration
    await asyncio.sleep(2)

    # Should be expired now
    result = await sync_session_store.get(session_id, None)
    assert result is None


async def test_session_expiration_async(async_session_store: SQLSpecSessionStore) -> None:
    """Test that expired sessions are not returned with async driver."""
    session_id = "test-async-expired"
    session_data = {"user_id": 123, "test": "data"}

    # Set session with very short expiration (1 second)
    await async_session_store.set(session_id, session_data, expires_in=1)

    # Should exist immediately
    result = await async_session_store.get(session_id)
    assert result == session_data

    # Wait for expiration
    await asyncio.sleep(2)

    # Should be expired now
    result = await async_session_store.get(session_id, None)
    assert result is None


async def test_sync_session_backend_integration(psycopg_sync_config: PsycopgSyncConfig) -> None:
    """Test session backend integration with Litestar app using sync Psycopg."""
    # Create session backend
    session_backend = SQLSpecSessionBackend(config=psycopg_sync_config, table_name="sync_integration_sessions")

    # Create Litestar app with session middleware
    @get("/set-session")
    async def set_session(request: "Any") -> dict:
        request.session["user_id"] = 12345
        request.session["username"] = "testuser"
        request.session["metadata"] = {"login_ip": "127.0.0.1", "user_agent": "test"}
        return {"status": "session set"}

    @get("/get-session")
    async def get_session(request: "Any") -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "metadata": request.session.get("metadata"),
        }

    @post("/update-session")
    async def update_session(request: "Any") -> dict:
        request.session["last_activity"] = "updated"
        request.session["visit_count"] = request.session.get("visit_count", 0) + 1
        return {"status": "session updated"}

    @post("/clear-session")
    async def clear_session(request: "Any") -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(backend=session_backend, key="test-sync-session", max_age=3600)

    app = Litestar(
        route_handlers=[set_session, get_session, update_session, clear_session], middleware=[session_config.middleware]
    )

    async with AsyncTestClient(app=app) as client:
        # Set session data
        response = await client.get("/set-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session set"}

        # Get session data
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        result = response.json()
        assert result["user_id"] == 12345
        assert result["username"] == "testuser"
        assert result["metadata"]["login_ip"] == "127.0.0.1"

        # Update session
        response = await client.post("/update-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session updated"}

        # Verify updates
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        result = response.json()
        assert result["user_id"] == 12345
        assert result["metadata"]["login_ip"] == "127.0.0.1"

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        result = response.json()
        assert result["user_id"] is None
        assert result["username"] is None
        assert result["metadata"] is None


async def test_async_session_backend_integration(psycopg_async_config: PsycopgAsyncConfig) -> None:
    """Test session backend integration with Litestar app using async Psycopg."""
    # Create session backend
    session_backend = SQLSpecSessionBackend(config=psycopg_async_config, table_name="async_integration_sessions")

    # Create Litestar app with session middleware
    @get("/set-session")
    async def set_session(request: "Any") -> dict:
        request.session["user_id"] = 54321
        request.session["username"] = "asyncuser"
        request.session["complex_data"] = {
            "preferences": {"theme": "light", "lang": "en"},
            "permissions": ["read", "write"],
        }
        return {"status": "session set"}

    @get("/get-session")
    async def get_session(request: "Any") -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "complex_data": request.session.get("complex_data"),
        }

    @post("/clear-session")
    async def clear_session(request: "Any") -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(backend=session_backend, key="test-async-session", max_age=3600)

    app = Litestar(route_handlers=[set_session, get_session, clear_session], middleware=[session_config.middleware])

    async with AsyncTestClient(app=app) as client:
        # Set session data
        response = await client.get("/set-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session set"}

        # Get session data
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        result = response.json()
        assert result["user_id"] == 54321
        assert result["username"] == "asyncuser"
        assert result["complex_data"]["preferences"]["theme"] == "light"
        assert result["complex_data"]["permissions"] == ["read", "write"]

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        result = response.json()
        assert result["user_id"] is None
        assert result["username"] is None
        assert result["complex_data"] is None


async def test_session_persistence_across_requests(psycopg_async_config: PsycopgAsyncConfig) -> None:
    """Test that sessions persist across multiple requests."""
    session_backend = SQLSpecSessionBackend(config=psycopg_async_config, table_name="persistence_test_sessions")

    @get("/increment")
    async def increment_counter(request: "Any") -> dict:
        count = request.session.get("count", 0)
        count += 1
        request.session["count"] = count
        request.session["timestamps"] = request.session.get("timestamps", [])
        request.session["timestamps"].append(datetime.now(timezone.utc).isoformat())
        return {"count": count, "total_requests": len(request.session["timestamps"])}

    @get("/get-data")
    async def get_data(request: "Any") -> dict:
        return {"count": request.session.get("count", 0), "timestamps": request.session.get("timestamps", [])}

    session_config = ServerSideSessionConfig(backend=session_backend, key="persistence-session")

    app = Litestar(route_handlers=[increment_counter, get_data], middleware=[session_config.middleware])

    async with AsyncTestClient(app=app) as client:
        # First request
        response = await client.get("/increment")
        result = response.json()
        assert result["count"] == 1
        assert result["total_requests"] == 1

        # Second request (should persist)
        response = await client.get("/increment")
        result = response.json()
        assert result["count"] == 2
        assert result["total_requests"] == 2

        # Third request (should persist)
        response = await client.get("/increment")
        result = response.json()
        assert result["count"] == 3
        assert result["total_requests"] == 3

        # Get data separately
        response = await client.get("/get-data")
        result = response.json()
        assert result["count"] == 3
        assert len(result["timestamps"]) == 3


async def test_large_data_handling(async_session_store: SQLSpecSessionStore) -> None:
    """Test handling of large session data."""
    session_id = "test-large-data"

    # Create large data structure
    large_data = {
        "large_array": list(range(10000)),  # 10K integers
        "large_text": "x" * 100000,  # 100KB string
        "nested_objects": [
            {"id": i, "data": f"item_{i}", "metadata": {"created": f"2023-{i % 12 + 1:02d}-01"}} for i in range(1000)
        ],
        "complex_structure": {
            f"level_{i}": {
                f"sublevel_{j}": {"value": i * j, "text": f"data_{i}_{j}", "array": list(range(j + 1))}
                for j in range(10)
            }
            for i in range(50)
        },
    }

    # Set large data
    await async_session_store.set(session_id, large_data, expires_in=3600)

    # Get and verify large data
    retrieved_data = await async_session_store.get(session_id)
    assert retrieved_data == large_data
    assert len(retrieved_data["large_array"]) == 10000
    assert len(retrieved_data["large_text"]) == 100000
    assert len(retrieved_data["nested_objects"]) == 1000
    assert len(retrieved_data["complex_structure"]) == 50


async def test_transaction_handling(
    async_session_store: SQLSpecSessionStore, psycopg_async_config: PsycopgAsyncConfig
) -> None:
    """Test transaction handling with session operations."""
    session_id = "test-transaction"
    initial_data = {"counter": 0, "operations": []}

    # Set initial session data
    await async_session_store.set(session_id, initial_data, expires_in=3600)

    # Test transaction rollback scenario
    async with psycopg_async_config.provide_session() as driver:
        try:
            # Start a transaction
            await driver.execute("BEGIN")

            # Update session data within transaction
            updated_data = {"counter": 1, "operations": ["op1"]}
            await async_session_store._set_session_data(
                driver, session_id, json.dumps(updated_data), datetime.now(timezone.utc) + timedelta(hours=1)
            )

            # Simulate an error that causes rollback
            await driver.execute("ROLLBACK")

        except Exception:
            await driver.execute("ROLLBACK")

    # Data should remain unchanged due to rollback
    retrieved_data = await async_session_store.get(session_id)
    assert retrieved_data == initial_data

    # Test successful transaction
    async with psycopg_async_config.provide_session() as driver:
        await driver.execute("BEGIN")

        try:
            # Update session data within transaction
            updated_data = {"counter": 2, "operations": ["op1", "op2"]}
            await async_session_store._set_session_data(
                driver, session_id, json.dumps(updated_data), datetime.now(timezone.utc) + timedelta(hours=1)
            )

            # Commit the transaction
            await driver.execute("COMMIT")

        except Exception:
            await driver.execute("ROLLBACK")
            raise

    # Data should be updated after commit
    retrieved_data = await async_session_store.get(session_id)
    assert retrieved_data == updated_data


async def test_concurrent_session_access(async_session_store: SQLSpecSessionStore) -> None:
    """Test concurrent access to sessions."""
    session_id = "concurrent-test"

    async def update_session(value: int) -> None:
        """Update session with a value."""
        data = {"value": value, "timestamp": datetime.now(timezone.utc).isoformat()}
        await async_session_store.set(session_id, data)

    # Create multiple concurrent updates
    tasks = [update_session(i) for i in range(20)]
    await asyncio.gather(*tasks)

    # One of the updates should have won
    result = await async_session_store.get(session_id)
    assert result is not None
    assert "value" in result
    assert 0 <= result["value"] <= 19
    assert "timestamp" in result


async def test_session_renewal(async_session_store: SQLSpecSessionStore) -> None:
    """Test session renewal functionality."""
    session_id = "test-renewal"
    session_data = {"user_id": 999, "activity": "browsing"}

    # Set session with short expiration
    await async_session_store.set(session_id, session_data, expires_in=2)

    # Get with renewal
    retrieved_data = await async_session_store.get(session_id, renew_for=timedelta(hours=1))
    assert retrieved_data == session_data

    # Wait past original expiration
    await asyncio.sleep(3)

    # Should still exist due to renewal
    result = await async_session_store.get(session_id)
    assert result == session_data


async def test_custom_types_storage(async_session_store: SQLSpecSessionStore) -> None:
    """Test storage of custom types in PostgreSQL."""
    session_id = "test-custom-types"

    # Test UUID storage
    user_uuid = str(uuid4())

    custom_data = {
        "user_uuid": user_uuid,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "decimal_value": "123.456789",  # High precision decimal as string
        "ip_address": "192.168.1.100",
        "json_object": {"nested": {"deep": {"value": True}}},
        "binary_data": "base64encodeddata==",
        "enum_value": "ACTIVE",
    }

    # Set custom data
    await async_session_store.set(session_id, custom_data, expires_in=3600)

    # Get and verify custom data
    retrieved_data = await async_session_store.get(session_id)
    assert retrieved_data == custom_data
    assert retrieved_data["user_uuid"] == user_uuid
    assert retrieved_data["decimal_value"] == "123.456789"


async def test_session_cleanup_expired(async_session_store: SQLSpecSessionStore) -> None:
    """Test cleanup of expired sessions."""
    # Create sessions with different expiration times
    await async_session_store.set("session1", {"data": 1}, expires_in=1)  # Will expire
    await async_session_store.set("session2", {"data": 2}, expires_in=3600)  # Won't expire
    await async_session_store.set("session3", {"data": 3}, expires_in=1)  # Will expire

    # Wait for some to expire
    await asyncio.sleep(2)

    # Delete expired sessions
    await async_session_store.delete_expired()

    # Check which sessions remain
    assert await async_session_store.get("session1", None) is None
    assert await async_session_store.get("session2") == {"data": 2}
    assert await async_session_store.get("session3", None) is None


async def test_session_exists_check(async_session_store: SQLSpecSessionStore) -> None:
    """Test session existence checks."""
    session_id = "test-exists"
    session_data = {"test": "data"}

    # Should not exist initially
    assert not await async_session_store.exists(session_id)

    # Create session
    await async_session_store.set(session_id, session_data, expires_in=3600)

    # Should exist now
    assert await async_session_store.exists(session_id)

    # Delete session
    await async_session_store.delete(session_id)

    # Should not exist after deletion
    assert not await async_session_store.exists(session_id)


async def test_session_expires_in(async_session_store: SQLSpecSessionStore) -> None:
    """Test getting session expiration time."""
    session_id = "test-expires-in"
    session_data = {"test": "data"}

    # Create session with 10 second expiration
    await async_session_store.set(session_id, session_data, expires_in=10)

    # Should have approximately 10 seconds left
    expires_in = await async_session_store.expires_in(session_id)
    assert 8 <= expires_in <= 10

    # Wait a bit
    await asyncio.sleep(2)

    # Should have less time left
    expires_in = await async_session_store.expires_in(session_id)
    assert 6 <= expires_in <= 8
