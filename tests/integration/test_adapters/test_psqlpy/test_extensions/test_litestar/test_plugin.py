"""Integration tests for SQLSpec Litestar session backend with PsqlPy adapter."""

import asyncio
import math
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK
from litestar.testing import AsyncTestClient

from sqlspec.extensions.litestar import SQLSpecSessionBackend, SQLSpecSessionStore


@pytest.fixture
async def session_store(psqlpy_config) -> SQLSpecSessionStore:
    """Create a session store instance for PsqlPy."""
    store = SQLSpecSessionStore(
        config=psqlpy_config,
        table_name="test_sessions",
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )
    yield store
    # Cleanup
    try:
        await psqlpy_config.close_pool()
    except Exception:
        pass


async def test_store_creation(session_store: SQLSpecSessionStore) -> None:
    """Test session store can be created with PsqlPy."""
    assert session_store is not None
    assert session_store._table_name == "test_sessions"
    assert session_store._session_id_column == "session_id"
    assert session_store._data_column == "data"
    assert session_store._expires_at_column == "expires_at"
    assert session_store._created_at_column == "created_at"


async def test_table_creation(session_store: SQLSpecSessionStore, psqlpy_config) -> None:
    """Test that session table is created automatically with PostgreSQL features."""
    async with psqlpy_config.provide_session() as driver:
        await session_store._ensure_table_exists(driver)

        # Verify table exists and has JSONB column type
        result = await driver.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'test_sessions'
            ORDER BY ordinal_position
        """)

        columns = {row["column_name"]: row for row in result.data}

        # Verify JSONB data column
        assert "data" in columns
        assert columns["data"]["data_type"] == "jsonb"
        assert columns["data"]["is_nullable"] == "YES"

        # Verify other columns
        assert "session_id" in columns
        assert columns["session_id"]["data_type"] == "character varying"
        assert "expires_at" in columns
        assert columns["expires_at"]["data_type"] == "timestamp with time zone"
        assert "created_at" in columns
        assert columns["created_at"]["data_type"] == "timestamp with time zone"


async def test_session_set_and_get_with_jsonb(session_store: SQLSpecSessionStore) -> None:
    """Test setting and getting complex session data using PostgreSQL JSONB."""
    session_id = "test-session-jsonb-123"
    # Complex nested data to test JSONB capabilities
    session_data = {
        "user_id": 42,
        "username": "testuser",
        "roles": ["user", "admin"],
        "preferences": {
            "theme": "dark",
            "language": "en",
            "notifications": {"email": True, "push": False, "sms": True},
        },
        "recent_activity": [
            {"action": "login", "timestamp": 1640995200},
            {"action": "view_profile", "timestamp": 1640995260},
            {"action": "update_settings", "timestamp": 1640995320},
        ],
        "metadata": None,  # Test null handling
    }

    # Set session data
    await session_store.set(session_id, session_data, expires_in=3600)

    # Get session data
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == session_data


async def test_large_session_data_handling(session_store: SQLSpecSessionStore) -> None:
    """Test handling of large session data with PsqlPy's performance benefits."""
    session_id = "test-session-large-data"

    # Create large session data (simulate complex application state)
    large_data = {
        "user_data": {
            "profile": {f"field_{i}": f"value_{i}" for i in range(1000)},
            "settings": {f"setting_{i}": i % 2 == 0 for i in range(500)},
            "history": [{"item": f"item_{i}", "value": i} for i in range(1000)],
        },
        "cache": {f"cache_key_{i}": f"cached_value_{i}" * 10 for i in range(100)},
        "temporary_state": list(range(2000)),
    }

    # Set large session data
    await session_store.set(session_id, large_data, expires_in=3600)

    # Get session data back
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == large_data


async def test_session_get_default(session_store: SQLSpecSessionStore) -> None:
    """Test getting non-existent session returns default."""
    result = await session_store.get("nonexistent-session", {"default": True})
    assert result == {"default": True}


async def test_session_delete(session_store: SQLSpecSessionStore) -> None:
    """Test deleting session data."""
    session_id = "test-session-delete"
    session_data = {"user_id": 99, "data": "to_be_deleted"}

    # Set session data
    await session_store.set(session_id, session_data)

    # Verify it exists
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == session_data

    # Delete session
    await session_store.delete(session_id)

    # Verify it's gone
    result = await session_store.get(session_id, None)
    assert result is None


async def test_session_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test that expired sessions are not returned."""
    session_id = "test-session-expired"
    session_data = {"user_id": 123, "timestamp": "expired_test"}

    # Set session with very short expiration (1 second)
    await session_store.set(session_id, session_data, expires_in=1)

    # Should exist immediately
    result = await session_store.get(session_id)
    assert result == session_data

    # Wait for expiration
    await asyncio.sleep(2)

    # Should be expired now
    result = await session_store.get(session_id, None)
    assert result is None


async def test_delete_expired_sessions(session_store: SQLSpecSessionStore) -> None:
    """Test deleting expired sessions with PostgreSQL efficiency."""
    # Create sessions with different expiration times
    await session_store.set("session1", {"data": 1}, expires_in=1)  # Will expire
    await session_store.set("session2", {"data": 2}, expires_in=3600)  # Won't expire
    await session_store.set("session3", {"data": 3}, expires_in=1)  # Will expire

    # Wait for some to expire
    await asyncio.sleep(2)

    # Delete expired sessions
    await session_store.delete_expired()

    # Check which sessions remain
    assert await session_store.get("session1", None) is None
    assert await session_store.get("session2") == {"data": 2}
    assert await session_store.get("session3", None) is None


async def test_session_backend_integration(psqlpy_config) -> None:
    """Test session backend integration with Litestar app using PsqlPy."""
    # Create session backend
    session_backend = SQLSpecSessionBackend(config=psqlpy_config, table_name="integration_sessions")

    # Create Litestar app with session middleware
    @get("/set-session")
    async def set_session(request: "Any") -> dict:
        request.session["user_id"] = 12345
        request.session["username"] = "psqlpy_testuser"
        request.session["connection_info"] = {
            "adapter": "psqlpy",
            "features": ["binary_protocol", "async_native", "high_performance"],
        }
        return {"status": "session set"}

    @get("/get-session")
    async def get_session(request: "Any") -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "connection_info": request.session.get("connection_info"),
        }

    @post("/clear-session")
    async def clear_session(request: "Any") -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(backend=session_backend, key="psqlpy-test-session", max_age=3600)

    app = Litestar(route_handlers=[set_session, get_session, clear_session], middleware=[session_config.middleware])

    try:
        async with AsyncTestClient(app=app) as client:
            # Set session data
            response = await client.get("/set-session")
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"status": "session set"}

            # Get session data
            response = await client.get("/get-session")
            assert response.status_code == HTTP_200_OK
            expected_data = {
                "user_id": 12345,
                "username": "psqlpy_testuser",
                "connection_info": {
                    "adapter": "psqlpy",
                    "features": ["binary_protocol", "async_native", "high_performance"],
                },
            }
            assert response.json() == expected_data

            # Clear session
            response = await client.post("/clear-session")
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"status": "session cleared"}

            # Verify session is cleared
            response = await client.get("/get-session")
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"user_id": None, "username": None, "connection_info": None}
    finally:
        await psqlpy_config.close_pool()


async def test_session_persistence_across_requests(psqlpy_config) -> None:
    """Test that sessions persist across multiple requests with PsqlPy performance."""
    session_backend = SQLSpecSessionBackend(config=psqlpy_config)

    @get("/increment")
    async def increment_counter(request: "Any") -> dict:
        count = request.session.get("count", 0)
        operations = request.session.get("operations", [])
        count += 1
        operations.append(f"increment_{count}")
        request.session["count"] = count
        request.session["operations"] = operations
        return {"count": count, "operations": operations}

    @get("/reset")
    async def reset_counter(request: "Any") -> dict:
        request.session["count"] = 0
        request.session["operations"] = ["reset"]
        return {"count": 0, "operations": ["reset"]}

    session_config = ServerSideSessionConfig(backend=session_backend, key="psqlpy-counter-session")

    app = Litestar(route_handlers=[increment_counter, reset_counter], middleware=[session_config.middleware])

    try:
        async with AsyncTestClient(app=app) as client:
            # First request
            response = await client.get("/increment")
            assert response.json() == {"count": 1, "operations": ["increment_1"]}

            # Second request (should persist)
            response = await client.get("/increment")
            assert response.json() == {"count": 2, "operations": ["increment_1", "increment_2"]}

            # Reset counter
            response = await client.get("/reset")
            assert response.json() == {"count": 0, "operations": ["reset"]}

            # Increment after reset
            response = await client.get("/increment")
            assert response.json() == {"count": 1, "operations": ["reset", "increment_1"]}
    finally:
        await psqlpy_config.close_pool()


async def test_concurrent_session_access_psqlpy(session_store: SQLSpecSessionStore) -> None:
    """Test concurrent access to sessions leveraging PsqlPy's async performance."""

    async def update_session_with_data(session_id: str, user_id: int, data: dict) -> None:
        """Update session with complex data structure."""
        session_data = {
            "user_id": user_id,
            "last_update": user_id,
            "data": data,
            "metadata": {"update_count": user_id, "concurrent_test": True},
        }
        await session_store.set(session_id, session_data)

    # Create multiple concurrent updates with different data
    session_id = "concurrent-psqlpy-test"
    complex_data = {"nested": {"values": list(range(100))}}

    tasks = [
        update_session_with_data(session_id, i, {**complex_data, "task_id": i})
        for i in range(20)  # More concurrent operations to test PsqlPy performance
    ]
    await asyncio.gather(*tasks)

    # Verify final state
    result = await session_store.get(session_id)
    assert result is not None
    assert "user_id" in result
    assert "data" in result
    assert "metadata" in result
    assert 0 <= result["user_id"] <= 19  # One of the values should be stored
    assert result["metadata"]["concurrent_test"] is True


async def test_binary_protocol_data_types(session_store: SQLSpecSessionStore) -> None:
    """Test various data types that benefit from PostgreSQL's binary protocol in PsqlPy."""
    session_id = "test-binary-protocol"

    # Test data with various types that benefit from binary protocol
    session_data = {
        "integers": [1, 2, 3, 1000000, -999999],
        "floats": [1.5, 2.7, math.pi, -0.001],
        "booleans": [True, False, True],
        "text_data": "Unicode text: 你好世界 🌍",
        "binary_like": "binary data simulation",
        "timestamps": ["2023-01-01T00:00:00Z", "2023-12-31T23:59:59Z"],
        "null_values": [None, None, None],
        "mixed_array": [1, "text", True, None, math.pi],
        "nested_structure": {"level1": {"level2": {"integers": [100, 200, 300], "text": "deeply nested"}}},
    }

    # Set and retrieve data
    await session_store.set(session_id, session_data, expires_in=3600)
    retrieved_data = await session_store.get(session_id)

    # Verify all data types are preserved correctly
    assert retrieved_data == session_data


async def test_high_throughput_operations(session_store: SQLSpecSessionStore) -> None:
    """Test high-throughput session operations that showcase PsqlPy's performance."""
    session_prefix = "throughput-test"
    num_sessions = 50

    # Create many sessions concurrently
    async def create_session(index: int) -> None:
        session_id = f"{session_prefix}-{index}"
        session_data = {
            "session_index": index,
            "data": {f"key_{i}": f"value_{i}" for i in range(10)},
            "performance_test": True,
        }
        await session_store.set(session_id, session_data, expires_in=3600)

    # Create sessions concurrently
    create_tasks = [create_session(i) for i in range(num_sessions)]
    await asyncio.gather(*create_tasks)

    # Read sessions concurrently
    async def read_session(index: int) -> dict:
        session_id = f"{session_prefix}-{index}"
        return await session_store.get(session_id)

    read_tasks = [read_session(i) for i in range(num_sessions)]
    results = await asyncio.gather(*read_tasks)

    # Verify all sessions were created and read correctly
    assert len(results) == num_sessions
    for i, result in enumerate(results):
        assert result is not None
        assert result["session_index"] == i
        assert result["performance_test"] is True

    # Clean up sessions concurrently
    async def delete_session(index: int) -> None:
        session_id = f"{session_prefix}-{index}"
        await session_store.delete(session_id)

    delete_tasks = [delete_session(i) for i in range(num_sessions)]
    await asyncio.gather(*delete_tasks)

    # Verify sessions are deleted
    verify_tasks = [read_session(i) for i in range(num_sessions)]
    verify_results = await asyncio.gather(*verify_tasks)
    for result in verify_results:
        assert result is None


async def test_postgresql_specific_features(session_store: SQLSpecSessionStore, psqlpy_config) -> None:
    """Test PostgreSQL-specific features available through PsqlPy."""
    session_id = "postgres-features-test"

    # Set initial session data
    session_data = {"user_id": 1001, "features": ["jsonb", "arrays", "uuid"], "config": {"theme": "dark", "lang": "en"}}
    await session_store.set(session_id, session_data, expires_in=3600)

    # Test direct JSONB operations via the driver
    async with psqlpy_config.provide_session() as driver:
        # Test JSONB path operations
        result = await driver.execute(
            """
            SELECT data->'config'->>'theme' as theme,
                   jsonb_array_length(data->'features') as feature_count
            FROM test_sessions
            WHERE session_id = %s
        """,
            [session_id],
        )

        assert len(result.data) == 1
        row = result.data[0]
        assert row["theme"] == "dark"
        assert row["feature_count"] == 3

        # Test JSONB update operations
        await driver.execute(
            """
            UPDATE test_sessions
            SET data = jsonb_set(data, '{config,theme}', '"light"')
            WHERE session_id = %s
        """,
            [session_id],
        )

    # Verify the update through the session store
    updated_data = await session_store.get(session_id)
    assert updated_data["config"]["theme"] == "light"
    # Other data should remain unchanged
    assert updated_data["user_id"] == 1001
    assert updated_data["features"] == ["jsonb", "arrays", "uuid"]
