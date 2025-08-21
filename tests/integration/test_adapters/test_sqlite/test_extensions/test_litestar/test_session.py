"""Integration tests for SQLite session backend."""

import asyncio
import tempfile
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware import DefineMiddleware
from litestar.middleware.session.base import SessionMiddleware
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.testing import AsyncTestClient

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend

pytestmark = [pytest.mark.sqlite, pytest.mark.integration]


@pytest.fixture
def sqlite_config() -> SqliteConfig:
    """Create SQLite configuration for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        return SqliteConfig(pool_config={"database": tmp_file.name})


@pytest.fixture
async def session_backend(sqlite_config: SqliteConfig) -> SQLSpecSessionBackend:
    """Create a session backend instance."""
    return SQLSpecSessionBackend(
        config=sqlite_config,
        table_name="test_sessions",
        session_lifetime=3600,
    )


async def test_sqlite_session_basic_operations(session_backend: SQLSpecSessionBackend) -> None:
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

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_middleware = DefineMiddleware(SessionMiddleware, backend=session_backend)

    app = Litestar(
        route_handlers=[set_session, get_session, clear_session],
        middleware=[session_middleware],
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


async def test_sqlite_session_persistence(session_backend: SQLSpecSessionBackend) -> None:
    """Test that sessions persist across requests."""
    
    @get("/counter")
    async def increment_counter(request: Any) -> dict:
        count = request.session.get("count", 0)
        count += 1
        request.session["count"] = count
        return {"count": count}

    session_middleware = DefineMiddleware(SessionMiddleware, backend=session_backend)

    app = Litestar(
        route_handlers=[increment_counter],
        middleware=[session_middleware],
    )

    async with AsyncTestClient(app=app) as client:
        # Multiple increments should persist
        for expected in range(1, 6):
            response = await client.get("/counter")
            assert response.json() == {"count": expected}


async def test_sqlite_session_expiration(session_backend: SQLSpecSessionBackend) -> None:
    """Test session expiration handling."""
    # Create backend with very short lifetime
    backend = SQLSpecSessionBackend(
        config=session_backend.store._config,
        table_name="test_expiring_sessions",
        session_lifetime=1,  # 1 second
    )
    
    @get("/set-data")
    async def set_data(request: Any) -> dict:
        request.session["test"] = "data"
        return {"status": "set"}

    @get("/get-data")
    async def get_data(request: Any) -> dict:
        return {"test": request.session.get("test")}

    session_middleware = DefineMiddleware(SessionMiddleware, backend=backend)

    app = Litestar(
        route_handlers=[set_data, get_data],
        middleware=[session_middleware],
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


async def test_sqlite_concurrent_sessions(session_backend: SQLSpecSessionBackend) -> None:
    """Test handling of concurrent sessions."""
    
    @get("/user/{user_id:int}")
    async def set_user(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        return {"user_id": user_id}

    @get("/whoami")
    async def get_user(request: Any) -> dict:
        return {"user_id": request.session.get("user_id")}

    session_middleware = DefineMiddleware(SessionMiddleware, backend=session_backend)

    app = Litestar(
        route_handlers=[set_user, get_user],
        middleware=[session_middleware],
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


async def test_sqlite_session_cleanup(sqlite_config: SqliteConfig) -> None:
    """Test expired session cleanup."""
    backend = SQLSpecSessionBackend(
        config=sqlite_config,
        table_name="test_cleanup_sessions",
        session_lifetime=1,
    )

    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(5):
        session_id = f"cleanup-test-{i}"
        session_ids.append(session_id)
        await backend.store.set(session_id, {"data": i}, expires_in=1)

    # Create one long-lived session
    await backend.store.set("persistent", {"data": "keep"}, expires_in=3600)

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await backend.delete_expired_sessions()

    # Check that expired sessions are gone
    for session_id in session_ids:
        result = await backend.store.get(session_id)
        assert result is None

    # Long-lived session should still exist
    result = await backend.store.get("persistent")
    assert result == {"data": "keep"}