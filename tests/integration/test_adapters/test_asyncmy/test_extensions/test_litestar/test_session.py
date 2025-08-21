"""Integration tests for AsyncMy (MySQL) session backend."""

import asyncio
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK
from litestar.testing import AsyncTestClient

from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend

pytestmark = [pytest.mark.asyncmy, pytest.mark.mysql, pytest.mark.integration]


@pytest.fixture
async def asyncmy_config() -> AsyncmyConfig:
    """Create AsyncMy configuration for testing."""
    return AsyncmyConfig(
        pool_config={
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "password",
            "database": "test",
            "minsize": 2,
            "maxsize": 10,
        }
    )


@pytest.fixture
async def session_backend(asyncmy_config: AsyncmyConfig) -> SQLSpecSessionBackend:
    """Create a session backend instance."""
    backend = SQLSpecSessionBackend(
        config=asyncmy_config,
        table_name="test_sessions_mysql",
        session_lifetime=3600,
    )
    # Ensure table exists
    async with asyncmy_config.provide_session() as driver:
        await backend.store._ensure_table_exists(driver)
    return backend


async def test_mysql_session_basic_operations(session_backend: SQLSpecSessionBackend) -> None:
    """Test basic session operations with MySQL backend."""
    
    @get("/set-session")
    async def set_session(request: Any) -> dict:
        request.session["user_id"] = 33333
        request.session["username"] = "mysqluser"
        request.session["preferences"] = {"theme": "auto", "timezone": "UTC"}
        request.session["roles"] = ["user", "editor"]
        return {"status": "session set"}

    @get("/get-session")
    async def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "preferences": request.session.get("preferences"),
            "roles": request.session.get("roles"),
        }

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(
        backend=session_backend,
        key="mysql-session",
        max_age=3600,
    )

    app = Litestar(
        route_handlers=[set_session, get_session, clear_session],
        middleware=[session_config.middleware],
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
        assert data["user_id"] == 33333
        assert data["username"] == "mysqluser"
        assert data["preferences"] == {"theme": "auto", "timezone": "UTC"}
        assert data["roles"] == ["user", "editor"]

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"user_id": None, "username": None, "preferences": None, "roles": None}


async def test_mysql_session_persistence(session_backend: SQLSpecSessionBackend) -> None:
    """Test that sessions persist across requests with MySQL."""
    
    @get("/cart/add/{item_id:int}")
    async def add_to_cart(request: Any, item_id: int) -> dict:
        cart = request.session.get("cart", [])
        cart.append({"item_id": item_id, "quantity": 1})
        request.session["cart"] = cart
        request.session["cart_count"] = len(cart)
        return {"cart": cart, "count": len(cart)}

    @get("/cart")
    async def get_cart(request: Any) -> dict:
        return {
            "cart": request.session.get("cart", []),
            "count": request.session.get("cart_count", 0),
        }

    session_config = ServerSideSessionConfig(
        backend=session_backend,
        key="mysql-cart",
    )

    app = Litestar(
        route_handlers=[add_to_cart, get_cart],
        middleware=[session_config.middleware],
    )

    async with AsyncTestClient(app=app) as client:
        # Add items to cart
        response = await client.get("/cart/add/101")
        assert response.json()["count"] == 1
        
        response = await client.get("/cart/add/102")
        assert response.json()["count"] == 2
        
        response = await client.get("/cart/add/103")
        assert response.json()["count"] == 3
        
        # Verify cart contents
        response = await client.get("/cart")
        data = response.json()
        assert data["count"] == 3
        assert len(data["cart"]) == 3
        assert data["cart"][0]["item_id"] == 101


async def test_mysql_session_expiration(session_backend: SQLSpecSessionBackend) -> None:
    """Test session expiration handling with MySQL."""
    # Create backend with very short lifetime
    backend = SQLSpecSessionBackend(
        config=session_backend.store._config,
        table_name="test_expiring_sessions_mysql",
        session_lifetime=1,  # 1 second
    )
    
    @get("/set-data")
    async def set_data(request: Any) -> dict:
        request.session["test"] = "mysql_data"
        request.session["timestamp"] = "2024-01-01T00:00:00"
        return {"status": "set"}

    @get("/get-data")
    async def get_data(request: Any) -> dict:
        return {
            "test": request.session.get("test"),
            "timestamp": request.session.get("timestamp"),
        }

    session_config = ServerSideSessionConfig(
        backend=backend,
        key="mysql-expiring",
        max_age=1,
    )

    app = Litestar(
        route_handlers=[set_data, get_data],
        middleware=[session_config.middleware],
    )

    async with AsyncTestClient(app=app) as client:
        # Set data
        response = await client.get("/set-data")
        assert response.json() == {"status": "set"}

        # Data should be available immediately
        response = await client.get("/get-data")
        assert response.json() == {"test": "mysql_data", "timestamp": "2024-01-01T00:00:00"}

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        response = await client.get("/get-data")
        assert response.json() == {"test": None, "timestamp": None}


async def test_mysql_concurrent_sessions(session_backend: SQLSpecSessionBackend) -> None:
    """Test handling of concurrent sessions with MySQL."""
    
    @get("/profile/{profile_id:int}")
    async def set_profile(request: Any, profile_id: int) -> dict:
        request.session["profile_id"] = profile_id
        request.session["db"] = "mysql"
        request.session["version"] = "8.0"
        return {"profile_id": profile_id}

    @get("/current-profile")
    async def get_profile(request: Any) -> dict:
        return {
            "profile_id": request.session.get("profile_id"),
            "db": request.session.get("db"),
            "version": request.session.get("version"),
        }

    session_config = ServerSideSessionConfig(
        backend=session_backend,
        key="mysql-concurrent",
    )

    app = Litestar(
        route_handlers=[set_profile, get_profile],
        middleware=[session_config.middleware],
    )

    async with AsyncTestClient(app=app) as client1, AsyncTestClient(app=app) as client2:
        # Set different profiles in different clients
        response1 = await client1.get("/profile/501")
        assert response1.json() == {"profile_id": 501}

        response2 = await client2.get("/profile/502")
        assert response2.json() == {"profile_id": 502}

        # Each client should maintain its own session
        response1 = await client1.get("/current-profile")
        assert response1.json() == {"profile_id": 501, "db": "mysql", "version": "8.0"}

        response2 = await client2.get("/current-profile")
        assert response2.json() == {"profile_id": 502, "db": "mysql", "version": "8.0"}


async def test_mysql_session_cleanup(asyncmy_config: AsyncmyConfig) -> None:
    """Test expired session cleanup with MySQL."""
    backend = SQLSpecSessionBackend(
        config=asyncmy_config,
        table_name="test_cleanup_sessions_mysql",
        session_lifetime=1,
    )

    # Ensure table exists
    async with asyncmy_config.provide_session() as driver:
        await backend.store._ensure_table_exists(driver)

    # Create multiple sessions with short expiration
    temp_sessions = []
    for i in range(7):
        session_id = f"mysql-temp-{i}"
        temp_sessions.append(session_id)
        await backend.store.set(session_id, {"data": i, "type": "temporary"}, expires_in=1)

    # Create permanent sessions
    perm_sessions = []
    for i in range(3):
        session_id = f"mysql-perm-{i}"
        perm_sessions.append(session_id)
        await backend.store.set(session_id, {"data": f"permanent-{i}"}, expires_in=3600)

    # Wait for temporary sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await backend.delete_expired_sessions()

    # Check that expired sessions are gone
    for session_id in temp_sessions:
        result = await backend.store.get(session_id)
        assert result is None

    # Permanent sessions should still exist
    for session_id in perm_sessions:
        result = await backend.store.get(session_id)
        assert result is not None


async def test_mysql_session_utf8_data(session_backend: SQLSpecSessionBackend) -> None:
    """Test storing UTF-8 and emoji data in MySQL sessions."""
    
    @post("/save-international")
    async def save_international(request: Any) -> dict:
        # Store various international characters and emojis
        request.session["messages"] = {
            "english": "Hello World",
            "chinese": "你好世界",
            "japanese": "こんにちは世界",
            "korean": "안녕하세요 세계",
            "arabic": "مرحبا بالعالم",
            "hebrew": "שלום עולם",
            "russian": "Привет мир",
            "emoji": "🌍🌎🌏 MySQL 🐬",
        }
        request.session["special_chars"] = "MySQL: 'quotes' \"double\" `backticks`"
        return {"status": "international data saved"}

    @get("/load-international")
    async def load_international(request: Any) -> dict:
        return {
            "messages": request.session.get("messages"),
            "special_chars": request.session.get("special_chars"),
        }

    session_config = ServerSideSessionConfig(
        backend=session_backend,
        key="mysql-utf8",
    )

    app = Litestar(
        route_handlers=[save_international, load_international],
        middleware=[session_config.middleware],
    )

    async with AsyncTestClient(app=app) as client:
        # Save international data
        response = await client.post("/save-international")
        assert response.json() == {"status": "international data saved"}

        # Load and verify international data
        response = await client.get("/load-international")
        data = response.json()
        
        assert data["messages"]["chinese"] == "你好世界"
        assert data["messages"]["japanese"] == "こんにちは世界"
        assert data["messages"]["emoji"] == "🌍🌎🌏 MySQL 🐬"
        assert data["special_chars"] == "MySQL: 'quotes' \"double\" `backticks`"