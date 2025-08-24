"""Integration tests for AsyncMy (MySQL) session backend with store integration."""

import asyncio
import tempfile
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK
from litestar.testing import AsyncTestClient

from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.extensions.litestar.session import SQLSpecSessionBackend, SQLSpecSessionConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = [pytest.mark.asyncmy, pytest.mark.mysql, pytest.mark.integration, pytest.mark.xdist_group("mysql")]


@pytest.fixture
async def asyncmy_config(mysql_service) -> AsyncmyConfig:
    """Create AsyncMy configuration with migration support."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = AsyncmyConfig(
            pool_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "minsize": 2,
                "maxsize": 10,
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],
            },
        )
        yield config
        await config.close_pool()


@pytest.fixture
async def session_store(asyncmy_config: AsyncmyConfig) -> SQLSpecSessionStore:
    """Create a session store with migrations applied."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(asyncmy_config)
    await commands.init(asyncmy_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    return SQLSpecSessionStore(asyncmy_config, table_name="litestar_sessions")


@pytest.fixture
def session_backend_config() -> SQLSpecSessionConfig:
    """Create session backend configuration."""
    return SQLSpecSessionConfig(key="asyncmy-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def session_backend(session_backend_config: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend instance."""
    return SQLSpecSessionBackend(config=session_backend_config)


async def test_mysql_migration_creates_correct_table(asyncmy_config: AsyncmyConfig) -> None:
    """Test that Litestar migration creates the correct table structure for MySQL."""
    # Apply migrations
    commands = AsyncMigrationCommands(asyncmy_config)
    await commands.init(asyncmy_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Verify table was created with correct MySQL-specific types
    async with asyncmy_config.provide_session() as driver:
        result = await driver.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = 'litestar_sessions'
            AND COLUMN_NAME IN ('data', 'expires_at')
        """)

        columns = {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in result.data}

        # MySQL should use JSON for data column (not JSONB or TEXT)
        assert columns.get("data") == "json"
        # MySQL uses DATETIME for timestamp columns
        assert columns.get("expires_at", "").lower() in {"datetime", "timestamp"}

        # Verify all expected columns exist
        result = await driver.execute("""
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = 'litestar_sessions'
        """)
        columns = {row["COLUMN_NAME"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns


async def test_mysql_session_basic_operations(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
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

    session_config = ServerSideSessionConfig(store=session_store, key="mysql-session", max_age=3600)

    app = Litestar(
        route_handlers=[set_session, get_session, clear_session],
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


async def test_mysql_session_persistence(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
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
        return {"cart": request.session.get("cart", []), "count": request.session.get("cart_count", 0)}

    session_config = ServerSideSessionConfig(store=session_store, key="mysql-cart", max_age=3600)

    app = Litestar(
        route_handlers=[add_to_cart, get_cart],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
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


async def test_mysql_session_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test session expiration handling with MySQL."""
    # No need to create a custom backend - just use the store with short expiration

    @get("/set-data")
    async def set_data(request: Any) -> dict:
        request.session["test"] = "mysql_data"
        request.session["timestamp"] = "2024-01-01T00:00:00"
        return {"status": "set"}

    @get("/get-data")
    async def get_data(request: Any) -> dict:
        return {"test": request.session.get("test"), "timestamp": request.session.get("timestamp")}

    session_config = ServerSideSessionConfig(
        store="sessions",  # Use the string name for the store
        key="mysql-expiring",
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
        assert response.json() == {"test": "mysql_data", "timestamp": "2024-01-01T00:00:00"}

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        response = await client.get("/get-data")
        assert response.json() == {"test": None, "timestamp": None}


async def test_mysql_concurrent_sessions(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
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

    session_config = ServerSideSessionConfig(store=session_store, key="mysql-concurrent", max_age=3600)

    app = Litestar(
        route_handlers=[set_profile, get_profile],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
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


async def test_mysql_session_cleanup(session_store: SQLSpecSessionStore) -> None:
    """Test expired session cleanup with MySQL."""
    # Create multiple sessions with short expiration
    temp_sessions = []
    for i in range(7):
        session_id = f"mysql-temp-{i}"
        temp_sessions.append(session_id)
        await session_store.set(session_id, {"data": i, "type": "temporary"}, expires_in=1)

    # Create permanent sessions
    perm_sessions = []
    for i in range(3):
        session_id = f"mysql-perm-{i}"
        perm_sessions.append(session_id)
        await session_store.set(session_id, {"data": f"permanent-{i}"}, expires_in=3600)

    # Wait for temporary sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await session_store.delete_expired()

    # Check that expired sessions are gone
    for session_id in temp_sessions:
        result = await session_store.get(session_id)
        assert result is None

    # Permanent sessions should still exist
    for session_id in perm_sessions:
        result = await session_store.get(session_id)
        assert result is not None


async def test_mysql_session_utf8_data(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
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
        return {"messages": request.session.get("messages"), "special_chars": request.session.get("special_chars")}

    session_config = ServerSideSessionConfig(store=session_store, key="mysql-utf8", max_age=3600)

    app = Litestar(
        route_handlers=[save_international, load_international],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
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


async def test_mysql_store_operations(session_store: SQLSpecSessionStore) -> None:
    """Test MySQL store operations directly."""
    # Test basic store operations
    session_id = "test-session-mysql"
    test_data = {
        "user_id": 999,
        "preferences": {"theme": "auto", "timezone": "America/New_York"},
        "tags": ["premium", "verified"],
        "metadata": {"last_login": "2024-01-01", "login_count": 42},
    }

    # Set data
    await session_store.set(session_id, test_data, expires_in=3600)

    # Get data
    result = await session_store.get(session_id)
    assert result == test_data

    # Check exists
    assert await session_store.exists(session_id) is True

    # Update with new data
    updated_data = {**test_data, "last_activity": "2024-01-02"}
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
