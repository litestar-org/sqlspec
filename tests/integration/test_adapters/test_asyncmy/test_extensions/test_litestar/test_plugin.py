"""Comprehensive Litestar integration tests for AsyncMy (MySQL) adapter.

This test suite validates the full integration between SQLSpec's AsyncMy adapter
and Litestar's session middleware, including MySQL-specific features.
"""

import asyncio
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK
from litestar.testing import AsyncTestClient

from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend, SQLSpecSessionStore

pytestmark = [pytest.mark.asyncmy, pytest.mark.mysql, pytest.mark.integration]


@pytest.fixture
async def session_store(asyncmy_config: AsyncmyConfig) -> SQLSpecSessionStore:
    """Create a session store instance using the proper asyncmy_config fixture."""
    store = SQLSpecSessionStore(
        config=asyncmy_config,
        table_name="litestar_test_sessions",
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )
    # Ensure table exists
    async with asyncmy_config.provide_session() as driver:
        await store._ensure_table_exists(driver)
    return store


@pytest.fixture
async def session_backend(asyncmy_config: AsyncmyConfig) -> SQLSpecSessionBackend:
    """Create a session backend instance using the proper asyncmy_config fixture."""
    backend = SQLSpecSessionBackend(
        config=asyncmy_config, table_name="litestar_test_sessions_backend", session_lifetime=3600
    )
    # Ensure table exists
    async with asyncmy_config.provide_session() as driver:
        await backend.store._ensure_table_exists(driver)
    return backend


async def test_session_store_creation(session_store: SQLSpecSessionStore) -> None:
    """Test that SessionStore can be created with AsyncMy configuration."""
    assert session_store is not None
    assert session_store._table_name == "litestar_test_sessions"
    assert session_store._session_id_column == "session_id"
    assert session_store._data_column == "data"
    assert session_store._expires_at_column == "expires_at"
    assert session_store._created_at_column == "created_at"


async def test_session_store_mysql_table_structure(
    session_store: SQLSpecSessionStore, asyncmy_config: AsyncmyConfig
) -> None:
    """Test that session table is created with proper MySQL structure."""
    async with asyncmy_config.provide_session() as driver:
        # Verify table exists with proper name
        result = await driver.execute("""
            SELECT TABLE_NAME, ENGINE, TABLE_COLLATION 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'litestar_test_sessions'
        """)
        assert len(result.data) == 1
        table_info = result.data[0]
        assert table_info["TABLE_NAME"] == "litestar_test_sessions"
        assert table_info["ENGINE"] == "InnoDB"
        assert "utf8mb4" in table_info["TABLE_COLLATION"]

        # Verify column structure with UTF8MB4 support
        result = await driver.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_SET_NAME, COLLATION_NAME
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'litestar_test_sessions'
            ORDER BY ORDINAL_POSITION
        """)
        columns = {row["COLUMN_NAME"]: row for row in result.data}

        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify UTF8MB4 charset for text columns
        for col_name, col_info in columns.items():
            if col_info["DATA_TYPE"] in ("varchar", "text", "longtext"):
                assert col_info["CHARACTER_SET_NAME"] == "utf8mb4"
                assert "utf8mb4" in col_info["COLLATION_NAME"]


async def test_basic_session_operations(session_backend: SQLSpecSessionBackend) -> None:
    """Test basic session operations through Litestar application."""

    @get("/set-session")
    async def set_session(request: Any) -> dict:
        request.session["user_id"] = 12345
        request.session["username"] = "mysql_user"
        request.session["preferences"] = {"theme": "dark", "language": "en", "timezone": "UTC"}
        request.session["roles"] = ["user", "editor", "mysql_admin"]
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

    session_config = ServerSideSessionConfig(backend=session_backend, key="mysql-basic-session", max_age=3600)

    app = Litestar(route_handlers=[set_session, get_session, clear_session], middleware=[session_config.middleware])

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
        assert data["username"] == "mysql_user"
        assert data["preferences"]["theme"] == "dark"
        assert data["roles"] == ["user", "editor", "mysql_admin"]

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"user_id": None, "username": None, "preferences": None, "roles": None}


async def test_session_persistence_across_requests(session_backend: SQLSpecSessionBackend) -> None:
    """Test that sessions persist across multiple requests with MySQL."""

    @get("/shopping-cart/add/{item_id:int}")
    async def add_to_cart(request: Any, item_id: int) -> dict:
        cart = request.session.get("cart", [])
        item = {
            "id": item_id,
            "name": f"Product {item_id}",
            "price": round(item_id * 9.99, 2),
            "quantity": 1,
            "added_at": "2024-01-01T12:00:00Z",
        }
        cart.append(item)
        request.session["cart"] = cart
        request.session["cart_count"] = len(cart)
        request.session["total_value"] = sum(item["price"] for item in cart)
        return {"item": item, "cart_count": len(cart)}

    @get("/shopping-cart")
    async def get_cart(request: Any) -> dict:
        return {
            "cart": request.session.get("cart", []),
            "count": request.session.get("cart_count", 0),
            "total": request.session.get("total_value", 0.0),
        }

    @post("/shopping-cart/checkout")
    async def checkout(request: Any) -> dict:
        cart = request.session.get("cart", [])
        total = request.session.get("total_value", 0.0)

        # Simulate checkout process
        order_id = f"mysql-order-{len(cart)}-{int(total * 100)}"
        request.session["last_order"] = {"order_id": order_id, "items": cart, "total": total, "status": "completed"}

        # Clear cart after checkout
        request.session.pop("cart", None)
        request.session.pop("cart_count", None)
        request.session.pop("total_value", None)

        return {"order_id": order_id, "total": total, "status": "completed"}

    session_config = ServerSideSessionConfig(backend=session_backend, key="mysql-shopping-cart", max_age=3600)

    app = Litestar(route_handlers=[add_to_cart, get_cart, checkout], middleware=[session_config.middleware])

    async with AsyncTestClient(app=app) as client:
        # Add items to cart
        response = await client.get("/shopping-cart/add/101")
        assert response.json()["cart_count"] == 1

        response = await client.get("/shopping-cart/add/202")
        assert response.json()["cart_count"] == 2

        response = await client.get("/shopping-cart/add/303")
        assert response.json()["cart_count"] == 3

        # Verify cart persistence
        response = await client.get("/shopping-cart")
        data = response.json()
        assert data["count"] == 3
        assert len(data["cart"]) == 3
        assert data["cart"][0]["id"] == 101
        assert data["cart"][1]["id"] == 202
        assert data["cart"][2]["id"] == 303
        assert data["total"] > 0

        # Checkout
        response = await client.post("/shopping-cart/checkout")
        assert response.status_code == HTTP_200_OK
        checkout_data = response.json()
        assert "order_id" in checkout_data
        assert checkout_data["status"] == "completed"

        # Verify cart is cleared but order history persists
        response = await client.get("/shopping-cart")
        data = response.json()
        assert data["count"] == 0
        assert len(data["cart"]) == 0


async def test_session_expiration(asyncmy_config: AsyncmyConfig) -> None:
    """Test session expiration handling with MySQL."""
    # Create backend with very short lifetime
    backend = SQLSpecSessionBackend(
        config=asyncmy_config,
        table_name="litestar_test_expiring_sessions",
        session_lifetime=1,  # 1 second
    )

    @get("/set-expiring-data")
    async def set_data(request: Any) -> dict:
        request.session["test_data"] = "mysql_expiring_data"
        request.session["timestamp"] = "2024-01-01T00:00:00Z"
        request.session["database"] = "MySQL"
        request.session["engine"] = "InnoDB"
        return {"status": "data set with short expiration"}

    @get("/get-expiring-data")
    async def get_data(request: Any) -> dict:
        return {
            "test_data": request.session.get("test_data"),
            "timestamp": request.session.get("timestamp"),
            "database": request.session.get("database"),
            "engine": request.session.get("engine"),
        }

    session_config = ServerSideSessionConfig(backend=backend, key="mysql-expiring-session", max_age=1)

    app = Litestar(route_handlers=[set_data, get_data], middleware=[session_config.middleware])

    async with AsyncTestClient(app=app) as client:
        # Set data
        response = await client.get("/set-expiring-data")
        assert response.json() == {"status": "data set with short expiration"}

        # Data should be available immediately
        response = await client.get("/get-expiring-data")
        data = response.json()
        assert data["test_data"] == "mysql_expiring_data"
        assert data["database"] == "MySQL"
        assert data["engine"] == "InnoDB"

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        response = await client.get("/get-expiring-data")
        assert response.json() == {"test_data": None, "timestamp": None, "database": None, "engine": None}


async def test_mysql_specific_utf8mb4_support(session_backend: SQLSpecSessionBackend) -> None:
    """Test MySQL UTF8MB4 support for international characters and emojis."""

    @post("/save-international-data")
    async def save_international(request: Any) -> dict:
        # Store various international characters, emojis, and MySQL-specific data
        request.session["messages"] = {
            "english": "Hello MySQL World",
            "chinese": "你好MySQL世界",
            "japanese": "こんにちはMySQLの世界",
            "korean": "안녕하세요 MySQL 세계",
            "arabic": "مرحبا بعالم MySQL",
            "hebrew": "שלום עולם MySQL",
            "russian": "Привет мир MySQL",
            "hindi": "हैलो MySQL दुनिया",
            "thai": "สวัสดี MySQL โลก",
            "emoji": "🐬 MySQL 🚀 Database 🌟 UTF8MB4 🎉",
            "complex_emoji": "👨‍💻👩‍💻🏴󠁧󠁢󠁳󠁣󠁴󠁿🇺🇳",
        }
        request.session["mysql_specific"] = {
            "sql_injection_test": "'; DROP TABLE users; --",
            "special_chars": "MySQL: 'quotes' \"double\" `backticks` \\backslash",
            "json_string": '{"nested": {"value": "test"}}',
            "null_byte": "text\x00with\x00nulls",
            "unicode_ranges": "𝐇𝐞𝐥𝐥𝐨 𝕎𝕠𝕣𝕝𝕕",  # Mathematical symbols
        }
        request.session["technical_data"] = {
            "server_info": "MySQL 8.0 InnoDB",
            "charset": "utf8mb4_unicode_ci",
            "features": ["JSON", "CTE", "Window Functions", "Spatial"],
        }
        return {"status": "international data saved to MySQL"}

    @get("/load-international-data")
    async def load_international(request: Any) -> dict:
        return {
            "messages": request.session.get("messages"),
            "mysql_specific": request.session.get("mysql_specific"),
            "technical_data": request.session.get("technical_data"),
        }

    session_config = ServerSideSessionConfig(backend=session_backend, key="mysql-utf8mb4-session", max_age=3600)

    app = Litestar(route_handlers=[save_international, load_international], middleware=[session_config.middleware])

    async with AsyncTestClient(app=app) as client:
        # Save international data
        response = await client.post("/save-international-data")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "international data saved to MySQL"}

        # Load and verify international data
        response = await client.get("/load-international-data")
        data = response.json()

        messages = data["messages"]
        assert messages["chinese"] == "你好MySQL世界"
        assert messages["japanese"] == "こんにちはMySQLの世界"
        assert messages["emoji"] == "🐬 MySQL 🚀 Database 🌟 UTF8MB4 🎉"
        assert messages["complex_emoji"] == "👨‍💻👩‍💻🏴󠁧󠁢󠁳󠁣󠁴󠁿🇺🇳"

        mysql_specific = data["mysql_specific"]
        assert mysql_specific["sql_injection_test"] == "'; DROP TABLE users; --"
        assert mysql_specific["unicode_ranges"] == "𝐇𝐞𝐥𝐥𝐨 𝕎𝕠𝕣𝕝𝕕"

        technical = data["technical_data"]
        assert technical["server_info"] == "MySQL 8.0 InnoDB"
        assert "JSON" in technical["features"]


async def test_large_data_handling(session_backend: SQLSpecSessionBackend) -> None:
    """Test handling of large data structures with MySQL backend."""

    @post("/save-large-dataset")
    async def save_large_data(request: Any) -> dict:
        # Create a large data structure to test MySQL's capacity
        large_dataset = {
            "users": [
                {
                    "id": i,
                    "username": f"mysql_user_{i}",
                    "email": f"user{i}@mysql-example.com",
                    "profile": {
                        "bio": f"Extended bio for user {i}. " + "MySQL " * 100,
                        "preferences": {
                            f"pref_{j}": {
                                "value": f"value_{j}",
                                "enabled": j % 2 == 0,
                                "metadata": {"type": "user_setting", "priority": j},
                            }
                            for j in range(50)
                        },
                        "tags": [f"mysql_tag_{k}" for k in range(30)],
                        "activity_log": [
                            {"action": f"action_{l}", "timestamp": f"2024-01-{l:02d}T12:00:00Z"} for l in range(1, 32)
                        ],
                    },
                }
                for i in range(200)  # Test MySQL's JSON capacity
            ],
            "analytics": {
                "daily_stats": [
                    {
                        "date": f"2024-{month:02d}-{day:02d}",
                        "metrics": {
                            "page_views": day * month * 1000,
                            "unique_visitors": day * month * 100,
                            "mysql_queries": day * month * 50,
                        },
                    }
                    for month in range(1, 13)
                    for day in range(1, 29)
                ],
                "metadata": {"database": "MySQL", "engine": "InnoDB", "version": "8.0"},
            },
            "configuration": {
                "mysql_settings": {f"setting_{i}": {"value": f"mysql_value_{i}", "active": True} for i in range(100)}
            },
        }

        request.session["large_dataset"] = large_dataset
        request.session["dataset_size"] = len(str(large_dataset))
        request.session["mysql_info"] = {"table_engine": "InnoDB", "charset": "utf8mb4", "json_support": True}

        return {
            "status": "large dataset saved",
            "users_count": len(large_dataset["users"]),
            "stats_count": len(large_dataset["analytics"]["daily_stats"]),
            "settings_count": len(large_dataset["configuration"]["mysql_settings"]),
        }

    @get("/load-large-dataset")
    async def load_large_data(request: Any) -> dict:
        dataset = request.session.get("large_dataset", {})
        return {
            "has_data": bool(dataset),
            "users_count": len(dataset.get("users", [])),
            "stats_count": len(dataset.get("analytics", {}).get("daily_stats", [])),
            "first_user": dataset.get("users", [{}])[0] if dataset.get("users") else None,
            "dataset_size": request.session.get("dataset_size", 0),
            "mysql_info": request.session.get("mysql_info"),
        }

    session_config = ServerSideSessionConfig(backend=session_backend, key="mysql-large-data-session", max_age=3600)

    app = Litestar(route_handlers=[save_large_data, load_large_data], middleware=[session_config.middleware])

    async with AsyncTestClient(app=app) as client:
        # Save large dataset
        response = await client.post("/save-large-dataset")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["status"] == "large dataset saved"
        assert data["users_count"] == 200
        assert data["stats_count"] > 300  # 12 months * ~28 days
        assert data["settings_count"] == 100

        # Load and verify large dataset
        response = await client.get("/load-large-dataset")
        data = response.json()
        assert data["has_data"] is True
        assert data["users_count"] == 200
        assert data["first_user"]["username"] == "mysql_user_0"
        assert data["dataset_size"] > 100000  # Should be a substantial size
        assert data["mysql_info"]["table_engine"] == "InnoDB"


async def test_concurrent_session_handling(session_backend: SQLSpecSessionBackend) -> None:
    """Test concurrent session access with MySQL's transaction handling."""

    @get("/profile/{profile_id:int}")
    async def set_profile(request: Any, profile_id: int) -> dict:
        request.session["profile_id"] = profile_id
        request.session["database"] = "MySQL"
        request.session["engine"] = "InnoDB"
        request.session["features"] = ["ACID", "Transactions", "Foreign Keys"]
        request.session["mysql_version"] = "8.0"
        request.session["connection_id"] = f"mysql_conn_{profile_id}"
        return {"profile_id": profile_id, "database": "MySQL"}

    @get("/current-profile")
    async def get_profile(request: Any) -> dict:
        return {
            "profile_id": request.session.get("profile_id"),
            "database": request.session.get("database"),
            "engine": request.session.get("engine"),
            "features": request.session.get("features"),
            "mysql_version": request.session.get("mysql_version"),
            "connection_id": request.session.get("connection_id"),
        }

    @post("/update-profile")
    async def update_profile(request: Any) -> dict:
        profile_id = request.session.get("profile_id")
        if profile_id is None:
            return {"error": "No profile set"}

        request.session["last_updated"] = "2024-01-01T12:00:00Z"
        request.session["update_count"] = request.session.get("update_count", 0) + 1
        request.session["mysql_transaction"] = True

        return {"profile_id": profile_id, "updated": True, "update_count": request.session["update_count"]}

    session_config = ServerSideSessionConfig(backend=session_backend, key="mysql-concurrent-session", max_age=3600)

    app = Litestar(route_handlers=[set_profile, get_profile, update_profile], middleware=[session_config.middleware])

    # Test with multiple concurrent clients
    async with (
        AsyncTestClient(app=app) as client1,
        AsyncTestClient(app=app) as client2,
        AsyncTestClient(app=app) as client3,
    ):
        # Set different profiles concurrently
        tasks = [client1.get("/profile/1001"), client2.get("/profile/1002"), client3.get("/profile/1003")]
        responses = await asyncio.gather(*tasks)

        for i, response in enumerate(responses, 1001):
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"profile_id": i, "database": "MySQL"}

        # Verify each client maintains its own session
        response1 = await client1.get("/current-profile")
        response2 = await client2.get("/current-profile")
        response3 = await client3.get("/current-profile")

        assert response1.json()["profile_id"] == 1001
        assert response1.json()["connection_id"] == "mysql_conn_1001"
        assert response2.json()["profile_id"] == 1002
        assert response2.json()["connection_id"] == "mysql_conn_1002"
        assert response3.json()["profile_id"] == 1003
        assert response3.json()["connection_id"] == "mysql_conn_1003"

        # Concurrent updates
        update_tasks = [
            client1.post("/update-profile"),
            client2.post("/update-profile"),
            client3.post("/update-profile"),
            client1.post("/update-profile"),  # Second update for client1
        ]
        update_responses = await asyncio.gather(*update_tasks)

        for response in update_responses:
            assert response.status_code == HTTP_200_OK
            assert response.json()["updated"] is True


async def test_session_cleanup_and_maintenance(asyncmy_config: AsyncmyConfig) -> None:
    """Test session cleanup and maintenance operations with MySQL."""
    backend = SQLSpecSessionBackend(
        config=asyncmy_config,
        table_name="litestar_test_cleanup_sessions",
        session_lifetime=1,  # Short lifetime for testing
    )

    # Create sessions with different lifetimes
    temp_sessions = []
    for i in range(10):
        session_id = f"mysql_temp_session_{i}"
        temp_sessions.append(session_id)
        await backend.store.set(
            session_id,
            {"data": i, "type": "temporary", "mysql_engine": "InnoDB", "created_for": "cleanup_test"},
            expires_in=1,
        )

    # Create permanent sessions
    perm_sessions = []
    for i in range(5):
        session_id = f"mysql_perm_session_{i}"
        perm_sessions.append(session_id)
        await backend.store.set(
            session_id,
            {"data": f"permanent_{i}", "type": "permanent", "mysql_engine": "InnoDB", "created_for": "cleanup_test"},
            expires_in=3600,
        )

    # Verify all sessions exist initially
    for session_id in temp_sessions + perm_sessions:
        result = await backend.store.get(session_id)
        assert result is not None
        assert result["mysql_engine"] == "InnoDB"

    # Wait for temporary sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await backend.delete_expired_sessions()

    # Verify temporary sessions are gone
    for session_id in temp_sessions:
        result = await backend.store.get(session_id)
        assert result is None

    # Verify permanent sessions still exist
    for session_id in perm_sessions:
        result = await backend.store.get(session_id)
        assert result is not None
        assert result["type"] == "permanent"


async def test_shopping_cart_pattern(session_backend: SQLSpecSessionBackend) -> None:
    """Test a complete shopping cart pattern typical for MySQL e-commerce applications."""

    @post("/cart/add")
    async def add_item(request: Any) -> dict:
        data = await request.json()
        cart = request.session.get("cart", {"items": [], "metadata": {}})

        item = {
            "id": data["item_id"],
            "name": data["name"],
            "price": data["price"],
            "quantity": data.get("quantity", 1),
            "category": data.get("category", "general"),
            "added_at": "2024-01-01T12:00:00Z",
            "mysql_id": f"mysql_{data['item_id']}",
        }

        cart["items"].append(item)
        cart["metadata"] = {
            "total_items": len(cart["items"]),
            "total_value": sum(item["price"] * item["quantity"] for item in cart["items"]),
            "last_modified": "2024-01-01T12:00:00Z",
            "database": "MySQL",
            "engine": "InnoDB",
        }

        request.session["cart"] = cart
        request.session["user_activity"] = {
            "last_action": "add_to_cart",
            "timestamp": "2024-01-01T12:00:00Z",
            "mysql_session": True,
        }

        return {"status": "item added", "cart_total": cart["metadata"]["total_items"]}

    @get("/cart")
    async def view_cart(request: Any) -> dict:
        cart = request.session.get("cart", {"items": [], "metadata": {}})
        return {
            "items": cart["items"],
            "metadata": cart["metadata"],
            "user_activity": request.session.get("user_activity"),
        }

    @post("/cart/checkout")
    async def checkout_cart(request: Any) -> dict:
        cart = request.session.get("cart", {"items": [], "metadata": {}})
        if not cart["items"]:
            return {"error": "Empty cart"}

        order = {
            "order_id": f"mysql_order_{len(cart['items'])}_{int(cart['metadata'].get('total_value', 0) * 100)}",
            "items": cart["items"],
            "total": cart["metadata"].get("total_value", 0),
            "checkout_time": "2024-01-01T12:00:00Z",
            "mysql_transaction": True,
            "engine": "InnoDB",
            "status": "completed",
        }

        # Store order history and clear cart
        order_history = request.session.get("order_history", [])
        order_history.append(order)
        request.session["order_history"] = order_history
        request.session.pop("cart", None)
        request.session["last_checkout"] = order["checkout_time"]

        return {"order": order, "status": "checkout completed"}

    @get("/orders")
    async def view_orders(request: Any) -> dict:
        return {
            "orders": request.session.get("order_history", []),
            "count": len(request.session.get("order_history", [])),
            "last_checkout": request.session.get("last_checkout"),
        }

    session_config = ServerSideSessionConfig(backend=session_backend, key="mysql-shopping-session", max_age=3600)

    app = Litestar(
        route_handlers=[add_item, view_cart, checkout_cart, view_orders], middleware=[session_config.middleware]
    )

    async with AsyncTestClient(app=app) as client:
        # Add multiple items to cart
        items_to_add = [
            {"item_id": 1, "name": "MySQL Book", "price": 29.99, "category": "books"},
            {"item_id": 2, "name": "Database Poster", "price": 15.50, "category": "decor"},
            {"item_id": 3, "name": "SQL Mug", "price": 12.99, "category": "drinkware", "quantity": 2},
        ]

        for item in items_to_add:
            response = await client.post("/cart/add", json=item)
            assert response.status_code == HTTP_200_OK
            assert "item added" in response.json()["status"]

        # View cart
        response = await client.get("/cart")
        cart_data = response.json()
        assert len(cart_data["items"]) == 3
        assert cart_data["metadata"]["total_items"] == 3
        assert cart_data["metadata"]["database"] == "MySQL"
        assert cart_data["user_activity"]["mysql_session"] is True

        # Checkout
        response = await client.post("/cart/checkout")
        assert response.status_code == HTTP_200_OK
        checkout_data = response.json()
        assert checkout_data["status"] == "checkout completed"
        assert checkout_data["order"]["mysql_transaction"] is True

        # Verify cart is cleared
        response = await client.get("/cart")
        cart_data = response.json()
        assert len(cart_data["items"]) == 0

        # View order history
        response = await client.get("/orders")
        orders_data = response.json()
        assert orders_data["count"] == 1
        assert orders_data["orders"][0]["engine"] == "InnoDB"
        assert "last_checkout" in orders_data
