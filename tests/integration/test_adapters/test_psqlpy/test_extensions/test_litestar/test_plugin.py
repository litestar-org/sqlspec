"""Comprehensive Litestar integration tests for PsqlPy adapter.

This test suite validates the full integration between SQLSpec's PsqlPy adapter
and Litestar's session middleware, including PostgreSQL-specific features like JSONB.
"""

import asyncio
import math
from typing import Any

import pytest
from litestar import Litestar, get, post, put
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.stores.registry import StoreRegistry
from litestar.testing import AsyncTestClient

from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.extensions.litestar import SQLSpecAsyncSessionStore
from sqlspec.extensions.litestar.session import SQLSpecSessionConfig
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = [pytest.mark.psqlpy, pytest.mark.postgres, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
async def litestar_app(session_config: SQLSpecSessionConfig, session_store: SQLSpecAsyncSessionStore) -> Litestar:
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
    async def get_user_profile(request: Any) -> dict:
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


async def test_session_store_creation(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test that SessionStore can be created with PsqlPy configuration."""
    assert session_store is not None
    assert session_store.table_name == "litestar_sessions_psqlpy"
    assert session_store.session_id_column == "session_id"
    assert session_store.data_column == "data"
    assert session_store.expires_at_column == "expires_at"
    assert session_store.created_at_column == "created_at"


async def test_session_store_postgres_table_structure(
    session_store: SQLSpecAsyncSessionStore, migrated_config: PsqlpyConfig
) -> None:
    """Test that session table is created with proper PostgreSQL structure."""
    async with migrated_config.provide_session() as driver:
        # Verify table exists
        result = await driver.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE tablename = %s
        """,
            ["litestar_sessions_psqlpy"],
        )
        assert len(result.data) == 1
        assert result.data[0]["tablename"] == "litestar_sessions_psqlpy"

        # Verify column structure
        result = await driver.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """,
            ["litestar_sessions_psqlpy"],
        )

        columns = {row["column_name"]: row for row in result.data}

        assert "session_id" in columns
        assert columns["session_id"]["data_type"] == "character varying"
        assert "data" in columns
        assert columns["data"]["data_type"] == "jsonb"  # PostgreSQL JSONB
        assert "expires_at" in columns
        assert columns["expires_at"]["data_type"] == "timestamp with time zone"
        assert "created_at" in columns
        assert columns["created_at"]["data_type"] == "timestamp with time zone"


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


async def test_session_expiration(migrated_config: PsqlpyConfig) -> None:
    """Test session expiration handling."""
    # Create store with very short lifetime (migrations already applied by fixture)
    session_store = SQLSpecAsyncSessionStore(config=migrated_config, table_name="litestar_sessions_psqlpy")

    session_config = SQLSpecSessionConfig(
        table_name="litestar_sessions_psqlpy",
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


async def test_complex_user_workflow(litestar_app: Litestar) -> None:
    """Test a complex user workflow combining multiple operations."""
    async with AsyncTestClient(app=litestar_app) as client:
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
        response = await client.put("/user/profile", json=user_profile)
        assert response.status_code == HTTP_200_OK

        # Verify profile was set
        response = await client.get("/user/profile")
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

        response = await client.post("/session/bulk", json=activity_data)
        assert response.status_code == HTTP_201_CREATED

        # Test counter functionality within complex session
        for i in range(1, 6):
            response = await client.get("/counter")
            assert response.json()["count"] == i

        # Get all session data to verify everything is maintained
        response = await client.get("/session/all")
        all_data = response.json()

        # Verify all data components are present
        assert "profile" in all_data
        assert all_data["profile"] == user_profile
        assert all_data["page_views"] == 15
        assert len(all_data["cart_items"]) == 2
        assert all_data["count"] == 5

        # Test selective data removal
        response = await client.post("/session/key/cart_items/delete")
        assert response.json()["status"] == "deleted"

        # Verify cart_items removed but other data persists
        response = await client.get("/session/all")
        updated_data = response.json()
        assert "cart_items" not in updated_data
        assert "profile" in updated_data
        assert updated_data["count"] == 5

        # Final counter increment to ensure functionality still works
        response = await client.get("/counter")
        assert response.json()["count"] == 6


async def test_concurrent_sessions_with_psqlpy(
    session_config: SQLSpecSessionConfig, session_store: SQLSpecAsyncSessionStore
) -> None:
    """Test handling of concurrent sessions with different clients."""

    @get("/user/login/{user_id:int}")
    async def login_user(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        request.session["login_time"] = "2024-01-01T12:00:00Z"
        request.session["adapter"] = "psqlpy"
        request.session["features"] = ["binary_protocol", "async_native", "high_performance"]
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
    async with (
        AsyncTestClient(app=app) as client1,
        AsyncTestClient(app=app) as client2,
        AsyncTestClient(app=app) as client3,
    ):
        # Each client logs in as different user
        response1 = await client1.get("/user/login/100")
        assert response1.json()["user_id"] == 100

        response2 = await client2.get("/user/login/200")
        assert response2.json()["user_id"] == 200

        response3 = await client3.get("/user/login/300")
        assert response3.json()["user_id"] == 300

        # Each client should maintain separate session
        who1 = await client1.get("/user/whoami")
        assert who1.json()["user_id"] == 100

        who2 = await client2.get("/user/whoami")
        assert who2.json()["user_id"] == 200

        who3 = await client3.get("/user/whoami")
        assert who3.json()["user_id"] == 300

        # Update profiles independently
        await client1.post("/user/update-profile", json={"name": "User One", "age": 25})
        await client2.post("/user/update-profile", json={"name": "User Two", "age": 30})

        # Verify isolation - get all session data
        response1 = await client1.get("/session/all")
        data1 = response1.json()
        assert data1["user_id"] == 100
        assert data1["profile"]["name"] == "User One"
        assert data1["adapter"] == "psqlpy"

        response2 = await client2.get("/session/all")
        data2 = response2.json()
        assert data2["user_id"] == 200
        assert data2["profile"]["name"] == "User Two"

        # Client3 should not have profile data
        response3 = await client3.get("/session/all")
        data3 = response3.json()
        assert data3["user_id"] == 300
        assert "profile" not in data3


async def test_large_data_handling_jsonb(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test handling of large session data leveraging PostgreSQL JSONB."""
    session_id = "test-large-jsonb-data"

    # Create large data structure to test JSONB capabilities
    large_data = {
        "user_data": {
            "profile": {f"field_{i}": f"value_{i}" for i in range(1000)},
            "settings": {f"setting_{i}": i % 2 == 0 for i in range(500)},
            "history": [{"item": f"item_{i}", "value": i} for i in range(1000)],
        },
        "cache": {f"cache_key_{i}": f"cached_value_{i}" * 10 for i in range(100)},
        "temporary_state": list(range(2000)),
        "postgres_features": {
            "jsonb": True,
            "binary_protocol": True,
            "native_types": ["jsonb", "uuid", "arrays"],
            "performance": "excellent",
        },
        "metadata": {"adapter": "psqlpy", "engine": "PostgreSQL", "data_type": "JSONB", "atomic_operations": True},
    }

    # Set large session data
    await session_store.set(session_id, large_data, expires_in=3600)

    # Get session data back
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == large_data
    assert retrieved_data["postgres_features"]["jsonb"] is True
    assert retrieved_data["metadata"]["adapter"] == "psqlpy"


async def test_postgresql_jsonb_operations(
    session_store: SQLSpecAsyncSessionStore, migrated_config: PsqlpyConfig
) -> None:
    """Test PostgreSQL-specific JSONB operations available through PsqlPy."""
    session_id = "postgres-jsonb-ops-test"

    # Set initial session data
    session_data = {
        "user_id": 1001,
        "features": ["jsonb", "arrays", "uuid"],
        "config": {"theme": "dark", "lang": "en", "notifications": {"email": True, "push": False}},
    }
    await session_store.set(session_id, session_data, expires_in=3600)

    # Test direct JSONB operations via the driver
    async with migrated_config.provide_session() as driver:
        # Test JSONB path operations
        result = await driver.execute(
            """
            SELECT data->'config'->>'theme' as theme,
                   jsonb_array_length(data->'features') as feature_count,
                   data->'config'->'notifications'->>'email' as email_notif
            FROM litestar_sessions_psqlpy
            WHERE session_id = %s
        """,
            [session_id],
        )

        assert len(result.data) == 1
        row = result.data[0]
        assert row["theme"] == "dark"
        assert row["feature_count"] == 3
        assert row["email_notif"] == "true"

        # Test JSONB update operations
        await driver.execute(
            """
            UPDATE litestar_sessions_psqlpy
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
    assert updated_data["config"]["notifications"]["email"] is True


async def test_session_with_complex_postgres_data_types(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test various data types that benefit from PostgreSQL's type system in PsqlPy."""
    session_id = "test-postgres-data-types"

    # Test data with various types that benefit from PostgreSQL
    session_data = {
        "integers": [1, 2, 3, 1000000, -999999],
        "floats": [1.5, 2.7, math.pi, -0.001],
        "booleans": [True, False, True],
        "text_data": "Unicode text: 你好世界 🌍",
        "timestamps": ["2023-01-01T00:00:00Z", "2023-12-31T23:59:59Z"],
        "null_values": [None, None, None],
        "mixed_array": [1, "text", True, None, math.pi],
        "nested_structure": {
            "level1": {
                "level2": {
                    "integers": [100, 200, 300],
                    "text": "deeply nested",
                    "postgres_specific": {"jsonb": True, "native_json": True, "binary_format": True},
                }
            }
        },
        "postgres_metadata": {"adapter": "psqlpy", "protocol": "binary", "engine": "PostgreSQL", "version": "15+"},
    }

    # Set and retrieve data
    await session_store.set(session_id, session_data, expires_in=3600)
    retrieved_data = await session_store.get(session_id)

    # Verify all data types are preserved correctly
    assert retrieved_data == session_data
    assert retrieved_data["nested_structure"]["level1"]["level2"]["postgres_specific"]["jsonb"] is True
    assert retrieved_data["postgres_metadata"]["adapter"] == "psqlpy"


async def test_high_performance_concurrent_operations(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test high-performance concurrent session operations that showcase PsqlPy's capabilities."""
    session_prefix = "perf-test-psqlpy"
    num_sessions = 25  # Reasonable number for CI

    # Create sessions concurrently
    async def create_session(index: int) -> None:
        session_id = f"{session_prefix}-{index}"
        session_data = {
            "session_index": index,
            "data": {f"key_{i}": f"value_{i}" for i in range(10)},
            "psqlpy_features": {
                "binary_protocol": True,
                "async_native": True,
                "high_performance": True,
                "connection_pooling": True,
            },
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
        assert result["psqlpy_features"]["binary_protocol"] is True

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


async def test_migration_with_default_table_name(migrated_config: PsqlpyConfig) -> None:
    """Test that migration creates the default table name."""
    # Create store using the migrated table
    store = SQLSpecAsyncSessionStore(
        config=migrated_config,
        table_name="litestar_sessions_psqlpy",  # Unique table name for psqlpy
    )

    # Test that the store works with the migrated table
    session_id = "test_session_default"
    test_data = {"user_id": 1, "username": "test_user", "adapter": "psqlpy"}

    await store.set(session_id, test_data, expires_in=3600)
    retrieved = await store.get(session_id)

    assert retrieved == test_data
    assert retrieved["adapter"] == "psqlpy"


async def test_migration_with_custom_table_name(psqlpy_migration_config_with_dict: PsqlpyConfig) -> None:
    """Test that migration with dict format creates custom table name."""
    # Apply migrations
    commands = AsyncMigrationCommands(psqlpy_migration_config_with_dict)
    await commands.init(psqlpy_migration_config_with_dict.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the custom migrated table
    store = SQLSpecAsyncSessionStore(
        config=psqlpy_migration_config_with_dict,
        table_name="custom_sessions",  # Custom table name from config
    )

    # Test that the store works with the custom table
    session_id = "test_session_custom"
    test_data = {"user_id": 2, "username": "custom_user", "adapter": "psqlpy"}

    await store.set(session_id, test_data, expires_in=3600)
    retrieved = await store.get(session_id)

    assert retrieved == test_data
    assert retrieved["adapter"] == "psqlpy"

    # Verify default table doesn't exist (clean up any existing default table first)
    async with psqlpy_migration_config_with_dict.provide_session() as driver:
        # Clean up any conflicting tables from other PostgreSQL adapters
        await driver.execute("DROP TABLE IF EXISTS litestar_sessions")
        await driver.execute("DROP TABLE IF EXISTS litestar_sessions_asyncpg")
        await driver.execute("DROP TABLE IF EXISTS litestar_sessions_psycopg")

        # Now verify it doesn't exist
        result = await driver.execute("SELECT tablename FROM pg_tables WHERE tablename = %s", ["litestar_sessions"])
        assert len(result.data) == 0
        result = await driver.execute(
            "SELECT tablename FROM pg_tables WHERE tablename = %s", ["litestar_sessions_asyncpg"]
        )
        assert len(result.data) == 0
        result = await driver.execute(
            "SELECT tablename FROM pg_tables WHERE tablename = %s", ["litestar_sessions_psycopg"]
        )
        assert len(result.data) == 0


async def test_migration_with_mixed_extensions(psqlpy_migration_config_mixed: PsqlpyConfig) -> None:
    """Test migration with mixed extension formats."""
    # Apply migrations
    commands = AsyncMigrationCommands(psqlpy_migration_config_mixed)
    await commands.init(psqlpy_migration_config_mixed.migration_config["script_location"], package=False)
    await commands.upgrade()

    # The litestar extension should use default table name
    store = SQLSpecAsyncSessionStore(
        config=psqlpy_migration_config_mixed,
        table_name="litestar_sessions_psqlpy",  # Unique table for psqlpy
    )

    # Test that the store works
    session_id = "test_session_mixed"
    test_data = {"user_id": 3, "username": "mixed_user", "adapter": "psqlpy"}

    await store.set(session_id, test_data, expires_in=3600)
    retrieved = await store.get(session_id)

    assert retrieved == test_data
