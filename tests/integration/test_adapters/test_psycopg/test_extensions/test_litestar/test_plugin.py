"""Comprehensive Litestar integration tests for Psycopg adapter.

This test suite validates the full integration between SQLSpec's Psycopg adapter
and Litestar's session middleware, including PostgreSQL-specific features.
"""

import asyncio
import json
import time
from typing import Any

import pytest
from litestar import Litestar, get, post, put
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.stores.registry import StoreRegistry
from litestar.testing import AsyncTestClient, TestClient

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.extensions.litestar import SQLSpecSessionConfig, SQLSpecSessionStore

pytestmark = [pytest.mark.psycopg, pytest.mark.postgres, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
def sync_session_store(psycopg_sync_migrated_config: PsycopgSyncConfig) -> SQLSpecSessionStore:
    """Create a session store using the migrated sync config."""
    return SQLSpecSessionStore(
        config=psycopg_sync_migrated_config,
        table_name="litestar_sessions_psycopg_sync",
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


@pytest.fixture
async def async_session_store(psycopg_async_migrated_config: PsycopgAsyncConfig) -> SQLSpecSessionStore:
    """Create a session store using the migrated async config."""
    return SQLSpecSessionStore(
        config=psycopg_async_migrated_config,
        table_name="litestar_sessions_psycopg_async",
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


@pytest.fixture
def sync_session_config() -> SQLSpecSessionConfig:
    """Create a session config for sync tests."""
    return SQLSpecSessionConfig(table_name="litestar_sessions_psycopg_sync", store="sessions", max_age=3600)


@pytest.fixture
async def async_session_config() -> SQLSpecSessionConfig:
    """Create a session config for async tests."""
    return SQLSpecSessionConfig(table_name="litestar_sessions_psycopg_async", store="sessions", max_age=3600)


@pytest.fixture
def sync_litestar_app(sync_session_config: SQLSpecSessionConfig, sync_session_store: SQLSpecSessionStore) -> Litestar:
    """Create a Litestar app with session middleware for sync testing."""

    @get("/session/set/{key:str}")
    def set_session_value(request: Any, key: str) -> dict:
        """Set a session value."""
        value = request.query_params.get("value", "default")
        request.session[key] = value
        return {"status": "set", "key": key, "value": value}

    @get("/session/get/{key:str}")
    def get_session_value(request: Any, key: str) -> dict:
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
    def get_all_session(request: Any) -> dict:
        """Get all session data."""
        return dict(request.session)

    @post("/session/clear")
    def clear_session(request: Any) -> dict:
        """Clear all session data."""
        request.session.clear()
        return {"status": "cleared"}

    @post("/session/key/{key:str}/delete")
    def delete_session_key(request: Any, key: str) -> dict:
        """Delete a specific session key."""
        if key in request.session:
            del request.session[key]
            return {"status": "deleted", "key": key}
        return {"status": "not found", "key": key}

    @get("/counter")
    def counter(request: Any) -> dict:
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
    def get_user_profile(request: Any) -> dict[str, Any]:
        """Get user profile data."""
        profile = request.session.get("profile")
        if not profile:
            return {"error": "No profile found"}
        return {"profile": profile}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", sync_session_store)

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
        middleware=[sync_session_config.middleware],
        stores=stores,
    )


@pytest.fixture
async def async_litestar_app(
    async_session_config: SQLSpecSessionConfig, async_session_store: SQLSpecSessionStore
) -> Litestar:
    """Create a Litestar app with session middleware for async testing."""

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
    stores.register("sessions", async_session_store)

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
        middleware=[async_session_config.middleware],
        stores=stores,
    )


def test_sync_store_creation(sync_session_store: SQLSpecSessionStore) -> None:
    """Test that sync session store can be created."""
    assert sync_session_store is not None
    assert sync_session_store.table_name == "litestar_sessions_psycopg_sync"
    assert sync_session_store.session_id_column == "session_id"
    assert sync_session_store.data_column == "data"
    assert sync_session_store.expires_at_column == "expires_at"
    assert sync_session_store.created_at_column == "created_at"


async def test_async_store_creation(async_session_store: SQLSpecSessionStore) -> None:
    """Test that async session store can be created."""
    assert async_session_store is not None
    assert async_session_store.table_name == "litestar_sessions_psycopg_async"
    assert async_session_store.session_id_column == "session_id"
    assert async_session_store.data_column == "data"
    assert async_session_store.expires_at_column == "expires_at"
    assert async_session_store.created_at_column == "created_at"


def test_sync_table_verification(
    sync_session_store: SQLSpecSessionStore, psycopg_sync_migrated_config: PsycopgSyncConfig
) -> None:
    """Test that session table exists with proper schema for sync driver."""
    with psycopg_sync_migrated_config.provide_session() as driver:
        result = driver.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'litestar_sessions_psycopg_sync' ORDER BY ordinal_position"
        )

        columns = {row["column_name"]: row["data_type"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Check PostgreSQL-specific types
        assert "jsonb" in columns["data"].lower()
        assert "timestamp" in columns["expires_at"].lower()


async def test_async_table_verification(
    async_session_store: SQLSpecSessionStore, psycopg_async_migrated_config: PsycopgAsyncConfig
) -> None:
    """Test that session table exists with proper schema for async driver."""
    async with psycopg_async_migrated_config.provide_session() as driver:
        result = await driver.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'litestar_sessions_psycopg_async' ORDER BY ordinal_position"
        )

        columns = {row["column_name"]: row["data_type"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Check PostgreSQL-specific types
        assert "jsonb" in columns["data"].lower()
        assert "timestamp" in columns["expires_at"].lower()


def test_sync_basic_session_operations(sync_litestar_app: Litestar) -> None:
    """Test basic session get/set/delete operations with sync driver."""
    with TestClient(app=sync_litestar_app) as client:
        # Set a simple value
        response = client.get("/session/set/username?value=psycopg_sync_user")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "set", "key": "username", "value": "psycopg_sync_user"}

        # Get the value back
        response = client.get("/session/get/username")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"key": "username", "value": "psycopg_sync_user"}

        # Set another value
        response = client.get("/session/set/user_id?value=12345")
        assert response.status_code == HTTP_200_OK

        # Get all session data
        response = client.get("/session/all")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["username"] == "psycopg_sync_user"
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


async def test_async_basic_session_operations(async_litestar_app: Litestar) -> None:
    """Test basic session get/set/delete operations with async driver."""
    async with AsyncTestClient(app=async_litestar_app) as client:
        # Set a simple value
        response = await client.get("/session/set/username?value=psycopg_async_user")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "set", "key": "username", "value": "psycopg_async_user"}

        # Get the value back
        response = await client.get("/session/get/username")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"key": "username", "value": "psycopg_async_user"}

        # Set another value
        response = await client.get("/session/set/user_id?value=54321")
        assert response.status_code == HTTP_200_OK

        # Get all session data
        response = await client.get("/session/all")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["username"] == "psycopg_async_user"
        assert data["user_id"] == "54321"

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
        assert response.json() == {"key": "user_id", "value": "54321"}


def test_sync_bulk_session_operations(sync_litestar_app: Litestar) -> None:
    """Test bulk session operations with sync driver."""
    with TestClient(app=sync_litestar_app) as client:
        # Set multiple values at once
        bulk_data = {
            "user_id": 42,
            "username": "postgresql_sync",
            "email": "sync@postgresql.com",
            "preferences": {"theme": "dark", "notifications": True, "language": "en"},
            "roles": ["user", "admin"],
            "last_login": "2024-01-15T10:30:00Z",
            "postgres_info": {"version": "15+", "features": ["JSONB", "ACID", "SQL"]},
        }

        response = client.post("/session/bulk", json=bulk_data)
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "bulk set", "count": 7}

        # Verify all data was set
        response = client.get("/session/all")
        assert response.status_code == HTTP_200_OK
        data = response.json()

        for key, expected_value in bulk_data.items():
            assert data[key] == expected_value


async def test_async_bulk_session_operations(async_litestar_app: Litestar) -> None:
    """Test bulk session operations with async driver."""
    async with AsyncTestClient(app=async_litestar_app) as client:
        # Set multiple values at once
        bulk_data = {
            "user_id": 84,
            "username": "postgresql_async",
            "email": "async@postgresql.com",
            "preferences": {"theme": "light", "notifications": False, "language": "es"},
            "roles": ["editor", "reviewer"],
            "last_login": "2024-01-16T14:30:00Z",
            "postgres_info": {"version": "15+", "features": ["JSONB", "ACID", "Async"]},
        }

        response = await client.post("/session/bulk", json=bulk_data)
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "bulk set", "count": 7}

        # Verify all data was set
        response = await client.get("/session/all")
        assert response.status_code == HTTP_200_OK
        data = response.json()

        for key, expected_value in bulk_data.items():
            assert data[key] == expected_value


def test_sync_session_persistence(sync_litestar_app: Litestar) -> None:
    """Test that sessions persist across multiple requests with sync driver."""
    with TestClient(app=sync_litestar_app) as client:
        # Test counter functionality across multiple requests
        expected_counts = [1, 2, 3, 4, 5]

        for expected_count in expected_counts:
            response = client.get("/counter")
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"count": expected_count}

        # Verify count persists after setting other data
        response = client.get("/session/set/postgres_sync?value=persistence_test")
        assert response.status_code == HTTP_200_OK

        response = client.get("/counter")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"count": 6}


async def test_async_session_persistence(async_litestar_app: Litestar) -> None:
    """Test that sessions persist across multiple requests with async driver."""
    async with AsyncTestClient(app=async_litestar_app) as client:
        # Test counter functionality across multiple requests
        expected_counts = [1, 2, 3, 4, 5]

        for expected_count in expected_counts:
            response = await client.get("/counter")
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"count": expected_count}

        # Verify count persists after setting other data
        response = await client.get("/session/set/postgres_async?value=persistence_test")
        assert response.status_code == HTTP_200_OK

        response = await client.get("/counter")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"count": 6}


def test_sync_session_expiration(psycopg_sync_migrated_config: PsycopgSyncConfig) -> None:
    """Test session expiration handling with sync driver."""
    # Create store with very short lifetime
    session_store = SQLSpecSessionStore(
        config=psycopg_sync_migrated_config, table_name="litestar_sessions_psycopg_sync"
    )

    session_config = SQLSpecSessionConfig(
        table_name="litestar_sessions_psycopg_sync",
        store="sessions",
        max_age=1,  # 1 second
    )

    @get("/set-temp")
    def set_temp_data(request: Any) -> dict:
        request.session["temp_data"] = "will_expire_sync"
        request.session["postgres_sync"] = True
        return {"status": "set"}

    @get("/get-temp")
    def get_temp_data(request: Any) -> dict:
        return {"temp_data": request.session.get("temp_data"), "postgres_sync": request.session.get("postgres_sync")}

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
        assert response.json() == {"temp_data": "will_expire_sync", "postgres_sync": True}

        # Wait for expiration
        time.sleep(2)

        # Data should be expired (new session created)
        response = client.get("/get-temp")
        assert response.json() == {"temp_data": None, "postgres_sync": None}


async def test_async_session_expiration(psycopg_async_migrated_config: PsycopgAsyncConfig) -> None:
    """Test session expiration handling with async driver."""
    # Create store with very short lifetime
    session_store = SQLSpecSessionStore(
        config=psycopg_async_migrated_config, table_name="litestar_sessions_psycopg_async"
    )

    session_config = SQLSpecSessionConfig(
        table_name="litestar_sessions_psycopg_async",
        store="sessions",
        max_age=1,  # 1 second
    )

    @get("/set-temp")
    async def set_temp_data(request: Any) -> dict:
        request.session["temp_data"] = "will_expire_async"
        request.session["postgres_async"] = True
        return {"status": "set"}

    @get("/get-temp")
    async def get_temp_data(request: Any) -> dict:
        return {"temp_data": request.session.get("temp_data"), "postgres_async": request.session.get("postgres_async")}

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
        assert response.json() == {"temp_data": "will_expire_async", "postgres_async": True}

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired (new session created)
        response = await client.get("/get-temp")
        assert response.json() == {"temp_data": None, "postgres_async": None}


async def test_postgresql_jsonb_features(
    async_session_store: SQLSpecSessionStore, psycopg_async_migrated_config: PsycopgAsyncConfig
) -> None:
    """Test PostgreSQL-specific JSONB features."""
    session_id = "test-jsonb-session"
    complex_data = {
        "user_profile": {
            "name": "John Doe PostgreSQL",
            "age": 30,
            "settings": {
                "theme": "dark",
                "notifications": True,
                "preferences": ["email", "sms"],
                "postgres_features": ["JSONB", "GIN", "BTREE"],
            },
        },
        "permissions": {
            "admin": False,
            "modules": ["users", "reports", "postgres_admin"],
            "database_access": ["read", "write", "execute"],
        },
        "arrays": [1, 2, 3, "postgresql", {"nested": True, "jsonb": True}],
        "null_value": None,
        "boolean_value": True,
        "numeric_value": 123.45,
        "postgres_metadata": {"version": "15+", "encoding": "UTF8", "collation": "en_US.UTF-8"},
    }

    # Set complex JSONB data
    await async_session_store.set(session_id, complex_data, expires_in=3600)

    # Get and verify complex data
    retrieved_data = await async_session_store.get(session_id)
    assert retrieved_data == complex_data

    # Test direct JSONB queries
    async with psycopg_async_migrated_config.provide_session() as driver:
        # Query JSONB field directly
        result = await driver.execute(
            "SELECT data->>'user_profile' as profile FROM litestar_sessions_psycopg_async WHERE session_id = %s",
            [session_id],
        )
        assert len(result.data) == 1

        profile_data = json.loads(result.data[0]["profile"])
        assert profile_data["name"] == "John Doe PostgreSQL"
        assert profile_data["age"] == 30
        assert "JSONB" in profile_data["settings"]["postgres_features"]


async def test_postgresql_concurrent_sessions(
    async_session_config: SQLSpecSessionConfig, async_session_store: SQLSpecSessionStore
) -> None:
    """Test concurrent session handling with PostgreSQL backend."""

    @get("/user/{user_id:int}/login")
    async def user_login(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        request.session["username"] = f"postgres_user_{user_id}"
        request.session["login_time"] = "2024-01-01T12:00:00Z"
        request.session["database"] = "PostgreSQL"
        request.session["connection_type"] = "async"
        request.session["postgres_features"] = ["JSONB", "MVCC", "WAL"]
        return {"status": "logged in", "user_id": user_id}

    @get("/user/profile")
    async def get_profile(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "database": request.session.get("database"),
            "connection_type": request.session.get("connection_type"),
            "postgres_features": request.session.get("postgres_features"),
        }

    @post("/user/activity")
    async def log_activity(request: Any) -> dict:
        user_id = request.session.get("user_id")
        if user_id is None:
            return {"error": "Not logged in"}

        activities = request.session.get("activities", [])
        activity = {
            "action": "page_view",
            "timestamp": "2024-01-01T12:00:00Z",
            "user_id": user_id,
            "postgres_transaction": True,
            "jsonb_stored": True,
        }
        activities.append(activity)
        request.session["activities"] = activities
        request.session["activity_count"] = len(activities)

        return {"status": "activity logged", "count": len(activities)}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", async_session_store)

    app = Litestar(
        route_handlers=[user_login, get_profile, log_activity],
        middleware=[async_session_config.middleware],
        stores=stores,
    )

    # Test with multiple concurrent users
    async with (
        AsyncTestClient(app=app) as client1,
        AsyncTestClient(app=app) as client2,
        AsyncTestClient(app=app) as client3,
    ):
        # Concurrent logins
        login_tasks = [
            client1.get("/user/2001/login"),
            client2.get("/user/2002/login"),
            client3.get("/user/2003/login"),
        ]
        responses = await asyncio.gather(*login_tasks)

        for i, response in enumerate(responses, 2001):
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"status": "logged in", "user_id": i}

        # Verify each client has correct session
        profile_responses = await asyncio.gather(
            client1.get("/user/profile"), client2.get("/user/profile"), client3.get("/user/profile")
        )

        assert profile_responses[0].json()["user_id"] == 2001
        assert profile_responses[0].json()["username"] == "postgres_user_2001"
        assert profile_responses[0].json()["database"] == "PostgreSQL"
        assert "JSONB" in profile_responses[0].json()["postgres_features"]

        assert profile_responses[1].json()["user_id"] == 2002
        assert profile_responses[2].json()["user_id"] == 2003

        # Log activities concurrently
        activity_tasks = [
            client.post("/user/activity")
            for client in [client1, client2, client3]
            for _ in range(3)  # 3 activities per user
        ]

        activity_responses = await asyncio.gather(*activity_tasks)
        for response in activity_responses:
            assert response.status_code == HTTP_201_CREATED
            assert "activity logged" in response.json()["status"]


async def test_sync_store_crud_operations(sync_session_store: SQLSpecSessionStore) -> None:
    """Test direct store CRUD operations with sync driver."""
    session_id = "test-sync-session-crud"

    # Test data with PostgreSQL-specific types
    test_data = {
        "user_id": 12345,
        "username": "postgres_sync_testuser",
        "preferences": {
            "theme": "dark",
            "language": "en",
            "notifications": True,
            "postgres_settings": {"jsonb_ops": True, "gin_index": True},
        },
        "tags": ["admin", "user", "premium", "postgresql"],
        "metadata": {
            "last_login": "2024-01-15T10:30:00Z",
            "login_count": 42,
            "is_verified": True,
            "database_info": {"engine": "PostgreSQL", "version": "15+"},
        },
    }

    # CREATE
    await sync_session_store.set(session_id, test_data, expires_in=3600)

    # READ
    retrieved_data = await sync_session_store.get(session_id)
    assert retrieved_data == test_data

    # UPDATE (overwrite)
    updated_data = {**test_data, "last_activity": "2024-01-15T11:00:00Z", "postgres_updated": True}
    await sync_session_store.set(session_id, updated_data, expires_in=3600)

    retrieved_updated = await sync_session_store.get(session_id)
    assert retrieved_updated == updated_data
    assert "last_activity" in retrieved_updated
    assert retrieved_updated["postgres_updated"] is True

    # EXISTS
    assert await sync_session_store.exists(session_id) is True
    assert await sync_session_store.exists("nonexistent") is False

    # EXPIRES_IN
    expires_in = await sync_session_store.expires_in(session_id)
    assert 3500 < expires_in <= 3600  # Should be close to 3600

    # DELETE
    await sync_session_store.delete(session_id)

    # Verify deletion
    assert await sync_session_store.get(session_id) is None
    assert await sync_session_store.exists(session_id) is False


async def test_async_store_crud_operations(async_session_store: SQLSpecSessionStore) -> None:
    """Test direct store CRUD operations with async driver."""
    session_id = "test-async-session-crud"

    # Test data with PostgreSQL-specific types
    test_data = {
        "user_id": 54321,
        "username": "postgres_async_testuser",
        "preferences": {
            "theme": "light",
            "language": "es",
            "notifications": False,
            "postgres_settings": {"jsonb_ops": True, "async_pool": True},
        },
        "tags": ["editor", "reviewer", "postgresql", "async"],
        "metadata": {
            "last_login": "2024-01-16T14:30:00Z",
            "login_count": 84,
            "is_verified": True,
            "database_info": {"engine": "PostgreSQL", "version": "15+", "async": True},
        },
    }

    # CREATE
    await async_session_store.set(session_id, test_data, expires_in=3600)

    # READ
    retrieved_data = await async_session_store.get(session_id)
    assert retrieved_data == test_data

    # UPDATE (overwrite)
    updated_data = {**test_data, "last_activity": "2024-01-16T15:00:00Z", "postgres_updated": True}
    await async_session_store.set(session_id, updated_data, expires_in=3600)

    retrieved_updated = await async_session_store.get(session_id)
    assert retrieved_updated == updated_data
    assert "last_activity" in retrieved_updated
    assert retrieved_updated["postgres_updated"] is True

    # EXISTS
    assert await async_session_store.exists(session_id) is True
    assert await async_session_store.exists("nonexistent") is False

    # EXPIRES_IN
    expires_in = await async_session_store.expires_in(session_id)
    assert 3500 < expires_in <= 3600  # Should be close to 3600

    # DELETE
    await async_session_store.delete(session_id)

    # Verify deletion
    assert await async_session_store.get(session_id) is None
    assert await async_session_store.exists(session_id) is False


async def test_sync_large_data_handling(sync_session_store: SQLSpecSessionStore) -> None:
    """Test handling of large session data with sync driver."""
    session_id = "test-sync-large-data"

    # Create large data structure
    large_data = {
        "postgres_info": {
            "engine": "PostgreSQL",
            "version": "15+",
            "features": ["JSONB", "ACID", "MVCC", "WAL", "GIN", "BTREE"],
            "connection_type": "sync",
        },
        "large_array": list(range(5000)),  # 5k integers
        "large_text": "PostgreSQL " * 10000,  # Large text with PostgreSQL
        "nested_structure": {
            f"postgres_key_{i}": {
                "value": f"postgres_data_{i}",
                "numbers": list(range(i, i + 50)),
                "text": f"{'PostgreSQL_content_' * 50}{i}",
                "metadata": {"created": f"2024-01-{(i % 28) + 1:02d}", "postgres": True},
            }
            for i in range(100)  # 100 nested objects
        },
        "metadata": {
            "size": "large",
            "created_at": "2024-01-15T10:30:00Z",
            "version": 1,
            "database": "PostgreSQL",
            "driver": "psycopg_sync",
        },
    }

    # Store large data
    await sync_session_store.set(session_id, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = await sync_session_store.get(session_id)
    assert retrieved_data == large_data
    assert len(retrieved_data["large_array"]) == 5000
    assert "PostgreSQL" in retrieved_data["large_text"]
    assert len(retrieved_data["nested_structure"]) == 100
    assert retrieved_data["metadata"]["database"] == "PostgreSQL"

    # Cleanup
    await sync_session_store.delete(session_id)


async def test_async_large_data_handling(async_session_store: SQLSpecSessionStore) -> None:
    """Test handling of large session data with async driver."""
    session_id = "test-async-large-data"

    # Create large data structure
    large_data = {
        "postgres_info": {
            "engine": "PostgreSQL",
            "version": "15+",
            "features": ["JSONB", "ACID", "MVCC", "WAL", "Async"],
            "connection_type": "async",
        },
        "large_array": list(range(7500)),  # 7.5k integers
        "large_text": "AsyncPostgreSQL " * 8000,  # Large text
        "nested_structure": {
            f"async_postgres_key_{i}": {
                "value": f"async_postgres_data_{i}",
                "numbers": list(range(i, i + 75)),
                "text": f"{'AsyncPostgreSQL_content_' * 40}{i}",
                "metadata": {"created": f"2024-01-{(i % 28) + 1:02d}", "async_postgres": True},
            }
            for i in range(125)  # 125 nested objects
        },
        "metadata": {
            "size": "large",
            "created_at": "2024-01-16T14:30:00Z",
            "version": 2,
            "database": "PostgreSQL",
            "driver": "psycopg_async",
        },
    }

    # Store large data
    await async_session_store.set(session_id, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = await async_session_store.get(session_id)
    assert retrieved_data == large_data
    assert len(retrieved_data["large_array"]) == 7500
    assert "AsyncPostgreSQL" in retrieved_data["large_text"]
    assert len(retrieved_data["nested_structure"]) == 125
    assert retrieved_data["metadata"]["database"] == "PostgreSQL"

    # Cleanup
    await async_session_store.delete(session_id)


def test_sync_complex_user_workflow(sync_litestar_app: Litestar) -> None:
    """Test a complex user workflow with sync driver."""
    with TestClient(app=sync_litestar_app) as client:
        # User registration workflow
        user_profile = {
            "user_id": 98765,
            "username": "postgres_sync_complex_user",
            "email": "complex@postgresql.sync.com",
            "profile": {
                "first_name": "PostgreSQL",
                "last_name": "SyncUser",
                "age": 35,
                "preferences": {
                    "theme": "dark",
                    "language": "en",
                    "notifications": {"email": True, "push": False, "sms": True},
                    "postgres_settings": {"jsonb_preference": True, "gin_index": True},
                },
            },
            "permissions": ["read", "write", "admin", "postgres_admin"],
            "last_login": "2024-01-15T10:30:00Z",
            "database_info": {"engine": "PostgreSQL", "driver": "psycopg_sync"},
        }

        # Set user profile
        response = client.put("/user/profile", json=user_profile)
        assert response.status_code == HTTP_200_OK

        # Verify profile was set
        response = client.get("/user/profile")
        assert response.status_code == HTTP_200_OK
        assert response.json()["profile"] == user_profile

        # Update session with additional activity data
        activity_data = {
            "page_views": 25,
            "session_start": "2024-01-15T10:30:00Z",
            "postgres_queries": [
                {"query": "SELECT * FROM users", "time": "10ms"},
                {"query": "INSERT INTO logs", "time": "5ms"},
            ],
        }

        response = client.post("/session/bulk", json=activity_data)
        assert response.status_code == HTTP_201_CREATED

        # Test counter functionality within complex session
        for i in range(1, 4):
            response = client.get("/counter")
            assert response.json()["count"] == i

        # Get all session data to verify everything is maintained
        response = client.get("/session/all")
        all_data = response.json()

        # Verify all data components are present
        assert "profile" in all_data
        assert all_data["profile"] == user_profile
        assert all_data["page_views"] == 25
        assert len(all_data["postgres_queries"]) == 2
        assert all_data["count"] == 3


async def test_async_complex_user_workflow(async_litestar_app: Litestar) -> None:
    """Test a complex user workflow with async driver."""
    async with AsyncTestClient(app=async_litestar_app) as client:
        # User registration workflow
        user_profile = {
            "user_id": 56789,
            "username": "postgres_async_complex_user",
            "email": "complex@postgresql.async.com",
            "profile": {
                "first_name": "PostgreSQL",
                "last_name": "AsyncUser",
                "age": 28,
                "preferences": {
                    "theme": "light",
                    "language": "es",
                    "notifications": {"email": False, "push": True, "sms": False},
                    "postgres_settings": {"async_pool": True, "connection_pooling": True},
                },
            },
            "permissions": ["read", "write", "editor", "async_admin"],
            "last_login": "2024-01-16T14:30:00Z",
            "database_info": {"engine": "PostgreSQL", "driver": "psycopg_async"},
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
            "page_views": 35,
            "session_start": "2024-01-16T14:30:00Z",
            "async_postgres_queries": [
                {"query": "SELECT * FROM async_users", "time": "8ms"},
                {"query": "INSERT INTO async_logs", "time": "3ms"},
                {"query": "UPDATE user_preferences", "time": "12ms"},
            ],
        }

        response = await client.post("/session/bulk", json=activity_data)
        assert response.status_code == HTTP_201_CREATED

        # Test counter functionality within complex session
        for i in range(1, 5):
            response = await client.get("/counter")
            assert response.json()["count"] == i

        # Get all session data to verify everything is maintained
        response = await client.get("/session/all")
        all_data = response.json()

        # Verify all data components are present
        assert "profile" in all_data
        assert all_data["profile"] == user_profile
        assert all_data["page_views"] == 35
        assert len(all_data["async_postgres_queries"]) == 3
        assert all_data["count"] == 4
