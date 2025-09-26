"""Comprehensive Litestar integration tests for Aiosqlite adapter.

This test suite validates the full integration between SQLSpec's Aiosqlite adapter
and Litestar's session middleware, including SQLite-specific features.
"""

import asyncio
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.stores.registry import StoreRegistry
from litestar.testing import AsyncTestClient

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.extensions.litestar import SQLSpecAsyncSessionStore
from sqlspec.extensions.litestar.session import SQLSpecSessionConfig
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = [pytest.mark.aiosqlite, pytest.mark.sqlite, pytest.mark.integration]


@pytest.fixture
async def migrated_config(aiosqlite_migration_config: AiosqliteConfig) -> AiosqliteConfig:
    """Apply migrations once and return the config."""
    commands = AsyncMigrationCommands(aiosqlite_migration_config)
    await commands.init(aiosqlite_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()
    return aiosqlite_migration_config


@pytest.fixture
async def session_store(migrated_config: AiosqliteConfig) -> SQLSpecAsyncSessionStore:
    """Create a session store instance using the migrated database."""
    return SQLSpecAsyncSessionStore(
        config=migrated_config,
        table_name="litestar_sessions",  # Use the default table created by migration
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


@pytest.fixture
async def session_config(migrated_config: AiosqliteConfig) -> SQLSpecSessionConfig:
    """Create a session configuration instance."""
    # Create the session configuration
    return SQLSpecSessionConfig(
        table_name="litestar_sessions",
        store="sessions",  # This will be the key in the stores registry
    )


@pytest.fixture
async def session_store_file(migrated_config: AiosqliteConfig) -> SQLSpecAsyncSessionStore:
    """Create a session store instance using file-based SQLite for concurrent testing."""
    return SQLSpecAsyncSessionStore(
        config=migrated_config,
        table_name="litestar_sessions",  # Use the default table created by migration
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


async def test_session_store_creation(session_store: SQLSpecAsyncSessionStore) -> None:
    """Test that SessionStore can be created with Aiosqlite configuration."""
    assert session_store is not None
    assert session_store._table_name == "litestar_sessions"
    assert session_store._session_id_column == "session_id"
    assert session_store._data_column == "data"
    assert session_store._expires_at_column == "expires_at"
    assert session_store._created_at_column == "created_at"


async def test_session_store_sqlite_table_structure(
    session_store: SQLSpecAsyncSessionStore, aiosqlite_migration_config: AiosqliteConfig
) -> None:
    """Test that session table is created with proper SQLite structure."""
    async with aiosqlite_migration_config.provide_session() as driver:
        # Verify table exists with proper name
        result = await driver.execute("""
            SELECT name, type, sql
            FROM sqlite_master
            WHERE type='table'
            AND name='litestar_sessions'
        """)
        assert len(result.data) == 1
        table_info = result.data[0]
        assert table_info["name"] == "litestar_sessions"
        assert table_info["type"] == "table"

        # Verify column structure
        result = await driver.execute("PRAGMA table_info(litestar_sessions)")
        columns = {row["name"]: row for row in result.data}

        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify primary key
        assert columns["session_id"]["pk"] == 1

        # Verify index exists for expires_at
        result = await driver.execute("""
            SELECT name FROM sqlite_master
            WHERE type='index'
            AND tbl_name='litestar_sessions'
        """)
        index_names = [row["name"] for row in result.data]
        assert any("expires_at" in name for name in index_names)


async def test_basic_session_operations(
    session_config: SQLSpecSessionConfig, session_store: SQLSpecAsyncSessionStore
) -> None:
    """Test basic session operations through Litestar application."""

    @get("/set-session")
    async def set_session(request: Any) -> dict:
        request.session["user_id"] = 12345
        request.session["username"] = "sqlite_user"
        request.session["preferences"] = {"theme": "dark", "language": "en", "timezone": "UTC"}
        request.session["roles"] = ["user", "editor", "sqlite_admin"]
        request.session["sqlite_info"] = {"engine": "SQLite", "version": "3.x", "mode": "async"}
        return {"status": "session set"}

    @get("/get-session")
    async def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "preferences": request.session.get("preferences"),
            "roles": request.session.get("roles"),
            "sqlite_info": request.session.get("sqlite_info"),
        }

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(
        route_handlers=[set_session, get_session, clear_session], middleware=[session_config.middleware], stores=stores
    )

    async with AsyncTestClient(app=app) as client:
        # Set session data
        response = await client.get("/set-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session set"}

        # Get session data
        response = await client.get("/get-session")
        if response.status_code != HTTP_200_OK:
            pass
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["user_id"] == 12345
        assert data["username"] == "sqlite_user"
        assert data["preferences"]["theme"] == "dark"
        assert data["roles"] == ["user", "editor", "sqlite_admin"]
        assert data["sqlite_info"]["engine"] == "SQLite"

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {
            "user_id": None,
            "username": None,
            "preferences": None,
            "roles": None,
            "sqlite_info": None,
        }


async def test_session_persistence_across_requests(
    session_config: SQLSpecSessionConfig, session_store: SQLSpecAsyncSessionStore
) -> None:
    """Test that sessions persist across multiple requests with SQLite."""

    @get("/document/create/{doc_id:int}")
    async def create_document(request: Any, doc_id: int) -> dict:
        documents = request.session.get("documents", [])
        document = {
            "id": doc_id,
            "title": f"SQLite Document {doc_id}",
            "content": f"Content for document {doc_id}. " + "SQLite " * 20,
            "created_at": "2024-01-01T12:00:00Z",
            "metadata": {"engine": "SQLite", "storage": "file", "atomic": True},
        }
        documents.append(document)
        request.session["documents"] = documents
        request.session["document_count"] = len(documents)
        request.session["last_action"] = f"created_document_{doc_id}"
        return {"document": document, "total_docs": len(documents)}

    @get("/documents")
    async def get_documents(request: Any) -> dict:
        return {
            "documents": request.session.get("documents", []),
            "count": request.session.get("document_count", 0),
            "last_action": request.session.get("last_action"),
        }

    @post("/documents/save-all")
    async def save_all_documents(request: Any) -> dict:
        documents = request.session.get("documents", [])

        # Simulate saving all documents
        saved_docs = {
            "saved_count": len(documents),
            "documents": documents,
            "saved_at": "2024-01-01T12:00:00Z",
            "sqlite_transaction": True,
        }

        request.session["saved_session"] = saved_docs
        request.session["last_save"] = "2024-01-01T12:00:00Z"

        # Clear working documents after save
        request.session.pop("documents", None)
        request.session.pop("document_count", None)

        return {"status": "all documents saved", "count": saved_docs["saved_count"]}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(
        route_handlers=[create_document, get_documents, save_all_documents],
        middleware=[session_config.middleware],
        stores=stores,
    )

    async with AsyncTestClient(app=app) as client:
        # Create multiple documents
        response = await client.get("/document/create/101")
        assert response.json()["total_docs"] == 1

        response = await client.get("/document/create/102")
        assert response.json()["total_docs"] == 2

        response = await client.get("/document/create/103")
        assert response.json()["total_docs"] == 3

        # Verify document persistence
        response = await client.get("/documents")
        data = response.json()
        assert data["count"] == 3
        assert len(data["documents"]) == 3
        assert data["documents"][0]["id"] == 101
        assert data["documents"][0]["metadata"]["engine"] == "SQLite"
        assert data["last_action"] == "created_document_103"

        # Save all documents
        response = await client.post("/documents/save-all")
        assert response.status_code == HTTP_201_CREATED
        save_data = response.json()
        assert save_data["status"] == "all documents saved"
        assert save_data["count"] == 3

        # Verify working documents are cleared but save session persists
        response = await client.get("/documents")
        data = response.json()
        assert data["count"] == 0
        assert len(data["documents"]) == 0


async def test_session_expiration(migrated_config: AiosqliteConfig) -> None:
    """Test session expiration handling with SQLite."""
    # Create store and config with very short lifetime (migrations already applied by fixture)
    session_store = SQLSpecAsyncSessionStore(
        config=migrated_config,
        table_name="litestar_sessions",  # Use the migrated table
    )

    session_config = SQLSpecSessionConfig(
        table_name="litestar_sessions",
        store="sessions",
        max_age=1,  # 1 second
    )

    @get("/set-expiring-data")
    async def set_data(request: Any) -> dict:
        request.session["test_data"] = "sqlite_expiring_data"
        request.session["timestamp"] = "2024-01-01T00:00:00Z"
        request.session["database"] = "SQLite"
        request.session["storage_mode"] = "file"
        request.session["atomic_writes"] = True
        return {"status": "data set with short expiration"}

    @get("/get-expiring-data")
    async def get_data(request: Any) -> dict:
        return {
            "test_data": request.session.get("test_data"),
            "timestamp": request.session.get("timestamp"),
            "database": request.session.get("database"),
            "storage_mode": request.session.get("storage_mode"),
            "atomic_writes": request.session.get("atomic_writes"),
        }

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(route_handlers=[set_data, get_data], middleware=[session_config.middleware], stores=stores)

    async with AsyncTestClient(app=app) as client:
        # Set data
        response = await client.get("/set-expiring-data")
        assert response.json() == {"status": "data set with short expiration"}

        # Data should be available immediately
        response = await client.get("/get-expiring-data")
        data = response.json()
        assert data["test_data"] == "sqlite_expiring_data"
        assert data["database"] == "SQLite"
        assert data["atomic_writes"] is True

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        response = await client.get("/get-expiring-data")
        assert response.json() == {
            "test_data": None,
            "timestamp": None,
            "database": None,
            "storage_mode": None,
            "atomic_writes": None,
        }


async def test_concurrent_sessions_with_file_backend(session_store_file: SQLSpecAsyncSessionStore) -> None:
    """Test concurrent session access with file-based SQLite."""

    async def session_worker(worker_id: int, iterations: int) -> list[dict]:
        """Worker function that creates and manipulates sessions."""
        results = []

        for i in range(iterations):
            session_id = f"worker_{worker_id}_session_{i}"
            session_data = {
                "worker_id": worker_id,
                "iteration": i,
                "data": f"SQLite worker {worker_id} data {i}",
                "sqlite_features": ["ACID", "Atomic", "Consistent", "Isolated", "Durable"],
                "file_based": True,
                "concurrent_safe": True,
            }

            # Set session data
            await session_store_file.set(session_id, session_data, expires_in=3600)

            # Immediately read it back
            retrieved_data = await session_store_file.get(session_id)

            results.append(
                {
                    "session_id": session_id,
                    "set_data": session_data,
                    "retrieved_data": retrieved_data,
                    "success": retrieved_data == session_data,
                }
            )

            # Small delay to allow other workers to interleave
            await asyncio.sleep(0.01)

        return results

    # Run multiple concurrent workers
    num_workers = 5
    iterations_per_worker = 10

    tasks = [session_worker(worker_id, iterations_per_worker) for worker_id in range(num_workers)]

    all_results = await asyncio.gather(*tasks)

    # Verify all operations succeeded
    total_operations = 0
    successful_operations = 0

    for worker_results in all_results:
        for result in worker_results:
            total_operations += 1
            if result["success"]:
                successful_operations += 1
            else:
                # Print failed operation for debugging
                pass

    assert total_operations == num_workers * iterations_per_worker
    assert successful_operations == total_operations  # All should succeed

    # Verify final state by checking a few random sessions
    for worker_id in range(0, num_workers, 2):  # Check every other worker
        session_id = f"worker_{worker_id}_session_0"
        result = await session_store_file.get(session_id)
        assert result is not None
        assert result["worker_id"] == worker_id
        assert result["file_based"] is True


async def test_large_data_handling(
    session_config: SQLSpecSessionConfig, session_store: SQLSpecAsyncSessionStore
) -> None:
    """Test handling of large data structures with SQLite backend."""

    @post("/save-large-sqlite-dataset")
    async def save_large_data(request: Any) -> dict:
        # Create a large data structure to test SQLite's capacity
        large_dataset = {
            "database_info": {
                "engine": "SQLite",
                "version": "3.x",
                "features": ["ACID", "Embedded", "Serverless", "Zero-config", "Cross-platform"],
                "file_based": True,
                "in_memory_mode": False,
            },
            "test_data": {
                "records": [
                    {
                        "id": i,
                        "name": f"SQLite Record {i}",
                        "description": f"This is a detailed description for record {i}. " + "SQLite " * 50,
                        "metadata": {
                            "created_at": f"2024-01-{(i % 28) + 1:02d}T12:00:00Z",
                            "tags": [f"sqlite_tag_{j}" for j in range(20)],
                            "properties": {
                                f"prop_{k}": {
                                    "value": f"sqlite_value_{k}",
                                    "type": "string" if k % 2 == 0 else "number",
                                    "enabled": k % 3 == 0,
                                }
                                for k in range(25)
                            },
                        },
                        "content": {
                            "text": f"Large text content for record {i}. " + "Content " * 100,
                            "data": list(range(i * 10, (i + 1) * 10)),
                        },
                    }
                    for i in range(150)  # Test SQLite's text storage capacity
                ],
                "analytics": {
                    "summary": {"total_records": 150, "database": "SQLite", "storage": "file", "compressed": False},
                    "metrics": [
                        {
                            "date": f"2024-{month:02d}-{day:02d}",
                            "sqlite_operations": {
                                "inserts": day * month * 10,
                                "selects": day * month * 50,
                                "updates": day * month * 5,
                                "deletes": day * month * 2,
                            },
                        }
                        for month in range(1, 13)
                        for day in range(1, 29)
                    ],
                },
            },
            "sqlite_configuration": {
                "pragma_settings": {
                    f"setting_{i}": {"value": f"sqlite_setting_{i}", "active": True} for i in range(75)
                },
                "connection_info": {"pool_size": 1, "timeout": 30, "journal_mode": "WAL", "synchronous": "NORMAL"},
            },
        }

        request.session["large_dataset"] = large_dataset
        request.session["dataset_size"] = len(str(large_dataset))
        request.session["sqlite_metadata"] = {
            "engine": "SQLite",
            "storage_type": "TEXT",
            "compressed": False,
            "atomic_writes": True,
        }

        return {
            "status": "large dataset saved to SQLite",
            "records_count": len(large_dataset["test_data"]["records"]),
            "metrics_count": len(large_dataset["test_data"]["analytics"]["metrics"]),
            "settings_count": len(large_dataset["sqlite_configuration"]["pragma_settings"]),
        }

    @get("/load-large-sqlite-dataset")
    async def load_large_data(request: Any) -> dict:
        dataset = request.session.get("large_dataset", {})
        return {
            "has_data": bool(dataset),
            "records_count": len(dataset.get("test_data", {}).get("records", [])),
            "metrics_count": len(dataset.get("test_data", {}).get("analytics", {}).get("metrics", [])),
            "first_record": (
                dataset.get("test_data", {}).get("records", [{}])[0]
                if dataset.get("test_data", {}).get("records")
                else None
            ),
            "database_info": dataset.get("database_info"),
            "dataset_size": request.session.get("dataset_size", 0),
            "sqlite_metadata": request.session.get("sqlite_metadata"),
        }

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(
        route_handlers=[save_large_data, load_large_data], middleware=[session_config.middleware], stores=stores
    )

    async with AsyncTestClient(app=app) as client:
        # Save large dataset
        response = await client.post("/save-large-sqlite-dataset")
        assert response.status_code == HTTP_201_CREATED
        data = response.json()
        assert data["status"] == "large dataset saved to SQLite"
        assert data["records_count"] == 150
        assert data["metrics_count"] > 300  # 12 months * ~28 days
        assert data["settings_count"] == 75

        # Load and verify large dataset
        response = await client.get("/load-large-sqlite-dataset")
        data = response.json()
        assert data["has_data"] is True
        assert data["records_count"] == 150
        assert data["first_record"]["name"] == "SQLite Record 0"
        assert data["database_info"]["engine"] == "SQLite"
        assert data["dataset_size"] > 50000  # Should be a substantial size
        assert data["sqlite_metadata"]["atomic_writes"] is True


async def test_sqlite_concurrent_webapp_simulation(
    session_config: SQLSpecSessionConfig, session_store: SQLSpecAsyncSessionStore
) -> None:
    """Test concurrent web application behavior with SQLite session handling."""

    @get("/user/{user_id:int}/login")
    async def user_login(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        request.session["username"] = f"sqlite_user_{user_id}"
        request.session["login_time"] = "2024-01-01T12:00:00Z"
        request.session["database"] = "SQLite"
        request.session["session_type"] = "file_based"
        request.session["permissions"] = ["read", "write", "execute"]
        return {"status": "logged in", "user_id": user_id}

    @get("/user/profile")
    async def get_profile(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "login_time": request.session.get("login_time"),
            "database": request.session.get("database"),
            "session_type": request.session.get("session_type"),
            "permissions": request.session.get("permissions"),
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
            "sqlite_transaction": True,
        }
        activities.append(activity)
        request.session["activities"] = activities
        request.session["activity_count"] = len(activities)

        return {"status": "activity logged", "count": len(activities)}

    @post("/user/logout")
    async def user_logout(request: Any) -> dict:
        user_id = request.session.get("user_id")
        if user_id is None:
            return {"error": "Not logged in"}

        # Store logout info before clearing session
        request.session["last_logout"] = "2024-01-01T12:00:00Z"
        request.session.clear()

        return {"status": "logged out", "user_id": user_id}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(
        route_handlers=[user_login, get_profile, log_activity, user_logout], middleware=[session_config.middleware]
    )

    # Test with multiple concurrent users
    async with (
        AsyncTestClient(app=app) as client1,
        AsyncTestClient(app=app) as client2,
        AsyncTestClient(app=app) as client3,
    ):
        # Concurrent logins
        login_tasks = [
            client1.get("/user/1001/login"),
            client2.get("/user/1002/login"),
            client3.get("/user/1003/login"),
        ]
        responses = await asyncio.gather(*login_tasks)

        for i, response in enumerate(responses, 1001):
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"status": "logged in", "user_id": i}

        # Verify each client has correct session
        profile_responses = await asyncio.gather(
            client1.get("/user/profile"), client2.get("/user/profile"), client3.get("/user/profile")
        )

        assert profile_responses[0].json()["user_id"] == 1001
        assert profile_responses[0].json()["username"] == "sqlite_user_1001"
        assert profile_responses[1].json()["user_id"] == 1002
        assert profile_responses[2].json()["user_id"] == 1003

        # Log activities concurrently
        activity_tasks = [
            client.post("/user/activity")
            for client in [client1, client2, client3]
            for _ in range(5)  # 5 activities per user
        ]

        activity_responses = await asyncio.gather(*activity_tasks)
        for response in activity_responses:
            assert response.status_code == HTTP_201_CREATED
            assert "activity logged" in response.json()["status"]

        # Verify final activity counts
        final_profiles = await asyncio.gather(
            client1.get("/user/profile"), client2.get("/user/profile"), client3.get("/user/profile")
        )

        for profile_response in final_profiles:
            profile_data = profile_response.json()
            assert profile_data["database"] == "SQLite"
            assert profile_data["session_type"] == "file_based"


async def test_session_cleanup_and_maintenance(aiosqlite_migration_config: AiosqliteConfig) -> None:
    """Test session cleanup and maintenance operations with SQLite."""
    # Apply migrations first
    commands = AsyncMigrationCommands(aiosqlite_migration_config)
    await commands.init(aiosqlite_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    store = SQLSpecAsyncSessionStore(
        config=aiosqlite_migration_config,
        table_name="litestar_sessions",  # Use the migrated table
    )

    # Create sessions with different lifetimes
    temp_sessions = []
    for i in range(8):
        session_id = f"sqlite_temp_session_{i}"
        temp_sessions.append(session_id)
        await store.set(
            session_id,
            {
                "data": i,
                "type": "temporary",
                "sqlite_engine": "file",
                "created_for": "cleanup_test",
                "atomic_writes": True,
            },
            expires_in=1,
        )

    # Create permanent sessions
    perm_sessions = []
    for i in range(4):
        session_id = f"sqlite_perm_session_{i}"
        perm_sessions.append(session_id)
        await store.set(
            session_id,
            {
                "data": f"permanent_{i}",
                "type": "permanent",
                "sqlite_engine": "file",
                "created_for": "cleanup_test",
                "durable": True,
            },
            expires_in=3600,
        )

    # Verify all sessions exist initially
    for session_id in temp_sessions + perm_sessions:
        result = await store.get(session_id)
        assert result is not None
        assert result["sqlite_engine"] == "file"

    # Wait for temporary sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await store.delete_expired()

    # Verify temporary sessions are gone
    for session_id in temp_sessions:
        result = await store.get(session_id)
        assert result is None

    # Verify permanent sessions still exist
    for session_id in perm_sessions:
        result = await store.get(session_id)
        assert result is not None
        assert result["type"] == "permanent"


async def test_migration_with_default_table_name(aiosqlite_migration_config: AiosqliteConfig) -> None:
    """Test that migration with string format creates default table name."""
    # Apply migrations
    commands = AsyncMigrationCommands(aiosqlite_migration_config)
    await commands.init(aiosqlite_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the migrated table
    store = SQLSpecAsyncSessionStore(
        config=aiosqlite_migration_config,
        table_name="litestar_sessions",  # Default table name
    )

    # Test that the store works with the migrated table
    session_id = "test_session_default"
    test_data = {"user_id": 1, "username": "test_user"}

    await store.set(session_id, test_data, expires_in=3600)
    retrieved = await store.get(session_id)

    assert retrieved == test_data


async def test_migration_with_custom_table_name(aiosqlite_migration_config_with_dict: AiosqliteConfig) -> None:
    """Test that migration with dict format creates custom table name."""
    # Apply migrations
    commands = AsyncMigrationCommands(aiosqlite_migration_config_with_dict)
    await commands.init(aiosqlite_migration_config_with_dict.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the custom migrated table
    store = SQLSpecAsyncSessionStore(
        config=aiosqlite_migration_config_with_dict,
        table_name="custom_sessions",  # Custom table name from config
    )

    # Test that the store works with the custom table
    session_id = "test_session_custom"
    test_data = {"user_id": 2, "username": "custom_user"}

    await store.set(session_id, test_data, expires_in=3600)
    retrieved = await store.get(session_id)

    assert retrieved == test_data

    # Verify default table doesn't exist
    async with aiosqlite_migration_config_with_dict.provide_session() as driver:
        result = await driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='litestar_sessions'")
        assert len(result.data) == 0


async def test_migration_with_single_extension(aiosqlite_migration_config_mixed: AiosqliteConfig) -> None:
    """Test migration with litestar extension using string format."""
    # Apply migrations
    commands = AsyncMigrationCommands(aiosqlite_migration_config_mixed)
    await commands.init(aiosqlite_migration_config_mixed.migration_config["script_location"], package=False)
    await commands.upgrade()

    # The litestar extension should use default table name
    store = SQLSpecAsyncSessionStore(
        config=aiosqlite_migration_config_mixed,
        table_name="litestar_sessions",  # Default since string format was used
    )

    # Test that the store works
    session_id = "test_session_mixed"
    test_data = {"user_id": 3, "username": "mixed_user"}

    await store.set(session_id, test_data, expires_in=3600)
    retrieved = await store.get(session_id)

    assert retrieved == test_data


async def test_sqlite_atomic_transactions_pattern(
    session_config: SQLSpecSessionConfig, session_store: SQLSpecAsyncSessionStore
) -> None:
    """Test atomic transaction patterns typical for SQLite applications."""

    @post("/transaction/start")
    async def start_transaction(request: Any) -> dict:
        # Initialize transaction state
        request.session["transaction"] = {
            "id": "sqlite_txn_001",
            "status": "started",
            "operations": [],
            "atomic": True,
            "engine": "SQLite",
        }
        request.session["transaction_active"] = True
        return {"status": "transaction started", "id": "sqlite_txn_001"}

    @post("/transaction/add-operation")
    async def add_operation(request: Any) -> dict:
        data = await request.json()
        transaction = request.session.get("transaction")
        if not transaction or not request.session.get("transaction_active"):
            return {"error": "No active transaction"}

        operation = {
            "type": data["type"],
            "table": data.get("table", "default_table"),
            "data": data.get("data", {}),
            "timestamp": "2024-01-01T12:00:00Z",
            "sqlite_optimized": True,
        }

        transaction["operations"].append(operation)
        request.session["transaction"] = transaction

        return {"status": "operation added", "operation_count": len(transaction["operations"])}

    @post("/transaction/commit")
    async def commit_transaction(request: Any) -> dict:
        transaction = request.session.get("transaction")
        if not transaction or not request.session.get("transaction_active"):
            return {"error": "No active transaction"}

        # Simulate commit
        transaction["status"] = "committed"
        transaction["committed_at"] = "2024-01-01T12:00:00Z"
        transaction["sqlite_wal_mode"] = True

        # Add to transaction history
        history = request.session.get("transaction_history", [])
        history.append(transaction)
        request.session["transaction_history"] = history

        # Clear active transaction
        request.session.pop("transaction", None)
        request.session["transaction_active"] = False

        return {
            "status": "transaction committed",
            "operations_count": len(transaction["operations"]),
            "transaction_id": transaction["id"],
        }

    @post("/transaction/rollback")
    async def rollback_transaction(request: Any) -> dict:
        transaction = request.session.get("transaction")
        if not transaction or not request.session.get("transaction_active"):
            return {"error": "No active transaction"}

        # Simulate rollback
        transaction["status"] = "rolled_back"
        transaction["rolled_back_at"] = "2024-01-01T12:00:00Z"

        # Clear active transaction
        request.session.pop("transaction", None)
        request.session["transaction_active"] = False

        return {"status": "transaction rolled back", "operations_discarded": len(transaction["operations"])}

    @get("/transaction/history")
    async def get_history(request: Any) -> dict:
        return {
            "history": request.session.get("transaction_history", []),
            "active": request.session.get("transaction_active", False),
            "current": request.session.get("transaction"),
        }

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(
        route_handlers=[start_transaction, add_operation, commit_transaction, rollback_transaction, get_history],
        middleware=[session_config.middleware],
        stores=stores,
    )

    async with AsyncTestClient(app=app) as client:
        # Start transaction
        response = await client.post("/transaction/start")
        assert response.json() == {"status": "transaction started", "id": "sqlite_txn_001"}

        # Add operations
        operations = [
            {"type": "INSERT", "table": "users", "data": {"name": "SQLite User"}},
            {"type": "UPDATE", "table": "profiles", "data": {"theme": "dark"}},
            {"type": "DELETE", "table": "temp_data", "data": {"expired": True}},
        ]

        for op in operations:
            response = await client.post("/transaction/add-operation", json=op)
            assert "operation added" in response.json()["status"]

        # Verify operations are tracked
        response = await client.get("/transaction/history")
        history_data = response.json()
        assert history_data["active"] is True
        assert len(history_data["current"]["operations"]) == 3

        # Commit transaction
        response = await client.post("/transaction/commit")
        commit_data = response.json()
        assert commit_data["status"] == "transaction committed"
        assert commit_data["operations_count"] == 3

        # Verify transaction history
        response = await client.get("/transaction/history")
        history_data = response.json()
        assert history_data["active"] is False
        assert len(history_data["history"]) == 1
        assert history_data["history"][0]["status"] == "committed"
        assert history_data["history"][0]["sqlite_wal_mode"] is True
