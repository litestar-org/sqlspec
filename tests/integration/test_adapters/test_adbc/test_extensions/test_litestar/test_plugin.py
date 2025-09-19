"""Comprehensive Litestar integration tests for ADBC adapter.

This test suite validates the full integration between SQLSpec's ADBC adapter
and Litestar's session middleware, including Arrow-native database connectivity
features across multiple database backends (PostgreSQL, SQLite, DuckDB, etc.).

ADBC is a sync-only adapter that provides efficient columnar data transfer
using the Arrow format for optimal performance.
"""

import asyncio
import time
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.stores.registry import StoreRegistry
from litestar.testing import TestClient

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore
from sqlspec.extensions.litestar.session import SQLSpecSessionConfig
from sqlspec.migrations.commands import SyncMigrationCommands
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing

pytestmark = [pytest.mark.adbc, pytest.mark.postgres, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
def migrated_config(adbc_migration_config: AdbcConfig) -> AdbcConfig:
    """Apply migrations once and return the config for ADBC (sync)."""
    commands = SyncMigrationCommands(adbc_migration_config)
    commands.init(adbc_migration_config.migration_config["script_location"], package=False)
    commands.upgrade()
    return adbc_migration_config


@pytest.fixture
def session_store(migrated_config: AdbcConfig) -> SQLSpecSessionStore:
    """Create a session store instance using the migrated database for ADBC."""
    return SQLSpecSessionStore(
        config=migrated_config,
        table_name="litestar_sessions_adbc",  # Use the unique table for ADBC
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


@pytest.fixture
def session_config() -> SQLSpecSessionConfig:
    """Create a session configuration instance for ADBC."""
    return SQLSpecSessionConfig(
        table_name="litestar_sessions_adbc",
        store="sessions",  # This will be the key in the stores registry
    )


@xfail_if_driver_missing
def test_session_store_creation(session_store: SQLSpecSessionStore) -> None:
    """Test that SessionStore can be created with ADBC configuration."""
    assert session_store is not None
    assert session_store._table_name == "litestar_sessions_adbc"
    assert session_store._session_id_column == "session_id"
    assert session_store._data_column == "data"
    assert session_store._expires_at_column == "expires_at"
    assert session_store._created_at_column == "created_at"


@xfail_if_driver_missing
def test_session_store_adbc_table_structure(session_store: SQLSpecSessionStore, migrated_config: AdbcConfig) -> None:
    """Test that session table is created with proper ADBC-compatible structure."""
    with migrated_config.provide_session() as driver:
        # Verify table exists with proper name
        result = driver.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_name = 'litestar_sessions_adbc'
            AND table_schema = 'public'
        """)
        assert len(result.data) == 1
        table_info = result.data[0]
        assert table_info["table_name"] == "litestar_sessions_adbc"
        assert table_info["table_type"] == "BASE TABLE"

        # Verify column structure
        result = driver.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'litestar_sessions_adbc'
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        columns = {row["column_name"]: row for row in result.data}

        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify data types for PostgreSQL
        assert columns["session_id"]["data_type"] in ("text", "character varying")
        assert columns["data"]["data_type"] == "jsonb"  # ADBC uses JSONB for efficient storage
        assert columns["expires_at"]["data_type"] in ("timestamp with time zone", "timestamptz")
        assert columns["created_at"]["data_type"] in ("timestamp with time zone", "timestamptz")

        # Verify index exists for expires_at
        result = driver.execute("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'litestar_sessions_adbc'
            AND schemaname = 'public'
        """)
        index_names = [row["indexname"] for row in result.data]
        assert any("expires_at" in name for name in index_names)


@xfail_if_driver_missing
def test_basic_session_operations(session_config: SQLSpecSessionConfig, session_store: SQLSpecSessionStore) -> None:
    """Test basic session operations through Litestar application with ADBC."""

    @get("/set-session")
    def set_session(request: Any) -> dict:
        request.session["user_id"] = 12345
        request.session["username"] = "adbc_user"
        request.session["preferences"] = {"theme": "dark", "language": "en", "timezone": "UTC"}
        request.session["roles"] = ["user", "editor", "adbc_admin"]
        request.session["adbc_info"] = {"engine": "ADBC", "version": "1.x", "arrow_native": True}
        return {"status": "session set"}

    @get("/get-session")
    def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "preferences": request.session.get("preferences"),
            "roles": request.session.get("roles"),
            "adbc_info": request.session.get("adbc_info"),
        }

    @post("/clear-session")
    def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(
        route_handlers=[set_session, get_session, clear_session], middleware=[session_config.middleware], stores=stores
    )

    with TestClient(app=app) as client:
        # Set session data
        response = client.get("/set-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session set"}

        # Get session data
        response = client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["user_id"] == 12345
        assert data["username"] == "adbc_user"
        assert data["preferences"]["theme"] == "dark"
        assert data["roles"] == ["user", "editor", "adbc_admin"]
        assert data["adbc_info"]["arrow_native"] is True

        # Clear session
        response = client.post("/clear-session")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {
            "user_id": None,
            "username": None,
            "preferences": None,
            "roles": None,
            "adbc_info": None,
        }


@xfail_if_driver_missing
def test_session_persistence_across_requests(
    session_config: SQLSpecSessionConfig, session_store: SQLSpecSessionStore
) -> None:
    """Test that sessions persist across multiple requests with ADBC."""

    @get("/document/create/{doc_id:int}")
    def create_document(request: Any, doc_id: int) -> dict:
        documents = request.session.get("documents", [])
        document = {
            "id": doc_id,
            "title": f"ADBC Document {doc_id}",
            "content": f"Content for document {doc_id}. " + "ADBC Arrow-native " * 20,
            "created_at": "2024-01-01T12:00:00Z",
            "metadata": {"engine": "ADBC", "arrow_format": True, "columnar": True},
        }
        documents.append(document)
        request.session["documents"] = documents
        request.session["document_count"] = len(documents)
        request.session["last_action"] = f"created_document_{doc_id}"
        return {"document": document, "total_docs": len(documents)}

    @get("/documents")
    def get_documents(request: Any) -> dict:
        return {
            "documents": request.session.get("documents", []),
            "count": request.session.get("document_count", 0),
            "last_action": request.session.get("last_action"),
        }

    @post("/documents/save-all")
    def save_all_documents(request: Any) -> dict:
        documents = request.session.get("documents", [])

        # Simulate saving all documents with ADBC efficiency
        saved_docs = {
            "saved_count": len(documents),
            "documents": documents,
            "saved_at": "2024-01-01T12:00:00Z",
            "adbc_arrow_batch": True,
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

    with TestClient(app=app) as client:
        # Create multiple documents
        response = client.get("/document/create/101")
        assert response.json()["total_docs"] == 1

        response = client.get("/document/create/102")
        assert response.json()["total_docs"] == 2

        response = client.get("/document/create/103")
        assert response.json()["total_docs"] == 3

        # Verify document persistence
        response = client.get("/documents")
        data = response.json()
        assert data["count"] == 3
        assert len(data["documents"]) == 3
        assert data["documents"][0]["id"] == 101
        assert data["documents"][0]["metadata"]["arrow_format"] is True
        assert data["last_action"] == "created_document_103"

        # Save all documents
        response = client.post("/documents/save-all")
        assert response.status_code == HTTP_201_CREATED
        save_data = response.json()
        assert save_data["status"] == "all documents saved"
        assert save_data["count"] == 3

        # Verify working documents are cleared but save session persists
        response = client.get("/documents")
        data = response.json()
        assert data["count"] == 0
        assert len(data["documents"]) == 0


@xfail_if_driver_missing
def test_session_expiration(migrated_config: AdbcConfig) -> None:
    """Test session expiration handling with ADBC."""
    # Create store and config with very short lifetime (migrations already applied by fixture)
    session_store = SQLSpecSessionStore(
        config=migrated_config,
        table_name="litestar_sessions_adbc",  # Use the migrated table
    )

    session_config = SQLSpecSessionConfig(
        table_name="litestar_sessions_adbc",
        store="sessions",
        max_age=1,  # 1 second
    )

    @get("/set-expiring-data")
    def set_data(request: Any) -> dict:
        request.session["test_data"] = "adbc_expiring_data"
        request.session["timestamp"] = "2024-01-01T00:00:00Z"
        request.session["database"] = "ADBC"
        request.session["arrow_native"] = True
        request.session["columnar_storage"] = True
        return {"status": "data set with short expiration"}

    @get("/get-expiring-data")
    def get_data(request: Any) -> dict:
        return {
            "test_data": request.session.get("test_data"),
            "timestamp": request.session.get("timestamp"),
            "database": request.session.get("database"),
            "arrow_native": request.session.get("arrow_native"),
            "columnar_storage": request.session.get("columnar_storage"),
        }

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(route_handlers=[set_data, get_data], middleware=[session_config.middleware], stores=stores)

    with TestClient(app=app) as client:
        # Set data
        response = client.get("/set-expiring-data")
        assert response.json() == {"status": "data set with short expiration"}

        # Data should be available immediately
        response = client.get("/get-expiring-data")
        data = response.json()
        assert data["test_data"] == "adbc_expiring_data"
        assert data["database"] == "ADBC"
        assert data["arrow_native"] is True

        # Wait for expiration
        time.sleep(2)

        # Data should be expired
        response = client.get("/get-expiring-data")
        assert response.json() == {
            "test_data": None,
            "timestamp": None,
            "database": None,
            "arrow_native": None,
            "columnar_storage": None,
        }


@xfail_if_driver_missing
def test_large_data_handling_adbc(session_config: SQLSpecSessionConfig, session_store: SQLSpecSessionStore) -> None:
    """Test handling of large data structures with ADBC Arrow format optimization."""

    @post("/save-large-adbc-dataset")
    def save_large_data(request: Any) -> dict:
        # Create a large data structure to test ADBC's Arrow format capacity
        large_dataset = {
            "database_info": {
                "engine": "ADBC",
                "version": "1.x",
                "features": ["Arrow-native", "Columnar", "Multi-database", "Zero-copy", "High-performance"],
                "arrow_format": True,
                "backends": ["PostgreSQL", "SQLite", "DuckDB", "BigQuery", "Snowflake"],
            },
            "test_data": {
                "records": [
                    {
                        "id": i,
                        "name": f"ADBC Record {i}",
                        "description": f"This is an Arrow-optimized record {i}. " + "ADBC " * 50,
                        "metadata": {
                            "created_at": f"2024-01-{(i % 28) + 1:02d}T12:00:00Z",
                            "tags": [f"adbc_tag_{j}" for j in range(20)],
                            "arrow_properties": {
                                f"prop_{k}": {
                                    "value": f"adbc_value_{k}",
                                    "type": "arrow_string" if k % 2 == 0 else "arrow_number",
                                    "columnar": k % 3 == 0,
                                }
                                for k in range(25)
                            },
                        },
                        "columnar_data": {
                            "text": f"Large columnar content for record {i}. " + "Arrow " * 100,
                            "data": list(range(i * 10, (i + 1) * 10)),
                        },
                    }
                    for i in range(150)  # Test ADBC's columnar storage capacity
                ],
                "analytics": {
                    "summary": {"total_records": 150, "database": "ADBC", "format": "Arrow", "compressed": True},
                    "metrics": [
                        {
                            "date": f"2024-{month:02d}-{day:02d}",
                            "adbc_operations": {
                                "arrow_reads": day * month * 10,
                                "columnar_writes": day * month * 50,
                                "batch_operations": day * month * 5,
                                "zero_copy_transfers": day * month * 2,
                            },
                        }
                        for month in range(1, 13)
                        for day in range(1, 29)
                    ],
                },
            },
            "adbc_configuration": {
                "driver_settings": {f"setting_{i}": {"value": f"adbc_setting_{i}", "active": True} for i in range(75)},
                "connection_info": {
                    "arrow_batch_size": 1000,
                    "timeout": 30,
                    "compression": "snappy",
                    "columnar_format": "arrow",
                },
            },
        }

        request.session["large_dataset"] = large_dataset
        request.session["dataset_size"] = len(str(large_dataset))
        request.session["adbc_metadata"] = {
            "engine": "ADBC",
            "storage_type": "JSONB",
            "compressed": True,
            "arrow_optimized": True,
        }

        return {
            "status": "large dataset saved to ADBC",
            "records_count": len(large_dataset["test_data"]["records"]),
            "metrics_count": len(large_dataset["test_data"]["analytics"]["metrics"]),
            "settings_count": len(large_dataset["adbc_configuration"]["driver_settings"]),
        }

    @get("/load-large-adbc-dataset")
    def load_large_data(request: Any) -> dict:
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
            "adbc_metadata": request.session.get("adbc_metadata"),
        }

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(
        route_handlers=[save_large_data, load_large_data], middleware=[session_config.middleware], stores=stores
    )

    with TestClient(app=app) as client:
        # Save large dataset
        response = client.post("/save-large-adbc-dataset")
        assert response.status_code == HTTP_201_CREATED
        data = response.json()
        assert data["status"] == "large dataset saved to ADBC"
        assert data["records_count"] == 150
        assert data["metrics_count"] > 300  # 12 months * ~28 days
        assert data["settings_count"] == 75

        # Load and verify large dataset
        response = client.get("/load-large-adbc-dataset")
        data = response.json()
        assert data["has_data"] is True
        assert data["records_count"] == 150
        assert data["first_record"]["name"] == "ADBC Record 0"
        assert data["database_info"]["arrow_format"] is True
        assert data["dataset_size"] > 50000  # Should be a substantial size
        assert data["adbc_metadata"]["arrow_optimized"] is True


@xfail_if_driver_missing
def test_session_cleanup_and_maintenance(adbc_migration_config: AdbcConfig) -> None:
    """Test session cleanup and maintenance operations with ADBC."""
    # Apply migrations first
    commands = SyncMigrationCommands(adbc_migration_config)
    commands.init(adbc_migration_config.migration_config["script_location"], package=False)
    commands.upgrade()

    store = SQLSpecSessionStore(
        config=adbc_migration_config,
        table_name="litestar_sessions_adbc",  # Use the migrated table
    )

    # Create sessions with different lifetimes using the public async API
    # The store handles sync/async conversion internally

    async def setup_and_test_sessions() -> None:
        temp_sessions = []
        for i in range(8):
            session_id = f"adbc_temp_session_{i}"
            temp_sessions.append(session_id)
            await store.set(
                session_id,
                {
                    "data": i,
                    "type": "temporary",
                    "adbc_engine": "arrow",
                    "created_for": "cleanup_test",
                    "columnar_format": True,
                },
                expires_in=1,
            )

        # Create permanent sessions
        perm_sessions = []
        for i in range(4):
            session_id = f"adbc_perm_session_{i}"
            perm_sessions.append(session_id)
            await store.set(
                session_id,
                {
                    "data": f"permanent_{i}",
                    "type": "permanent",
                    "adbc_engine": "arrow",
                    "created_for": "cleanup_test",
                    "durable": True,
                },
                expires_in=3600,
            )

        # Verify all sessions exist initially
        for session_id in temp_sessions + perm_sessions:
            result = await store.get(session_id)
            assert result is not None
            assert result["adbc_engine"] == "arrow"

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

    asyncio.run(setup_and_test_sessions())


@xfail_if_driver_missing
def test_migration_with_default_table_name(adbc_migration_config: AdbcConfig) -> None:
    """Test that migration with string format creates default table name for ADBC."""
    # Apply migrations
    commands = SyncMigrationCommands(adbc_migration_config)
    commands.init(adbc_migration_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Create store using the migrated table
    store = SQLSpecSessionStore(
        config=adbc_migration_config,
        table_name="litestar_sessions_adbc",  # Default table name
    )

    # Test that the store works with the migrated table
    async def test_store() -> None:
        session_id = "test_session_default"
        test_data = {"user_id": 1, "username": "test_user", "adbc_features": {"arrow_native": True}}

        await store.set(session_id, test_data, expires_in=3600)
        retrieved = await store.get(session_id)

        assert retrieved == test_data

    asyncio.run(test_store())


@xfail_if_driver_missing
def test_migration_with_custom_table_name(adbc_migration_config_with_dict: AdbcConfig) -> None:
    """Test that migration with dict format creates custom table name for ADBC."""
    # Apply migrations
    commands = SyncMigrationCommands(adbc_migration_config_with_dict)
    commands.init(adbc_migration_config_with_dict.migration_config["script_location"], package=False)
    commands.upgrade()

    # Create store using the custom migrated table
    store = SQLSpecSessionStore(
        config=adbc_migration_config_with_dict,
        table_name="custom_adbc_sessions",  # Custom table name from config
    )

    # Test that the store works with the custom table
    async def test_custom_table() -> None:
        session_id = "test_session_custom"
        test_data = {"user_id": 2, "username": "custom_user", "adbc_features": {"arrow_native": True}}

        await store.set(session_id, test_data, expires_in=3600)
        retrieved = await store.get(session_id)

        assert retrieved == test_data

    asyncio.run(test_custom_table())

    # Verify custom table exists and has correct structure
    with adbc_migration_config_with_dict.provide_session() as driver:
        result = driver.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = 'custom_adbc_sessions'
            AND table_schema = 'public'
        """)
        assert len(result.data) == 1
        assert result.data[0]["table_name"] == "custom_adbc_sessions"


@xfail_if_driver_missing
def test_migration_with_mixed_extensions(adbc_migration_config_mixed: AdbcConfig) -> None:
    """Test migration with mixed extension formats for ADBC."""
    # Apply migrations
    commands = SyncMigrationCommands(adbc_migration_config_mixed)
    commands.init(adbc_migration_config_mixed.migration_config["script_location"], package=False)
    commands.upgrade()

    # The litestar extension should use default table name
    store = SQLSpecSessionStore(
        config=adbc_migration_config_mixed,
        table_name="litestar_sessions_adbc",  # Default since string format was used
    )

    # Test that the store works
    async def test_mixed_extensions() -> None:
        session_id = "test_session_mixed"
        test_data = {"user_id": 3, "username": "mixed_user", "adbc_features": {"arrow_native": True}}

        await store.set(session_id, test_data, expires_in=3600)
        retrieved = await store.get(session_id)

        assert retrieved == test_data

    asyncio.run(test_mixed_extensions())
