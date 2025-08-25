"""Comprehensive Litestar integration tests for BigQuery adapter.

This test suite validates the full integration between SQLSpec's BigQuery adapter
and Litestar's session middleware, including BigQuery-specific features.
"""

from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.stores.registry import StoreRegistry
from litestar.testing import TestClient

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore
from sqlspec.extensions.litestar.session import SQLSpecSessionConfig
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.sync_tools import run_

pytestmark = [pytest.mark.bigquery, pytest.mark.integration]


@pytest.fixture
def migrated_config(bigquery_migration_config: BigQueryConfig) -> BigQueryConfig:
    """Apply migrations once and return the config."""
    commands = SyncMigrationCommands(bigquery_migration_config)
    commands.init(bigquery_migration_config.migration_config["script_location"], package=False)
    commands.upgrade()
    return bigquery_migration_config


@pytest.fixture
def session_store(migrated_config: BigQueryConfig) -> SQLSpecSessionStore:
    """Create a session store instance using the migrated database."""
    return SQLSpecSessionStore(
        config=migrated_config,
        table_name="litestar_sessions",  # Use the default table created by migration
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


@pytest.fixture
def session_config(migrated_config: BigQueryConfig) -> SQLSpecSessionConfig:
    """Create a session configuration instance."""
    # Create the session configuration
    return SQLSpecSessionConfig(
        table_name="litestar_sessions",
        store="sessions",  # This will be the key in the stores registry
    )


def test_session_store_creation(session_store: SQLSpecSessionStore) -> None:
    """Test that SessionStore can be created with BigQuery configuration."""
    assert session_store is not None
    assert session_store._table_name == "litestar_sessions"
    assert session_store._session_id_column == "session_id"
    assert session_store._data_column == "data"
    assert session_store._expires_at_column == "expires_at"
    assert session_store._created_at_column == "created_at"


def test_session_store_bigquery_table_structure(
    session_store: SQLSpecSessionStore, bigquery_migration_config: BigQueryConfig, table_schema_prefix: str
) -> None:
    """Test that session table is created with proper BigQuery structure."""
    with bigquery_migration_config.provide_session() as driver:
        # Verify table exists with proper name (BigQuery uses fully qualified names)

        # Check table schema using information schema
        result = driver.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM `{table_schema_prefix}`.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = 'litestar_sessions'
            ORDER BY ordinal_position
        """)

        columns = {row["column_name"]: row for row in result.data}

        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify BigQuery data types
        assert columns["session_id"]["data_type"] == "STRING"
        assert columns["data"]["data_type"] == "JSON"
        assert columns["expires_at"]["data_type"] == "TIMESTAMP"
        assert columns["created_at"]["data_type"] == "TIMESTAMP"


def test_basic_session_operations(session_config: SQLSpecSessionConfig, session_store: SQLSpecSessionStore) -> None:
    """Test basic session operations through Litestar application."""

    @get("/set-session")
    def set_session(request: Any) -> dict:
        request.session["user_id"] = 12345
        request.session["username"] = "bigquery_user"
        request.session["preferences"] = {"theme": "dark", "language": "en", "timezone": "UTC"}
        request.session["roles"] = ["user", "editor", "bigquery_admin"]
        request.session["bigquery_info"] = {"engine": "BigQuery", "cloud": "google", "mode": "sync"}
        return {"status": "session set"}

    @get("/get-session")
    def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "preferences": request.session.get("preferences"),
            "roles": request.session.get("roles"),
            "bigquery_info": request.session.get("bigquery_info"),
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
        assert data["username"] == "bigquery_user"
        assert data["preferences"]["theme"] == "dark"
        assert data["roles"] == ["user", "editor", "bigquery_admin"]
        assert data["bigquery_info"]["engine"] == "BigQuery"

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
            "bigquery_info": None,
        }


def test_session_persistence_across_requests(
    session_config: SQLSpecSessionConfig, session_store: SQLSpecSessionStore
) -> None:
    """Test that sessions persist across multiple requests with BigQuery."""

    @get("/document/create/{doc_id:int}")
    def create_document(request: Any, doc_id: int) -> dict:
        documents = request.session.get("documents", [])
        document = {
            "id": doc_id,
            "title": f"BigQuery Document {doc_id}",
            "content": f"Content for document {doc_id}. " + "BigQuery " * 20,
            "created_at": "2024-01-01T12:00:00Z",
            "metadata": {"engine": "BigQuery", "storage": "cloud", "analytics": True},
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

        # Simulate saving all documents
        saved_docs = {
            "saved_count": len(documents),
            "documents": documents,
            "saved_at": "2024-01-01T12:00:00Z",
            "bigquery_analytics": True,
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
        assert data["documents"][0]["metadata"]["engine"] == "BigQuery"
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


def test_large_data_handling(session_config: SQLSpecSessionConfig, session_store: SQLSpecSessionStore) -> None:
    """Test handling of large data structures with BigQuery backend."""

    @post("/save-large-bigquery-dataset")
    def save_large_data(request: Any) -> dict:
        # Create a large data structure to test BigQuery's JSON capacity
        large_dataset = {
            "database_info": {
                "engine": "BigQuery",
                "version": "2.0",
                "features": ["Analytics", "ML", "Scalable", "Columnar", "Cloud-native"],
                "cloud_based": True,
                "serverless": True,
            },
            "test_data": {
                "records": [
                    {
                        "id": i,
                        "name": f"BigQuery Record {i}",
                        "description": f"This is a detailed description for record {i}. " + "BigQuery " * 30,
                        "metadata": {
                            "created_at": f"2024-01-{(i % 28) + 1:02d}T12:00:00Z",
                            "tags": [f"bq_tag_{j}" for j in range(15)],
                            "properties": {
                                f"prop_{k}": {
                                    "value": f"bigquery_value_{k}",
                                    "type": "analytics" if k % 2 == 0 else "ml_feature",
                                    "enabled": k % 3 == 0,
                                }
                                for k in range(20)
                            },
                        },
                        "content": {
                            "text": f"Large analytical content for record {i}. " + "Analytics " * 50,
                            "data": list(range(i * 5, (i + 1) * 5)),
                        },
                    }
                    for i in range(100)  # Test BigQuery's JSON storage capacity
                ],
                "analytics": {
                    "summary": {"total_records": 100, "database": "BigQuery", "storage": "cloud", "compressed": True},
                    "metrics": [
                        {
                            "date": f"2024-{month:02d}-{day:02d}",
                            "bigquery_operations": {
                                "queries": day * month * 20,
                                "scanned_gb": day * month * 0.5,
                                "slots_used": day * month * 10,
                                "jobs_completed": day * month * 15,
                            },
                        }
                        for month in range(1, 7)  # Smaller dataset for cloud processing
                        for day in range(1, 16)
                    ],
                },
            },
            "bigquery_configuration": {
                "project_settings": {f"setting_{i}": {"value": f"bq_setting_{i}", "active": True} for i in range(25)},
                "connection_info": {"location": "us-central1", "dataset": "analytics", "pricing": "on_demand"},
            },
        }

        request.session["large_dataset"] = large_dataset
        request.session["dataset_size"] = len(str(large_dataset))
        request.session["bigquery_metadata"] = {
            "engine": "BigQuery",
            "storage_type": "JSON",
            "compressed": True,
            "cloud_native": True,
        }

        return {
            "status": "large dataset saved to BigQuery",
            "records_count": len(large_dataset["test_data"]["records"]),
            "metrics_count": len(large_dataset["test_data"]["analytics"]["metrics"]),
            "settings_count": len(large_dataset["bigquery_configuration"]["project_settings"]),
        }

    @get("/load-large-bigquery-dataset")
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
            "bigquery_metadata": request.session.get("bigquery_metadata"),
        }

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    app = Litestar(
        route_handlers=[save_large_data, load_large_data], middleware=[session_config.middleware], stores=stores
    )

    with TestClient(app=app) as client:
        # Save large dataset
        response = client.post("/save-large-bigquery-dataset")
        assert response.status_code == HTTP_201_CREATED
        data = response.json()
        assert data["status"] == "large dataset saved to BigQuery"
        assert data["records_count"] == 100
        assert data["metrics_count"] > 80  # 6 months * ~15 days
        assert data["settings_count"] == 25

        # Load and verify large dataset
        response = client.get("/load-large-bigquery-dataset")
        data = response.json()
        assert data["has_data"] is True
        assert data["records_count"] == 100
        assert data["first_record"]["name"] == "BigQuery Record 0"
        assert data["database_info"]["engine"] == "BigQuery"
        assert data["dataset_size"] > 30000  # Should be a substantial size
        assert data["bigquery_metadata"]["cloud_native"] is True


def test_migration_with_default_table_name(bigquery_migration_config: BigQueryConfig) -> None:
    """Test that migration with string format creates default table name."""
    # Apply migrations
    commands = SyncMigrationCommands(bigquery_migration_config)
    commands.init(bigquery_migration_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Create store using the migrated table
    store = SQLSpecSessionStore(
        config=bigquery_migration_config,
        table_name="litestar_sessions",  # Default table name
    )

    # Test that the store works with the migrated table
    session_id = "test_session_default"
    test_data = {"user_id": 1, "username": "test_user"}

    run_(store.set)(session_id, test_data, expires_in=3600)
    retrieved = run_(store.get)(session_id)

    assert retrieved == test_data


def test_migration_with_custom_table_name(
    bigquery_migration_config_with_dict: BigQueryConfig, table_schema_prefix: str
) -> None:
    """Test that migration with dict format creates custom table name."""
    # Apply migrations
    commands = SyncMigrationCommands(bigquery_migration_config_with_dict)
    commands.init(bigquery_migration_config_with_dict.migration_config["script_location"], package=False)
    commands.upgrade()

    # Create store using the custom migrated table
    store = SQLSpecSessionStore(
        config=bigquery_migration_config_with_dict,
        table_name="custom_sessions",  # Custom table name from config
    )

    # Test that the store works with the custom table
    session_id = "test_session_custom"
    test_data = {"user_id": 2, "username": "custom_user"}

    run_(store.set)(session_id, test_data, expires_in=3600)
    retrieved = run_(store.get)(session_id)

    assert retrieved == test_data

    # Verify default table doesn't exist
    with bigquery_migration_config_with_dict.provide_session() as driver:
        # In BigQuery, we check if the table exists in information schema
        result = driver.execute(f"""
            SELECT table_name
            FROM `{table_schema_prefix}`.INFORMATION_SCHEMA.TABLES
            WHERE table_name = 'litestar_sessions'
        """)
        assert len(result.data) == 0


def test_migration_with_mixed_extensions(bigquery_migration_config_mixed: BigQueryConfig) -> None:
    """Test migration with mixed extension formats."""
    # Apply migrations
    commands = SyncMigrationCommands(bigquery_migration_config_mixed)
    commands.init(bigquery_migration_config_mixed.migration_config["script_location"], package=False)
    commands.upgrade()

    # The litestar extension should use default table name
    store = SQLSpecSessionStore(
        config=bigquery_migration_config_mixed,
        table_name="litestar_sessions",  # Default since string format was used
    )

    # Test that the store works
    session_id = "test_session_mixed"
    test_data = {"user_id": 3, "username": "mixed_user"}

    run_(store.set)(session_id, test_data, expires_in=3600)
    retrieved = run_(store.get)(session_id)

    assert retrieved == test_data
