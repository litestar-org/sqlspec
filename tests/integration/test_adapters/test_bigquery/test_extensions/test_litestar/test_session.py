"""Integration tests for BigQuery session backend with store integration."""

import asyncio
import tempfile
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.testing import AsyncTestClient

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.extensions.litestar.session import SQLSpecSessionBackend, SQLSpecSessionConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands

pytestmark = [pytest.mark.bigquery, pytest.mark.integration]


@pytest.fixture
def bigquery_config(bigquery_service) -> BigQueryConfig:
    """Create BigQuery configuration with migration support."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        return BigQueryConfig(
            pool_config={
                "project": bigquery_service.project,
                "dataset": bigquery_service.dataset,
                "credentials": bigquery_service.credentials,
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],
            },
        )


@pytest.fixture
async def session_store(bigquery_config: BigQueryConfig) -> SQLSpecSessionStore:
    """Create a session store with migrations applied."""
    # Apply migrations synchronously (BigQuery uses sync commands)
    commands = SyncMigrationCommands(bigquery_config)
    commands.init(bigquery_config.migration_config["script_location"], package=False)
    commands.upgrade()

    return SQLSpecSessionStore(bigquery_config, table_name="litestar_sessions")


@pytest.fixture
def session_backend_config() -> SQLSpecSessionConfig:
    """Create session backend configuration."""
    return SQLSpecSessionConfig(
        key="bigquery-session",
        max_age=3600,
        table_name="litestar_sessions",
    )


@pytest.fixture
def session_backend(session_backend_config: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend instance."""
    return SQLSpecSessionBackend(config=session_backend_config)


def test_bigquery_migration_creates_correct_table(bigquery_config: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test that Litestar migration creates the correct table structure for BigQuery."""
    # Apply migrations
    commands = SyncMigrationCommands(bigquery_config)
    commands.init(bigquery_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Verify table was created with correct BigQuery-specific types
    with bigquery_config.provide_session() as driver:
        result = driver.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM `{table_schema_prefix}`.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = 'litestar_sessions'
            ORDER BY ordinal_position
        """)
        assert len(result.data) > 0

        columns = {row["column_name"]: row for row in result.data}

        # BigQuery should use JSON for data column and TIMESTAMP for datetime columns
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify BigQuery-specific data types
        assert columns["session_id"]["data_type"] == "STRING"
        assert columns["data"]["data_type"] == "JSON"
        assert columns["expires_at"]["data_type"] == "TIMESTAMP"
        assert columns["created_at"]["data_type"] == "TIMESTAMP"


async def test_bigquery_session_basic_operations(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test basic session operations with BigQuery backend."""

    @get("/set-session")
    async def set_session(request: Any) -> dict:
        request.session["user_id"] = 12345
        request.session["username"] = "testuser"
        request.session["preferences"] = {"theme": "dark", "lang": "en"}
        request.session["bigquery_features"] = {"analytics": True, "ml": True, "serverless": True}
        return {"status": "session set"}

    @get("/get-session")
    async def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "preferences": request.session.get("preferences"),
            "bigquery_features": request.session.get("bigquery_features"),
        }

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(
        store=session_store,
        key="bigquery-session",
        max_age=3600,
    )

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
        assert data["user_id"] == 12345
        assert data["username"] == "testuser"
        assert data["preferences"]["theme"] == "dark"
        assert data["bigquery_features"]["analytics"] is True

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"user_id": None, "username": None, "preferences": None, "bigquery_features": None}


async def test_bigquery_session_complex_data_types(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test BigQuery-specific complex data types in sessions."""

    @post("/save-analytics-session")
    async def save_analytics(request: Any) -> dict:
        # Store BigQuery-friendly data structures
        request.session["analytics_data"] = {
            "queries": [
                {"sql": "SELECT COUNT(*) FROM users", "bytes_processed": 1024},
                {"sql": "SELECT AVG(score) FROM tests", "bytes_processed": 2048},
            ],
            "dataset_info": {
                "project": "test-project",
                "dataset": "analytics",
                "tables": ["users", "tests", "sessions"],
            },
            "performance_metrics": {"slots_used": 100, "job_duration_ms": 5000, "bytes_billed": 1048576},
            "ml_models": [
                {"name": "user_segmentation", "type": "clustering", "accuracy": 0.85},
                {"name": "churn_prediction", "type": "classification", "auc": 0.92},
            ],
        }
        return {"status": "analytics session saved"}

    @get("/load-analytics-session")
    async def load_analytics(request: Any) -> dict:
        analytics = request.session.get("analytics_data", {})
        return {
            "has_analytics": bool(analytics),
            "query_count": len(analytics.get("queries", [])),
            "table_count": len(analytics.get("dataset_info", {}).get("tables", [])),
            "model_count": len(analytics.get("ml_models", [])),
            "first_query": analytics.get("queries", [{}])[0] if analytics.get("queries") else None,
        }

    session_config = ServerSideSessionConfig(
        store=session_store,
        key="bigquery-analytics",
        max_age=3600,
    )

    app = Litestar(
        route_handlers=[save_analytics, load_analytics],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Save analytics session
        response = await client.post("/save-analytics-session")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "analytics session saved"}

        # Load and verify analytics session
        response = await client.get("/load-analytics-session")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["has_analytics"] is True
        assert data["query_count"] == 2
        assert data["table_count"] == 3
        assert data["model_count"] == 2
        assert data["first_query"]["bytes_processed"] == 1024


async def test_bigquery_session_large_json_handling(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test BigQuery's ability to handle large JSON session data."""

    @post("/save-large-session")
    async def save_large_session(request: Any) -> dict:
        # Create a reasonably large JSON structure suitable for BigQuery
        large_data = {
            "user_profile": {
                "personal": {f"field_{i}": f"value_{i}" for i in range(50)},
                "preferences": {f"pref_{i}": i % 2 == 0 for i in range(30)},
                "history": [{"action": f"action_{i}", "timestamp": f"2024-01-{i % 28 + 1:02d}"} for i in range(100)],
            },
            "analytics": {
                "events": [
                    {"name": f"event_{i}", "properties": {f"prop_{j}": j for j in range(10)}} for i in range(25)
                ],
                "segments": {f"segment_{i}": {"size": i * 100, "active": i % 3 == 0} for i in range(20)},
            },
        }
        request.session["large_data"] = large_data
        return {"status": "large session saved", "size": len(str(large_data))}

    @get("/load-large-session")
    async def load_large_session(request: Any) -> dict:
        large_data = request.session.get("large_data", {})
        return {
            "has_data": bool(large_data),
            "personal_fields": len(large_data.get("user_profile", {}).get("personal", {})),
            "preferences_count": len(large_data.get("user_profile", {}).get("preferences", {})),
            "history_events": len(large_data.get("user_profile", {}).get("history", [])),
            "analytics_events": len(large_data.get("analytics", {}).get("events", [])),
            "segments_count": len(large_data.get("analytics", {}).get("segments", {})),
        }

    session_config = ServerSideSessionConfig(
        store=session_store,
        key="bigquery-large",
        max_age=3600,
    )

    app = Litestar(
        route_handlers=[save_large_session, load_large_session],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Save large session
        response = await client.post("/save-large-session")
        assert response.status_code == HTTP_201_CREATED
        data = response.json()
        assert data["status"] == "large session saved"
        assert data["size"] > 10000  # Should be substantial

        # Load and verify large session
        response = await client.get("/load-large-session")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["has_data"] is True
        assert data["personal_fields"] == 50
        assert data["preferences_count"] == 30
        assert data["history_events"] == 100
        assert data["analytics_events"] == 25
        assert data["segments_count"] == 20


async def test_bigquery_session_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test session expiration handling with BigQuery."""
    # No need to create a custom backend - just use the store with short expiration

    @get("/set-data")
    async def set_data(request: Any) -> dict:
        request.session["test"] = "bigquery_data"
        request.session["cloud"] = "gcp"
        return {"status": "set"}

    @get("/get-data")
    async def get_data(request: Any) -> dict:
        return {"test": request.session.get("test"), "cloud": request.session.get("cloud")}

    session_config = ServerSideSessionConfig(
        store="sessions",  # Use the string name for the store
        key="bigquery-expiring",
        max_age=1,  # 1 second expiration
    )

    app = Litestar(
        route_handlers=[set_data, get_data],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Set data
        response = await client.get("/set-data")
        assert response.json() == {"status": "set"}

        # Data should be available immediately
        response = await client.get("/get-data")
        assert response.json() == {"test": "bigquery_data", "cloud": "gcp"}

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        response = await client.get("/get-data")
        assert response.json() == {"test": None, "cloud": None}


async def test_bigquery_session_cleanup(session_store: SQLSpecSessionStore) -> None:
    """Test expired session cleanup with BigQuery."""
    # Create multiple sessions with short expiration
    temp_sessions = []
    for i in range(5):
        session_id = f"bigquery-temp-{i}"
        temp_sessions.append(session_id)
        await session_store.set(session_id, {"query": f"SELECT {i} FROM dataset", "type": "temporary"}, expires_in=1)

    # Create permanent sessions
    perm_sessions = []
    for i in range(3):
        session_id = f"bigquery-perm-{i}"
        perm_sessions.append(session_id)
        await session_store.set(
            session_id, {"query": f"SELECT * FROM table_{i}", "type": "permanent"}, expires_in=3600
        )

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
        assert result["type"] == "permanent"


async def test_bigquery_store_operations(session_store: SQLSpecSessionStore) -> None:
    """Test BigQuery store operations directly."""
    # Test basic store operations
    session_id = "test-session-bigquery"
    test_data = {
        "user_id": 999888,
        "preferences": {"analytics": True, "ml_features": True},
        "datasets": ["sales", "users", "events"],
        "queries": [
            {"sql": "SELECT COUNT(*) FROM sales", "bytes": 1024},
            {"sql": "SELECT AVG(score) FROM users", "bytes": 2048},
        ],
        "performance": {"slots_used": 200, "duration_ms": 1500},
    }

    # Set data
    await session_store.set(session_id, test_data, expires_in=3600)

    # Get data
    result = await session_store.get(session_id)
    assert result == test_data

    # Check exists
    assert await session_store.exists(session_id) is True

    # Update with BigQuery-specific data
    updated_data = {**test_data, "last_job": "bquxjob_12345678"}
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