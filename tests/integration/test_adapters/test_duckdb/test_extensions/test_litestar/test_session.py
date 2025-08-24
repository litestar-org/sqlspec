"""Integration tests for DuckDB session backend with store integration."""

import asyncio
import tempfile
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK
from litestar.testing import AsyncTestClient

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.litestar.session import SQLSpecSessionBackend, SQLSpecSessionConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.sync_tools import async_

pytestmark = [pytest.mark.duckdb, pytest.mark.integration, pytest.mark.xdist_group("duckdb")]


@pytest.fixture
def duckdb_config() -> DuckDBConfig:
    """Create DuckDB configuration with migration support."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sessions.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        return DuckDBConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],
            },
        )


@pytest.fixture
async def session_store(duckdb_config: DuckDBConfig) -> SQLSpecSessionStore:
    """Create a session store with migrations applied."""

    # Apply migrations synchronously (DuckDB uses sync commands like SQLite)
    @async_
    def apply_migrations() -> None:
        commands = SyncMigrationCommands(duckdb_config)
        commands.init(duckdb_config.migration_config["script_location"], package=False)
        commands.upgrade()

    # Run migrations
    await apply_migrations()

    return SQLSpecSessionStore(duckdb_config, table_name="litestar_sessions")


@pytest.fixture
def session_backend_config() -> SQLSpecSessionConfig:
    """Create session backend configuration."""
    return SQLSpecSessionConfig(key="duckdb-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def session_backend(session_backend_config: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend instance."""
    return SQLSpecSessionBackend(config=session_backend_config)


async def test_duckdb_migration_creates_correct_table(duckdb_config: DuckDBConfig) -> None:
    """Test that Litestar migration creates the correct table structure for DuckDB."""

    # Apply migrations synchronously
    @async_
    def apply_migrations():
        commands = SyncMigrationCommands(duckdb_config)
        commands.init(duckdb_config.migration_config["script_location"], package=False)
        commands.upgrade()

    await apply_migrations()

    # Verify table was created with correct DuckDB-specific types
    with duckdb_config.provide_session() as driver:
        result = driver.execute("PRAGMA table_info('litestar_sessions')")
        columns = {row["name"]: row["type"] for row in result.data}

        # DuckDB should use JSON or VARCHAR for data column
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify the data type is appropriate for JSON storage
        assert columns["data"] in ["JSON", "VARCHAR", "TEXT"]


async def test_duckdb_session_basic_operations(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test basic session operations with DuckDB backend."""

    @get("/set-session")
    async def set_session(request: Any) -> dict:
        request.session["user_id"] = 77777
        request.session["username"] = "duckdbuser"
        request.session["preferences"] = {"theme": "system", "analytics": False}
        request.session["features"] = ["analytics", "vectorization"]
        return {"status": "session set"}

    @get("/get-session")
    async def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "preferences": request.session.get("preferences"),
            "features": request.session.get("features"),
        }

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(store=session_store, key="duckdb-session", max_age=3600)

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
        assert data["user_id"] == 77777
        assert data["username"] == "duckdbuser"
        assert data["preferences"] == {"theme": "system", "analytics": False}
        assert data["features"] == ["analytics", "vectorization"]

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"user_id": None, "username": None, "preferences": None, "features": None}


async def test_duckdb_session_persistence(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test that sessions persist across requests with DuckDB."""

    @get("/analytics/event/{event_type}")
    async def track_event(request: Any, event_type: str) -> dict:
        events = request.session.get("events", [])
        events.append({"type": event_type, "timestamp": "2024-01-01T12:00:00"})
        request.session["events"] = events
        request.session["event_count"] = len(events)
        return {"events": events, "count": len(events)}

    @get("/analytics/summary")
    async def get_summary(request: Any) -> dict:
        return {"events": request.session.get("events", []), "count": request.session.get("event_count", 0)}

    session_config = ServerSideSessionConfig(store=session_store, key="duckdb-analytics", max_age=3600)

    app = Litestar(
        route_handlers=[track_event, get_summary],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Track multiple events
        response = await client.get("/analytics/event/page_view")
        assert response.json()["count"] == 1

        response = await client.get("/analytics/event/click")
        assert response.json()["count"] == 2

        response = await client.get("/analytics/event/form_submit")
        assert response.json()["count"] == 3

        # Verify analytics summary
        response = await client.get("/analytics/summary")
        data = response.json()
        assert data["count"] == 3
        assert len(data["events"]) == 3
        assert data["events"][0]["type"] == "page_view"
        assert data["events"][1]["type"] == "click"
        assert data["events"][2]["type"] == "form_submit"


async def test_duckdb_session_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test session expiration handling with DuckDB."""
    # No need to create a custom backend - just use the store with short expiration

    @get("/set-data")
    async def set_data(request: Any) -> dict:
        request.session["test"] = "duckdb_data"
        request.session["db_type"] = "analytical"
        return {"status": "set"}

    @get("/get-data")
    async def get_data(request: Any) -> dict:
        return {"test": request.session.get("test"), "db_type": request.session.get("db_type")}

    session_config = ServerSideSessionConfig(
        store="sessions",  # Use the string name for the store
        key="duckdb-expiring",
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
        assert response.json() == {"test": "duckdb_data", "db_type": "analytical"}

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        response = await client.get("/get-data")
        assert response.json() == {"test": None, "db_type": None}


async def test_duckdb_concurrent_sessions(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test handling of concurrent sessions with DuckDB."""

    @get("/query/{query_id:int}")
    async def execute_query(request: Any, query_id: int) -> dict:
        request.session["query_id"] = query_id
        request.session["db"] = "duckdb"
        request.session["engine"] = "analytical"
        return {"query_id": query_id}

    @get("/current-query")
    async def get_current_query(request: Any) -> dict:
        return {
            "query_id": request.session.get("query_id"),
            "db": request.session.get("db"),
            "engine": request.session.get("engine"),
        }

    session_config = ServerSideSessionConfig(store=session_store, key="duckdb-concurrent", max_age=3600)

    app = Litestar(
        route_handlers=[execute_query, get_current_query],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
    )

    async with AsyncTestClient(app=app) as client1, AsyncTestClient(app=app) as client2:
        # Execute different queries in different clients
        response1 = await client1.get("/query/1001")
        assert response1.json() == {"query_id": 1001}

        response2 = await client2.get("/query/1002")
        assert response2.json() == {"query_id": 1002}

        # Each client should maintain its own session
        response1 = await client1.get("/current-query")
        assert response1.json() == {"query_id": 1001, "db": "duckdb", "engine": "analytical"}

        response2 = await client2.get("/current-query")
        assert response2.json() == {"query_id": 1002, "db": "duckdb", "engine": "analytical"}


async def test_duckdb_session_cleanup(session_store: SQLSpecSessionStore) -> None:
    """Test expired session cleanup with DuckDB."""
    # Create multiple sessions with short expiration
    temp_sessions = []
    for i in range(8):
        session_id = f"duckdb-temp-{i}"
        temp_sessions.append(session_id)
        await session_store.set(session_id, {"query": f"SELECT {i}", "type": "temporary"}, expires_in=1)

    # Create permanent sessions
    perm_sessions = []
    for i in range(2):
        session_id = f"duckdb-perm-{i}"
        perm_sessions.append(session_id)
        await session_store.set(session_id, {"query": f"SELECT * FROM table_{i}", "type": "permanent"}, expires_in=3600)

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


async def test_duckdb_session_analytical_data(
    session_backend: SQLSpecSessionBackend, session_store: SQLSpecSessionStore
) -> None:
    """Test storing analytical data structures in DuckDB sessions."""

    @post("/save-analysis")
    async def save_analysis(request: Any) -> dict:
        # Store analytical data typical for DuckDB use cases
        request.session["dataset"] = {
            "name": "sales_data",
            "rows": 1000000,
            "columns": ["date", "product", "revenue", "quantity"],
            "aggregations": {"total_revenue": 50000000.75, "avg_quantity": 12.5},
        }
        request.session["query_history"] = [
            "SELECT SUM(revenue) FROM sales",
            "SELECT product, COUNT(*) FROM sales GROUP BY product",
            "SELECT DATE_PART('month', date) as month, AVG(revenue) FROM sales GROUP BY month",
        ]
        request.session["performance"] = {"execution_time_ms": 125.67, "rows_scanned": 1000000, "cache_hit": True}
        return {"status": "analysis saved"}

    @get("/load-analysis")
    async def load_analysis(request: Any) -> dict:
        return {
            "dataset": request.session.get("dataset"),
            "query_history": request.session.get("query_history"),
            "performance": request.session.get("performance"),
        }

    session_config = ServerSideSessionConfig(store=session_store, key="duckdb-analysis", max_age=3600)

    app = Litestar(
        route_handlers=[save_analysis, load_analysis],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Save analysis data
        response = await client.post("/save-analysis")
        assert response.json() == {"status": "analysis saved"}

        # Load and verify analysis data
        response = await client.get("/load-analysis")
        data = response.json()

        # Verify dataset info
        assert data["dataset"]["name"] == "sales_data"
        assert data["dataset"]["rows"] == 1000000
        assert data["dataset"]["aggregations"]["total_revenue"] == 50000000.75

        # Verify query history
        assert len(data["query_history"]) == 3
        assert "SUM(revenue)" in data["query_history"][0]

        # Verify performance metrics
        assert data["performance"]["cache_hit"] is True
        assert data["performance"]["execution_time_ms"] == 125.67


async def test_duckdb_store_operations(session_store: SQLSpecSessionStore) -> None:
    """Test DuckDB store operations directly."""
    # Test basic store operations
    session_id = "test-session-duckdb"
    test_data = {
        "user_id": 2024,
        "preferences": {"vectorization": True, "parallel_processing": 4},
        "datasets": ["sales", "inventory", "customers"],
        "stats": {"queries_executed": 42, "avg_execution_time": 89.5},
    }

    # Set data
    await session_store.set(session_id, test_data, expires_in=3600)

    # Get data
    result = await session_store.get(session_id)
    assert result == test_data

    # Check exists
    assert await session_store.exists(session_id) is True

    # Update with analytical workload data
    updated_data = {**test_data, "last_query": "SELECT * FROM sales WHERE date > '2024-01-01'"}
    await session_store.set(session_id, updated_data, expires_in=7200)

    # Get updated data
    result = await session_store.get(session_id)
    assert result == updated_data

    # Test renewal
    result = await session_store.get(session_id, renew_for=10800)
    assert result == updated_data

    # Delete data
    await session_store.delete(session_id)

    # Verify deleted
    result = await session_store.get(session_id)
    assert result is None
    assert await session_store.exists(session_id) is False
