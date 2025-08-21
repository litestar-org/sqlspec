"""Integration tests for DuckDB session backend."""

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
from sqlspec.extensions.litestar import SQLSpecSessionBackend

pytestmark = [pytest.mark.duckdb, pytest.mark.integration]


@pytest.fixture
def duckdb_config() -> DuckDBConfig:
    """Create DuckDB configuration for testing."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
        return DuckDBConfig(pool_config={"database": tmp_file.name})


@pytest.fixture
async def session_backend(duckdb_config: DuckDBConfig) -> SQLSpecSessionBackend:
    """Create a session backend instance."""
    return SQLSpecSessionBackend(
        config=duckdb_config,
        table_name="test_sessions_duckdb",
        session_lifetime=3600,
    )


async def test_duckdb_session_basic_operations(session_backend: SQLSpecSessionBackend) -> None:
    """Test basic session operations with DuckDB backend."""
    
    @get("/set-session")
    async def set_session(request: Any) -> dict:
        request.session["user_id"] = 98765
        request.session["username"] = "duckuser"
        request.session["analytics"] = {"views": 100, "clicks": 50}
        return {"status": "session set"}

    @get("/get-session")
    async def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "analytics": request.session.get("analytics"),
        }

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(
        backend=session_backend,
        key="duckdb-session",
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
        assert data["user_id"] == 98765
        assert data["username"] == "duckuser"
        assert data["analytics"] == {"views": 100, "clicks": 50}

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"user_id": None, "username": None, "analytics": None}


async def test_duckdb_session_persistence(session_backend: SQLSpecSessionBackend) -> None:
    """Test that sessions persist across requests with DuckDB."""
    
    @get("/analytics/{metric}")
    async def track_metric(request: Any, metric: str) -> dict:
        metrics = request.session.get("metrics", {})
        metrics[metric] = metrics.get(metric, 0) + 1
        request.session["metrics"] = metrics
        return {"metrics": metrics}

    session_config = ServerSideSessionConfig(
        backend=session_backend,
        key="duckdb-metrics",
    )

    app = Litestar(
        route_handlers=[track_metric],
        middleware=[session_config.middleware],
    )

    async with AsyncTestClient(app=app) as client:
        # Track multiple metrics
        response = await client.get("/analytics/pageview")
        assert response.json() == {"metrics": {"pageview": 1}}
        
        response = await client.get("/analytics/click")
        assert response.json() == {"metrics": {"pageview": 1, "click": 1}}
        
        response = await client.get("/analytics/pageview")
        assert response.json() == {"metrics": {"pageview": 2, "click": 1}}
        
        response = await client.get("/analytics/conversion")
        assert response.json() == {"metrics": {"pageview": 2, "click": 1, "conversion": 1}}


async def test_duckdb_session_expiration(session_backend: SQLSpecSessionBackend) -> None:
    """Test session expiration handling with DuckDB."""
    # Create backend with very short lifetime
    backend = SQLSpecSessionBackend(
        config=session_backend.store._config,
        table_name="test_expiring_sessions_duckdb",
        session_lifetime=1,  # 1 second
    )
    
    @get("/set-data")
    async def set_data(request: Any) -> dict:
        request.session["test"] = "duckdb_data"
        return {"status": "set"}

    @get("/get-data")
    async def get_data(request: Any) -> dict:
        return {"test": request.session.get("test")}

    session_config = ServerSideSessionConfig(
        backend=backend,
        key="duckdb-expiring",
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
        assert response.json() == {"test": "duckdb_data"}

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        response = await client.get("/get-data")
        assert response.json() == {"test": None}


async def test_duckdb_concurrent_sessions(session_backend: SQLSpecSessionBackend) -> None:
    """Test handling of concurrent sessions with DuckDB."""
    
    @get("/dataset/{dataset_id:int}")
    async def set_dataset(request: Any, dataset_id: int) -> dict:
        request.session["dataset_id"] = dataset_id
        request.session["engine"] = "duckdb"
        return {"dataset_id": dataset_id}

    @get("/current-dataset")
    async def get_dataset(request: Any) -> dict:
        return {
            "dataset_id": request.session.get("dataset_id"),
            "engine": request.session.get("engine"),
        }

    session_config = ServerSideSessionConfig(
        backend=session_backend,
        key="duckdb-concurrent",
    )

    app = Litestar(
        route_handlers=[set_dataset, get_dataset],
        middleware=[session_config.middleware],
    )

    async with AsyncTestClient(app=app) as client1, AsyncTestClient(app=app) as client2:
        # Set different datasets in different clients
        response1 = await client1.get("/dataset/1001")
        assert response1.json() == {"dataset_id": 1001}

        response2 = await client2.get("/dataset/2002")
        assert response2.json() == {"dataset_id": 2002}

        # Each client should maintain its own session
        response1 = await client1.get("/current-dataset")
        assert response1.json() == {"dataset_id": 1001, "engine": "duckdb"}

        response2 = await client2.get("/current-dataset")
        assert response2.json() == {"dataset_id": 2002, "engine": "duckdb"}


async def test_duckdb_session_cleanup(duckdb_config: DuckDBConfig) -> None:
    """Test expired session cleanup with DuckDB."""
    backend = SQLSpecSessionBackend(
        config=duckdb_config,
        table_name="test_cleanup_sessions_duckdb",
        session_lifetime=1,
    )

    # Create multiple sessions with short expiration
    for i in range(5):
        session_id = f"duckdb-cleanup-{i}"
        await backend.store.set(session_id, {"data": i}, expires_in=1)

    # Create long-lived session
    await backend.store.set("duckdb-persistent", {"data": "keep"}, expires_in=3600)

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await backend.delete_expired_sessions()

    # Check that expired sessions are gone
    for i in range(5):
        result = await backend.store.get(f"duckdb-cleanup-{i}")
        assert result is None

    # Long-lived session should still exist
    result = await backend.store.get("duckdb-persistent")
    assert result == {"data": "keep"}


async def test_duckdb_session_analytical_data(session_backend: SQLSpecSessionBackend) -> None:
    """Test storing analytical data structures in DuckDB sessions."""
    
    @post("/save-analytics")
    async def save_analytics(request: Any) -> dict:
        # Store analytical data typical for DuckDB use cases
        request.session["timeseries"] = [
            {"timestamp": f"2024-01-{i:02d}", "value": i * 10.5, "category": f"cat_{i % 3}"}
            for i in range(1, 31)
        ]
        request.session["aggregations"] = {
            "sum": 465.0,
            "avg": 15.5,
            "min": 0.0,
            "max": 294.0,
            "count": 30,
        }
        request.session["dimensions"] = {
            "geography": ["US", "EU", "APAC"],
            "product": ["A", "B", "C"],
            "channel": ["web", "mobile", "api"],
        }
        return {"status": "analytics saved"}

    @get("/load-analytics")
    async def load_analytics(request: Any) -> dict:
        return {
            "timeseries": request.session.get("timeseries"),
            "aggregations": request.session.get("aggregations"),
            "dimensions": request.session.get("dimensions"),
        }

    session_config = ServerSideSessionConfig(
        backend=session_backend,
        key="duckdb-analytics",
    )

    app = Litestar(
        route_handlers=[save_analytics, load_analytics],
        middleware=[session_config.middleware],
    )

    async with AsyncTestClient(app=app) as client:
        # Save analytical data
        response = await client.post("/save-analytics")
        assert response.json() == {"status": "analytics saved"}

        # Load and verify analytical data
        response = await client.get("/load-analytics")
        data = response.json()
        
        # Verify timeseries
        assert len(data["timeseries"]) == 30
        assert data["timeseries"][0]["timestamp"] == "2024-01-01"
        assert data["timeseries"][0]["value"] == 10.5
        
        # Verify aggregations
        assert data["aggregations"]["sum"] == 465.0
        assert data["aggregations"]["avg"] == 15.5
        assert data["aggregations"]["count"] == 30
        
        # Verify dimensions
        assert data["dimensions"]["geography"] == ["US", "EU", "APAC"]
        assert data["dimensions"]["product"] == ["A", "B", "C"]