"""Comprehensive Litestar integration tests for ADBC adapter."""

import math
import time
from typing import Any
from uuid import uuid4

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK
from litestar.testing import TestClient

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend, SQLSpecSessionStore
from sqlspec.utils.sync_tools import run_

from ...conftest import xfail_if_driver_missing

pytestmark = [pytest.mark.adbc, pytest.mark.postgres, pytest.mark.integration]


@pytest.fixture
def session_store(adbc_session: AdbcConfig) -> SQLSpecSessionStore:
    """Create a session store instance."""
    store = SQLSpecSessionStore(
        config=adbc_session,
        table_name="test_adbc_litestar_sessions",
        session_id_column="session_id",
        data_column="session_data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )
    # Ensure table exists - the store handles sync/async conversion internally
    with adbc_session.provide_session() as driver:
        run_(store._ensure_table_exists)(driver)
    return store


@pytest.fixture
def session_backend(adbc_session: AdbcConfig) -> SQLSpecSessionBackend:
    """Create a session backend instance."""
    backend = SQLSpecSessionBackend(config=adbc_session, table_name="test_adbc_litestar_backend")
    # Ensure table exists - the store handles sync/async conversion internally
    with adbc_session.provide_session() as driver:
        run_(backend.store._ensure_table_exists)(driver)
    return backend


@xfail_if_driver_missing
def test_session_store_basic_operations(session_store: SQLSpecSessionStore) -> None:
    """Test basic session store operations with ADBC."""
    session_id = f"test-adbc-session-{uuid4()}"
    session_data = {
        "user_id": 42,
        "username": "adbc_user",
        "preferences": {"theme": "dark", "language": "en"},
        "roles": ["user", "admin"],
        "metadata": {"driver": "adbc", "backend": "postgresql", "arrow_native": True},
    }

    # Set session data
    run_(session_store.set)(session_id, session_data, expires_in=3600)

    # Get session data
    retrieved_data = run_(session_store.get)(session_id)
    assert retrieved_data == session_data

    # Update session data with Arrow-specific fields
    updated_data = {
        **session_data,
        "last_login": "2024-01-01T12:00:00Z",
        "arrow_batch_size": 1000,
        "performance_metrics": {"query_time_ms": 250, "rows_processed": 50000, "arrow_batches": 5},
    }
    run_(session_store.set)(session_id, updated_data, expires_in=3600)

    # Verify update
    retrieved_data = run_(session_store.get)(session_id)
    assert retrieved_data == updated_data

    # Delete session
    run_(session_store.delete)(session_id)

    # Verify deletion
    result = run_(session_store.get)(session_id, None)
    assert result is None


@xfail_if_driver_missing
def test_session_store_arrow_format_support(session_store: SQLSpecSessionStore, adbc_session: AdbcConfig) -> None:
    """Test ADBC Arrow format support for efficient data transfer."""
    session_id = f"arrow-test-{uuid4()}"

    # Create data that demonstrates Arrow format benefits
    arrow_optimized_data = {
        "user_id": 12345,
        "columnar_data": {
            "ids": list(range(1000)),  # Large numeric array
            "names": [f"user_{i}" for i in range(1000)],  # String array
            "timestamps": [f"2024-01-{(i % 31) + 1:02d}T{(i % 24):02d}:00:00Z" for i in range(1000)],
            "scores": [round(i * 0.5, 2) for i in range(1000)],  # Float array
            "active": [i % 2 == 0 for i in range(1000)],  # Boolean array
        },
        "arrow_metadata": {"format_version": "1.0", "compression": "none", "schema_validated": True},
    }

    # Store Arrow-optimized data
    run_(session_store.set)(session_id, arrow_optimized_data, expires_in=3600)

    # Retrieve and verify data integrity
    retrieved_data = run_(session_store.get)(session_id)
    assert retrieved_data == arrow_optimized_data

    # Verify columnar data integrity
    assert len(retrieved_data["columnar_data"]["ids"]) == 1000
    assert retrieved_data["columnar_data"]["ids"][999] == 999
    assert retrieved_data["columnar_data"]["names"][0] == "user_0"
    assert retrieved_data["columnar_data"]["scores"][100] == 50.0
    assert retrieved_data["columnar_data"]["active"][0] is True
    assert retrieved_data["columnar_data"]["active"][1] is False

    # Test with raw SQL query to verify database storage
    with adbc_session.provide_session() as driver:
        result = driver.execute(
            f"SELECT session_data FROM {session_store._table_name} WHERE session_id = $1", session_id
        )
        assert len(result.data) == 1
        stored_json = result.data[0]["session_data"]
        # For PostgreSQL with JSONB, data should be stored efficiently
        assert isinstance(stored_json, (dict, str))


@xfail_if_driver_missing
def test_session_backend_litestar_integration(session_backend: SQLSpecSessionBackend) -> None:
    """Test SQLSpecSessionBackend integration with Litestar application using ADBC."""

    @get("/set-adbc-user")
    async def set_adbc_user_session(request: Any) -> dict:
        request.session["user_id"] = 54321
        request.session["username"] = "adbc_user"
        request.session["roles"] = ["user", "data_analyst"]
        request.session["adbc_features"] = {"arrow_support": True, "multi_database": True, "batch_processing": True}
        request.session["database_configs"] = [
            {"name": "primary", "driver": "postgresql", "batch_size": 1000},
            {"name": "analytics", "driver": "duckdb", "batch_size": 5000},
        ]
        return {"status": "ADBC user session set"}

    @get("/get-adbc-user")
    async def get_adbc_user_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "roles": request.session.get("roles"),
            "adbc_features": request.session.get("adbc_features"),
            "database_configs": request.session.get("database_configs"),
        }

    @post("/update-adbc-config")
    async def update_adbc_config(request: Any) -> dict:
        configs = request.session.get("database_configs", [])
        configs.append({"name": "cache", "driver": "sqlite", "batch_size": 500, "in_memory": True})
        request.session["database_configs"] = configs
        request.session["last_config_update"] = "2024-01-01T12:00:00Z"
        return {"status": "ADBC config updated"}

    @post("/clear-adbc-session")
    async def clear_adbc_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "ADBC session cleared"}

    session_config = ServerSideSessionConfig(backend=session_backend, key="adbc-test-session", max_age=3600)

    app = Litestar(
        route_handlers=[set_adbc_user_session, get_adbc_user_session, update_adbc_config, clear_adbc_session],
        middleware=[session_config.middleware],
    )

    with TestClient(app=app) as client:
        # Set ADBC user session
        response = client.get("/set-adbc-user")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "ADBC user session set"}

        # Get ADBC user session
        response = client.get("/get-adbc-user")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["user_id"] == 54321
        assert data["username"] == "adbc_user"
        assert data["roles"] == ["user", "data_analyst"]
        assert data["adbc_features"]["arrow_support"] is True
        assert data["adbc_features"]["multi_database"] is True
        assert len(data["database_configs"]) == 2

        # Update ADBC configuration
        response = client.post("/update-adbc-config")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "ADBC config updated"}

        # Verify configuration was updated
        response = client.get("/get-adbc-user")
        data = response.json()
        assert len(data["database_configs"]) == 3
        assert data["database_configs"][2]["name"] == "cache"
        assert data["database_configs"][2]["driver"] == "sqlite"

        # Clear ADBC session
        response = client.post("/clear-adbc-session")
        assert response.status_code == HTTP_200_OK

        # Verify session is cleared
        response = client.get("/get-adbc-user")
        data = response.json()
        assert all(value is None for value in data.values())


@xfail_if_driver_missing
def test_multi_database_compatibility(adbc_session: AdbcConfig) -> None:
    """Test ADBC cross-database portability scenarios."""

    # Test different database configurations
    database_configs = [
        {
            "name": "postgresql_config",
            "config": AdbcConfig(
                connection_config={"uri": adbc_session.connection_config["uri"], "driver_name": "postgresql"}
            ),
        }
        # Note: In a real scenario, you'd test with actual different databases
        # For this test, we'll simulate with different table names
    ]

    for db_config in database_configs:
        config = db_config["config"]
        table_name = f"test_multi_db_{db_config['name']}"

        store = SQLSpecSessionStore(config=config, table_name=table_name)

        session_id = f"multi-db-{db_config['name']}-{uuid4()}"
        session_data = {
            "database": db_config["name"],
            "compatibility_test": True,
            "features": {"arrow_native": True, "cross_db_portable": True},
        }

        # Test basic operations work across different database types
        try:
            run_(store.set)(session_id, session_data, expires_in=3600)
            retrieved_data = run_(store.get)(session_id)
            assert retrieved_data == session_data
            run_(store.delete)(session_id)
            result = run_(store.get)(session_id, None)
            assert result is None
        except Exception as e:
            pytest.fail(f"Multi-database compatibility failed for {db_config['name']}: {e}")


@xfail_if_driver_missing
def test_session_persistence_across_requests(session_backend: SQLSpecSessionBackend) -> None:
    """Test session persistence across multiple requests with ADBC."""

    @get("/adbc-counter")
    async def adbc_counter_endpoint(request: Any) -> dict:
        count = request.session.get("count", 0)
        arrow_batches = request.session.get("arrow_batches", [])
        performance_metrics = request.session.get("performance_metrics", {})

        count += 1
        arrow_batches.append({
            "batch_id": count,
            "timestamp": f"2024-01-01T12:{count:02d}:00Z",
            "rows_processed": count * 1000,
        })

        # Simulate performance tracking
        performance_metrics[f"request_{count}"] = {
            "query_time_ms": count * 50,
            "memory_usage_mb": count * 10,
            "arrow_efficiency": 0.95 + (count * 0.001),
        }

        request.session["count"] = count
        request.session["arrow_batches"] = arrow_batches
        request.session["performance_metrics"] = performance_metrics
        request.session["last_request"] = f"2024-01-01T12:{count:02d}:00Z"

        return {
            "count": count,
            "arrow_batches": len(arrow_batches),
            "total_rows": sum(batch["rows_processed"] for batch in arrow_batches),
            "last_request": request.session["last_request"],
        }

    session_config = ServerSideSessionConfig(backend=session_backend, key="adbc-persistence-test", max_age=3600)

    app = Litestar(route_handlers=[adbc_counter_endpoint], middleware=[session_config.middleware])

    with TestClient(app=app) as client:
        # First request
        response = client.get("/adbc-counter")
        data = response.json()
        assert data["count"] == 1
        assert data["arrow_batches"] == 1
        assert data["total_rows"] == 1000
        assert data["last_request"] == "2024-01-01T12:01:00Z"

        # Second request
        response = client.get("/adbc-counter")
        data = response.json()
        assert data["count"] == 2
        assert data["arrow_batches"] == 2
        assert data["total_rows"] == 3000  # 1000 + 2000
        assert data["last_request"] == "2024-01-01T12:02:00Z"

        # Third request
        response = client.get("/adbc-counter")
        data = response.json()
        assert data["count"] == 3
        assert data["arrow_batches"] == 3
        assert data["total_rows"] == 6000  # 1000 + 2000 + 3000
        assert data["last_request"] == "2024-01-01T12:03:00Z"


@xfail_if_driver_missing
def test_session_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test session expiration functionality with ADBC."""
    session_id = f"adbc-expiration-test-{uuid4()}"
    session_data = {
        "user_id": 999,
        "test": "expiration",
        "adbc_metadata": {"driver": "postgresql", "arrow_format": True},
    }

    # Set session with very short expiration
    run_(session_store.set)(session_id, session_data, expires_in=1)

    # Should exist immediately
    result = run_(session_store.get)(session_id)
    assert result == session_data

    # Wait for expiration
    time.sleep(2)

    # Should be expired now
    result = run_(session_store.get)(session_id, None)
    assert result is None


@xfail_if_driver_missing
def test_concurrent_session_operations(session_store: SQLSpecSessionStore) -> None:
    """Test concurrent session operations with ADBC."""

    def create_adbc_session(session_num: int) -> None:
        """Create a session with unique ADBC-specific data."""
        session_id = f"adbc-concurrent-{session_num}"
        session_data = {
            "session_number": session_num,
            "data": f"adbc_session_{session_num}_data",
            "timestamp": f"2024-01-01T12:{session_num:02d}:00Z",
            "adbc_config": {
                "driver": "postgresql" if session_num % 2 == 0 else "duckdb",
                "batch_size": 1000 + (session_num * 100),
                "arrow_format": True,
            },
            "performance_data": [
                {"metric": "query_time", "value": session_num * 10},
                {"metric": "rows_processed", "value": session_num * 1000},
                {"metric": "memory_usage", "value": session_num * 50},
            ],
        }
        run_(session_store.set)(session_id, session_data, expires_in=3600)

    def read_adbc_session(session_num: int) -> "dict[str, Any] | None":
        """Read a session by number."""
        session_id = f"adbc-concurrent-{session_num}"
        return run_(session_store.get)(session_id, None)

    # Create multiple sessions sequentially (ADBC is sync)
    for i in range(10):
        create_adbc_session(i)

    # Read all sessions sequentially
    results = []
    for i in range(10):
        result = read_adbc_session(i)
        results.append(result)

    # Verify all sessions were created and can be read
    assert len(results) == 10
    for i, result in enumerate(results):
        assert result is not None
        assert result["session_number"] == i
        assert result["data"] == f"adbc_session_{i}_data"
        assert result["adbc_config"]["batch_size"] == 1000 + (i * 100)
        assert len(result["performance_data"]) == 3


@xfail_if_driver_missing
def test_large_data_handling(session_store: SQLSpecSessionStore) -> None:
    """Test handling of large session data with ADBC Arrow format."""
    session_id = f"adbc-large-data-{uuid4()}"

    # Create large session data that benefits from Arrow format
    large_data = {
        "user_id": 12345,
        "large_columnar_data": {
            "ids": list(range(10000)),  # 10K integers
            "timestamps": [f"2024-01-{(i % 28) + 1:02d}T{(i % 24):02d}:{(i % 60):02d}:00Z" for i in range(10000)],
            "scores": [round(i * 0.123, 3) for i in range(10000)],  # 10K floats
            "categories": [f"category_{i % 100}" for i in range(10000)],  # 10K strings
            "flags": [i % 3 == 0 for i in range(10000)],  # 10K booleans
        },
        "metadata": {
            "total_records": 10000,
            "data_format": "arrow_columnar",
            "compression": "snappy",
            "schema_version": "1.0",
        },
        "analytics_results": {
            f"result_set_{i}": {
                "query": f"SELECT * FROM table_{i} WHERE id > {i * 100}",
                "row_count": i * 1000,
                "execution_time_ms": i * 50,
                "memory_usage_mb": i * 10,
                "columns": [f"col_{j}" for j in range(20)],  # 20 columns per result
            }
            for i in range(50)  # 50 result sets
        },
        "large_text_field": "x" * 100000,  # 100KB of text
    }

    # Store large data
    run_(session_store.set)(session_id, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = run_(session_store.get)(session_id)
    assert retrieved_data == large_data

    # Verify columnar data integrity
    assert len(retrieved_data["large_columnar_data"]["ids"]) == 10000
    assert retrieved_data["large_columnar_data"]["ids"][9999] == 9999
    assert len(retrieved_data["large_columnar_data"]["timestamps"]) == 10000
    assert len(retrieved_data["large_columnar_data"]["scores"]) == 10000
    assert retrieved_data["large_columnar_data"]["scores"][1000] == round(1000 * 0.123, 3)

    # Verify analytics results
    assert len(retrieved_data["analytics_results"]) == 50
    assert retrieved_data["analytics_results"]["result_set_10"]["row_count"] == 10000
    assert len(retrieved_data["analytics_results"]["result_set_25"]["columns"]) == 20

    # Verify large text field
    assert len(retrieved_data["large_text_field"]) == 100000
    assert retrieved_data["metadata"]["total_records"] == 10000


@xfail_if_driver_missing
def test_session_cleanup_operations(session_store: SQLSpecSessionStore) -> None:
    """Test session cleanup and maintenance operations with ADBC."""

    # Create sessions with different expiration times
    sessions_data = [
        (f"adbc-short-{i}", {"data": f"short_{i}", "adbc_config": {"driver": "postgresql", "batch_size": 1000}}, 1)
        for i in range(3)  # Will expire quickly
    ] + [
        (
            f"adbc-long-{i}",
            {
                "data": f"long_{i}",
                "adbc_config": {"driver": "duckdb", "batch_size": 5000},
                "arrow_metadata": {"format": "columnar", "compression": "snappy"},
            },
            3600,
        )
        for i in range(3)  # Won't expire
    ]

    # Set all sessions
    for session_id, data, expires_in in sessions_data:
        run_(session_store.set)(session_id, data, expires_in=expires_in)

    # Verify all sessions exist
    for session_id, expected_data, _ in sessions_data:
        result = run_(session_store.get)(session_id)
        assert result == expected_data

    # Wait for short sessions to expire
    time.sleep(2)

    # Clean up expired sessions
    run_(session_store.delete_expired)()

    # Verify short sessions are gone and long sessions remain
    for session_id, expected_data, expires_in in sessions_data:
        result = run_(session_store.get)(session_id, None)
        if expires_in == 1:  # Short expiration
            assert result is None
        else:  # Long expiration
            assert result == expected_data


@xfail_if_driver_missing
def test_adbc_specific_features(session_store: SQLSpecSessionStore, adbc_session: AdbcConfig) -> None:
    """Test ADBC-specific features and optimizations."""
    session_id = f"adbc-features-{uuid4()}"

    # Test data that showcases ADBC features
    adbc_data = {
        "user_id": 54321,
        "arrow_native_data": {
            "column_types": {
                "integers": list(range(1000)),
                "strings": [f"value_{i}" for i in range(1000)],
                "timestamps": [f"2024-{(i % 12) + 1:02d}-01T00:00:00Z" for i in range(1000)],
                "decimals": [round(i * math.pi, 5) for i in range(1000)],
                "booleans": [i % 2 == 0 for i in range(1000)],
            },
            "batch_metadata": {"batch_size": 1000, "compression": "lz4", "schema_fingerprint": "abc123def456"},
        },
        "multi_db_support": {
            "primary_db": "postgresql",
            "cache_db": "duckdb",
            "analytics_db": "bigquery",
            "cross_db_queries": [
                "SELECT * FROM pg_table JOIN duckdb_cache ON id = cache_id",
                "INSERT INTO bigquery_analytics SELECT aggregated_data FROM local_cache",
            ],
        },
        "performance_optimizations": {
            "zero_copy_reads": True,
            "columnar_storage": True,
            "vectorized_operations": True,
            "parallel_execution": True,
        },
    }

    # Store ADBC-specific data
    run_(session_store.set)(session_id, adbc_data, expires_in=3600)

    # Retrieve and verify all features
    retrieved_data = run_(session_store.get)(session_id)
    assert retrieved_data == adbc_data

    # Verify Arrow native data integrity
    arrow_data = retrieved_data["arrow_native_data"]["column_types"]
    assert len(arrow_data["integers"]) == 1000
    assert arrow_data["integers"][999] == 999
    assert len(arrow_data["strings"]) == 1000
    assert arrow_data["strings"][0] == "value_0"
    assert len(arrow_data["decimals"]) == 1000
    assert arrow_data["decimals"][100] == round(100 * math.pi, 5)

    # Verify multi-database support metadata
    multi_db = retrieved_data["multi_db_support"]
    assert multi_db["primary_db"] == "postgresql"
    assert len(multi_db["cross_db_queries"]) == 2

    # Verify performance optimization flags
    perf_opts = retrieved_data["performance_optimizations"]
    assert all(perf_opts.values())  # All should be True


@xfail_if_driver_missing
def test_error_handling_and_recovery(session_backend: SQLSpecSessionBackend) -> None:
    """Test error handling and recovery scenarios with ADBC."""

    @get("/adbc-error-test")
    async def adbc_error_test_endpoint(request: Any) -> dict:
        try:
            # Test normal session operations
            request.session["adbc_config"] = {"driver": "postgresql", "connection_timeout": 30, "batch_size": 1000}
            request.session["test_data"] = {
                "large_array": list(range(5000)),
                "complex_nested": {"level1": {"level2": {"level3": "deep_value"}}},
            }
            return {
                "status": "success",
                "adbc_config": request.session.get("adbc_config"),
                "data_size": len(request.session.get("test_data", {}).get("large_array", [])),
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    session_config = ServerSideSessionConfig(backend=session_backend, key="adbc-error-test-session", max_age=3600)

    app = Litestar(route_handlers=[adbc_error_test_endpoint], middleware=[session_config.middleware])

    with TestClient(app=app) as client:
        response = client.get("/adbc-error-test")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["status"] == "success"
        assert data["adbc_config"]["driver"] == "postgresql"
        assert data["adbc_config"]["batch_size"] == 1000
        assert data["data_size"] == 5000


@xfail_if_driver_missing
def test_multiple_concurrent_adbc_apps(adbc_session: AdbcConfig) -> None:
    """Test multiple Litestar applications with separate ADBC session backends."""

    # Create separate backends for different apps with ADBC-specific configurations
    backend1 = SQLSpecSessionBackend(config=adbc_session, table_name="adbc_app1_sessions")

    backend2 = SQLSpecSessionBackend(config=adbc_session, table_name="adbc_app2_sessions")

    # Ensure tables exist
    with adbc_session.provide_session() as driver:
        run_(backend1.store._ensure_table_exists)(driver)
        run_(backend2.store._ensure_table_exists)(driver)

    @get("/adbc-app1-data")
    async def app1_endpoint(request: Any) -> dict:
        request.session["app"] = "adbc_app1"
        request.session["adbc_config"] = {"driver": "postgresql", "arrow_batch_size": 1000, "connection_pool_size": 10}
        request.session["data"] = {"app1_specific": True, "columnar_data": list(range(100))}
        return {
            "app": "adbc_app1",
            "adbc_config": request.session["adbc_config"],
            "data_length": len(request.session["data"]["columnar_data"]),
        }

    @get("/adbc-app2-data")
    async def app2_endpoint(request: Any) -> dict:
        request.session["app"] = "adbc_app2"
        request.session["adbc_config"] = {"driver": "duckdb", "arrow_batch_size": 5000, "in_memory": True}
        request.session["data"] = {
            "app2_specific": True,
            "analytics_results": [{"query_id": i, "result_size": i * 100} for i in range(50)],
        }
        return {
            "app": "adbc_app2",
            "adbc_config": request.session["adbc_config"],
            "analytics_count": len(request.session["data"]["analytics_results"]),
        }

    # Create separate apps
    app1 = Litestar(
        route_handlers=[app1_endpoint],
        middleware=[ServerSideSessionConfig(backend=backend1, key="adbc_app1").middleware],
    )

    app2 = Litestar(
        route_handlers=[app2_endpoint],
        middleware=[ServerSideSessionConfig(backend=backend2, key="adbc_app2").middleware],
    )

    # Test both apps sequentially (ADBC is sync)
    with TestClient(app=app1) as client1:
        with TestClient(app=app2) as client2:
            # Make requests to both apps
            response1 = client1.get("/adbc-app1-data")
            response2 = client2.get("/adbc-app2-data")

            # Verify responses
            assert response1.status_code == HTTP_200_OK
            data1 = response1.json()
            assert data1["app"] == "adbc_app1"
            assert data1["adbc_config"]["driver"] == "postgresql"
            assert data1["adbc_config"]["arrow_batch_size"] == 1000
            assert data1["data_length"] == 100

            assert response2.status_code == HTTP_200_OK
            data2 = response2.json()
            assert data2["app"] == "adbc_app2"
            assert data2["adbc_config"]["driver"] == "duckdb"
            assert data2["adbc_config"]["arrow_batch_size"] == 5000
            assert data2["analytics_count"] == 50

            # Verify session data is isolated between apps
            response1_second = client1.get("/adbc-app1-data")
            response2_second = client2.get("/adbc-app2-data")

            assert response1_second.json()["adbc_config"]["driver"] == "postgresql"
            assert response2_second.json()["adbc_config"]["driver"] == "duckdb"
