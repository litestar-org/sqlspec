"""Comprehensive Litestar integration tests for DuckDB adapter.

This module tests the integration between DuckDB adapter and Litestar web framework
through SQLSpec's SessionStore implementation. It focuses on testing analytical
data storage patterns that are particularly relevant for DuckDB use cases.

Tests Covered:
- Basic session store operations with DuckDB
- Complex analytical data types and structures
- Session expiration handling with large datasets
- Concurrent analytical session operations
- Large analytical session data handling
- Session cleanup and maintenance operations

Note:
SQLSpecSessionBackend integration tests are currently disabled due to breaking
changes in Litestar 2.17.0 that require implementing a new store_in_message method.
This would need to be addressed in the main SQLSpec library.

The tests use in-memory DuckDB databases for isolation and focus on analytical
workflows typical of DuckDB usage patterns including:
- Query execution results and metadata
- Dataset schemas and file references
- Performance metrics and execution statistics
- Export configurations and analytical pipelines
"""

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore

pytestmark = [pytest.mark.duckdb, pytest.mark.integration]


@pytest.fixture
def duckdb_config() -> DuckDBConfig:
    """Create DuckDB configuration for testing."""
    import uuid

    # Use a unique memory database identifier to avoid configuration conflicts
    db_identifier = f":memory:{uuid.uuid4().hex}"
    return DuckDBConfig(pool_config={"database": db_identifier})


@pytest.fixture
def session_store(duckdb_config: DuckDBConfig) -> SQLSpecSessionStore:
    """Create a session store instance."""
    store = SQLSpecSessionStore(
        config=duckdb_config,
        table_name="test_litestar_sessions_duckdb",
        session_id_column="session_id",
        data_column="session_data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )
    # Ensure table exists (DuckDB is sync)
    with duckdb_config.provide_session() as driver:
        import asyncio

        asyncio.run(store._ensure_table_exists(driver))
    return store


# Note: SQLSpecSessionBackend tests are disabled due to breaking changes in Litestar 2.17.0
# that require implementing store_in_message method. This would need to be fixed in the main library.


async def test_session_store_basic_operations(session_store: SQLSpecSessionStore) -> None:
    """Test basic session store operations with DuckDB."""
    session_id = f"test-session-{uuid4()}"
    session_data = {
        "user_id": 42,
        "username": "duckdb_user",
        "analytics": {
            "queries_run": 15,
            "datasets_accessed": ["sales", "marketing", "analytics"],
            "export_formats": ["parquet", "csv", "json"],
        },
        "preferences": {"engine": "duckdb", "compression": "zstd"},
        "query_history": [
            {"sql": "SELECT * FROM sales WHERE year > 2020", "duration_ms": 45.2},
            {"sql": "SELECT AVG(amount) FROM marketing", "duration_ms": 12.8},
        ],
    }

    # Set session data
    await session_store.set(session_id, session_data, expires_in=3600)

    # Get session data
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == session_data

    # Update session data with analytical workflow
    updated_data = {
        **session_data,
        "last_query": "SELECT * FROM parquet_scan('large_dataset.parquet')",
        "result_size": 1000000,
        "execution_context": {"memory_limit": "1GB", "threads": 4, "enable_object_cache": True},
    }
    await session_store.set(session_id, updated_data, expires_in=3600)

    # Verify update
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == updated_data
    assert retrieved_data["result_size"] == 1000000

    # Delete session
    await session_store.delete(session_id)

    # Verify deletion
    result = await session_store.get(session_id, None)
    assert result is None


async def test_session_store_analytical_data_types(
    session_store: SQLSpecSessionStore, duckdb_config: DuckDBConfig
) -> None:
    """Test DuckDB-specific analytical data types and structures."""
    session_id = f"analytical-test-{uuid4()}"

    # Complex analytical data that showcases DuckDB capabilities
    analytical_data = {
        "query_plan": {
            "operation": "PROJECTION",
            "columns": ["customer_id", "total_revenue", "order_count"],
            "children": [
                {
                    "operation": "AGGREGATE",
                    "group_by": ["customer_id"],
                    "aggregates": {"total_revenue": "SUM(amount)", "order_count": "COUNT(*)"},
                    "children": [
                        {
                            "operation": "FILTER",
                            "condition": "date >= '2024-01-01'",
                            "children": [
                                {
                                    "operation": "PARQUET_SCAN",
                                    "file": "s3://bucket/orders/*.parquet",
                                    "projected_columns": ["customer_id", "amount", "date"],
                                }
                            ],
                        }
                    ],
                }
            ],
        },
        "execution_stats": {
            "rows_scanned": 50_000_000,
            "rows_filtered": 25_000_000,
            "rows_output": 150_000,
            "execution_time_ms": 2_847.5,
            "memory_usage_mb": 512.75,
            "spill_to_disk": False,
        },
        "result_preview": [
            {"customer_id": 1001, "total_revenue": 15_432.50, "order_count": 23},
            {"customer_id": 1002, "total_revenue": 28_901.75, "order_count": 41},
            {"customer_id": 1003, "total_revenue": 8_234.25, "order_count": 12},
        ],
        "export_options": {
            "formats": ["parquet", "csv", "json", "arrow"],
            "compression": ["gzip", "snappy", "zstd"],
            "destinations": ["s3", "local", "azure_blob"],
        },
        "metadata": {
            "schema_version": "1.2.0",
            "query_fingerprint": "abc123def456",
            "cache_key": "analytical_query_2024_01_20",
            "extensions_used": ["httpfs", "parquet", "json"],
        },
    }

    # Store analytical data
    await session_store.set(session_id, analytical_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == analytical_data

    # Verify data structure integrity
    assert retrieved_data["execution_stats"]["rows_scanned"] == 50_000_000
    assert retrieved_data["query_plan"]["operation"] == "PROJECTION"
    assert len(retrieved_data["result_preview"]) == 3
    assert "httpfs" in retrieved_data["metadata"]["extensions_used"]

    # Verify data is stored efficiently in database
    with duckdb_config.provide_session() as driver:
        result = driver.execute(
            f"SELECT session_data FROM {session_store._table_name} WHERE session_id = ?", session_id
        )
        assert len(result.data) == 1
        stored_data = result.data[0]["session_data"]
        # DuckDB stores JSON data as string, not parsed dict
        assert isinstance(stored_data, str)  # Should be stored as JSON string


# NOTE: SQLSpecSessionBackend integration tests are disabled
# due to breaking changes in Litestar 2.17.0 requiring implementation of store_in_message method


async def test_session_expiration_with_large_datasets(session_store: SQLSpecSessionStore) -> None:
    """Test session expiration functionality with large analytical datasets."""
    session_id = f"large-dataset-{uuid4()}"

    # Create large analytical dataset session
    large_dataset_session = {
        "dataset_info": {
            "name": "customer_analytics_2024",
            "size_gb": 15.7,
            "row_count": 25_000_000,
            "column_count": 45,
            "partitions": 100,
        },
        "query_results": [
            {
                "query_id": f"q_{i}",
                "result_rows": i * 10_000,
                "execution_time_ms": i * 25.5,
                "memory_usage_mb": i * 128,
                "cache_hit": i % 3 == 0,
            }
            for i in range(1, 21)  # 20 query results
        ],
        "performance_metrics": {
            "total_queries": 20,
            "avg_execution_time_ms": 267.5,
            "total_memory_peak_mb": 2048,
            "cache_hit_ratio": 0.35,
            "disk_spill_events": 3,
        },
        "file_references": [f"/data/partition_{i:03d}.parquet" for i in range(100)],
    }

    # Set session with very short expiration
    await session_store.set(session_id, large_dataset_session, expires_in=1)

    # Should exist immediately
    result = await session_store.get(session_id)
    assert result == large_dataset_session
    assert result["dataset_info"]["size_gb"] == 15.7
    assert len(result["query_results"]) == 20

    # Wait for expiration
    await asyncio.sleep(2)

    # Should be expired now
    result = await session_store.get(session_id, None)
    assert result is None


async def test_concurrent_analytical_sessions(session_store: SQLSpecSessionStore) -> None:
    """Test concurrent analytical session operations with DuckDB."""

    async def create_analysis_session(analyst_id: int) -> None:
        """Create an analytical session for a specific analyst."""
        session_id = f"analyst-{analyst_id}"
        session_data = {
            "analyst_id": analyst_id,
            "analysis_name": f"customer_analysis_{analyst_id}",
            "datasets": [f"dataset_{analyst_id}_{j}" for j in range(5)],
            "query_results": [
                {"query_id": f"q_{analyst_id}_{k}", "result_size": k * 1000, "execution_time": k * 15.2}
                for k in range(1, 11)
            ],
            "export_history": [
                {"format": "parquet", "timestamp": f"2024-01-20T1{analyst_id}:00:00Z"},
                {"format": "csv", "timestamp": f"2024-01-20T1{analyst_id}:15:00Z"},
            ],
            "performance": {
                "total_memory_gb": analyst_id * 0.5,
                "total_queries": 10,
                "avg_query_time_ms": analyst_id * 25.0,
            },
        }
        await session_store.set(session_id, session_data, expires_in=3600)

    async def read_analysis_session(analyst_id: int) -> "dict[str, Any] | None":
        """Read an analytical session by analyst ID."""
        session_id = f"analyst-{analyst_id}"
        return await session_store.get(session_id, None)

    # Create multiple analytical sessions concurrently
    create_tasks = [create_analysis_session(i) for i in range(1, 11)]
    await asyncio.gather(*create_tasks)

    # Read all sessions concurrently
    read_tasks = [read_analysis_session(i) for i in range(1, 11)]
    results = await asyncio.gather(*read_tasks)

    # Verify all sessions were created and can be read
    assert len(results) == 10
    for i, result in enumerate(results, 1):
        assert result is not None
        assert result["analyst_id"] == i
        assert result["analysis_name"] == f"customer_analysis_{i}"
        assert len(result["datasets"]) == 5
        assert len(result["query_results"]) == 10
        assert result["performance"]["total_memory_gb"] == i * 0.5


async def test_large_analytical_session_data(session_store: SQLSpecSessionStore) -> None:
    """Test handling of very large analytical session data with DuckDB."""
    session_id = f"large-analysis-{uuid4()}"

    # Create extremely large analytical session data
    large_analytical_data = {
        "analysis_metadata": {
            "project_id": "enterprise_analytics_2024",
            "analyst_team": ["data_scientist_1", "data_engineer_2", "analyst_3"],
            "analysis_type": "comprehensive_customer_journey",
            "data_sources": ["crm", "web_analytics", "transaction_logs", "support_tickets"],
        },
        "query_execution_log": [
            {
                "query_id": f"query_{i:06d}",
                "sql": f"SELECT * FROM analytics_table_{i % 100} WHERE date >= '2024-01-{(i % 28) + 1:02d}'",
                "execution_time_ms": (i * 12.7) % 1000,
                "rows_returned": (i * 1000) % 100000,
                "memory_usage_mb": (i * 64) % 2048,
                "cache_hit": i % 5 == 0,
                "error_message": None if i % 50 != 0 else f"Timeout error for query {i}",
            }
            for i in range(1, 2001)  # 2000 query executions
        ],
        "dataset_schemas": {
            f"table_{i}": {
                "columns": [
                    {"name": f"col_{j}", "type": "VARCHAR" if j % 3 == 0 else "INTEGER", "nullable": j % 7 == 0}
                    for j in range(20)
                ],
                "row_count": i * 100000,
                "size_mb": i * 50.5,
                "partitions": max(1, i // 10),
            }
            for i in range(1, 101)  # 100 table schemas
        },
        "performance_timeline": [
            {
                "timestamp": f"2024-01-20T{h:02d}:{m:02d}:00Z",
                "memory_usage_gb": (h * 60 + m) * 0.1,
                "cpu_usage_percent": ((h * 60 + m) * 2) % 100,
                "active_queries": (h * 60 + m) % 20,
                "cache_hit_ratio": 0.8 - ((h * 60 + m) % 100) * 0.005,
            }
            for h in range(24)
            for m in range(0, 60, 15)  # Every 15 minutes for 24 hours
        ],
        "export_manifests": {
            f"export_{i}": {
                "files": [f"/exports/batch_{i}/part_{j:04d}.parquet" for j in range(50)],
                "total_size_gb": i * 2.5,
                "row_count": i * 500000,
                "compression_ratio": 0.75 + (i % 10) * 0.02,
                "checksum": f"sha256_{i:032d}",
            }
            for i in range(1, 21)  # 20 export manifests
        },
    }

    # Store large analytical data
    await session_store.set(session_id, large_analytical_data, expires_in=3600)

    # Retrieve and verify
    retrieved_data = await session_store.get(session_id)
    assert retrieved_data == large_analytical_data
    assert len(retrieved_data["query_execution_log"]) == 2000
    assert len(retrieved_data["dataset_schemas"]) == 100
    assert len(retrieved_data["performance_timeline"]) == 96  # 24 * 4 (every 15 min)
    assert len(retrieved_data["export_manifests"]) == 20

    # Verify specific data integrity
    first_query = retrieved_data["query_execution_log"][0]
    assert first_query["query_id"] == "query_000001"
    assert first_query["execution_time_ms"] == 12.7

    last_schema = retrieved_data["dataset_schemas"]["table_100"]
    assert last_schema["row_count"] == 10000000
    assert len(last_schema["columns"]) == 20


async def test_session_analytics_cleanup_operations(session_store: SQLSpecSessionStore) -> None:
    """Test analytical session cleanup and maintenance operations."""

    # Create analytical sessions with different lifecycles
    short_term_sessions = [
        (f"temp-analysis-{i}", {"type": "exploratory", "data": f"temp_{i}", "priority": "low"}, 1)
        for i in range(5)  # Will expire quickly
    ]

    long_term_sessions = [
        (f"production-analysis-{i}", {"type": "production", "data": f"prod_{i}", "priority": "high"}, 3600)
        for i in range(5)  # Won't expire soon
    ]

    # Set all sessions
    for session_id, data, expires_in in short_term_sessions + long_term_sessions:
        await session_store.set(session_id, data, expires_in=expires_in)

    # Verify all sessions exist
    for session_id, expected_data, _ in short_term_sessions + long_term_sessions:
        result = await session_store.get(session_id)
        assert result == expected_data

    # Wait for short-term sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await session_store.delete_expired()

    # Verify short-term sessions are gone and long-term sessions remain
    for session_id, expected_data, expires_in in short_term_sessions + long_term_sessions:
        result = await session_store.get(session_id, None)
        if expires_in == 1:  # Short expiration
            assert result is None
        else:  # Long expiration
            assert result == expected_data
            assert result["priority"] == "high"


# Additional DuckDB-specific extension tests could be added here
# once the Litestar session backend compatibility issues are resolved
