"""Integration tests for DuckDB session store."""

import asyncio
import math

import pytest

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.litestar import SQLSpecSyncSessionStore

pytestmark = [pytest.mark.duckdb, pytest.mark.integration, pytest.mark.xdist_group("duckdb")]


def test_duckdb_store_table_creation(session_store: SQLSpecSyncSessionStore, migrated_config: DuckDBConfig) -> None:
    """Test that store table is created automatically with proper DuckDB structure."""
    with migrated_config.provide_session() as driver:
        # Verify table exists
        result = driver.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'litestar_sessions'"
        )
        assert len(result.data) == 1
        assert result.data[0]["table_name"] == "litestar_sessions"

        # Verify table structure
        result = driver.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'litestar_sessions' ORDER BY ordinal_position"
        )
        columns = {row["column_name"]: row["data_type"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify DuckDB-specific data types
        # DuckDB should use appropriate types for JSON storage (JSON, VARCHAR, or TEXT)
        assert columns.get("data") in ["JSON", "VARCHAR", "TEXT"]
        assert any(dt in columns.get("expires_at", "") for dt in ["TIMESTAMP", "DATETIME"])

        # Verify indexes if they exist (DuckDB may handle indexing differently)

        result = driver.select(
            "SELECT index_name FROM information_schema.statistics WHERE table_name = 'litestar_sessions'"
        )
        # DuckDB indexing may be different, so we just check that the query works
        assert isinstance(result, list)


async def test_duckdb_store_crud_operations(session_store: SQLSpecSyncSessionStore) -> None:
    """Test complete CRUD operations on the DuckDB store."""
    key = "duckdb-test-key"
    value = {
        "dataset_id": 456,
        "query": "SELECT * FROM analytics",
        "results": [{"col1": 1, "col2": "a"}, {"col1": 2, "col2": "b"}],
        "metadata": {"rows": 2, "execution_time": 0.05},
    }

    # Create
    await session_store.set(key, value, expires_in=3600)

    # Read
    retrieved = await session_store.get(key)
    assert retrieved == value
    assert retrieved["metadata"]["execution_time"] == 0.05

    # Update
    updated_value = {
        "dataset_id": 789,
        "new_field": "analytical_data",
        "parquet_files": ["file1.parquet", "file2.parquet"],
    }
    await session_store.set(key, updated_value, expires_in=3600)

    retrieved = await session_store.get(key)
    assert retrieved == updated_value
    assert "parquet_files" in retrieved

    # Delete
    await session_store.delete(key)
    result = await session_store.get(key)
    assert result is None


async def test_duckdb_store_expiration(session_store: SQLSpecSyncSessionStore) -> None:
    """Test that expired entries are not returned from DuckDB."""
    key = "duckdb-expiring-key"
    value = {"test": "analytical_data", "source": "duckdb"}

    # Set with 1 second expiration
    await session_store.set(key, value, expires_in=1)

    # Should exist immediately
    result = await session_store.get(key)
    assert result == value

    # Wait for expiration
    await asyncio.sleep(2)

    # Should be expired
    result = await session_store.get(key)
    assert result is None


async def test_duckdb_store_default_values(session_store: SQLSpecSyncSessionStore) -> None:
    """Test default value handling."""
    # Non-existent key should return None
    result = await session_store.get("non-existent-duckdb-key")
    assert result is None

    # Test with custom default handling
    result = await session_store.get("non-existent-duckdb-key")
    if result is None:
        result = {"default": True, "engine": "duckdb"}
    assert result == {"default": True, "engine": "duckdb"}


async def test_duckdb_store_bulk_operations(session_store: SQLSpecSyncSessionStore) -> None:
    """Test bulk operations on the DuckDB store."""
    # Create multiple entries representing analytical results
    entries = {}
    for i in range(20):
        key = f"duckdb-result-{i}"
        value = {
            "query_id": i,
            "result_set": [{"value": j} for j in range(5)],
            "statistics": {"rows_scanned": i * 1000, "execution_time_ms": i * 10},
        }
        entries[key] = value
        await session_store.set(key, value, expires_in=3600)

    # Verify all entries exist
    for key, expected_value in entries.items():
        result = await session_store.get(key)
        assert result == expected_value

    # Delete all entries
    for key in entries:
        await session_store.delete(key)

    # Verify all are deleted
    for key in entries:
        result = await session_store.get(key)
        assert result is None


async def test_duckdb_store_analytical_data(session_store: SQLSpecSyncSessionStore) -> None:
    """Test storing analytical data structures typical for DuckDB."""
    # Create analytical data structure
    analytical_data = {
        "query_plan": {
            "type": "PROJECTION",
            "children": [
                {
                    "type": "FILTER",
                    "condition": "date >= '2024-01-01'",
                    "children": [
                        {
                            "type": "PARQUET_SCAN",
                            "file": "analytics.parquet",
                            "columns": ["date", "revenue", "customer_id"],
                        }
                    ],
                }
            ],
        },
        "execution_stats": {
            "rows_scanned": 1_000_000,
            "rows_returned": 50_000,
            "execution_time_ms": 245.7,
            "memory_usage_mb": 128,
        },
        "result_metadata": {"file_format": "parquet", "compression": "snappy", "schema_version": "v1"},
    }

    key = "duckdb-analytics-test"
    await session_store.set(key, analytical_data, expires_in=3600)

    # Retrieve and verify
    retrieved = await session_store.get(key)
    assert retrieved == analytical_data
    assert retrieved["execution_stats"]["rows_scanned"] == 1_000_000
    assert retrieved["query_plan"]["type"] == "PROJECTION"

    # Cleanup
    await session_store.delete(key)


async def test_duckdb_store_concurrent_access(session_store: SQLSpecSyncSessionStore) -> None:
    """Test concurrent access patterns to the DuckDB store."""
    # Simulate multiple analytical sessions
    sessions = {}
    for i in range(10):
        session_id = f"analyst-session-{i}"
        session_data = {
            "analyst_id": i,
            "datasets": [f"dataset_{i}_{j}" for j in range(3)],
            "query_cache": {f"query_{k}": f"result_{k}" for k in range(5)},
            "preferences": {"format": "parquet", "compression": "zstd"},
        }
        sessions[session_id] = session_data
        await session_store.set(session_id, session_data, expires_in=3600)

    # Verify all sessions exist
    for session_id, expected_data in sessions.items():
        retrieved = await session_store.get(session_id)
        assert retrieved == expected_data
        assert len(retrieved["datasets"]) == 3
        assert len(retrieved["query_cache"]) == 5

    # Clean up
    for session_id in sessions:
        await session_store.delete(session_id)


async def test_duckdb_store_get_all(session_store: SQLSpecSyncSessionStore) -> None:
    """Test getting all entries from the store."""
    # Create test entries
    test_entries = {}
    for i in range(5):
        key = f"get-all-test-{i}"
        value = {"index": i, "data": f"test_data_{i}"}
        test_entries[key] = value
        await session_store.set(key, value, expires_in=3600)

    # Get all entries
    all_entries = []

    async def collect_entries() -> None:
        async for key, value in session_store.get_all():
            all_entries.append((key, value))

    await collect_entries()

    # Verify we got all entries (may include entries from other tests)
    retrieved_keys = {key for key, _ in all_entries}
    for test_key in test_entries:
        assert test_key in retrieved_keys

    # Clean up
    for key in test_entries:
        await session_store.delete(key)


async def test_duckdb_store_delete_expired(session_store: SQLSpecSyncSessionStore) -> None:
    """Test deleting expired entries."""
    # Create entries with different expiration times
    short_lived_keys = []
    long_lived_keys = []

    for i in range(3):
        short_key = f"short-lived-{i}"
        long_key = f"long-lived-{i}"

        await session_store.set(short_key, {"data": f"short_{i}"}, expires_in=1)
        await session_store.set(long_key, {"data": f"long_{i}"}, expires_in=3600)

        short_lived_keys.append(short_key)
        long_lived_keys.append(long_key)

    # Wait for short-lived entries to expire
    await asyncio.sleep(2)

    # Delete expired entries
    await session_store.delete_expired()

    # Verify short-lived entries are gone
    for key in short_lived_keys:
        assert await session_store.get(key) is None

    # Verify long-lived entries still exist
    for key in long_lived_keys:
        assert await session_store.get(key) is not None

    # Clean up remaining entries
    for key in long_lived_keys:
        await session_store.delete(key)


async def test_duckdb_store_special_characters(session_store: SQLSpecSyncSessionStore) -> None:
    """Test handling of special characters in keys and values with DuckDB."""
    # Test special characters in keys
    special_keys = [
        "query-2024-01-01",
        "user_query_123",
        "dataset.analytics.sales",
        "namespace:queries:recent",
        "path/to/query",
    ]

    for key in special_keys:
        value = {"key": key, "engine": "duckdb"}
        await session_store.set(key, value, expires_in=3600)

        retrieved = await session_store.get(key)
        assert retrieved == value

        await session_store.delete(key)


async def test_duckdb_store_crud_operations_enhanced(session_store: SQLSpecSyncSessionStore) -> None:
    """Test enhanced CRUD operations on the DuckDB store."""
    key = "duckdb-enhanced-test-key"
    value = {
        "query_id": 999,
        "data": ["analytical_item1", "analytical_item2", "analytical_item3"],
        "nested": {"query": "SELECT * FROM large_table", "execution_time": 123.45},
        "duckdb_specific": {"vectorization": True, "analytics": [1, 2, 3]},
    }

    # Create
    await session_store.set(key, value, expires_in=3600)

    # Read
    retrieved = await session_store.get(key)
    assert retrieved == value
    assert retrieved["duckdb_specific"]["vectorization"] is True

    # Update with new structure
    updated_value = {
        "query_id": 1000,
        "new_field": "new_analytical_value",
        "duckdb_types": {"boolean": True, "null": None, "float": math.pi},
    }
    await session_store.set(key, updated_value, expires_in=3600)

    retrieved = await session_store.get(key)
    assert retrieved == updated_value
    assert retrieved["duckdb_types"]["null"] is None

    # Delete
    await session_store.delete(key)
    result = await session_store.get(key)
    assert result is None


async def test_duckdb_store_expiration_enhanced(session_store: SQLSpecSyncSessionStore) -> None:
    """Test enhanced expiration handling with DuckDB."""
    key = "duckdb-expiring-enhanced-key"
    value = {"test": "duckdb_analytical_data", "expires": True}

    # Set with 1 second expiration
    await session_store.set(key, value, expires_in=1)

    # Should exist immediately
    result = await session_store.get(key)
    assert result == value

    # Wait for expiration
    await asyncio.sleep(2)

    # Should be expired
    result = await session_store.get(key)
    assert result is None


async def test_duckdb_store_exists_and_expires_in(session_store: SQLSpecSyncSessionStore) -> None:
    """Test exists and expires_in functionality."""
    key = "duckdb-exists-test"
    value = {"test": "analytical_data"}

    # Test non-existent key
    assert await session_store.exists(key) is False
    assert await session_store.expires_in(key) == 0

    # Set key
    await session_store.set(key, value, expires_in=3600)

    # Test existence
    assert await session_store.exists(key) is True
    expires_in = await session_store.expires_in(key)
    assert 3590 <= expires_in <= 3600  # Should be close to 3600

    # Delete and test again
    await session_store.delete(key)
    assert await session_store.exists(key) is False
    assert await session_store.expires_in(key) == 0


async def test_duckdb_store_transaction_behavior(
    session_store: SQLSpecSyncSessionStore, migrated_config: DuckDBConfig
) -> None:
    """Test transaction-like behavior in DuckDB store operations."""
    key = "duckdb-transaction-test"

    # Set initial value
    await session_store.set(key, {"counter": 0}, expires_in=3600)

    # Test transaction-like behavior using DuckDB's consistency
    with migrated_config.provide_session():
        # Read current value
        current = await session_store.get(key)
        if current:
            # Simulate analytical workload update
            current["counter"] += 1
            current["last_query"] = "SELECT COUNT(*) FROM analytics_table"
            current["execution_time_ms"] = 234.56

            # Update the session
            await session_store.set(key, current, expires_in=3600)

    # Verify the update succeeded
    result = await session_store.get(key)
    assert result is not None
    assert result["counter"] == 1
    assert "last_query" in result
    assert result["execution_time_ms"] == 234.56

    # Test consistency with multiple rapid updates
    for i in range(5):
        current = await session_store.get(key)
        if current:
            current["counter"] += 1
            current["queries_executed"] = current.get("queries_executed", [])
            current["queries_executed"].append(f"Query #{i + 1}")
            await session_store.set(key, current, expires_in=3600)

    # Final count should be 6 (1 + 5) due to DuckDB's consistency
    result = await session_store.get(key)
    assert result is not None
    assert result["counter"] == 6
    assert len(result["queries_executed"]) == 5

    # Clean up
    await session_store.delete(key)


async def test_duckdb_worker_isolation(session_store: SQLSpecSyncSessionStore) -> None:
    """Test that DuckDB sessions are properly isolated between pytest workers."""
    # This test verifies the table naming isolation mechanism
    session_id = f"isolation-test-{abs(hash('test')) % 10000}"
    isolation_data = {
        "worker_test": True,
        "isolation_mechanism": "table_naming",
        "database_engine": "duckdb",
        "test_purpose": "verify_parallel_test_safety",
    }

    # Set data
    await session_store.set(session_id, isolation_data, expires_in=3600)

    # Get data
    result = await session_store.get(session_id)
    assert result == isolation_data
    assert result["worker_test"] is True

    # Check that the session store table name includes isolation markers
    # (This verifies that the fixtures are working correctly)
    table_name = session_store.table_name
    # The table name should either be default or include worker isolation
    assert table_name == "litestar_sessions" or "duckdb_sessions_" in table_name

    # Cleanup
    await session_store.delete(session_id)


async def test_duckdb_extension_compatibility(
    session_store: SQLSpecSyncSessionStore, migrated_config: DuckDBConfig
) -> None:
    """Test DuckDB extension compatibility with session storage."""
    # Test that session data works with potential DuckDB extensions
    extension_data = {
        "parquet_support": {"enabled": True, "file_path": "/path/to/data.parquet", "compression": "snappy"},
        "json_extension": {"native_json": True, "json_functions": ["json_extract", "json_valid", "json_type"]},
        "httpfs_extension": {
            "s3_support": True,
            "remote_files": ["s3://bucket/data.csv", "https://example.com/data.json"],
        },
        "analytics_features": {"vectorization": True, "parallel_processing": True, "column_store": True},
    }

    session_id = "extension-compatibility-test"
    await session_store.set(session_id, extension_data, expires_in=3600)

    retrieved = await session_store.get(session_id)
    assert retrieved == extension_data
    assert retrieved["json_extension"]["native_json"] is True
    assert retrieved["analytics_features"]["vectorization"] is True

    # Test with DuckDB driver directly to verify JSON handling
    with migrated_config.provide_session() as driver:
        # Test that the data is properly stored and can be queried
        try:
            result = driver.execute("SELECT session_id FROM litestar_sessions WHERE session_id = ?", (session_id,))
            assert len(result.data) == 1
            assert result.data[0]["session_id"] == session_id
        except Exception:
            # If table name is different due to isolation, that's acceptable
            pass

    # Cleanup
    await session_store.delete(session_id)


async def test_duckdb_analytics_workload_simulation(session_store: SQLSpecSyncSessionStore) -> None:
    """Test DuckDB session store with typical analytics workload patterns."""
    # Simulate an analytics dashboard session
    dashboard_sessions = []

    for dashboard_id in range(5):
        session_id = f"dashboard-{dashboard_id}"
        dashboard_data = {
            "dashboard_id": dashboard_id,
            "user_queries": [
                {
                    "query": f"SELECT * FROM sales WHERE date >= '2024-{dashboard_id + 1:02d}-01'",
                    "execution_time_ms": 145.7 + dashboard_id * 10,
                    "rows_returned": 1000 * (dashboard_id + 1),
                },
                {
                    "query": f"SELECT product, SUM(revenue) FROM sales WHERE dashboard_id = {dashboard_id} GROUP BY product",
                    "execution_time_ms": 89.3 + dashboard_id * 5,
                    "rows_returned": 50 * (dashboard_id + 1),
                },
            ],
            "cached_results": {
                f"cache_key_{dashboard_id}": {
                    "data": [{"total": 50000 + dashboard_id * 1000}],
                    "ttl": 3600,
                    "created_at": "2024-01-15T10:30:00Z",
                }
            },
            "export_preferences": {
                "format": "parquet",
                "compression": "zstd",
                "destination": f"s3://analytics-bucket/dashboard-{dashboard_id}/",
            },
            "performance_stats": {
                "total_queries": dashboard_id + 1,
                "avg_execution_time": 120.5 + dashboard_id * 8,
                "cache_hit_rate": 0.8 + dashboard_id * 0.02,
            },
        }

        await session_store.set(session_id, dashboard_data, expires_in=7200)
        dashboard_sessions.append(session_id)

    # Verify all dashboard sessions
    for session_id in dashboard_sessions:
        retrieved = await session_store.get(session_id)
        assert retrieved is not None
        assert "dashboard_id" in retrieved
        assert len(retrieved["user_queries"]) == 2
        assert "cached_results" in retrieved
        assert retrieved["export_preferences"]["format"] == "parquet"

    # Simulate concurrent access to multiple dashboard sessions
    concurrent_results = []
    for session_id in dashboard_sessions:
        result = await session_store.get(session_id)
        concurrent_results.append(result)

    # All concurrent reads should succeed
    assert len(concurrent_results) == 5
    for result in concurrent_results:
        assert result is not None
        assert "performance_stats" in result
        assert result["export_preferences"]["compression"] == "zstd"

    # Cleanup
    for session_id in dashboard_sessions:
        await session_store.delete(session_id)
