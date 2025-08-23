"""Integration tests for DuckDB session store."""

import time

import pytest

from sqlspec.extensions.litestar import SQLSpecSessionStore
from sqlspec.utils.sync_tools import run_

pytestmark = [pytest.mark.duckdb, pytest.mark.integration, pytest.mark.xdist_group("duckdb")]


def test_duckdb_store_crud_operations(session_store: SQLSpecSessionStore) -> None:
    """Test complete CRUD operations on the DuckDB store."""
    key = "duckdb-test-key"
    value = {
        "dataset_id": 456,
        "query": "SELECT * FROM analytics",
        "results": [{"col1": 1, "col2": "a"}, {"col1": 2, "col2": "b"}],
        "metadata": {"rows": 2, "execution_time": 0.05},
    }

    # Create
    run_(session_store.set)(key, value, expires_in=3600)

    # Read
    retrieved = run_(session_store.get)(key)
    assert retrieved == value
    assert retrieved["metadata"]["execution_time"] == 0.05

    # Update
    updated_value = {
        "dataset_id": 789,
        "new_field": "analytical_data",
        "parquet_files": ["file1.parquet", "file2.parquet"],
    }
    run_(session_store.set)(key, updated_value, expires_in=3600)

    retrieved = run_(session_store.get)(key)
    assert retrieved == updated_value
    assert "parquet_files" in retrieved

    # Delete
    run_(session_store.delete)(key)
    result = run_(session_store.get)(key)
    assert result is None


def test_duckdb_store_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test that expired entries are not returned from DuckDB."""
    key = "duckdb-expiring-key"
    value = {"test": "analytical_data", "source": "duckdb"}

    # Set with 1 second expiration
    run_(session_store.set)(key, value, expires_in=1)

    # Should exist immediately
    result = run_(session_store.get)(key)
    assert result == value

    # Wait for expiration
    time.sleep(2)

    # Should be expired
    result = run_(session_store.get)(key)
    assert result is None


def test_duckdb_store_bulk_operations(session_store: SQLSpecSessionStore) -> None:
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
        run_(session_store.set)(key, value, expires_in=3600)

    # Verify all entries exist
    for key, expected_value in entries.items():
        result = run_(session_store.get)(key)
        assert result == expected_value

    # Delete all entries
    for key in entries:
        run_(session_store.delete)(key)

    # Verify all are deleted
    for key in entries:
        result = run_(session_store.get)(key)
        assert result is None


def test_duckdb_store_analytical_data(session_store: SQLSpecSessionStore) -> None:
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
    run_(session_store.set)(key, analytical_data, expires_in=3600)

    # Retrieve and verify
    retrieved = run_(session_store.get)(key)
    assert retrieved == analytical_data
    assert retrieved["execution_stats"]["rows_scanned"] == 1_000_000
    assert retrieved["query_plan"]["type"] == "PROJECTION"

    # Cleanup
    run_(session_store.delete)(key)


def test_duckdb_store_concurrent_access(session_store: SQLSpecSessionStore) -> None:
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
        run_(session_store.set)(session_id, session_data, expires_in=3600)

    # Verify all sessions exist
    for session_id, expected_data in sessions.items():
        retrieved = run_(session_store.get)(session_id)
        assert retrieved == expected_data
        assert len(retrieved["datasets"]) == 3
        assert len(retrieved["query_cache"]) == 5

    # Clean up
    for session_id in sessions:
        run_(session_store.delete)(session_id)


def test_duckdb_store_get_all(session_store: SQLSpecSessionStore) -> None:
    """Test getting all entries from the store."""
    # Create test entries
    test_entries = {}
    for i in range(5):
        key = f"get-all-test-{i}"
        value = {"index": i, "data": f"test_data_{i}"}
        test_entries[key] = value
        run_(session_store.set)(key, value, expires_in=3600)

    # Get all entries
    all_entries = []

    async def collect_entries():
        async for key, value in session_store.get_all():
            all_entries.append((key, value))

    run_(collect_entries)()

    # Verify we got all entries (may include entries from other tests)
    retrieved_keys = {key for key, _ in all_entries}
    for test_key in test_entries:
        assert test_key in retrieved_keys

    # Clean up
    for key in test_entries:
        run_(session_store.delete)(key)


def test_duckdb_store_delete_expired(session_store: SQLSpecSessionStore) -> None:
    """Test deleting expired entries."""
    # Create entries with different expiration times
    short_lived_keys = []
    long_lived_keys = []

    for i in range(3):
        short_key = f"short-lived-{i}"
        long_key = f"long-lived-{i}"

        run_(session_store.set)(short_key, {"data": f"short_{i}"}, expires_in=1)
        run_(session_store.set)(long_key, {"data": f"long_{i}"}, expires_in=3600)

        short_lived_keys.append(short_key)
        long_lived_keys.append(long_key)

    # Wait for short-lived entries to expire
    time.sleep(2)

    # Delete expired entries
    run_(session_store.delete_expired)()

    # Verify short-lived entries are gone
    for key in short_lived_keys:
        assert run_(session_store.get)(key) is None

    # Verify long-lived entries still exist
    for key in long_lived_keys:
        assert run_(session_store.get)(key) is not None

    # Clean up remaining entries
    for key in long_lived_keys:
        run_(session_store.delete)(key)


def test_duckdb_store_special_characters(session_store: SQLSpecSessionStore) -> None:
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
        run_(session_store.set)(key, value, expires_in=3600)

        retrieved = run_(session_store.get)(key)
        assert retrieved == value

        run_(session_store.delete)(key)

    # Test special characters in values
    special_values = [
        {"sql": "SELECT * FROM 'path with spaces/data.parquet'"},
        {"message": "Query failed: Can't parse 'invalid_date'"},
        {"json_data": {"nested": 'quotes "inside" strings'}},
        {"unicode": "Analytics 📊 Dashboard 🚀"},
        {"newlines": "Line 1\nLine 2\tTabbed content"},
    ]

    for i, value in enumerate(special_values):
        key = f"special-value-{i}"
        run_(session_store.set)(key, value, expires_in=3600)

        retrieved = run_(session_store.get)(key)
        assert retrieved == value

        run_(session_store.delete)(key)
