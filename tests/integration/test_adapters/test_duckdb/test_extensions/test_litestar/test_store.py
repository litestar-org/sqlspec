"""Integration tests for DuckDB session store."""

import asyncio
import tempfile

import pytest

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore

pytestmark = [pytest.mark.duckdb, pytest.mark.integration]


@pytest.fixture
def duckdb_config() -> DuckDBConfig:
    """Create DuckDB configuration for testing."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
        return DuckDBConfig(pool_config={"database": tmp_file.name})


@pytest.fixture
async def store(duckdb_config: DuckDBConfig) -> SQLSpecSessionStore:
    """Create a session store instance."""
    return SQLSpecSessionStore(
        config=duckdb_config,
        table_name="test_store_duckdb",
        session_id_column="key",
        data_column="value",
        expires_at_column="expires",
        created_at_column="created",
    )


async def test_duckdb_store_table_creation(store: SQLSpecSessionStore, duckdb_config: DuckDBConfig) -> None:
    """Test that store table is created automatically."""
    async with duckdb_config.provide_session() as driver:
        await store._ensure_table_exists(driver)

        # Verify table exists
        result = await driver.execute("SELECT * FROM information_schema.tables WHERE table_name = 'test_store_duckdb'")
        assert len(result.data) == 1

        # Verify table structure
        result = await driver.execute("PRAGMA table_info('test_store_duckdb')")
        columns = {row["name"] for row in result.data}
        assert "key" in columns
        assert "value" in columns
        assert "expires" in columns
        assert "created" in columns


async def test_duckdb_store_crud_operations(store: SQLSpecSessionStore) -> None:
    """Test complete CRUD operations on the DuckDB store."""
    key = "duckdb-test-key"
    value = {
        "dataset_id": 456,
        "query": "SELECT * FROM analytics",
        "results": [{"col1": 1, "col2": "a"}, {"col1": 2, "col2": "b"}],
        "metadata": {"rows": 2, "execution_time": 0.05},
    }

    # Create
    await store.set(key, value, expires_in=3600)

    # Read
    retrieved = await store.get(key)
    assert retrieved == value
    assert retrieved["metadata"]["execution_time"] == 0.05

    # Update
    updated_value = {
        "dataset_id": 789,
        "new_field": "analytical_data",
        "parquet_files": ["file1.parquet", "file2.parquet"],
    }
    await store.set(key, updated_value, expires_in=3600)

    retrieved = await store.get(key)
    assert retrieved == updated_value
    assert "parquet_files" in retrieved

    # Delete
    await store.delete(key)
    result = await store.get(key)
    assert result is None


async def test_duckdb_store_expiration(store: SQLSpecSessionStore) -> None:
    """Test that expired entries are not returned from DuckDB."""
    key = "duckdb-expiring-key"
    value = {"test": "analytical_data", "source": "duckdb"}

    # Set with 1 second expiration
    await store.set(key, value, expires_in=1)

    # Should exist immediately
    result = await store.get(key)
    assert result == value

    # Wait for expiration
    await asyncio.sleep(2)

    # Should be expired
    result = await store.get(key, default={"expired": True})
    assert result == {"expired": True}


async def test_duckdb_store_bulk_operations(store: SQLSpecSessionStore) -> None:
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
        await store.set(key, value, expires_in=3600)

    # Verify all entries exist
    for key, expected_value in entries.items():
        result = await store.get(key)
        assert result == expected_value

    # Delete all entries
    for key in entries:
        await store.delete(key)

    # Verify all are deleted
    for key in entries:
        result = await store.get(key)
        assert result is None


async def test_duckdb_store_analytical_data(store: SQLSpecSessionStore) -> None:
    """Test storing analytical data structures typical for DuckDB."""
    # Create analytical data structure
    analytical_data = {
        "query_plan": {
            "type": "PROJECTION",
            "children": [
                {
                    "type": "FILTER",
                    "condition": "year > 2020",
                    "children": [{"type": "TABLE_SCAN", "table": "sales", "columns": ["year", "amount"]}],
                }
            ],
        },
        "statistics": {
            "total_rows": 1000000,
            "filtered_rows": 250000,
            "output_rows": 250000,
            "execution_time_ms": 45.7,
            "memory_usage_mb": 128.5,
        },
        "result_preview": [
            {"year": 2021, "amount": 100000.50},
            {"year": 2022, "amount": 150000.75},
            {"year": 2023, "amount": 200000.25},
        ],
        "export_formats": ["parquet", "csv", "json", "arrow"],
    }

    key = "duckdb-analytical"
    await store.set(key, analytical_data, expires_in=3600)

    # Retrieve and verify
    retrieved = await store.get(key)
    assert retrieved == analytical_data
    assert retrieved["statistics"]["execution_time_ms"] == 45.7
    assert retrieved["query_plan"]["type"] == "PROJECTION"
    assert len(retrieved["result_preview"]) == 3


async def test_duckdb_store_concurrent_access(store: SQLSpecSessionStore) -> None:
    """Test concurrent access to the DuckDB store."""

    async def update_query_result(key: str, query_id: int) -> None:
        """Update a query result in the store."""
        await store.set(key, {"query_id": query_id, "status": "completed", "rows": query_id * 100}, expires_in=3600)

    # Create concurrent updates simulating multiple query results
    key = "duckdb-concurrent-query"
    tasks = [update_query_result(key, i) for i in range(30)]
    await asyncio.gather(*tasks)

    # The last update should win
    result = await store.get(key)
    assert result is not None
    assert "query_id" in result
    assert 0 <= result["query_id"] <= 29
    assert result["status"] == "completed"


async def test_duckdb_store_get_all(store: SQLSpecSessionStore) -> None:
    """Test retrieving all entries from the DuckDB store."""
    # Create multiple query results with different expiration times
    await store.set("duckdb-query-1", {"query": "SELECT 1", "status": "completed"}, expires_in=3600)
    await store.set("duckdb-query-2", {"query": "SELECT 2", "status": "completed"}, expires_in=3600)
    await store.set("duckdb-query-3", {"query": "SELECT 3", "status": "running"}, expires_in=1)

    # Get all entries
    all_entries = {}
    async for key, value in store.get_all():
        if key.startswith("duckdb-query-"):
            all_entries[key] = value

    # Should have all three initially
    assert len(all_entries) >= 2
    assert all_entries.get("duckdb-query-1") == {"query": "SELECT 1", "status": "completed"}
    assert all_entries.get("duckdb-query-2") == {"query": "SELECT 2", "status": "completed"}

    # Wait for one to expire
    await asyncio.sleep(2)

    # Get all again
    all_entries = {}
    async for key, value in store.get_all():
        if key.startswith("duckdb-query-"):
            all_entries[key] = value

    # Should only have non-expired entries
    assert "duckdb-query-1" in all_entries
    assert "duckdb-query-2" in all_entries
    assert "duckdb-query-3" not in all_entries


async def test_duckdb_store_delete_expired(store: SQLSpecSessionStore) -> None:
    """Test deletion of expired entries in DuckDB."""
    # Create entries representing temporary and permanent query results
    temp_queries = ["duckdb-temp-1", "duckdb-temp-2"]
    perm_queries = ["duckdb-perm-1", "duckdb-perm-2"]

    for key in temp_queries:
        await store.set(key, {"type": "temporary", "data": key}, expires_in=1)

    for key in perm_queries:
        await store.set(key, {"type": "permanent", "data": key}, expires_in=3600)

    # Wait for temporary queries to expire
    await asyncio.sleep(2)

    # Delete expired entries
    await store.delete_expired()

    # Check which entries remain
    for key in temp_queries:
        assert await store.get(key) is None

    for key in perm_queries:
        result = await store.get(key)
        assert result is not None
        assert result["type"] == "permanent"


async def test_duckdb_store_special_characters(store: SQLSpecSessionStore) -> None:
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
        await store.set(key, value, expires_in=3600)
        retrieved = await store.get(key)
        assert retrieved == value

    # Test DuckDB-specific data types in values
    special_value = {
        "sql_query": "SELECT * FROM 'data.parquet' WHERE year > 2020",
        "file_paths": ["/data/file1.parquet", "/data/file2.csv"],
        "decimal_values": [123.456789, 987.654321],
        "large_integers": [9223372036854775807, -9223372036854775808],  # int64 range
        "nested_arrays": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        "struct_data": {"nested": {"deeply": {"nested": {"value": 42}}}},
        "null_values": [None, "not_null", None],
        "unicode": "DuckDB: 🦆 Analytics データ分析",
    }

    await store.set("duckdb-special-value", special_value, expires_in=3600)
    retrieved = await store.get("duckdb-special-value")
    assert retrieved == special_value
    assert retrieved["large_integers"][0] == 9223372036854775807
    assert retrieved["null_values"][0] is None
