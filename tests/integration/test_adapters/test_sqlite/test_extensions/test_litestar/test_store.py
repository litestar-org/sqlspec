"""Integration tests for SQLite session store."""

import asyncio
import tempfile

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore

pytestmark = [pytest.mark.sqlite, pytest.mark.integration]


@pytest.fixture
def sqlite_config() -> SqliteConfig:
    """Create SQLite configuration for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        return SqliteConfig(pool_config={"database": tmp_file.name})


@pytest.fixture
async def store(sqlite_config: SqliteConfig) -> SQLSpecSessionStore:
    """Create a session store instance."""
    return SQLSpecSessionStore(
        config=sqlite_config,
        table_name="test_store",
        session_id_column="key",
        data_column="value",
        expires_at_column="expires",
        created_at_column="created",
    )


async def test_sqlite_store_table_creation(store: SQLSpecSessionStore, sqlite_config: SqliteConfig) -> None:
    """Test that store table is created automatically."""
    async with sqlite_config.provide_session() as driver:
        await store._ensure_table_exists(driver)

        # Verify table exists
        result = await driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test_store'")
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test_store"

        # Verify table structure
        result = await driver.execute("PRAGMA table_info(test_store)")
        columns = {row["name"] for row in result.data}
        assert "key" in columns
        assert "value" in columns
        assert "expires" in columns
        assert "created" in columns


async def test_sqlite_store_crud_operations(store: SQLSpecSessionStore) -> None:
    """Test complete CRUD operations on the store."""
    key = "test-key"
    value = {"user_id": 123, "data": ["item1", "item2"], "nested": {"key": "value"}}

    # Create
    await store.set(key, value, expires_in=3600)

    # Read
    retrieved = await store.get(key)
    assert retrieved == value

    # Update
    updated_value = {"user_id": 456, "new_field": "new_value"}
    await store.set(key, updated_value, expires_in=3600)

    retrieved = await store.get(key)
    assert retrieved == updated_value

    # Delete
    await store.delete(key)
    result = await store.get(key)
    assert result is None


async def test_sqlite_store_expiration(store: SQLSpecSessionStore) -> None:
    """Test that expired entries are not returned."""
    key = "expiring-key"
    value = {"test": "data"}

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


async def test_sqlite_store_default_values(store: SQLSpecSessionStore) -> None:
    """Test default value handling."""
    # Non-existent key with default
    result = await store.get("non-existent", default={"default": True})
    assert result == {"default": True}

    # Non-existent key without default (should return None)
    result = await store.get("non-existent")
    assert result is None


async def test_sqlite_store_bulk_operations(store: SQLSpecSessionStore) -> None:
    """Test bulk operations on the store."""
    # Create multiple entries
    entries = {}
    for i in range(10):
        key = f"bulk-key-{i}"
        value = {"index": i, "data": f"value-{i}"}
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


async def test_sqlite_store_large_data(store: SQLSpecSessionStore) -> None:
    """Test storing large data structures."""
    # Create a large data structure
    large_data = {
        "users": [{"id": i, "name": f"user_{i}", "email": f"user{i}@example.com"} for i in range(100)],
        "settings": {f"setting_{i}": {"value": i, "enabled": i % 2 == 0} for i in range(50)},
        "logs": [f"Log entry {i}: " + "x" * 100 for i in range(50)],
    }

    key = "large-data"
    await store.set(key, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved = await store.get(key)
    assert retrieved == large_data
    assert len(retrieved["users"]) == 100
    assert len(retrieved["settings"]) == 50
    assert len(retrieved["logs"]) == 50


async def test_sqlite_store_concurrent_access(store: SQLSpecSessionStore) -> None:
    """Test concurrent access to the store."""

    async def update_value(key: str, value: int) -> None:
        """Update a value in the store."""
        await store.set(key, {"value": value}, expires_in=3600)

    # Create concurrent updates
    key = "concurrent-key"
    tasks = [update_value(key, i) for i in range(20)]
    await asyncio.gather(*tasks)

    # The last update should win
    result = await store.get(key)
    assert result is not None
    assert "value" in result
    assert 0 <= result["value"] <= 19


async def test_sqlite_store_get_all(store: SQLSpecSessionStore) -> None:
    """Test retrieving all entries from the store."""
    # Create multiple entries with different expiration times
    await store.set("key1", {"data": 1}, expires_in=3600)
    await store.set("key2", {"data": 2}, expires_in=3600)
    await store.set("key3", {"data": 3}, expires_in=1)  # Will expire soon

    # Get all entries
    all_entries = {}
    async for key, value in store.get_all():
        all_entries[key] = value

    # Should have all three initially
    assert len(all_entries) >= 2  # At least the non-expiring ones
    assert all_entries.get("key1") == {"data": 1}
    assert all_entries.get("key2") == {"data": 2}

    # Wait for one to expire
    await asyncio.sleep(2)

    # Get all again
    all_entries = {}
    async for key, value in store.get_all():
        all_entries[key] = value

    # Should only have non-expired entries
    assert "key1" in all_entries
    assert "key2" in all_entries
    assert "key3" not in all_entries  # Should be expired


async def test_sqlite_store_delete_expired(store: SQLSpecSessionStore) -> None:
    """Test deletion of expired entries."""
    # Create entries with different expiration times
    await store.set("short1", {"data": 1}, expires_in=1)
    await store.set("short2", {"data": 2}, expires_in=1)
    await store.set("long1", {"data": 3}, expires_in=3600)
    await store.set("long2", {"data": 4}, expires_in=3600)

    # Wait for short-lived entries to expire
    await asyncio.sleep(2)

    # Delete expired entries
    await store.delete_expired()

    # Check which entries remain
    assert await store.get("short1") is None
    assert await store.get("short2") is None
    assert await store.get("long1") == {"data": 3}
    assert await store.get("long2") == {"data": 4}


async def test_sqlite_store_special_characters(store: SQLSpecSessionStore) -> None:
    """Test handling of special characters in keys and values."""
    # Test special characters in keys
    special_keys = [
        "key-with-dash",
        "key_with_underscore",
        "key.with.dots",
        "key:with:colons",
        "key/with/slashes",
        "key@with@at",
        "key#with#hash",
    ]

    for key in special_keys:
        value = {"key": key}
        await store.set(key, value, expires_in=3600)
        retrieved = await store.get(key)
        assert retrieved == value

    # Test special characters in values
    special_value = {
        "unicode": "こんにちは世界",
        "emoji": "🚀🎉😊",
        "quotes": "He said \"hello\" and 'goodbye'",
        "newlines": "line1\nline2\nline3",
        "tabs": "col1\tcol2\tcol3",
        "special": "!@#$%^&*()[]{}|\\<>?,./",
    }

    await store.set("special-value", special_value, expires_in=3600)
    retrieved = await store.get("special-value")
    assert retrieved == special_value