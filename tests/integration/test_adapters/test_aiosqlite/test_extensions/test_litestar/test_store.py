"""Integration tests for aiosqlite session store with migration support."""

import asyncio
import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.extensions.litestar import SQLSpecAsyncSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = [pytest.mark.anyio, pytest.mark.aiosqlite, pytest.mark.integration, pytest.mark.xdist_group("aiosqlite")]


@pytest.fixture
async def aiosqlite_config() -> "AsyncGenerator[AiosqliteConfig, None]":
    """Create aiosqlite configuration with migration support."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "store.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],  # Include Litestar migrations
            },
        )
        yield config
        # Cleanup
        await config.close_pool()


@pytest.fixture
async def store(aiosqlite_config: AiosqliteConfig) -> SQLSpecAsyncSessionStore:
    """Create a session store instance with migrations applied."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(aiosqlite_config)
    await commands.init(aiosqlite_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Use the migrated table structure
    return SQLSpecAsyncSessionStore(
        config=aiosqlite_config,
        table_name="litestar_sessions",
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


async def test_aiosqlite_store_table_creation(
    store: SQLSpecAsyncSessionStore, aiosqlite_config: AiosqliteConfig
) -> None:
    """Test that store table is created via migrations."""
    async with aiosqlite_config.provide_session() as driver:
        # Verify table exists (created by migrations)
        result = await driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='litestar_sessions'")
        assert len(result.data) == 1
        assert result.data[0]["name"] == "litestar_sessions"

        # Verify table structure
        result = await driver.execute("PRAGMA table_info(litestar_sessions)")
        columns = {row["name"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns


async def test_aiosqlite_store_crud_operations(store: SQLSpecAsyncSessionStore) -> None:
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


async def test_aiosqlite_store_expiration(store: SQLSpecAsyncSessionStore) -> None:
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
    result = await store.get(key)
    assert result is None


async def test_aiosqlite_store_default_values(store: SQLSpecAsyncSessionStore) -> None:
    """Test default value handling."""
    # Non-existent key should return None
    result = await store.get("non-existent")
    assert result is None

    # Test with our own default handling
    result = await store.get("non-existent")
    if result is None:
        result = {"default": True}
    assert result == {"default": True}


async def test_aiosqlite_store_bulk_operations(store: SQLSpecAsyncSessionStore) -> None:
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


async def test_aiosqlite_store_large_data(store: SQLSpecAsyncSessionStore) -> None:
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


async def test_aiosqlite_store_concurrent_access(store: SQLSpecAsyncSessionStore) -> None:
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


async def test_aiosqlite_store_get_all(store: SQLSpecAsyncSessionStore) -> None:
    """Test retrieving all entries from the store."""
    # Create multiple entries with different expiration times
    await store.set("key1", {"data": 1}, expires_in=3600)
    await store.set("key2", {"data": 2}, expires_in=3600)
    await store.set("key3", {"data": 3}, expires_in=1)  # Will expire soon

    # Get all entries
    all_entries = {key: value async for key, value in store.get_all()}

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


async def test_aiosqlite_store_delete_expired(store: SQLSpecAsyncSessionStore) -> None:
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


async def test_aiosqlite_store_special_characters(store: SQLSpecAsyncSessionStore) -> None:
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
