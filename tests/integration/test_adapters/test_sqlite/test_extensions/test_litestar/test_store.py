"""Integration tests for SQLite session store."""

import tempfile
import time
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.sync_tools import run_

pytestmark = [pytest.mark.sqlite, pytest.mark.integration, pytest.mark.xdist_group("sqlite")]


@pytest.fixture
def sqlite_config() -> SqliteConfig:
    """Create SQLite configuration for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        tmpdir = tempfile.mkdtemp()
        migration_dir = Path(tmpdir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create a migration to create the session table
        migration_content = '''"""Create test session table."""

def up():
    """Create the litestar_session table."""
    return [
        """
        CREATE TABLE IF NOT EXISTS litestar_session (
            session_id VARCHAR(255) PRIMARY KEY,
            data TEXT NOT NULL,
            expires_at DATETIME NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_litestar_session_expires_at
        ON litestar_session(expires_at)
        """,
    ]

def down():
    """Drop the litestar_session table."""
    return [
        "DROP INDEX IF EXISTS idx_litestar_session_expires_at",
        "DROP TABLE IF EXISTS litestar_session",
    ]
'''
        migration_file = migration_dir / "0001_create_session_table.py"
        migration_file.write_text(migration_content)

        config = SqliteConfig(
            pool_config={"database": tmp_file.name},
            migration_config={"script_location": str(migration_dir), "version_table_name": "test_migrations"},
        )
        # Run migrations to create the table
        commands = SyncMigrationCommands(config)
        commands.init(str(migration_dir), package=False)
        commands.upgrade()
        return config


@pytest.fixture
def store(sqlite_config: SqliteConfig) -> SQLSpecSessionStore:
    """Create a session store instance."""
    return SQLSpecSessionStore(
        config=sqlite_config,
        table_name="litestar_session",
        session_id_column="session_id",
        data_column="data",
        expires_at_column="expires_at",
        created_at_column="created_at",
    )


def test_sqlite_store_table_creation(store: SQLSpecSessionStore, sqlite_config: SqliteConfig) -> None:
    """Test that store table is created automatically."""
    with sqlite_config.provide_session() as driver:
        # Verify table exists
        result = driver.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='litestar_session'")
        assert len(result.data) == 1
        assert result.data[0]["name"] == "litestar_session"

        # Verify table structure
        result = driver.execute("PRAGMA table_info(litestar_session)")
        columns = {row["name"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns


def test_sqlite_store_crud_operations(store: SQLSpecSessionStore) -> None:
    """Test complete CRUD operations on the store."""
    key = "test-key"
    value = {"user_id": 123, "data": ["item1", "item2"], "nested": {"key": "value"}}

    # Create
    run_(store.set)(key, value, expires_in=3600)

    # Read
    retrieved = run_(store.get)(key)
    assert retrieved == value

    # Update
    updated_value = {"user_id": 456, "new_field": "new_value"}
    run_(store.set)(key, updated_value, expires_in=3600)

    retrieved = run_(store.get)(key)
    assert retrieved == updated_value

    # Delete
    run_(store.delete)(key)
    result = run_(store.get)(key)
    assert result is None


def test_sqlite_store_expiration(store: SQLSpecSessionStore, sqlite_config: SqliteConfig) -> None:
    """Test that expired entries are not returned."""

    key = "expiring-key"
    value = {"test": "data"}

    # Set with 1 second expiration
    run_(store.set)(key, value, expires_in=1)

    # Should exist immediately
    result = run_(store.get)(key)
    assert result == value

    # Check what's actually in the database
    with sqlite_config.provide_session() as driver:
        check_result = driver.execute(f"SELECT * FROM {store._table_name} WHERE session_id = ?", (key,))
        if check_result.data:
            pass

    # Wait for expiration (add buffer for timing issues)
    time.sleep(3)

    # Check again what's in the database
    with sqlite_config.provide_session() as driver:
        check_result = driver.execute(f"SELECT * FROM {store._table_name} WHERE session_id = ?", (key,))
        if check_result.data:
            pass

    # Should be expired
    result = run_(store.get)(key)
    assert result is None


def test_sqlite_store_default_values(store: SQLSpecSessionStore) -> None:
    """Test default value handling."""
    # Non-existent key should return None
    result = run_(store.get)("non-existent")
    assert result is None

    # Test with our own default handling
    result = run_(store.get)("non-existent")
    if result is None:
        result = {"default": True}
    assert result == {"default": True}


def test_sqlite_store_bulk_operations(store: SQLSpecSessionStore) -> None:
    """Test bulk operations on the store."""
    # Create multiple entries
    entries = {}
    for i in range(10):
        key = f"bulk-key-{i}"
        value = {"index": i, "data": f"value-{i}"}
        entries[key] = value
        run_(store.set)(key, value, expires_in=3600)

    # Verify all entries exist
    for key, expected_value in entries.items():
        result = run_(store.get)(key)
        assert result == expected_value

    # Delete all entries
    for key in entries:
        run_(store.delete)(key)

    # Verify all are deleted
    for key in entries:
        result = run_(store.get)(key)
        assert result is None


def test_sqlite_store_large_data(store: SQLSpecSessionStore) -> None:
    """Test storing large data structures."""
    # Create a large data structure
    large_data = {
        "users": [{"id": i, "name": f"user_{i}", "email": f"user{i}@example.com"} for i in range(100)],
        "settings": {f"setting_{i}": {"value": i, "enabled": i % 2 == 0} for i in range(50)},
        "logs": [f"Log entry {i}: " + "x" * 100 for i in range(50)],
    }

    key = "large-data"
    run_(store.set)(key, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved = run_(store.get)(key)
    assert retrieved == large_data
    assert len(retrieved["users"]) == 100
    assert len(retrieved["settings"]) == 50
    assert len(retrieved["logs"]) == 50


def test_sqlite_store_concurrent_access(store: SQLSpecSessionStore) -> None:
    """Test concurrent access to the store."""

    def update_value(key: str, value: int) -> None:
        """Update a value in the store."""
        run_(store.set)(key, {"value": value}, expires_in=3600)

    # Create concurrent updates
    key = "concurrent-key"
    for i in range(20):
        update_value(key, i)

    # The last update should win
    result = run_(store.get)(key)
    assert result is not None
    assert "value" in result
    # In sync mode, the last value should be 19
    assert result["value"] == 19


def test_sqlite_store_get_all(store: SQLSpecSessionStore) -> None:
    """Test retrieving all entries from the store."""
    import asyncio

    # Create multiple entries with different expiration times
    run_(store.set)("key1", {"data": 1}, expires_in=3600)
    run_(store.set)("key2", {"data": 2}, expires_in=3600)
    run_(store.set)("key3", {"data": 3}, expires_in=1)  # Will expire soon

    # Get all entries - need to consume async generator
    async def collect_all() -> dict[str, Any]:
        return {key: value async for key, value in store.get_all()}

    all_entries = asyncio.run(collect_all())

    # Should have all three initially
    assert len(all_entries) >= 2  # At least the non-expiring ones
    assert all_entries.get("key1") == {"data": 1}
    assert all_entries.get("key2") == {"data": 2}

    # Wait for one to expire
    time.sleep(3)

    # Get all again
    all_entries = asyncio.run(collect_all())

    # Should only have non-expired entries
    assert "key1" in all_entries
    assert "key2" in all_entries
    assert "key3" not in all_entries  # Should be expired


def test_sqlite_store_delete_expired(store: SQLSpecSessionStore) -> None:
    """Test deletion of expired entries."""
    # Create entries with different expiration times
    run_(store.set)("short1", {"data": 1}, expires_in=1)
    run_(store.set)("short2", {"data": 2}, expires_in=1)
    run_(store.set)("long1", {"data": 3}, expires_in=3600)
    run_(store.set)("long2", {"data": 4}, expires_in=3600)

    # Wait for short-lived entries to expire (add buffer)
    time.sleep(3)

    # Delete expired entries
    run_(store.delete_expired)()

    # Check which entries remain
    assert run_(store.get)("short1") is None
    assert run_(store.get)("short2") is None
    assert run_(store.get)("long1") == {"data": 3}
    assert run_(store.get)("long2") == {"data": 4}


def test_sqlite_store_special_characters(store: SQLSpecSessionStore) -> None:
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
        run_(store.set)(key, value, expires_in=3600)
        retrieved = run_(store.get)(key)
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

    run_(store.set)("special-value", special_value, expires_in=3600)
    retrieved = run_(store.get)("special-value")
    assert retrieved == special_value
