"""Integration tests for SQLite session store."""

import asyncio
import math
import tempfile
import time
from pathlib import Path
from typing import Any

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.sync_tools import async_, run_

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


async def test_sqlite_store_bulk_operations(store: SQLSpecSessionStore) -> None:
    """Test bulk operations on the SQLite store."""

    @async_
    async def run_bulk_test():
        # Create multiple entries efficiently
        entries = {}
        tasks = []
        for i in range(25):  # More entries to test SQLite performance
            key = f"sqlite-bulk-{i}"
            value = {"index": i, "data": f"value-{i}", "metadata": {"created_by": "test", "batch": i // 5}}
            entries[key] = value
            tasks.append(store.set(key, value, expires_in=3600))

        # Execute all inserts concurrently (SQLite will serialize them)
        await asyncio.gather(*tasks)

        # Verify all entries exist
        verify_tasks = [store.get(key) for key in entries]
        results = await asyncio.gather(*verify_tasks)

        for (key, expected_value), result in zip(entries.items(), results):
            assert result == expected_value

        # Delete all entries concurrently
        delete_tasks = [store.delete(key) for key in entries]
        await asyncio.gather(*delete_tasks)

        # Verify all are deleted
        verify_tasks = [store.get(key) for key in entries]
        results = await asyncio.gather(*verify_tasks)
        assert all(result is None for result in results)

    await run_bulk_test()


def test_sqlite_store_large_data(store: SQLSpecSessionStore) -> None:
    """Test storing large data structures in SQLite."""
    # Create a large data structure that tests SQLite's JSON capabilities
    large_data = {
        "users": [
            {
                "id": i,
                "name": f"user_{i}",
                "email": f"user{i}@example.com",
                "profile": {
                    "bio": f"Bio text for user {i} " + "x" * 100,
                    "tags": [f"tag_{j}" for j in range(10)],
                    "settings": {f"setting_{j}": j for j in range(20)},
                },
            }
            for i in range(100)  # Test SQLite capacity
        ],
        "analytics": {
            "metrics": {f"metric_{i}": {"value": i * 1.5, "timestamp": f"2024-01-{i:02d}"} for i in range(1, 32)},
            "events": [{"type": f"event_{i}", "data": "x" * 300} for i in range(50)],
        },
    }

    key = "sqlite-large-data"
    run_(store.set)(key, large_data, expires_in=3600)

    # Retrieve and verify
    retrieved = run_(store.get)(key)
    assert retrieved == large_data
    assert len(retrieved["users"]) == 100
    assert len(retrieved["analytics"]["metrics"]) == 31
    assert len(retrieved["analytics"]["events"]) == 50


async def test_sqlite_store_concurrent_access(store: SQLSpecSessionStore) -> None:
    """Test concurrent access to the SQLite store."""

    async def update_value(key: str, value: int) -> None:
        """Update a value in the store."""
        await store.set(key, {"value": value, "operation": f"update_{value}"}, expires_in=3600)

    @async_
    async def run_concurrent_test():
        # Create many concurrent updates to test SQLite's concurrency handling
        key = "sqlite-concurrent-key"
        tasks = [update_value(key, i) for i in range(50)]
        await asyncio.gather(*tasks)

        # The last update should win
        result = await store.get(key)
        assert result is not None
        assert "value" in result
        assert 0 <= result["value"] <= 49
        assert "operation" in result

    await run_concurrent_test()


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
    """Test handling of special characters in keys and values with SQLite."""
    # Test special characters in keys (SQLite specific)
    special_keys = [
        "key-with-dash",
        "key_with_underscore",
        "key.with.dots",
        "key:with:colons",
        "key/with/slashes",
        "key@with@at",
        "key#with#hash",
        "key$with$dollar",
        "key%with%percent",
        "key&with&ampersand",
        "key'with'quote",  # Single quote
        'key"with"doublequote',  # Double quote
    ]

    for key in special_keys:
        value = {"key": key, "sqlite": True}
        run_(store.set)(key, value, expires_in=3600)
        retrieved = run_(store.get)(key)
        assert retrieved == value

    # Test SQLite-specific data types and special characters in values
    special_value = {
        "unicode": "SQLite: 💾 База данных データベース",
        "emoji": "🚀🎉😊💾🔥💻",
        "quotes": "He said \"hello\" and 'goodbye' and `backticks`",
        "newlines": "line1\nline2\r\nline3",
        "tabs": "col1\tcol2\tcol3",
        "special": "!@#$%^&*()[]{}|\\<>?,./",
        "sqlite_arrays": [1, 2, 3, [4, 5, [6, 7]]],
        "sqlite_json": {"nested": {"deep": {"value": 42}}},
        "null_handling": {"null": None, "not_null": "value"},
        "escape_chars": "\\n\\t\\r\\b\\f",
        "sql_injection_attempt": "'; DROP TABLE test; --",  # Should be safely handled
        "boolean_types": {"true": True, "false": False},
        "numeric_types": {"int": 123, "float": 123.456, "pi": math.pi},
    }

    run_(store.set)("sqlite-special-value", special_value, expires_in=3600)
    retrieved = run_(store.get)("sqlite-special-value")
    assert retrieved == special_value
    assert retrieved["null_handling"]["null"] is None
    assert retrieved["sqlite_arrays"][3] == [4, 5, [6, 7]]
    assert retrieved["boolean_types"]["true"] is True
    assert retrieved["numeric_types"]["pi"] == math.pi


def test_sqlite_store_crud_operations_enhanced(store: SQLSpecSessionStore) -> None:
    """Test enhanced CRUD operations on the SQLite store."""
    key = "sqlite-test-key"
    value = {
        "user_id": 999,
        "data": ["item1", "item2", "item3"],
        "nested": {"key": "value", "number": 123.45},
        "sqlite_specific": {"text": True, "array": [1, 2, 3]},
    }

    # Create
    run_(store.set)(key, value, expires_in=3600)

    # Read
    retrieved = run_(store.get)(key)
    assert retrieved == value
    assert retrieved["sqlite_specific"]["text"] is True

    # Update with new structure
    updated_value = {
        "user_id": 1000,
        "new_field": "new_value",
        "sqlite_types": {"boolean": True, "null": None, "float": math.pi},
    }
    run_(store.set)(key, updated_value, expires_in=3600)

    retrieved = run_(store.get)(key)
    assert retrieved == updated_value
    assert retrieved["sqlite_types"]["null"] is None

    # Delete
    run_(store.delete)(key)
    result = run_(store.get)(key)
    assert result is None


def test_sqlite_store_expiration_enhanced(store: SQLSpecSessionStore) -> None:
    """Test enhanced expiration handling with SQLite."""
    key = "sqlite-expiring-key"
    value = {"test": "sqlite_data", "expires": True}

    # Set with 1 second expiration
    run_(store.set)(key, value, expires_in=1)

    # Should exist immediately
    result = run_(store.get)(key)
    assert result == value

    # Wait for expiration
    time.sleep(2)

    # Should be expired
    result = run_(store.get)(key)
    assert result is None


def test_sqlite_store_exists_and_expires_in(store: SQLSpecSessionStore) -> None:
    """Test exists and expires_in functionality."""
    key = "sqlite-exists-test"
    value = {"test": "data"}

    # Test non-existent key
    assert run_(store.exists)(key) is False
    assert run_(store.expires_in)(key) == 0

    # Set key
    run_(store.set)(key, value, expires_in=3600)

    # Test existence
    assert run_(store.exists)(key) is True
    expires_in = run_(store.expires_in)(key)
    assert 3590 <= expires_in <= 3600  # Should be close to 3600

    # Delete and test again
    run_(store.delete)(key)
    assert run_(store.exists)(key) is False
    assert run_(store.expires_in)(key) == 0


async def test_sqlite_store_transaction_behavior() -> None:
    """Test transaction-like behavior in SQLite store operations."""
    # Create a separate database for this test to avoid locking issues
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "transaction_test.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Apply migrations and create store
        @async_
        def setup_database():
            migration_config = SqliteConfig(
                pool_config={"database": str(db_path)},
                migration_config={
                    "script_location": str(migration_dir),
                    "version_table_name": "sqlspec_migrations",
                    "include_extensions": ["litestar"],
                },
            )
            commands = SyncMigrationCommands(migration_config)
            commands.init(migration_config.migration_config["script_location"], package=False)
            commands.upgrade()
            if migration_config.pool_instance:
                migration_config.close_pool()

        await setup_database()
        await asyncio.sleep(0.1)

        # Create fresh store
        store_config = SqliteConfig(pool_config={"database": str(db_path)})
        store = SQLSpecSessionStore(store_config, table_name="litestar_sessions")

        key = "sqlite-transaction-test"

        # Set initial value
        await store.set(key, {"counter": 0}, expires_in=3600)

        async def increment_counter() -> None:
            """Increment counter in a sequential manner."""
            current = await store.get(key)
            if current:
                current["counter"] += 1
                await store.set(key, current, expires_in=3600)

        # Run multiple increments sequentially (SQLite will handle this well)
        for _ in range(10):
            await increment_counter()

        # Final count should be 10 due to SQLite's sequential processing
        result = await store.get(key)
        assert result is not None
        assert "counter" in result
        assert result["counter"] == 10

        # Clean up
        if store_config.pool_instance:
            store_config.close_pool()
