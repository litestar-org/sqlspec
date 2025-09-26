"""Integration tests for SQLite session backend with store integration."""

import asyncio
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecSyncSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.sync_tools import async_

pytestmark = [pytest.mark.sqlite, pytest.mark.integration, pytest.mark.xdist_group("sqlite")]


@pytest.fixture
def sqlite_config(request: pytest.FixtureRequest) -> Generator[SqliteConfig, None, None]:
    """Create SQLite configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create unique names for test isolation (based on advanced-alchemy pattern)
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_sqlite_{table_suffix}"
        session_table = f"litestar_sessions_sqlite_{table_suffix}"

        db_path = Path(temp_dir) / f"sessions_{table_suffix}.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = SqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": migration_table,
                "include_extensions": [{"name": "litestar", "session_table": session_table}],
            },
        )
        yield config
        if config.pool_instance:
            config.close_pool()


@pytest.fixture
async def session_store(sqlite_config: SqliteConfig) -> SQLSpecSyncSessionStore:
    """Create a session store with migrations applied using unique table names."""

    # Apply migrations synchronously (SQLite uses sync commands)
    def apply_migrations() -> None:
        commands = SyncMigrationCommands(sqlite_config)
        commands.init(sqlite_config.migration_config["script_location"], package=False)
        commands.upgrade()
        # Explicitly close any connections after migration
        if sqlite_config.pool_instance:
            sqlite_config.close_pool()

    # Run migrations
    await async_(apply_migrations)()

    # Give a brief delay to ensure file locks are released
    await asyncio.sleep(0.1)

    # Extract the unique session table name from the migration config extensions
    session_table_name = "litestar_sessions_sqlite"  # default for sqlite
    for ext in sqlite_config.migration_config.get("include_extensions", []):
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions_sqlite")
            break

    return SQLSpecSyncSessionStore(sqlite_config, table_name=session_table_name)


# Removed unused session backend fixtures - using store directly


async def test_sqlite_migration_creates_correct_table(sqlite_config: SqliteConfig) -> None:
    """Test that Litestar migration creates the correct table structure for SQLite."""

    # Apply migrations synchronously (SQLite uses sync commands)
    def apply_migrations() -> None:
        commands = SyncMigrationCommands(sqlite_config)
        commands.init(sqlite_config.migration_config["script_location"], package=False)
        commands.upgrade()

    # Run migrations
    await async_(apply_migrations)()

    # Get the session table name from the migration config
    extensions = sqlite_config.migration_config.get("include_extensions", [])
    session_table = "litestar_sessions"  # default
    for ext in extensions:  # type: ignore[union-attr]
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table = ext.get("session_table", "litestar_sessions")

    # Verify table was created with correct SQLite-specific types
    with sqlite_config.provide_session() as driver:
        result = driver.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{session_table}'")
        assert len(result.data) == 1
        create_sql = result.data[0]["sql"]

        # SQLite should use TEXT for data column (not JSONB or JSON)
        assert "TEXT" in create_sql
        assert "DATETIME" in create_sql or "TIMESTAMP" in create_sql
        assert session_table in create_sql

        # Verify columns exist
        result = driver.execute(f"PRAGMA table_info({session_table})")
        columns = {row["name"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns


async def test_sqlite_session_basic_operations(session_store: SQLSpecSyncSessionStore) -> None:
    """Test basic session operations with SQLite backend."""

    # Test only direct store operations which should work
    test_data = {"user_id": 123, "name": "test"}
    await session_store.set("test-key", test_data, expires_in=3600)
    result = await session_store.get("test-key")
    assert result == test_data

    # Test deletion
    await session_store.delete("test-key")
    result = await session_store.get("test-key")
    assert result is None


async def test_sqlite_session_persistence(session_store: SQLSpecSyncSessionStore) -> None:
    """Test that sessions persist across operations with SQLite."""

    # Test multiple set/get operations persist data
    session_id = "persistent-test"

    # Set initial data
    await session_store.set(session_id, {"count": 1}, expires_in=3600)
    result = await session_store.get(session_id)
    assert result == {"count": 1}

    # Update data
    await session_store.set(session_id, {"count": 2}, expires_in=3600)
    result = await session_store.get(session_id)
    assert result == {"count": 2}


async def test_sqlite_session_expiration(session_store: SQLSpecSyncSessionStore) -> None:
    """Test session expiration handling with SQLite."""

    # Test direct store expiration
    session_id = "expiring-test"

    # Set data with short expiration
    await session_store.set(session_id, {"test": "data"}, expires_in=1)

    # Data should be available immediately
    result = await session_store.get(session_id)
    assert result == {"test": "data"}

    # Wait for expiration
    await asyncio.sleep(2)

    # Data should be expired
    result = await session_store.get(session_id)
    assert result is None


async def test_sqlite_concurrent_sessions(session_store: SQLSpecSyncSessionStore) -> None:
    """Test handling of concurrent sessions with SQLite."""

    # Test multiple concurrent session operations
    session_ids = ["session1", "session2", "session3"]

    # Set different data in different sessions
    await session_store.set(session_ids[0], {"user_id": 101}, expires_in=3600)
    await session_store.set(session_ids[1], {"user_id": 202}, expires_in=3600)
    await session_store.set(session_ids[2], {"user_id": 303}, expires_in=3600)

    # Each session should maintain its own data
    result1 = await session_store.get(session_ids[0])
    assert result1 == {"user_id": 101}

    result2 = await session_store.get(session_ids[1])
    assert result2 == {"user_id": 202}

    result3 = await session_store.get(session_ids[2])
    assert result3 == {"user_id": 303}


async def test_sqlite_session_cleanup(session_store: SQLSpecSyncSessionStore) -> None:
    """Test expired session cleanup with SQLite."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(10):
        session_id = f"sqlite-cleanup-{i}"
        session_ids.append(session_id)
        await session_store.set(session_id, {"data": i}, expires_in=1)

    # Create long-lived sessions
    persistent_ids = []
    for i in range(3):
        session_id = f"sqlite-persistent-{i}"
        persistent_ids.append(session_id)
        await session_store.set(session_id, {"data": f"keep-{i}"}, expires_in=3600)

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await session_store.delete_expired()

    # Check that expired sessions are gone
    for session_id in session_ids:
        result = await session_store.get(session_id)
        assert result is None

    # Long-lived sessions should still exist
    for session_id in persistent_ids:
        result = await session_store.get(session_id)
        assert result is not None


async def test_sqlite_store_operations(session_store: SQLSpecSyncSessionStore) -> None:
    """Test SQLite store operations directly."""
    # Test basic store operations
    session_id = "test-session-sqlite"
    test_data = {"user_id": 123, "name": "test"}

    # Set data
    await session_store.set(session_id, test_data, expires_in=3600)

    # Get data
    result = await session_store.get(session_id)
    assert result == test_data

    # Check exists
    assert await session_store.exists(session_id) is True

    # Update with renewal
    updated_data = {"user_id": 124, "name": "updated"}
    await session_store.set(session_id, updated_data, expires_in=7200)

    # Get updated data
    result = await session_store.get(session_id)
    assert result == updated_data

    # Delete data
    await session_store.delete(session_id)

    # Verify deleted
    result = await session_store.get(session_id)
    assert result is None
    assert await session_store.exists(session_id) is False
