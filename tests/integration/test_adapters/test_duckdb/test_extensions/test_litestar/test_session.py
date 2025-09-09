"""Integration tests for DuckDB session backend with store integration."""

import asyncio
import tempfile
from pathlib import Path

import pytest

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.sync_tools import async_

pytestmark = [pytest.mark.duckdb, pytest.mark.integration, pytest.mark.xdist_group("duckdb")]


@pytest.fixture
def duckdb_config(request: pytest.FixtureRequest) -> DuckDBConfig:
    """Create DuckDB configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sessions.duckdb"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Get worker ID for table isolation in parallel testing
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        session_table = f"litestar_sessions_duckdb_{table_suffix}"
        migration_table = f"sqlspec_migrations_duckdb_{table_suffix}"

        return DuckDBConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": migration_table,
                "include_extensions": [{"name": "litestar", "session_table": session_table}],
            },
        )


@pytest.fixture
async def session_store(duckdb_config: DuckDBConfig) -> SQLSpecSessionStore:
    """Create a session store with migrations applied using unique table names."""

    # Apply migrations synchronously (DuckDB uses sync commands like SQLite)
    @async_
    def apply_migrations() -> None:
        commands = SyncMigrationCommands(duckdb_config)
        commands.init(duckdb_config.migration_config["script_location"], package=False)
        commands.upgrade()

    # Run migrations
    await apply_migrations()

    # Extract the unique session table name from the migration config extensions
    session_table_name = "litestar_sessions_duckdb"  # unique for duckdb
    for ext in duckdb_config.migration_config.get("include_extensions", []):
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions_duckdb")
            break

    return SQLSpecSessionStore(duckdb_config, table_name=session_table_name)


async def test_duckdb_migration_creates_correct_table(duckdb_config: DuckDBConfig) -> None:
    """Test that Litestar migration creates the correct table structure for DuckDB."""

    # Apply migrations
    @async_
    def apply_migrations():
        commands = SyncMigrationCommands(duckdb_config)
        commands.init(duckdb_config.migration_config["script_location"], package=False)
        commands.upgrade()

    await apply_migrations()

    # Get the session table name from the migration config
    extensions = duckdb_config.migration_config.get("include_extensions", [])
    session_table = "litestar_sessions"  # default
    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table = ext.get("session_table", "litestar_sessions")

    # Verify table was created with correct DuckDB-specific types
    with duckdb_config.provide_session() as driver:
        result = driver.execute(f"PRAGMA table_info('{session_table}')")
        columns = {row["name"]: row["type"] for row in result.data}

        # DuckDB should use JSON or VARCHAR for data column
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify the data type is appropriate for JSON storage
        assert columns["data"] in ["JSON", "VARCHAR", "TEXT"]


async def test_duckdb_session_basic_operations(session_store: SQLSpecSessionStore) -> None:
    """Test basic session operations with DuckDB backend."""

    # Test only direct store operations
    test_data = {"user_id": 123, "name": "test"}
    await session_store.set("test-key", test_data, expires_in=3600)
    result = await session_store.get("test-key")
    assert result == test_data

    # Test deletion
    await session_store.delete("test-key")
    result = await session_store.get("test-key")
    assert result is None


async def test_duckdb_session_persistence(session_store: SQLSpecSessionStore) -> None:
    """Test that sessions persist across operations with DuckDB."""

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


async def test_duckdb_session_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test session expiration handling with DuckDB."""

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


async def test_duckdb_concurrent_sessions(session_store: SQLSpecSessionStore) -> None:
    """Test handling of concurrent sessions with DuckDB."""

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


async def test_duckdb_session_cleanup(session_store: SQLSpecSessionStore) -> None:
    """Test expired session cleanup with DuckDB."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(10):
        session_id = f"duckdb-cleanup-{i}"
        session_ids.append(session_id)
        await session_store.set(session_id, {"data": i}, expires_in=1)

    # Create long-lived sessions
    persistent_ids = []
    for i in range(3):
        session_id = f"duckdb-persistent-{i}"
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


async def test_duckdb_store_operations(session_store: SQLSpecSessionStore) -> None:
    """Test DuckDB store operations directly."""
    # Test basic store operations
    session_id = "test-session-duckdb"
    test_data = {"user_id": 789}

    # Set data
    await session_store.set(session_id, test_data, expires_in=3600)

    # Get data
    result = await session_store.get(session_id)
    assert result == test_data

    # Check exists
    assert await session_store.exists(session_id) is True

    # Update with renewal - use simple data to avoid conversion issues
    updated_data = {"user_id": 790}
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
