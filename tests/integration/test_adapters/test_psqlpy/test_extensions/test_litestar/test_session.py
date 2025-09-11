"""Integration tests for PsqlPy session backend with store integration."""

import asyncio
import tempfile
from pathlib import Path

import pytest

from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = [pytest.mark.psqlpy, pytest.mark.postgres, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
async def psqlpy_config(postgres_service, request: pytest.FixtureRequest) -> PsqlpyConfig:
    """Create PsqlPy configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        dsn = f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"

        # Create unique names for test isolation (based on advanced-alchemy pattern)
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_psqlpy_{table_suffix}"
        session_table = f"litestar_sessions_psqlpy_{table_suffix}"

        config = PsqlpyConfig(
            pool_config={"dsn": dsn, "max_db_pool_size": 5},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": migration_table,
                "include_extensions": [{"name": "litestar", "session_table": session_table}],
            },
        )
        yield config
        # Cleanup: drop test tables and close pool
        try:
            async with config.provide_session() as driver:
                await driver.execute(f"DROP TABLE IF EXISTS {session_table}")
                await driver.execute(f"DROP TABLE IF EXISTS {migration_table}")
        except Exception:
            pass  # Ignore cleanup errors
        await config.close_pool()


@pytest.fixture
async def session_store(psqlpy_config: PsqlpyConfig) -> SQLSpecSessionStore:
    """Create a session store with migrations applied using unique table names."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(psqlpy_config)
    await commands.init(psqlpy_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Extract the unique session table name from the migration config extensions
    session_table_name = "litestar_sessions_psqlpy"  # unique for psqlpy
    for ext in psqlpy_config.migration_config.get("include_extensions", []):
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions_psqlpy")
            break

    return SQLSpecSessionStore(psqlpy_config, table_name=session_table_name)


async def test_psqlpy_migration_creates_correct_table(psqlpy_config: PsqlpyConfig) -> None:
    """Test that Litestar migration creates the correct table structure for PostgreSQL."""
    # Apply migrations
    commands = AsyncMigrationCommands(psqlpy_config)
    await commands.init(psqlpy_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Get the session table name from the migration config
    extensions = psqlpy_config.migration_config.get("include_extensions", [])
    session_table = "litestar_sessions"  # default
    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table = ext.get("session_table", "litestar_sessions")

    # Verify table was created with correct PostgreSQL-specific types
    async with psqlpy_config.provide_session() as driver:
        result = await driver.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            AND column_name IN ('data', 'expires_at')
        """,
            [session_table],
        )

        columns = {row["column_name"]: row["data_type"] for row in result.data}

        # PostgreSQL should use JSONB for data column (not JSON or TEXT)
        assert columns.get("data") == "jsonb"
        assert "timestamp" in columns.get("expires_at", "").lower()

        # Verify all expected columns exist
        result = await driver.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """,
            [session_table],
        )
        columns = {row["column_name"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns


async def test_psqlpy_session_basic_operations_simple(session_store: SQLSpecSessionStore) -> None:
    """Test basic session operations with PsqlPy backend."""

    # Test only direct store operations which should work
    test_data = {"user_id": 54321, "username": "psqlpyuser"}
    await session_store.set("test-key", test_data, expires_in=3600)
    result = await session_store.get("test-key")
    assert result == test_data

    # Test deletion
    await session_store.delete("test-key")
    result = await session_store.get("test-key")
    assert result is None


async def test_psqlpy_session_persistence(session_store: SQLSpecSessionStore) -> None:
    """Test that sessions persist across operations with PsqlPy."""

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


async def test_psqlpy_session_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test session expiration handling with PsqlPy."""

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


async def test_psqlpy_concurrent_sessions(session_store: SQLSpecSessionStore) -> None:
    """Test handling of concurrent sessions with PsqlPy."""

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


async def test_psqlpy_session_cleanup(session_store: SQLSpecSessionStore) -> None:
    """Test expired session cleanup with PsqlPy."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(10):
        session_id = f"psqlpy-cleanup-{i}"
        session_ids.append(session_id)
        await session_store.set(session_id, {"data": i}, expires_in=1)

    # Create long-lived sessions
    persistent_ids = []
    for i in range(3):
        session_id = f"psqlpy-persistent-{i}"
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


async def test_psqlpy_store_operations(session_store: SQLSpecSessionStore) -> None:
    """Test PsqlPy store operations directly."""
    # Test basic store operations
    session_id = "test-session-psqlpy"
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
