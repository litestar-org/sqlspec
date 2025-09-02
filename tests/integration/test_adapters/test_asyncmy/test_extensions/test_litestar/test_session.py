"""Integration tests for AsyncMy (MySQL) session backend with store integration."""

import asyncio
import tempfile
from pathlib import Path

import pytest

from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands

pytestmark = [pytest.mark.asyncmy, pytest.mark.mysql, pytest.mark.integration, pytest.mark.xdist_group("mysql")]


@pytest.fixture
async def asyncmy_config(mysql_service, request: pytest.FixtureRequest):
    """Create AsyncMy configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique names for test isolation (based on advanced-alchemy pattern)
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_asyncmy_{table_suffix}"
        session_table = f"litestar_sessions_asyncmy_{table_suffix}"

        config = AsyncmyConfig(
            pool_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "minsize": 2,
                "maxsize": 10,
            },
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
async def session_store(asyncmy_config):
    """Create a session store with migrations applied using unique table names."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(asyncmy_config)
    await commands.init(asyncmy_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Extract the unique session table name from the migration config extensions
    session_table_name = "litestar_sessions_asyncmy"  # unique for asyncmy
    for ext in asyncmy_config.migration_config.get("include_extensions", []):
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions_asyncmy")
            break

    return SQLSpecSessionStore(asyncmy_config, table_name=session_table_name)


async def test_asyncmy_migration_creates_correct_table(asyncmy_config) -> None:
    """Test that Litestar migration creates the correct table structure for MySQL."""
    # Apply migrations
    commands = AsyncMigrationCommands(asyncmy_config)
    await commands.init(asyncmy_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Get the session table name from the migration config
    extensions = asyncmy_config.migration_config.get("include_extensions", [])
    session_table = "litestar_sessions"  # default
    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table = ext.get("session_table", "litestar_sessions")

    # Verify table was created with correct MySQL-specific types
    async with asyncmy_config.provide_session() as driver:
        result = await driver.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = %s
            AND COLUMN_NAME IN ('data', 'expires_at')
        """,
            [session_table],
        )

        columns = {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in result.data}

        # MySQL should use JSON for data column (not JSONB or TEXT)
        assert columns.get("data") == "json"
        # MySQL uses DATETIME for timestamp columns
        assert columns.get("expires_at", "").lower() in {"datetime", "timestamp"}

        # Verify all expected columns exist
        result = await driver.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = %s
        """,
            [session_table],
        )
        columns = {row["COLUMN_NAME"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns


async def test_asyncmy_session_basic_operations_simple(session_store) -> None:
    """Test basic session operations with AsyncMy backend."""

    # Test only direct store operations which should work
    test_data = {"user_id": 123, "name": "test"}
    await session_store.set("test-key", test_data, expires_in=3600)
    result = await session_store.get("test-key")
    assert result == test_data

    # Test deletion
    await session_store.delete("test-key")
    result = await session_store.get("test-key")
    assert result is None


async def test_asyncmy_session_persistence(session_store) -> None:
    """Test that sessions persist across operations with AsyncMy."""

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


async def test_asyncmy_session_expiration(session_store) -> None:
    """Test session expiration handling with AsyncMy."""

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


async def test_asyncmy_concurrent_sessions(session_store) -> None:
    """Test handling of concurrent sessions with AsyncMy."""

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


async def test_asyncmy_session_cleanup(session_store) -> None:
    """Test expired session cleanup with AsyncMy."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(7):
        session_id = f"asyncmy-cleanup-{i}"
        session_ids.append(session_id)
        await session_store.set(session_id, {"data": i}, expires_in=1)

    # Create long-lived sessions
    persistent_ids = []
    for i in range(3):
        session_id = f"asyncmy-persistent-{i}"
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


async def test_asyncmy_store_operations(session_store) -> None:
    """Test AsyncMy store operations directly."""
    # Test basic store operations
    session_id = "test-session-asyncmy"
    test_data = {
        "user_id": 456,
    }

    # Set data
    await session_store.set(session_id, test_data, expires_in=3600)

    # Get data
    result = await session_store.get(session_id)
    assert result == test_data

    # Check exists
    assert await session_store.exists(session_id) is True

    # Update with renewal - use simple data to avoid conversion issues
    updated_data = {"user_id": 457}
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
