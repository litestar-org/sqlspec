"""Integration tests for Psycopg session backend with store integration."""

import asyncio
import tempfile
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.extensions.litestar import SQLSpecAsyncSessionStore, SQLSpecSyncSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

pytestmark = [pytest.mark.psycopg, pytest.mark.postgres, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
def psycopg_sync_config(
    postgres_service: PostgresService, request: pytest.FixtureRequest
) -> Generator[PsycopgSyncConfig, None, None]:
    """Create Psycopg sync configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique names for test isolation (based on advanced-alchemy pattern)
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_psycopg_sync_{table_suffix}"
        session_table = f"litestar_sessions_psycopg_sync_{table_suffix}"

        config = PsycopgSyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
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
            with config.provide_session() as driver:
                driver.execute(f"DROP TABLE IF EXISTS {session_table}")
                driver.execute(f"DROP TABLE IF EXISTS {migration_table}")
        except Exception:
            pass  # Ignore cleanup errors
        if config.pool_instance:
            config.close_pool()


@pytest.fixture
async def psycopg_async_config(
    postgres_service: PostgresService, request: pytest.FixtureRequest
) -> AsyncGenerator[PsycopgAsyncConfig, None]:
    """Create Psycopg async configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique names for test isolation (based on advanced-alchemy pattern)
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_psycopg_async_{table_suffix}"
        session_table = f"litestar_sessions_psycopg_async_{table_suffix}"

        config = PsycopgAsyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
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
def sync_session_store(psycopg_sync_config: PsycopgSyncConfig) -> SQLSpecSyncSessionStore:
    """Create a sync session store with migrations applied using unique table names."""
    # Apply migrations to create the session table
    commands = SyncMigrationCommands(psycopg_sync_config)
    commands.init(psycopg_sync_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Extract the unique session table name from extensions config
    extensions = psycopg_sync_config.migration_config.get("include_extensions", [])
    session_table_name = "litestar_sessions_psycopg_sync"  # unique for psycopg sync
    for ext in extensions if isinstance(extensions, list) else []:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions_psycopg_sync")
            break

    return SQLSpecSyncSessionStore(psycopg_sync_config, table_name=session_table_name)


@pytest.fixture
async def async_session_store(psycopg_async_config: PsycopgAsyncConfig) -> SQLSpecAsyncSessionStore:
    """Create an async session store with migrations applied using unique table names."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(psycopg_async_config)
    await commands.init(psycopg_async_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Extract the unique session table name from extensions config
    extensions = psycopg_async_config.migration_config.get("include_extensions", [])
    session_table_name = "litestar_sessions_psycopg_async"  # unique for psycopg async
    for ext in extensions if isinstance(extensions, list) else []:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions_psycopg_async")
            break

    return SQLSpecAsyncSessionStore(psycopg_async_config, table_name=session_table_name)


def test_psycopg_sync_migration_creates_correct_table(psycopg_sync_config: PsycopgSyncConfig) -> None:
    """Test that Litestar migration creates the correct table structure for PostgreSQL with sync driver."""
    # Apply migrations
    commands = SyncMigrationCommands(psycopg_sync_config)
    commands.init(psycopg_sync_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Verify table was created with correct PostgreSQL-specific types
    with psycopg_sync_config.provide_session() as driver:
        # Get the actual table name from the migration context or extensions config
        extensions = psycopg_sync_config.migration_config.get("include_extensions", [])
        table_name = "litestar_sessions_psycopg_sync"  # unique for psycopg sync
        for ext in extensions if isinstance(extensions, list) else []:
            if isinstance(ext, dict) and ext.get("name") == "litestar":
                table_name = ext.get("session_table", "litestar_sessions_psycopg_sync")
                break

        result = driver.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            AND column_name IN ('data', 'expires_at')
        """,
            (table_name,),
        )

        columns = {row["column_name"]: row["data_type"] for row in result.data}

        # PostgreSQL should use JSONB for data column (not JSON or TEXT)
        assert columns.get("data") == "jsonb"
        assert "timestamp" in columns.get("expires_at", "").lower()

        # Verify all expected columns exist
        result = driver.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """,
            (table_name,),
        )
        columns = {row["column_name"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns


async def test_psycopg_async_migration_creates_correct_table(psycopg_async_config: PsycopgAsyncConfig) -> None:
    """Test that Litestar migration creates the correct table structure for PostgreSQL with async driver."""
    # Apply migrations
    commands = AsyncMigrationCommands(psycopg_async_config)
    await commands.init(psycopg_async_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Verify table was created with correct PostgreSQL-specific types
    async with psycopg_async_config.provide_session() as driver:
        # Get the actual table name from the migration context or extensions config
        extensions = psycopg_async_config.migration_config.get("include_extensions", [])
        table_name = "litestar_sessions_psycopg_async"  # unique for psycopg async
        for ext in extensions if isinstance(extensions, list) else []:  # type: ignore[union-attr]
            if isinstance(ext, dict) and ext.get("name") == "litestar":
                table_name = ext.get("session_table", "litestar_sessions_psycopg_async")
                break

        result = await driver.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            AND column_name IN ('data', 'expires_at')
        """,
            (table_name,),
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
            (table_name,),
        )
        columns = {row["column_name"] for row in result.data}
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns


async def test_psycopg_sync_session_basic_operations(sync_session_store: SQLSpecSyncSessionStore) -> None:
    """Test basic session operations with Psycopg sync backend."""

    # Test only direct store operations which should work
    test_data = {"user_id": 54321, "username": "psycopg_sync_user"}
    await sync_session_store.set("test-key", test_data, expires_in=3600)
    result = await sync_session_store.get("test-key")
    assert result == test_data

    # Test deletion
    await sync_session_store.delete("test-key")
    result = await sync_session_store.get("test-key")
    assert result is None


async def test_psycopg_async_session_basic_operations(async_session_store: SQLSpecSyncSessionStore) -> None:
    """Test basic session operations with Psycopg async backend."""

    # Test only direct store operations which should work
    test_data = {"user_id": 98765, "username": "psycopg_async_user"}
    await async_session_store.set("test-key", test_data, expires_in=3600)
    result = await async_session_store.get("test-key")
    assert result == test_data

    # Test deletion
    await async_session_store.delete("test-key")
    result = await async_session_store.get("test-key")
    assert result is None


async def test_psycopg_sync_session_persistence(sync_session_store: SQLSpecSyncSessionStore) -> None:
    """Test that sessions persist across operations with Psycopg sync driver."""

    # Test multiple set/get operations persist data
    session_id = "persistent-test-sync"

    # Set initial data
    await sync_session_store.set(session_id, {"count": 1}, expires_in=3600)
    result = await sync_session_store.get(session_id)
    assert result == {"count": 1}

    # Update data
    await sync_session_store.set(session_id, {"count": 2}, expires_in=3600)
    result = await sync_session_store.get(session_id)
    assert result == {"count": 2}


async def test_psycopg_async_session_persistence(async_session_store: SQLSpecSyncSessionStore) -> None:
    """Test that sessions persist across operations with Psycopg async driver."""

    # Test multiple set/get operations persist data
    session_id = "persistent-test-async"

    # Set initial data
    await async_session_store.set(session_id, {"count": 1}, expires_in=3600)
    result = await async_session_store.get(session_id)
    assert result == {"count": 1}

    # Update data
    await async_session_store.set(session_id, {"count": 2}, expires_in=3600)
    result = await async_session_store.get(session_id)
    assert result == {"count": 2}


async def test_psycopg_sync_session_expiration(sync_session_store: SQLSpecSyncSessionStore) -> None:
    """Test session expiration handling with Psycopg sync driver."""

    # Test direct store expiration
    session_id = "expiring-test-sync"

    # Set data with short expiration
    await sync_session_store.set(session_id, {"test": "data"}, expires_in=1)

    # Data should be available immediately
    result = await sync_session_store.get(session_id)
    assert result == {"test": "data"}

    # Wait for expiration

    await asyncio.sleep(2)

    # Data should be expired
    result = await sync_session_store.get(session_id)
    assert result is None


async def test_psycopg_async_session_expiration(async_session_store: SQLSpecSyncSessionStore) -> None:
    """Test session expiration handling with Psycopg async driver."""

    # Test direct store expiration
    session_id = "expiring-test-async"

    # Set data with short expiration
    await async_session_store.set(session_id, {"test": "data"}, expires_in=1)

    # Data should be available immediately
    result = await async_session_store.get(session_id)
    assert result == {"test": "data"}

    # Wait for expiration
    await asyncio.sleep(2)

    # Data should be expired
    result = await async_session_store.get(session_id)
    assert result is None


async def test_psycopg_sync_concurrent_sessions(sync_session_store: SQLSpecSyncSessionStore) -> None:
    """Test handling of concurrent sessions with Psycopg sync driver."""

    # Test multiple concurrent session operations
    session_ids = ["session1", "session2", "session3"]

    # Set different data in different sessions
    await sync_session_store.set(session_ids[0], {"user_id": 101}, expires_in=3600)
    await sync_session_store.set(session_ids[1], {"user_id": 202}, expires_in=3600)
    await sync_session_store.set(session_ids[2], {"user_id": 303}, expires_in=3600)

    # Each session should maintain its own data
    result1 = await sync_session_store.get(session_ids[0])
    assert result1 == {"user_id": 101}

    result2 = await sync_session_store.get(session_ids[1])
    assert result2 == {"user_id": 202}

    result3 = await sync_session_store.get(session_ids[2])
    assert result3 == {"user_id": 303}


async def test_psycopg_async_concurrent_sessions(async_session_store: SQLSpecSyncSessionStore) -> None:
    """Test handling of concurrent sessions with Psycopg async driver."""

    # Test multiple concurrent session operations
    session_ids = ["session1", "session2", "session3"]

    # Set different data in different sessions
    await async_session_store.set(session_ids[0], {"user_id": 101}, expires_in=3600)
    await async_session_store.set(session_ids[1], {"user_id": 202}, expires_in=3600)
    await async_session_store.set(session_ids[2], {"user_id": 303}, expires_in=3600)

    # Each session should maintain its own data
    result1 = await async_session_store.get(session_ids[0])
    assert result1 == {"user_id": 101}

    result2 = await async_session_store.get(session_ids[1])
    assert result2 == {"user_id": 202}

    result3 = await async_session_store.get(session_ids[2])
    assert result3 == {"user_id": 303}


async def test_psycopg_sync_session_cleanup(sync_session_store: SQLSpecSyncSessionStore) -> None:
    """Test expired session cleanup with Psycopg sync driver."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(10):
        session_id = f"psycopg-sync-cleanup-{i}"
        session_ids.append(session_id)
        await sync_session_store.set(session_id, {"data": i}, expires_in=1)

    # Create long-lived sessions
    persistent_ids = []
    for i in range(3):
        session_id = f"psycopg-sync-persistent-{i}"
        persistent_ids.append(session_id)
        await sync_session_store.set(session_id, {"data": f"keep-{i}"}, expires_in=3600)

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await sync_session_store.delete_expired()

    # Check that expired sessions are gone
    for session_id in session_ids:
        result = await sync_session_store.get(session_id)
        assert result is None

    # Long-lived sessions should still exist
    for session_id in persistent_ids:
        result = await sync_session_store.get(session_id)
        assert result is not None


async def test_psycopg_async_session_cleanup(async_session_store: SQLSpecSyncSessionStore) -> None:
    """Test expired session cleanup with Psycopg async driver."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(10):
        session_id = f"psycopg-async-cleanup-{i}"
        session_ids.append(session_id)
        await async_session_store.set(session_id, {"data": i}, expires_in=1)

    # Create long-lived sessions
    persistent_ids = []
    for i in range(3):
        session_id = f"psycopg-async-persistent-{i}"
        persistent_ids.append(session_id)
        await async_session_store.set(session_id, {"data": f"keep-{i}"}, expires_in=3600)

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await async_session_store.delete_expired()

    # Check that expired sessions are gone
    for session_id in session_ids:
        result = await async_session_store.get(session_id)
        assert result is None

    # Long-lived sessions should still exist
    for session_id in persistent_ids:
        result = await async_session_store.get(session_id)
        assert result is not None


async def test_psycopg_sync_store_operations(sync_session_store: SQLSpecSyncSessionStore) -> None:
    """Test Psycopg sync store operations directly."""
    # Test basic store operations
    session_id = "test-session-psycopg-sync"
    test_data = {"user_id": 789}

    # Set data
    await sync_session_store.set(session_id, test_data, expires_in=3600)

    # Get data
    result = await sync_session_store.get(session_id)
    assert result == test_data

    # Check exists
    assert await sync_session_store.exists(session_id) is True

    # Update with renewal - use simple data to avoid conversion issues
    updated_data = {"user_id": 790}
    await sync_session_store.set(session_id, updated_data, expires_in=7200)

    # Get updated data
    result = await sync_session_store.get(session_id)
    assert result == updated_data

    # Delete data
    await sync_session_store.delete(session_id)

    # Verify deleted
    result = await sync_session_store.get(session_id)
    assert result is None
    assert await sync_session_store.exists(session_id) is False


async def test_psycopg_async_store_operations(async_session_store: SQLSpecSyncSessionStore) -> None:
    """Test Psycopg async store operations directly."""
    # Test basic store operations
    session_id = "test-session-psycopg-async"
    test_data = {"user_id": 456}

    # Set data
    await async_session_store.set(session_id, test_data, expires_in=3600)

    # Get data
    result = await async_session_store.get(session_id)
    assert result == test_data

    # Check exists
    assert await async_session_store.exists(session_id) is True

    # Update with renewal - use simple data to avoid conversion issues
    updated_data = {"user_id": 457}
    await async_session_store.set(session_id, updated_data, expires_in=7200)

    # Get updated data
    result = await async_session_store.get(session_id)
    assert result == updated_data

    # Delete data
    await async_session_store.delete(session_id)

    # Verify deleted
    result = await async_session_store.get(session_id)
    assert result is None
    assert await async_session_store.exists(session_id) is False
