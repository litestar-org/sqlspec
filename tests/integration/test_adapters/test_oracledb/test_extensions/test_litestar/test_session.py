"""Integration tests for OracleDB session backend with store integration."""

import asyncio
import tempfile
from pathlib import Path

import pytest

from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.extensions.litestar import SQLSpecAsyncSessionStore, SQLSpecSyncSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

pytestmark = [pytest.mark.oracledb, pytest.mark.oracle, pytest.mark.integration, pytest.mark.xdist_group("oracle")]


@pytest.fixture
async def oracle_async_config(
    oracle_async_config: OracleAsyncConfig, request: pytest.FixtureRequest
) -> OracleAsyncConfig:
    """Create Oracle async configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique names for test isolation (based on advanced-alchemy pattern)
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_oracle_async_{table_suffix}"
        session_table = f"litestar_sessions_oracle_async_{table_suffix}"

        config = OracleAsyncConfig(
            pool_config=oracle_async_config.pool_config,
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
                await driver.execute(f"DROP TABLE {session_table}")
                await driver.execute(f"DROP TABLE {migration_table}")
        except Exception:
            pass  # Ignore cleanup errors
        await config.close_pool()


@pytest.fixture
def oracle_sync_config(oracle_sync_config: OracleSyncConfig, request: pytest.FixtureRequest) -> OracleSyncConfig:
    """Create Oracle sync configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique names for test isolation (based on advanced-alchemy pattern)
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_oracle_sync_{table_suffix}"
        session_table = f"litestar_sessions_oracle_sync_{table_suffix}"

        config = OracleSyncConfig(
            pool_config=oracle_sync_config.pool_config,
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
                driver.execute(f"DROP TABLE {session_table}")
                driver.execute(f"DROP TABLE {migration_table}")
        except Exception:
            pass  # Ignore cleanup errors
        config.close_pool()


@pytest.fixture
async def oracle_async_session_store(oracle_async_config: OracleAsyncConfig) -> SQLSpecAsyncSessionStore:
    """Create an async session store with migrations applied using unique table names."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(oracle_async_config)
    await commands.init(oracle_async_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Extract table name from migration config
    extensions = oracle_async_config.migration_config.get("include_extensions", [])
    litestar_ext = next((ext for ext in extensions if ext.get("name") == "litestar"), {})
    table_name = litestar_ext.get("session_table", "litestar_sessions")

    return SQLSpecAsyncSessionStore(oracle_async_config, table_name=table_name)


@pytest.fixture
def oracle_sync_session_store(oracle_sync_config: OracleSyncConfig) -> SQLSpecSyncSessionStore:
    """Create a sync session store with migrations applied using unique table names."""
    # Apply migrations to create the session table
    commands = SyncMigrationCommands(oracle_sync_config)
    commands.init(oracle_sync_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Extract table name from migration config
    extensions = oracle_sync_config.migration_config.get("include_extensions", [])
    litestar_ext = next((ext for ext in extensions if ext.get("name") == "litestar"), {})
    table_name = litestar_ext.get("session_table", "litestar_sessions")

    return SQLSpecSyncSessionStore(oracle_sync_config, table_name=table_name)


async def test_oracle_async_migration_creates_correct_table(oracle_async_config: OracleAsyncConfig) -> None:
    """Test that Litestar migration creates the correct table structure for Oracle."""
    # Apply migrations
    commands = AsyncMigrationCommands(oracle_async_config)
    await commands.init(oracle_async_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Get session table name from migration config extensions
    extensions = oracle_async_config.migration_config.get("include_extensions", [])
    litestar_ext = next((ext for ext in extensions if ext.get("name") == "litestar"), {})
    session_table_name = litestar_ext.get("session_table", "litestar_sessions")

    # Verify table was created with correct Oracle-specific types
    async with oracle_async_config.provide_session() as driver:
        result = await driver.execute(
            "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :1", (session_table_name.upper(),)
        )

        columns = {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in result.data}

        # Oracle should use CLOB for data column (not BLOB or VARCHAR2)
        assert columns.get("DATA") == "CLOB"
        assert "TIMESTAMP" in columns.get("EXPIRES_AT", "")

        # Verify all expected columns exist
        assert "SESSION_ID" in columns
        assert "DATA" in columns
        assert "EXPIRES_AT" in columns
        assert "CREATED_AT" in columns


def test_oracle_sync_migration_creates_correct_table(oracle_sync_config: OracleSyncConfig) -> None:
    """Test that Litestar migration creates the correct table structure for Oracle sync."""
    # Apply migrations
    commands = SyncMigrationCommands(oracle_sync_config)
    commands.init(oracle_sync_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Get session table name from migration config extensions
    extensions = oracle_sync_config.migration_config.get("include_extensions", [])
    litestar_ext = next((ext for ext in extensions if ext.get("name") == "litestar"), {})
    session_table_name = litestar_ext.get("session_table", "litestar_sessions")

    # Verify table was created with correct Oracle-specific types
    with oracle_sync_config.provide_session() as driver:
        result = driver.execute(
            "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :1", (session_table_name.upper(),)
        )

        columns = {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in result.data}

        # Oracle should use CLOB for data column
        assert columns.get("DATA") == "CLOB"
        assert "TIMESTAMP" in columns.get("EXPIRES_AT", "")

        # Verify all expected columns exist
        assert "SESSION_ID" in columns
        assert "DATA" in columns
        assert "EXPIRES_AT" in columns
        assert "CREATED_AT" in columns


async def test_oracle_async_store_operations(oracle_async_session_store: SQLSpecSyncSessionStore) -> None:
    """Test basic Oracle async store operations directly."""
    session_id = "test-session-oracle-async"
    test_data = {"user_id": 123, "name": "test"}

    # Set data
    await oracle_async_session_store.set(session_id, test_data, expires_in=3600)

    # Get data
    result = await oracle_async_session_store.get(session_id)
    assert result == test_data

    # Check exists
    assert await oracle_async_session_store.exists(session_id) is True

    # Update data
    updated_data = {"user_id": 123, "name": "updated_test"}
    await oracle_async_session_store.set(session_id, updated_data, expires_in=3600)

    # Get updated data
    result = await oracle_async_session_store.get(session_id)
    assert result == updated_data

    # Delete data
    await oracle_async_session_store.delete(session_id)

    # Verify deleted
    result = await oracle_async_session_store.get(session_id)
    assert result is None
    assert await oracle_async_session_store.exists(session_id) is False


def test_oracle_sync_store_operations(oracle_sync_session_store: SQLSpecSyncSessionStore) -> None:
    """Test basic Oracle sync store operations directly."""

    async def run_sync_test() -> None:
        session_id = "test-session-oracle-sync"
        test_data = {"user_id": 456, "name": "sync_test"}

        # Set data
        await oracle_sync_session_store.set(session_id, test_data, expires_in=3600)

        # Get data
        result = await oracle_sync_session_store.get(session_id)
        assert result == test_data

        # Check exists
        assert await oracle_sync_session_store.exists(session_id) is True

        # Update data
        updated_data = {"user_id": 456, "name": "updated_sync_test"}
        await oracle_sync_session_store.set(session_id, updated_data, expires_in=3600)

        # Get updated data
        result = await oracle_sync_session_store.get(session_id)
        assert result == updated_data

        # Delete data
        await oracle_sync_session_store.delete(session_id)

        # Verify deleted
        result = await oracle_sync_session_store.get(session_id)
        assert result is None
        assert await oracle_sync_session_store.exists(session_id) is False

    import asyncio

    asyncio.run(run_sync_test())


async def test_oracle_async_session_cleanup(oracle_async_session_store: SQLSpecSyncSessionStore) -> None:
    """Test expired session cleanup with Oracle async."""
    # Create sessions with short expiration
    session_ids = []
    for i in range(3):
        session_id = f"oracle-cleanup-{i}"
        session_ids.append(session_id)
        test_data = {"data": i, "type": "temporary"}
        await oracle_async_session_store.set(session_id, test_data, expires_in=1)

    # Create long-lived sessions
    persistent_ids = []
    for i in range(2):
        session_id = f"oracle-persistent-{i}"
        persistent_ids.append(session_id)
        test_data = {"data": f"keep-{i}", "type": "persistent"}
        await oracle_async_session_store.set(session_id, test_data, expires_in=3600)

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await oracle_async_session_store.delete_expired()

    # Check that expired sessions are gone
    for session_id in session_ids:
        result = await oracle_async_session_store.get(session_id)
        assert result is None

    # Long-lived sessions should still exist
    for i, session_id in enumerate(persistent_ids):
        result = await oracle_async_session_store.get(session_id)
        assert result is not None
        assert result["type"] == "persistent"
        assert result["data"] == f"keep-{i}"


def test_oracle_sync_session_cleanup(oracle_sync_session_store: SQLSpecSyncSessionStore) -> None:
    """Test expired session cleanup with Oracle sync."""

    async def run_sync_test() -> None:
        # Create sessions with short expiration
        session_ids = []
        for i in range(3):
            session_id = f"oracle-sync-cleanup-{i}"
            session_ids.append(session_id)
            test_data = {"data": i, "type": "temporary"}
            await oracle_sync_session_store.set(session_id, test_data, expires_in=1)

        # Create long-lived sessions
        persistent_ids = []
        for i in range(2):
            session_id = f"oracle-sync-persistent-{i}"
            persistent_ids.append(session_id)
            test_data = {"data": f"keep-{i}", "type": "persistent"}
            await oracle_sync_session_store.set(session_id, test_data, expires_in=3600)

        # Wait for short sessions to expire
        await asyncio.sleep(2)

        # Clean up expired sessions
        await oracle_sync_session_store.delete_expired()

        # Check that expired sessions are gone
        for session_id in session_ids:
            result = await oracle_sync_session_store.get(session_id)
            assert result is None

        # Long-lived sessions should still exist
        for i, session_id in enumerate(persistent_ids):
            result = await oracle_sync_session_store.get(session_id)
            assert result is not None
            assert result["type"] == "persistent"
            assert result["data"] == f"keep-{i}"

    import asyncio

    asyncio.run(run_sync_test())
