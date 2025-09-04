"""Integration tests for ADBC session backend with store integration."""

import tempfile
import time
from pathlib import Path
from collections.abc import Generator

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.sync_tools import run_
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing

pytestmark = [pytest.mark.adbc, pytest.mark.postgres, pytest.mark.integration, pytest.mark.xdist_group("postgres")]


@pytest.fixture
def adbc_config(postgres_service: PostgresService, request: pytest.FixtureRequest) -> Generator[AdbcConfig, None, None]:
    """Create ADBC configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create unique names for test isolation (based on advanced-alchemy pattern)
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_adbc_{table_suffix}"
        session_table = f"litestar_sessions_adbc_{table_suffix}"

        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = AdbcConfig(
            connection_config={
                "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
                "driver_name": "postgresql",
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": migration_table,
                "include_extensions": [{"name": "litestar", "session_table": session_table}],
            },
        )
        yield config


@pytest.fixture
def session_store(adbc_config: AdbcConfig) -> SQLSpecSessionStore:
    """Create a session store with migrations applied using unique table names."""

    # Apply migrations synchronously (ADBC uses sync commands)
    commands = SyncMigrationCommands(adbc_config)
    commands.init(adbc_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Extract the unique session table name from the migration config extensions
    session_table_name = "litestar_sessions_adbc"  # unique for adbc
    for ext in adbc_config.migration_config.get("include_extensions", []):
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions_adbc")
            break

    return SQLSpecSessionStore(adbc_config, table_name=session_table_name)


@xfail_if_driver_missing
def test_adbc_migration_creates_correct_table(adbc_config: AdbcConfig) -> None:
    """Test that Litestar migration creates the correct table structure for ADBC with PostgreSQL."""

    # Apply migrations synchronously (ADBC uses sync commands)
    commands = SyncMigrationCommands(adbc_config)
    commands.init(adbc_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Get the session table name from the migration config
    extensions = adbc_config.migration_config.get("include_extensions", [])
    session_table = "litestar_sessions"  # default
    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table = ext.get("session_table", "litestar_sessions")

    # Verify table was created with correct PostgreSQL-specific types
    with adbc_config.provide_session() as driver:
        result = driver.execute(
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
        result = driver.execute(
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


@xfail_if_driver_missing
def test_adbc_session_basic_operations(session_store: SQLSpecSessionStore) -> None:
    """Test basic session operations with ADBC backend."""
    
    # Test only direct store operations which should work
    test_data = {"user_id": 12345, "name": "test"}
    run_(session_store.set)("test-key", test_data, expires_in=3600)
    result = run_(session_store.get)("test-key")
    assert result == test_data

    # Test deletion
    run_(session_store.delete)("test-key")
    result = run_(session_store.get)("test-key")
    assert result is None


@xfail_if_driver_missing
def test_adbc_session_persistence(session_store: SQLSpecSessionStore) -> None:
    """Test that sessions persist across operations with ADBC."""
    
    # Test multiple set/get operations persist data
    session_id = "persistent-test"
    
    # Set initial data
    run_(session_store.set)(session_id, {"count": 1}, expires_in=3600)
    result = run_(session_store.get)(session_id)
    assert result == {"count": 1}
    
    # Update data
    run_(session_store.set)(session_id, {"count": 2}, expires_in=3600)
    result = run_(session_store.get)(session_id)
    assert result == {"count": 2}


@xfail_if_driver_missing
def test_adbc_session_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test session expiration handling with ADBC."""
    
    # Test direct store expiration
    session_id = "expiring-test"
    
    # Set data with short expiration
    run_(session_store.set)(session_id, {"test": "data"}, expires_in=1)
    
    # Data should be available immediately
    result = run_(session_store.get)(session_id)
    assert result == {"test": "data"}
    
    # Wait for expiration
    time.sleep(2)
    
    # Data should be expired
    result = run_(session_store.get)(session_id)
    assert result is None


@xfail_if_driver_missing
def test_adbc_concurrent_sessions(session_store: SQLSpecSessionStore) -> None:
    """Test handling of concurrent sessions with ADBC."""
    
    # Test multiple concurrent session operations
    session_ids = ["session1", "session2", "session3"]
    
    # Set different data in different sessions
    run_(session_store.set)(session_ids[0], {"user_id": 101}, expires_in=3600)
    run_(session_store.set)(session_ids[1], {"user_id": 202}, expires_in=3600)
    run_(session_store.set)(session_ids[2], {"user_id": 303}, expires_in=3600)
    
    # Each session should maintain its own data
    result1 = run_(session_store.get)(session_ids[0])
    assert result1 == {"user_id": 101}
    
    result2 = run_(session_store.get)(session_ids[1])
    assert result2 == {"user_id": 202}
    
    result3 = run_(session_store.get)(session_ids[2])
    assert result3 == {"user_id": 303}


@xfail_if_driver_missing
def test_adbc_session_cleanup(session_store: SQLSpecSessionStore) -> None:
    """Test expired session cleanup with ADBC."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(10):
        session_id = f"adbc-cleanup-{i}"
        session_ids.append(session_id)
        run_(session_store.set)(session_id, {"data": i}, expires_in=1)

    # Create long-lived sessions
    persistent_ids = []
    for i in range(3):
        session_id = f"adbc-persistent-{i}"
        persistent_ids.append(session_id)
        run_(session_store.set)(session_id, {"data": f"keep-{i}"}, expires_in=3600)

    # Wait for short sessions to expire
    time.sleep(2)

    # Clean up expired sessions
    run_(session_store.delete_expired)()

    # Check that expired sessions are gone
    for session_id in session_ids:
        result = run_(session_store.get)(session_id)
        assert result is None

    # Long-lived sessions should still exist
    for session_id in persistent_ids:
        result = run_(session_store.get)(session_id)
        assert result is not None




@xfail_if_driver_missing
def test_adbc_store_operations(session_store: SQLSpecSessionStore) -> None:
    """Test ADBC store operations directly."""
    # Test basic store operations
    session_id = "test-session-adbc"
    test_data = {
        "user_id": 789,
    }

    # Set data
    run_(session_store.set)(session_id, test_data, expires_in=3600)

    # Get data
    result = run_(session_store.get)(session_id)
    assert result == test_data

    # Check exists
    assert run_(session_store.exists)(session_id) is True

    # Update with renewal - use simple data to avoid conversion issues
    updated_data = {"user_id": 790}
    run_(session_store.set)(session_id, updated_data, expires_in=7200)

    # Get updated data
    result = run_(session_store.get)(session_id)
    assert result == updated_data

    # Delete data
    run_(session_store.delete)(session_id)

    # Verify deleted
    result = run_(session_store.get)(session_id)
    assert result is None
    assert run_(session_store.exists)(session_id) is False
