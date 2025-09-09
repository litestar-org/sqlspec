"""Integration tests for BigQuery session backend with store integration."""

import tempfile
import time
from pathlib import Path

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.sync_tools import run_

pytestmark = [pytest.mark.bigquery, pytest.mark.integration]


@pytest.fixture
def bigquery_config(bigquery_service, table_schema_prefix: str, request: pytest.FixtureRequest) -> BigQueryConfig:
    """Create BigQuery configuration with migration support and test isolation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique names for test isolation
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_bigquery_{table_suffix}"
        session_table = f"litestar_sessions_bigquery_{table_suffix}"

        return BigQueryConfig(
            connection_config={
                "project": bigquery_service.project,
                "dataset_id": table_schema_prefix,
                "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
                "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": migration_table,
                "include_extensions": [{"name": "litestar", "session_table": session_table}],
            },
        )


@pytest.fixture
def session_store(bigquery_config: BigQueryConfig) -> SQLSpecSessionStore:
    """Create a session store with migrations applied using unique table names."""
    # Apply migrations to create the session table
    commands = SyncMigrationCommands(bigquery_config)
    commands.init(bigquery_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Extract the unique session table name from the migration config extensions
    session_table_name = "litestar_sessions_bigquery"  # unique for bigquery
    for ext in bigquery_config.migration_config.get("include_extensions", []):
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions_bigquery")
            break

    return SQLSpecSessionStore(bigquery_config, table_name=session_table_name)


def test_bigquery_migration_creates_correct_table(bigquery_config: BigQueryConfig, table_schema_prefix: str) -> None:
    """Test that Litestar migration creates the correct table structure for BigQuery."""
    # Apply migrations
    commands = SyncMigrationCommands(bigquery_config)
    commands.init(bigquery_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Get the session table name from the migration config
    extensions = bigquery_config.migration_config.get("include_extensions", [])
    session_table = "litestar_sessions"  # default
    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table = ext.get("session_table", "litestar_sessions")

    # Verify table was created with correct BigQuery-specific types
    with bigquery_config.provide_session() as driver:
        result = driver.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM `{table_schema_prefix}`.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{session_table}'
            ORDER BY ordinal_position
        """)
        assert len(result.data) > 0

        columns = {row["column_name"]: row for row in result.data}

        # BigQuery should use JSON for data column and TIMESTAMP for datetime columns
        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify BigQuery-specific data types
        assert columns["session_id"]["data_type"] == "STRING"
        assert columns["data"]["data_type"] == "JSON"
        assert columns["expires_at"]["data_type"] == "TIMESTAMP"
        assert columns["created_at"]["data_type"] == "TIMESTAMP"


def test_bigquery_session_basic_operations_simple(session_store: SQLSpecSessionStore) -> None:
    """Test basic session operations with BigQuery backend."""

    # Test only direct store operations which should work
    test_data = {"user_id": 54321, "username": "bigqueryuser"}
    run_(session_store.set)("test-key", test_data, expires_in=3600)
    result = run_(session_store.get)("test-key")
    assert result == test_data

    # Test deletion
    run_(session_store.delete)("test-key")
    result = run_(session_store.get)("test-key")
    assert result is None


def test_bigquery_session_persistence(session_store: SQLSpecSessionStore) -> None:
    """Test that sessions persist across operations with BigQuery."""

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


def test_bigquery_session_expiration(session_store: SQLSpecSessionStore) -> None:
    """Test session expiration handling with BigQuery."""

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


def test_bigquery_concurrent_sessions(session_store: SQLSpecSessionStore) -> None:
    """Test handling of concurrent sessions with BigQuery."""

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


def test_bigquery_session_cleanup(session_store: SQLSpecSessionStore) -> None:
    """Test expired session cleanup with BigQuery."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(10):
        session_id = f"bigquery-cleanup-{i}"
        session_ids.append(session_id)
        run_(session_store.set)(session_id, {"data": i}, expires_in=1)

    # Create long-lived sessions
    persistent_ids = []
    for i in range(3):
        session_id = f"bigquery-persistent-{i}"
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


def test_bigquery_store_operations(session_store: SQLSpecSessionStore) -> None:
    """Test BigQuery store operations directly."""
    # Test basic store operations
    session_id = "test-session-bigquery"
    test_data = {"user_id": 789}

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
