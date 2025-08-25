"""Integration tests for ADBC session backend with store integration."""

import tempfile
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.testing import TestClient
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.sync_tools import run_
from tests.integration.test_adapters.test_adbc.conftest import xfail_if_driver_missing

pytestmark = [
    pytest.mark.adbc,
    pytest.mark.postgres,
    pytest.mark.integration,
    pytest.mark.xdist_group("postgres"),
]


@pytest.fixture
def adbc_config(
    postgres_service: PostgresService, request: pytest.FixtureRequest
) -> Generator[AdbcConfig, None, None]:
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

    # Extract the unique session table name from the migration config
    extensions = adbc_config.migration_config.get("include_extensions", [])
    session_table_name = "litestar_sessions"  # default

    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions")
            break

    return SQLSpecSessionStore(adbc_config, table_name=session_table_name)


@xfail_if_driver_missing
def test_adbc_migration_creates_correct_table(adbc_config: AdbcConfig) -> None:
    """Test that Litestar migration creates the correct table structure for ADBC with PostgreSQL."""

    # Apply migrations synchronously (ADBC uses sync commands)
    commands = SyncMigrationCommands(adbc_config)
    commands.init(adbc_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Get the actual table name from config
    extensions = adbc_config.migration_config.get("include_extensions", [])
    session_table_name = "litestar_sessions"  # default

    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions")
            break

    # Verify table was created with correct PostgreSQL-specific types
    with adbc_config.provide_session() as driver:
        result = driver.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_name = %s
            AND table_schema = 'public'
        """, (session_table_name,))
        assert len(result.data) == 1
        table_info = result.data[0]
        assert table_info["table_name"] == session_table_name
        assert table_info["table_type"] == "BASE TABLE"

        # Verify column structure - ADBC with PostgreSQL uses JSONB
        result = driver.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """, (session_table_name,))
        columns = {row["column_name"]: row for row in result.data}

        assert "session_id" in columns
        assert "data" in columns
        assert "expires_at" in columns
        assert "created_at" in columns

        # Verify data types for PostgreSQL with ADBC
        assert columns["session_id"]["data_type"] == "text"
        assert columns["data"]["data_type"] == "jsonb"  # ADBC uses JSONB for efficient Arrow transfer
        assert columns["expires_at"]["data_type"] in ("timestamp with time zone", "timestamptz")
        assert columns["created_at"]["data_type"] in ("timestamp with time zone", "timestamptz")


@xfail_if_driver_missing
def test_adbc_session_basic_operations(session_store: SQLSpecSessionStore) -> None:
    """Test basic session operations with ADBC backend."""

    @get("/set-session")
    def set_session(request: Any) -> dict:
        request.session["user_id"] = 12345
        request.session["username"] = "adbc_testuser"
        request.session["preferences"] = {"theme": "dark", "lang": "en", "arrow_native": True}
        request.session["engine"] = "ADBC"
        return {"status": "session set"}

    @get("/get-session")
    def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "preferences": request.session.get("preferences"),
            "engine": request.session.get("engine"),
        }

    @post("/update-session")
    def update_session(request: Any) -> dict:
        request.session["last_access"] = "2024-01-01T12:00:00"
        request.session["preferences"]["notifications"] = True
        request.session["adbc_features"] = ["Arrow", "Columnar", "Zero-copy"]
        return {"status": "session updated"}

    @post("/clear-session")
    def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(store="sessions", key="adbc-session", max_age=3600)

    # Create app with session store registered
    app = Litestar(
        route_handlers=[set_session, get_session, update_session, clear_session],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
    )

    with TestClient(app=app) as client:
        # Set session data
        response = client.get("/set-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session set"}

        # Get session data
        response = client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["user_id"] == 12345
        assert data["username"] == "adbc_testuser"
        assert data["preferences"]["arrow_native"] is True
        assert data["engine"] == "ADBC"

        # Update session
        response = client.post("/update-session")
        assert response.status_code == HTTP_201_CREATED

        # Verify update
        response = client.get("/get-session")
        data = response.json()
        assert data["preferences"]["notifications"] is True

        # Clear session
        response = client.post("/clear-session")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"user_id": None, "username": None, "preferences": None, "engine": None}


@xfail_if_driver_missing
def test_adbc_session_persistence(session_store: SQLSpecSessionStore) -> None:
    """Test that sessions persist across requests with ADBC."""

    @get("/counter")
    def increment_counter(request: Any) -> dict:
        count = request.session.get("count", 0)
        history = request.session.get("history", [])
        arrow_operations = request.session.get("arrow_operations", [])

        count += 1
        history.append(count)
        arrow_operations.append(f"arrow_op_{count}")

        request.session["count"] = count
        request.session["history"] = history
        request.session["arrow_operations"] = arrow_operations
        request.session["adbc_engine"] = "Arrow-native"

        return {
            "count": count,
            "history": history,
            "arrow_operations": arrow_operations,
            "engine": "ADBC",
        }

    session_config = ServerSideSessionConfig(store="sessions", key="adbc-persistence", max_age=3600)

    app = Litestar(
        route_handlers=[increment_counter],
        middleware=[session_config.middleware],
        stores={"sessions": session_store}
    )

    with TestClient(app=app) as client:
        # Multiple increments should persist with history
        for expected in range(1, 6):
            response = client.get("/counter")
            data = response.json()
            assert data["count"] == expected
            assert data["history"] == list(range(1, expected + 1))
            assert data["arrow_operations"] == [f"arrow_op_{i}" for i in range(1, expected + 1)]
            assert data["engine"] == "ADBC"


@xfail_if_driver_missing
def test_adbc_session_expiration() -> None:
    """Test session expiration handling with ADBC."""
    # Create a separate configuration for this test to avoid conflicts
    with tempfile.TemporaryDirectory() as temp_dir:
        from pytest_databases.docker import postgresql_url

        # Get PostgreSQL connection info
        postgres_url = postgresql_url()

        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create configuration
        config = AdbcConfig(
            connection_config={
                "uri": postgres_url,
                "driver_name": "postgresql",
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations_exp",
                "include_extensions": [{"name": "litestar", "session_table": "litestar_sessions_exp"}],
            },
        )

        # Apply migrations synchronously
        commands = SyncMigrationCommands(config)
        commands.init(config.migration_config["script_location"], package=False)
        commands.upgrade()

        # Create fresh store
        session_store = SQLSpecSessionStore(config, table_name="litestar_sessions_exp")

        # Test expiration
        session_id = "adbc-expiration-test-session"
        test_data = {
            "test": "adbc_data",
            "timestamp": "2024-01-01",
            "engine": "ADBC",
            "arrow_native": True
        }

        # Set data with 1 second expiration
        run_(session_store.set)(session_id, test_data, expires_in=1)

        # Data should be available immediately
        result = run_(session_store.get)(session_id)
        assert result == test_data

        # Wait for expiration
        time.sleep(2)

        # Data should be expired
        result = run_(session_store.get)(session_id)
        assert result is None


@xfail_if_driver_missing
def test_adbc_concurrent_sessions(session_store: SQLSpecSessionStore) -> None:
    """Test handling of concurrent sessions with ADBC."""

    @get("/user/{user_id:int}")
    def set_user(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        request.session["db"] = "ADBC"
        request.session["arrow_features"] = ["Columnar", "Zero-copy", "Multi-format"]
        return {"user_id": user_id, "engine": "ADBC"}

    @get("/whoami")
    def get_user(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "db": request.session.get("db"),
            "arrow_features": request.session.get("arrow_features"),
        }

    session_config = ServerSideSessionConfig(store="sessions", key="adbc-concurrent", max_age=3600)

    app = Litestar(
        route_handlers=[set_user, get_user],
        middleware=[session_config.middleware],
        stores={"sessions": session_store}
    )

    # Test with multiple concurrent clients
    with (
        TestClient(app=app) as client1,
        TestClient(app=app) as client2,
        TestClient(app=app) as client3,
    ):
        # Set different users in different clients
        response1 = client1.get("/user/101")
        assert response1.json() == {"user_id": 101, "engine": "ADBC"}

        response2 = client2.get("/user/202")
        assert response2.json() == {"user_id": 202, "engine": "ADBC"}

        response3 = client3.get("/user/303")
        assert response3.json() == {"user_id": 303, "engine": "ADBC"}

        # Each client should maintain its own session
        response1 = client1.get("/whoami")
        data1 = response1.json()
        assert data1["user_id"] == 101
        assert data1["db"] == "ADBC"
        assert "Columnar" in data1["arrow_features"]

        response2 = client2.get("/whoami")
        data2 = response2.json()
        assert data2["user_id"] == 202
        assert data2["db"] == "ADBC"

        response3 = client3.get("/whoami")
        data3 = response3.json()
        assert data3["user_id"] == 303
        assert data3["db"] == "ADBC"


@xfail_if_driver_missing
def test_adbc_session_cleanup() -> None:
    """Test expired session cleanup with ADBC."""
    # Create a separate configuration for this test to avoid conflicts
    with tempfile.TemporaryDirectory() as temp_dir:
        from pytest_databases.docker import postgresql_url

        # Get PostgreSQL connection info
        postgres_url = postgresql_url()

        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Apply migrations and create store
        config = AdbcConfig(
            connection_config={
                "uri": postgres_url,
                "driver_name": "postgresql",
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations_cleanup",
                "include_extensions": [{"name": "litestar", "session_table": "litestar_sessions_cleanup"}],
            },
        )
        commands = SyncMigrationCommands(config)
        commands.init(config.migration_config["script_location"], package=False)
        commands.upgrade()

        # Create fresh store
        session_store = SQLSpecSessionStore(config, table_name="litestar_sessions_cleanup")

        # Create multiple sessions with short expiration
        session_ids = []
        for i in range(10):
            session_id = f"adbc-cleanup-{i}"
            session_ids.append(session_id)
            run_(session_store.set)(
                session_id,
                {"data": i, "type": "temporary", "engine": "ADBC", "arrow_native": True},
                expires_in=1
            )

        # Create long-lived sessions
        persistent_ids = []
        for i in range(3):
            session_id = f"adbc-persistent-{i}"
            persistent_ids.append(session_id)
            run_(session_store.set)(
                session_id,
                {"data": f"keep-{i}", "type": "persistent", "engine": "ADBC", "columnar": True},
                expires_in=3600
            )

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
            assert result["type"] == "persistent"
            assert result["engine"] == "ADBC"


@xfail_if_driver_missing
def test_adbc_session_complex_data(session_store: SQLSpecSessionStore) -> None:
    """Test storing complex data structures in ADBC sessions with Arrow optimization."""

    @post("/save-complex")
    def save_complex(request: Any) -> dict:
        # Store various complex data types optimized for ADBC/Arrow
        request.session["nested"] = {
            "level1": {
                "level2": {
                    "level3": ["deep", "nested", "list", "with", "arrow"],
                    "number": 42.5,
                    "boolean": True,
                    "adbc_metadata": {"arrow_format": True, "columnar": True},
                }
            }
        }
        request.session["mixed_list"] = [1, "two", 3.0, {"four": 4}, [5, 6], {"arrow": True}]
        request.session["unicode"] = "ADBC Arrow: 🏹 База данных données データベース 数据库"
        request.session["null_value"] = None
        request.session["empty_dict"] = {}
        request.session["empty_list"] = []
        request.session["arrow_features"] = {
            "zero_copy": True,
            "columnar_format": True,
            "cross_language": True,
            "high_performance": True,
            "supported_types": ["int", "float", "string", "timestamp", "nested"],
        }
        return {"status": "complex ADBC data saved"}

    @get("/load-complex")
    def load_complex(request: Any) -> dict:
        return {
            "nested": request.session.get("nested"),
            "mixed_list": request.session.get("mixed_list"),
            "unicode": request.session.get("unicode"),
            "null_value": request.session.get("null_value"),
            "empty_dict": request.session.get("empty_dict"),
            "empty_list": request.session.get("empty_list"),
            "arrow_features": request.session.get("arrow_features"),
        }

    session_config = ServerSideSessionConfig(store="sessions", key="adbc-complex", max_age=3600)

    app = Litestar(
        route_handlers=[save_complex, load_complex],
        middleware=[session_config.middleware],
        stores={"sessions": session_store},
    )

    with TestClient(app=app) as client:
        # Save complex data
        response = client.post("/save-complex")
        assert response.json() == {"status": "complex ADBC data saved"}

        # Load and verify complex data
        response = client.get("/load-complex")
        data = response.json()

        # Verify nested structure
        assert data["nested"]["level1"]["level2"]["level3"] == ["deep", "nested", "list", "with", "arrow"]
        assert data["nested"]["level1"]["level2"]["number"] == 42.5
        assert data["nested"]["level1"]["level2"]["boolean"] is True
        assert data["nested"]["level1"]["level2"]["adbc_metadata"]["arrow_format"] is True

        # Verify mixed list
        expected_mixed = [1, "two", 3.0, {"four": 4}, [5, 6], {"arrow": True}]
        assert data["mixed_list"] == expected_mixed

        # Verify unicode with ADBC-specific content
        assert "ADBC Arrow: 🏹" in data["unicode"]
        assert "データベース" in data["unicode"]

        # Verify null and empty values
        assert data["null_value"] is None
        assert data["empty_dict"] == {}
        assert data["empty_list"] == []

        # Verify ADBC/Arrow specific features
        assert data["arrow_features"]["zero_copy"] is True
        assert data["arrow_features"]["columnar_format"] is True
        assert "timestamp" in data["arrow_features"]["supported_types"]


@xfail_if_driver_missing
def test_adbc_store_operations() -> None:
    """Test ADBC store operations directly with Arrow optimization."""
    # Create a separate configuration for this test to avoid conflicts
    with tempfile.TemporaryDirectory() as temp_dir:
        from pytest_databases.docker import postgresql_url

        # Get PostgreSQL connection info
        postgres_url = postgresql_url()

        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Apply migrations and create store
        config = AdbcConfig(
            connection_config={
                "uri": postgres_url,
                "driver_name": "postgresql",
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations_ops",
                "include_extensions": [{"name": "litestar", "session_table": "litestar_sessions_ops"}],
            },
        )
        commands = SyncMigrationCommands(config)
        commands.init(config.migration_config["script_location"], package=False)
        commands.upgrade()

        # Create fresh store
        session_store = SQLSpecSessionStore(config, table_name="litestar_sessions_ops")

        # Test basic store operations with ADBC/Arrow optimizations
        session_id = "test-session-adbc"
        test_data = {
            "user_id": 789,
            "preferences": {"theme": "blue", "lang": "es", "arrow_native": True},
            "tags": ["admin", "user", "adbc"],
            "arrow_metadata": {
                "engine": "ADBC",
                "format": "Arrow",
                "columnar": True,
                "zero_copy": True,
            },
        }

        # Set data
        run_(session_store.set)(session_id, test_data, expires_in=3600)

        # Get data
        result = run_(session_store.get)(session_id)
        assert result == test_data

        # Check exists
        assert run_(session_store.exists)(session_id) is True

        # Update with renewal and ADBC-specific data
        updated_data = {
            **test_data,
            "last_login": "2024-01-01",
            "arrow_operations": ["read", "write", "batch_process"],
        }
        run_(session_store.set)(session_id, updated_data, expires_in=7200)

        # Get updated data
        result = run_(session_store.get)(session_id)
        assert result == updated_data
        assert result["arrow_metadata"]["columnar"] is True
        assert "batch_process" in result["arrow_operations"]

        # Delete data
        run_(session_store.delete)(session_id)

        # Verify deleted
        result = run_(session_store.get)(session_id)
        assert result is None
        assert run_(session_store.exists)(session_id) is False
