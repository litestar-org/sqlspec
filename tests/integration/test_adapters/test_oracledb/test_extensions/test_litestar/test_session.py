"""Integration tests for OracleDB session backend with store integration."""

import asyncio
import tempfile
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from litestar.testing import AsyncTestClient

from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
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
async def oracle_async_session_store(oracle_async_config: OracleAsyncConfig) -> SQLSpecSessionStore:
    """Create an async session store with migrations applied using unique table names."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(oracle_async_config)
    await commands.init(oracle_async_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Extract the unique session table name from config
    extensions = oracle_async_config.migration_config.get("include_extensions", [])
    session_table_name = "litestar_sessions"
    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions")
            break

    return SQLSpecSessionStore(oracle_async_config, table_name=session_table_name)


@pytest.fixture
def oracle_sync_session_store(oracle_sync_config: OracleSyncConfig) -> SQLSpecSessionStore:
    """Create a sync session store with migrations applied using unique table names."""
    # Apply migrations to create the session table
    commands = SyncMigrationCommands(oracle_sync_config)
    commands.init(oracle_sync_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Extract the unique session table name from config
    extensions = oracle_sync_config.migration_config.get("include_extensions", [])
    session_table_name = "litestar_sessions"
    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions")
            break

    return SQLSpecSessionStore(oracle_sync_config, table_name=session_table_name)


async def test_oracle_async_migration_creates_correct_table(oracle_async_config: OracleAsyncConfig) -> None:
    """Test that Litestar migration creates the correct table structure for Oracle."""
    # Apply migrations
    commands = AsyncMigrationCommands(oracle_async_config)
    await commands.init(oracle_async_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Get the session table name
    extensions = oracle_async_config.migration_config.get("include_extensions", [])
    session_table_name = "litestar_sessions"
    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions")
            break

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

    # Get the session table name
    extensions = oracle_sync_config.migration_config.get("include_extensions", [])
    session_table_name = "litestar_sessions"
    for ext in extensions:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions")
            break

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


async def test_oracle_async_session_basic_operations(oracle_async_session_store: SQLSpecSessionStore) -> None:
    """Test basic session operations with Oracle async backend."""

    @get("/set-session")
    async def set_session(request: Any) -> dict:
        request.session["user_id"] = 12345
        request.session["username"] = "oracle_user"
        request.session["preferences"] = {"theme": "dark", "lang": "en"}
        request.session["oracle_features"] = {"plsql": True, "json": True, "vector": False}
        request.session["roles"] = ["admin", "user", "oracle_dba"]
        return {"status": "session set"}

    @get("/get-session")
    async def get_session(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "username": request.session.get("username"),
            "preferences": request.session.get("preferences"),
            "oracle_features": request.session.get("oracle_features"),
            "roles": request.session.get("roles"),
        }

    @post("/update-session")
    async def update_session(request: Any) -> dict:
        request.session["last_access"] = "2024-01-01T12:00:00"
        request.session["oracle_features"]["vector"] = True
        request.session["preferences"]["notifications"] = True
        return {"status": "session updated"}

    @post("/clear-session")
    async def clear_session(request: Any) -> dict:
        request.session.clear()
        return {"status": "session cleared"}

    session_config = ServerSideSessionConfig(store="sessions", key="oracle-async-session", max_age=3600)

    app = Litestar(
        route_handlers=[set_session, get_session, update_session, clear_session],
        middleware=[session_config.middleware],
        stores={"sessions": oracle_async_session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Set session data
        response = await client.get("/set-session")
        assert response.status_code == HTTP_200_OK
        assert response.json() == {"status": "session set"}

        # Get session data
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        data = response.json()
        assert data["user_id"] == 12345
        assert data["username"] == "oracle_user"
        assert data["preferences"] == {"theme": "dark", "lang": "en"}
        assert data["oracle_features"]["plsql"] is True
        assert data["roles"] == ["admin", "user", "oracle_dba"]

        # Update session
        response = await client.post("/update-session")
        assert response.status_code == HTTP_201_CREATED

        # Verify update
        response = await client.get("/get-session")
        data = response.json()
        assert data["oracle_features"]["vector"] is True
        assert data["preferences"]["notifications"] is True

        # Clear session
        response = await client.post("/clear-session")
        assert response.status_code == HTTP_201_CREATED
        assert response.json() == {"status": "session cleared"}

        # Verify session is cleared
        response = await client.get("/get-session")
        assert response.status_code == HTTP_200_OK
        expected_cleared = {
            "user_id": None,
            "username": None,
            "preferences": None,
            "oracle_features": None,
            "roles": None,
        }
        assert response.json() == expected_cleared


def test_oracle_sync_session_basic_operations(oracle_sync_session_store: SQLSpecSessionStore) -> None:
    """Test basic session operations with Oracle sync backend."""

    async def run_sync_test() -> None:
        @get("/set-session")
        async def set_session(request: Any) -> dict:
            request.session["user_id"] = 54321
            request.session["username"] = "oracle_sync_user"
            request.session["preferences"] = {"theme": "light", "lang": "fr"}
            request.session["database"] = {"type": "Oracle", "version": "23ai", "mode": "sync"}
            return {"status": "session set"}

        @get("/get-session")
        async def get_session(request: Any) -> dict:
            return {
                "user_id": request.session.get("user_id"),
                "username": request.session.get("username"),
                "preferences": request.session.get("preferences"),
                "database": request.session.get("database"),
            }

        session_config = ServerSideSessionConfig(store="sessions", key="oracle-sync-session", max_age=3600)

        app = Litestar(
            route_handlers=[set_session, get_session],
            middleware=[session_config.middleware],
            stores={"sessions": oracle_sync_session_store},
        )

        async with AsyncTestClient(app=app) as client:
            # Set session data
            response = await client.get("/set-session")
            assert response.status_code == HTTP_200_OK
            assert response.json() == {"status": "session set"}

            # Get session data
            response = await client.get("/get-session")
            assert response.status_code == HTTP_200_OK
            data = response.json()
            assert data["user_id"] == 54321
            assert data["username"] == "oracle_sync_user"
            assert data["preferences"] == {"theme": "light", "lang": "fr"}
            assert data["database"]["type"] == "Oracle"
            assert data["database"]["mode"] == "sync"

    asyncio.run(run_sync_test())


async def test_oracle_async_session_persistence(oracle_async_session_store: SQLSpecSessionStore) -> None:
    """Test that sessions persist across requests with Oracle async."""

    @get("/counter")
    async def increment_counter(request: Any) -> dict:
        count = request.session.get("count", 0)
        oracle_queries = request.session.get("oracle_queries", [])
        count += 1
        oracle_queries.append(f"SELECT {count} FROM DUAL")
        request.session["count"] = count
        request.session["oracle_queries"] = oracle_queries
        request.session["oracle_sid"] = f"ORCL_{count}"
        return {"count": count, "oracle_queries": oracle_queries, "oracle_sid": f"ORCL_{count}"}

    session_config = ServerSideSessionConfig(store="sessions", key="oracle-counter", max_age=3600)

    app = Litestar(
        route_handlers=[increment_counter],
        middleware=[session_config.middleware],
        stores={"sessions": oracle_async_session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Multiple increments should persist with Oracle query history
        for expected in range(1, 6):
            response = await client.get("/counter")
            data = response.json()
            assert data["count"] == expected
            assert len(data["oracle_queries"]) == expected
            assert data["oracle_queries"][-1] == f"SELECT {expected} FROM DUAL"
            assert data["oracle_sid"] == f"ORCL_{expected}"


def test_oracle_sync_session_persistence(oracle_sync_session_store: SQLSpecSessionStore) -> None:
    """Test that sessions persist across requests with Oracle sync."""

    async def run_sync_test() -> None:
        @get("/oracle-stats")
        async def oracle_stats(request: Any) -> dict:
            stats = request.session.get("stats", {"tables": 0, "indexes": 0, "sequences": 0})
            stats["tables"] += 1
            stats["indexes"] += 2
            stats["sequences"] += 1
            request.session["stats"] = stats
            request.session["oracle_session_id"] = f"SID_{stats['tables']}"
            return {"stats": stats, "oracle_session_id": f"SID_{stats['tables']}"}

        session_config = ServerSideSessionConfig(store="sessions", key="oracle-sync-stats", max_age=3600)

        app = Litestar(
            route_handlers=[oracle_stats],
            middleware=[session_config.middleware],
            stores={"sessions": oracle_sync_session_store},
        )

        async with AsyncTestClient(app=app) as client:
            # Multiple requests should accumulate Oracle statistics
            expected_stats = [
                {"tables": 1, "indexes": 2, "sequences": 1},
                {"tables": 2, "indexes": 4, "sequences": 2},
                {"tables": 3, "indexes": 6, "sequences": 3},
            ]

            for i, expected in enumerate(expected_stats, 1):
                response = await client.get("/oracle-stats")
                data = response.json()
                assert data["stats"] == expected
                assert data["oracle_session_id"] == f"SID_{i}"

    asyncio.run(run_sync_test())


async def test_oracle_async_session_expiration(oracle_async_session_store: SQLSpecSessionStore) -> None:
    """Test session expiration handling with Oracle async."""

    @get("/set-data")
    async def set_data(request: Any) -> dict:
        request.session["test"] = "oracle_data"
        request.session["timestamp"] = "2024-01-01"
        request.session["oracle_instance"] = "ORCL_TEST"
        request.session["plsql_enabled"] = True
        return {"status": "set"}

    @get("/get-data")
    async def get_data(request: Any) -> dict:
        return {
            "test": request.session.get("test"),
            "timestamp": request.session.get("timestamp"),
            "oracle_instance": request.session.get("oracle_instance"),
            "plsql_enabled": request.session.get("plsql_enabled"),
        }

    session_config = ServerSideSessionConfig(
        store="sessions",
        key="oracle-expiring",
        max_age=1,  # 1 second expiration
    )

    app = Litestar(
        route_handlers=[set_data, get_data],
        middleware=[session_config.middleware],
        stores={"sessions": oracle_async_session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Set data
        response = await client.get("/set-data")
        assert response.json() == {"status": "set"}

        # Data should be available immediately
        response = await client.get("/get-data")
        expected_data = {
            "test": "oracle_data",
            "timestamp": "2024-01-01",
            "oracle_instance": "ORCL_TEST",
            "plsql_enabled": True,
        }
        assert response.json() == expected_data

        # Wait for expiration
        await asyncio.sleep(2)

        # Data should be expired
        response = await client.get("/get-data")
        expected_expired = {"test": None, "timestamp": None, "oracle_instance": None, "plsql_enabled": None}
        assert response.json() == expected_expired


def test_oracle_sync_session_expiration(oracle_sync_session_store: SQLSpecSessionStore) -> None:
    """Test session expiration handling with Oracle sync."""

    async def run_sync_test() -> None:
        @get("/set-oracle-config")
        async def set_oracle_config(request: Any) -> dict:
            request.session["oracle_config"] = {
                "sga_size": "2GB",
                "pga_size": "1GB",
                "service_name": "ORCL_SERVICE",
                "tablespace": "USERS",
            }
            return {"status": "oracle config set"}

        @get("/get-oracle-config")
        async def get_oracle_config(request: Any) -> dict:
            return {"oracle_config": request.session.get("oracle_config")}

        session_config = ServerSideSessionConfig(
            store="sessions",
            key="oracle-sync-expiring",
            max_age=1,  # 1 second expiration
        )

        app = Litestar(
            route_handlers=[set_oracle_config, get_oracle_config],
            middleware=[session_config.middleware],
            stores={"sessions": oracle_sync_session_store},
        )

        async with AsyncTestClient(app=app) as client:
            # Set Oracle configuration
            response = await client.get("/set-oracle-config")
            assert response.json() == {"status": "oracle config set"}

            # Data should be available immediately
            response = await client.get("/get-oracle-config")
            data = response.json()
            assert data["oracle_config"]["sga_size"] == "2GB"
            assert data["oracle_config"]["service_name"] == "ORCL_SERVICE"

            # Wait for expiration
            await asyncio.sleep(2)

            # Data should be expired
            response = await client.get("/get-oracle-config")
            assert response.json() == {"oracle_config": None}

    asyncio.run(run_sync_test())


async def test_oracle_async_concurrent_sessions(oracle_async_session_store: SQLSpecSessionStore) -> None:
    """Test handling of concurrent sessions with Oracle async."""

    @get("/user/{user_id:int}")
    async def set_user(request: Any, user_id: int) -> dict:
        request.session["user_id"] = user_id
        request.session["db"] = "oracle"
        request.session["oracle_sid"] = f"ORCL_{user_id}"
        request.session["features"] = ["plsql", "json", "vector"] if user_id % 2 == 0 else ["plsql", "json"]
        return {"user_id": user_id, "oracle_sid": f"ORCL_{user_id}"}

    @get("/whoami")
    async def get_user(request: Any) -> dict:
        return {
            "user_id": request.session.get("user_id"),
            "db": request.session.get("db"),
            "oracle_sid": request.session.get("oracle_sid"),
            "features": request.session.get("features"),
        }

    session_config = ServerSideSessionConfig(store="sessions", key="oracle-concurrent", max_age=3600)

    app = Litestar(
        route_handlers=[set_user, get_user],
        middleware=[session_config.middleware],
        stores={"sessions": oracle_async_session_store},
    )

    # Test with multiple concurrent clients
    async with (
        AsyncTestClient(app=app) as client1,
        AsyncTestClient(app=app) as client2,
        AsyncTestClient(app=app) as client3,
    ):
        # Set different users in different clients
        response1 = await client1.get("/user/101")
        expected1 = {"user_id": 101, "oracle_sid": "ORCL_101"}
        assert response1.json() == expected1

        response2 = await client2.get("/user/202")
        expected2 = {"user_id": 202, "oracle_sid": "ORCL_202"}
        assert response2.json() == expected2

        response3 = await client3.get("/user/303")
        expected3 = {"user_id": 303, "oracle_sid": "ORCL_303"}
        assert response3.json() == expected3

        # Each client should maintain its own session with Oracle-specific data
        response1 = await client1.get("/whoami")
        data1 = response1.json()
        assert data1["user_id"] == 101
        assert data1["db"] == "oracle"
        assert data1["oracle_sid"] == "ORCL_101"
        assert data1["features"] == ["plsql", "json"]  # 101 % 2 != 0

        response2 = await client2.get("/whoami")
        data2 = response2.json()
        assert data2["user_id"] == 202
        assert data2["oracle_sid"] == "ORCL_202"
        assert data2["features"] == ["plsql", "json", "vector"]  # 202 % 2 == 0

        response3 = await client3.get("/whoami")
        data3 = response3.json()
        assert data3["user_id"] == 303
        assert data3["oracle_sid"] == "ORCL_303"
        assert data3["features"] == ["plsql", "json"]  # 303 % 2 != 0


def test_oracle_sync_concurrent_sessions(oracle_sync_session_store: SQLSpecSessionStore) -> None:
    """Test handling of concurrent sessions with Oracle sync."""

    async def run_sync_test() -> None:
        @get("/oracle-workspace/{workspace_id:int}")
        async def set_workspace(request: Any, workspace_id: int) -> dict:
            request.session["workspace_id"] = workspace_id
            request.session["oracle_workspace"] = f"WS_{workspace_id}"
            request.session["tablespaces"] = [f"TBS_{workspace_id}_DATA", f"TBS_{workspace_id}_INDEX"]
            return {"workspace_id": workspace_id}

        @get("/current-workspace")
        async def get_workspace(request: Any) -> dict:
            return {
                "workspace_id": request.session.get("workspace_id"),
                "oracle_workspace": request.session.get("oracle_workspace"),
                "tablespaces": request.session.get("tablespaces"),
            }

        session_config = ServerSideSessionConfig(store="sessions", key="oracle-sync-concurrent", max_age=3600)

        app = Litestar(
            route_handlers=[set_workspace, get_workspace],
            middleware=[session_config.middleware],
            stores={"sessions": oracle_sync_session_store},
        )

        # Test with multiple concurrent clients
        async with AsyncTestClient(app=app) as client1, AsyncTestClient(app=app) as client2:
            # Set different workspaces
            await client1.get("/oracle-workspace/100")
            await client2.get("/oracle-workspace/200")

            # Each client should maintain its own Oracle workspace
            response1 = await client1.get("/current-workspace")
            data1 = response1.json()
            assert data1["workspace_id"] == 100
            assert data1["oracle_workspace"] == "WS_100"
            assert data1["tablespaces"] == ["TBS_100_DATA", "TBS_100_INDEX"]

            response2 = await client2.get("/current-workspace")
            data2 = response2.json()
            assert data2["workspace_id"] == 200
            assert data2["oracle_workspace"] == "WS_200"
            assert data2["tablespaces"] == ["TBS_200_DATA", "TBS_200_INDEX"]

    asyncio.run(run_sync_test())


async def test_oracle_async_session_cleanup(oracle_async_session_store: SQLSpecSessionStore) -> None:
    """Test expired session cleanup with Oracle async."""
    # Create multiple sessions with short expiration
    session_ids = []
    for i in range(10):
        session_id = f"oracle-cleanup-{i}"
        session_ids.append(session_id)
        oracle_data = {
            "data": i,
            "type": "temporary",
            "oracle_instance": f"ORCL_TEMP_{i}",
            "plsql_package": f"PKG_TEMP_{i}",
        }
        await oracle_async_session_store.set(session_id, oracle_data, expires_in=1)

    # Create long-lived Oracle sessions
    persistent_ids = []
    for i in range(3):
        session_id = f"oracle-persistent-{i}"
        persistent_ids.append(session_id)
        oracle_data = {
            "data": f"keep-{i}",
            "type": "persistent",
            "oracle_instance": f"ORCL_PERSIST_{i}",
            "tablespace": f"TBS_PERSIST_{i}",
            "features": {"plsql": True, "json": True, "vector": i % 2 == 0},
        }
        await oracle_async_session_store.set(session_id, oracle_data, expires_in=3600)

    # Wait for short sessions to expire
    await asyncio.sleep(2)

    # Clean up expired sessions
    await oracle_async_session_store.delete_expired()

    # Check that expired sessions are gone
    for session_id in session_ids:
        result = await oracle_async_session_store.get(session_id)
        assert result is None

    # Long-lived Oracle sessions should still exist
    for i, session_id in enumerate(persistent_ids):
        result = await oracle_async_session_store.get(session_id)
        assert result is not None
        assert result["type"] == "persistent"
        assert result["oracle_instance"] == f"ORCL_PERSIST_{i}"
        assert result["features"]["plsql"] is True


def test_oracle_sync_session_cleanup(oracle_sync_session_store: SQLSpecSessionStore) -> None:
    """Test expired session cleanup with Oracle sync."""

    async def run_sync_test() -> None:
        # Create multiple Oracle sessions with short expiration
        session_ids = []
        for i in range(5):
            session_id = f"oracle-sync-cleanup-{i}"
            session_ids.append(session_id)
            oracle_data = {
                "data": i,
                "type": "temporary",
                "oracle_config": {"sga_size": f"{i}GB", "service": f"TEMP_SERVICE_{i}"},
            }
            await oracle_sync_session_store.set(session_id, oracle_data, expires_in=1)

        # Create long-lived Oracle sessions
        persistent_ids = []
        for i in range(2):
            session_id = f"oracle-sync-persistent-{i}"
            persistent_ids.append(session_id)
            oracle_data = {
                "data": f"keep-{i}",
                "type": "persistent",
                "oracle_config": {"sga_size": f"{i + 10}GB", "service": f"PERSISTENT_SERVICE_{i}"},
            }
            await oracle_sync_session_store.set(session_id, oracle_data, expires_in=3600)

        # Wait for short sessions to expire
        await asyncio.sleep(2)

        # Clean up expired sessions
        await oracle_sync_session_store.delete_expired()

        # Check that expired sessions are gone
        for session_id in session_ids:
            result = await oracle_sync_session_store.get(session_id)
            assert result is None

        # Long-lived Oracle sessions should still exist
        for i, session_id in enumerate(persistent_ids):
            result = await oracle_sync_session_store.get(session_id)
            assert result is not None
            assert result["type"] == "persistent"
            assert result["oracle_config"]["service"] == f"PERSISTENT_SERVICE_{i}"

    asyncio.run(run_sync_test())


async def test_oracle_async_session_complex_data(oracle_async_session_store: SQLSpecSessionStore) -> None:
    """Test storing complex Oracle-specific data structures in sessions."""

    @post("/save-oracle-complex")
    async def save_oracle_complex(request: Any) -> dict:
        # Store various complex Oracle data types
        request.session["oracle_config"] = {
            "database": {
                "instances": ["ORCL1", "ORCL2", "ORCL3"],
                "services": {"primary": "ORCL_PRIMARY", "standby": "ORCL_STANDBY"},
                "tablespaces": {"data": ["USERS", "TEMP", "UNDO"], "index": ["INDEX_TBS"], "lob": ["LOB_TBS"]},
            },
            "features": {
                "advanced_security": True,
                "partitioning": True,
                "compression": {"basic": True, "advanced": False},
                "flashback": {"database": True, "table": True, "query": True},
            },
            "performance": {
                "sga_components": {"shared_pool": "512MB", "buffer_cache": "1GB", "redo_log_buffer": "64MB"},
                "pga_target": "1GB",
            },
        }
        request.session["plsql_packages"] = ["DBMS_STATS", "DBMS_SCHEDULER", "DBMS_VECTOR"]
        request.session["unicode_oracle"] = "Oracle: 🔥 База данных データベース"
        request.session["null_values"] = {"null_field": None, "empty_dict": {}, "empty_list": []}
        return {"status": "oracle complex data saved"}

    @get("/load-oracle-complex")
    async def load_oracle_complex(request: Any) -> dict:
        return {
            "oracle_config": request.session.get("oracle_config"),
            "plsql_packages": request.session.get("plsql_packages"),
            "unicode_oracle": request.session.get("unicode_oracle"),
            "null_values": request.session.get("null_values"),
        }

    session_config = ServerSideSessionConfig(store="sessions", key="oracle-complex", max_age=3600)

    app = Litestar(
        route_handlers=[save_oracle_complex, load_oracle_complex],
        middleware=[session_config.middleware],
        stores={"sessions": oracle_async_session_store},
    )

    async with AsyncTestClient(app=app) as client:
        # Save complex Oracle data
        response = await client.post("/save-oracle-complex")
        assert response.json() == {"status": "oracle complex data saved"}

        # Load and verify complex Oracle data
        response = await client.get("/load-oracle-complex")
        data = response.json()

        # Verify Oracle database structure
        oracle_config = data["oracle_config"]
        assert oracle_config["database"]["instances"] == ["ORCL1", "ORCL2", "ORCL3"]
        assert oracle_config["database"]["services"]["primary"] == "ORCL_PRIMARY"
        assert "USERS" in oracle_config["database"]["tablespaces"]["data"]

        # Verify Oracle features
        assert oracle_config["features"]["advanced_security"] is True
        assert oracle_config["features"]["compression"]["basic"] is True
        assert oracle_config["features"]["compression"]["advanced"] is False

        # Verify performance settings
        assert oracle_config["performance"]["sga_components"]["shared_pool"] == "512MB"
        assert oracle_config["performance"]["pga_target"] == "1GB"

        # Verify PL/SQL packages
        assert data["plsql_packages"] == ["DBMS_STATS", "DBMS_SCHEDULER", "DBMS_VECTOR"]

        # Verify unicode and null handling
        assert data["unicode_oracle"] == "Oracle: 🔥 База данных データベース"
        assert data["null_values"]["null_field"] is None
        assert data["null_values"]["empty_dict"] == {}
        assert data["null_values"]["empty_list"] == []


async def test_oracle_async_store_operations(oracle_async_session_store: SQLSpecSessionStore) -> None:
    """Test Oracle async store operations directly."""
    # Test basic Oracle store operations
    session_id = "test-session-oracle-async"
    oracle_test_data = {
        "user_id": 789,
        "oracle_preferences": {"default_tablespace": "USERS", "temp_tablespace": "TEMP", "profile": "DEFAULT"},
        "oracle_roles": ["DBA", "RESOURCE", "CONNECT"],
        "plsql_features": {"packages": True, "functions": True, "procedures": True, "triggers": True},
    }

    # Set Oracle data
    await oracle_async_session_store.set(session_id, oracle_test_data, expires_in=3600)

    # Get Oracle data
    result = await oracle_async_session_store.get(session_id)
    assert result == oracle_test_data

    # Check exists
    assert await oracle_async_session_store.exists(session_id) is True

    # Update with renewal and Oracle-specific additions
    updated_oracle_data = {
        **oracle_test_data,
        "last_login": "2024-01-01",
        "oracle_session": {"sid": 123, "serial": 456, "machine": "oracle_client"},
    }
    await oracle_async_session_store.set(session_id, updated_oracle_data, expires_in=7200)

    # Get updated Oracle data
    result = await oracle_async_session_store.get(session_id)
    assert result == updated_oracle_data
    assert result["oracle_session"]["sid"] == 123

    # Delete Oracle data
    await oracle_async_session_store.delete(session_id)

    # Verify deleted
    result = await oracle_async_session_store.get(session_id)
    assert result is None
    assert await oracle_async_session_store.exists(session_id) is False


def test_oracle_sync_store_operations(oracle_sync_session_store: SQLSpecSessionStore) -> None:
    """Test Oracle sync store operations directly."""

    async def run_sync_test() -> None:
        # Test basic Oracle sync store operations
        session_id = "test-session-oracle-sync"
        oracle_sync_test_data = {
            "user_id": 987,
            "oracle_workspace": {"schema": "HR", "default_tablespace": "HR_DATA", "quota": "100M"},
            "oracle_objects": ["TABLE", "VIEW", "INDEX", "SEQUENCE", "TRIGGER", "PACKAGE"],
            "database_links": [{"name": "REMOTE_DB", "connect_string": "remote.example.com:1521/REMOTE"}],
        }

        # Set Oracle sync data
        await oracle_sync_session_store.set(session_id, oracle_sync_test_data, expires_in=3600)

        # Get Oracle sync data
        result = await oracle_sync_session_store.get(session_id)
        assert result == oracle_sync_test_data

        # Check exists
        assert await oracle_sync_session_store.exists(session_id) is True

        # Update with Oracle-specific sync additions
        updated_sync_data = {
            **oracle_sync_test_data,
            "sync_timestamp": "2024-01-01T12:00:00Z",
            "oracle_version": {"version": "23ai", "edition": "Enterprise"},
        }
        await oracle_sync_session_store.set(session_id, updated_sync_data, expires_in=7200)

        # Get updated sync data
        result = await oracle_sync_session_store.get(session_id)
        assert result == updated_sync_data
        assert result["oracle_version"]["edition"] == "Enterprise"

        # Delete sync data
        await oracle_sync_session_store.delete(session_id)

        # Verify deleted
        result = await oracle_sync_session_store.get(session_id)
        assert result is None
        assert await oracle_sync_session_store.exists(session_id) is False

    asyncio.run(run_sync_test())
