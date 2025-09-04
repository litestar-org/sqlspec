"""Shared fixtures for Litestar extension tests with DuckDB."""

import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post, put
from litestar.status_codes import HTTP_404_NOT_FOUND
from litestar.stores.registry import StoreRegistry

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend, SQLSpecSessionConfig, SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands


@pytest.fixture
def duckdb_migration_config(request: pytest.FixtureRequest) -> Generator[DuckDBConfig, None, None]:
    """Create DuckDB configuration with migration support using string format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sessions.duckdb"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_duckdb_{abs(hash(request.node.nodeid)) % 1000000}"

        config = DuckDBConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": ["litestar"],  # Simple string format
            },
        )
        yield config
        if config.pool_instance:
            config.close_pool()


@pytest.fixture
def duckdb_migration_config_with_dict(request: pytest.FixtureRequest) -> Generator[DuckDBConfig, None, None]:
    """Create DuckDB configuration with migration support using dict format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sessions.duckdb"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Get worker ID for table isolation in parallel testing
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        session_table = f"duckdb_sessions_{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_duckdb_dict_{abs(hash(request.node.nodeid)) % 1000000}"

        config = DuckDBConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": [
                    {"name": "litestar", "session_table": session_table}
                ],  # Dict format with custom table name
            },
        )
        yield config
        if config.pool_instance:
            config.close_pool()


@pytest.fixture
def duckdb_migration_config_mixed(request: pytest.FixtureRequest) -> Generator[DuckDBConfig, None, None]:
    """Create DuckDB configuration with mixed extension formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sessions.duckdb"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_duckdb_mixed_{abs(hash(request.node.nodeid)) % 1000000}"

        config = DuckDBConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": [
                    "litestar",  # String format - will use default table name
                    {"name": "other_ext", "option": "value"},  # Dict format for hypothetical extension
                ],
            },
        )
        yield config
        if config.pool_instance:
            config.close_pool()


@pytest.fixture
def migrated_config(request: pytest.FixtureRequest) -> DuckDBConfig:
    """Apply migrations to the config (backward compatibility)."""
    tmpdir = tempfile.mkdtemp()
    db_path = Path(tmpdir) / "test.duckdb"
    migration_dir = Path(tmpdir) / "migrations"

    # Create unique version table name using adapter and test node ID
    table_name = f"sqlspec_migrations_duckdb_{abs(hash(request.node.nodeid)) % 1000000}"

    # Create a separate config for migrations to avoid connection issues
    migration_config = DuckDBConfig(
        pool_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": table_name,
            "include_extensions": ["litestar"],  # Include litestar extension migrations
        },
    )

    commands = SyncMigrationCommands(migration_config)
    commands.init(str(migration_dir), package=False)
    commands.upgrade()

    # Close the migration pool to release the database lock
    if migration_config.pool_instance:
        migration_config.close_pool()

    # Return a fresh config for the tests
    return DuckDBConfig(
        pool_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": table_name,
            "include_extensions": ["litestar"],
        },
    )


@pytest.fixture
def session_store_default(duckdb_migration_config: DuckDBConfig) -> SQLSpecSessionStore:
    """Create a session store with default table name."""
    # Apply migrations to create the session table
    commands = SyncMigrationCommands(duckdb_migration_config)
    commands.init(duckdb_migration_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Create store using the default migrated table
    return SQLSpecSessionStore(
        duckdb_migration_config,
        table_name="litestar_sessions",  # Default table name
    )


@pytest.fixture
def session_backend_config_default() -> SQLSpecSessionConfig:
    """Create session backend configuration with default table name."""
    return SQLSpecSessionConfig(key="duckdb-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def session_backend_default(session_backend_config_default: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with default configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_default)


@pytest.fixture
def session_store_custom(duckdb_migration_config_with_dict: DuckDBConfig) -> SQLSpecSessionStore:
    """Create a session store with custom table name."""
    # Apply migrations to create the session table with custom name
    commands = SyncMigrationCommands(duckdb_migration_config_with_dict)
    commands.init(duckdb_migration_config_with_dict.migration_config["script_location"], package=False)
    commands.upgrade()

    # Extract custom table name from migration config
    litestar_ext = None
    for ext in duckdb_migration_config_with_dict.migration_config["include_extensions"]:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            litestar_ext = ext
            break

    table_name = litestar_ext["session_table"] if litestar_ext else "litestar_sessions"

    # Create store using the custom migrated table
    return SQLSpecSessionStore(
        duckdb_migration_config_with_dict,
        table_name=table_name,  # Custom table name from config
    )


@pytest.fixture
def session_backend_config_custom(duckdb_migration_config_with_dict: DuckDBConfig) -> SQLSpecSessionConfig:
    """Create session backend configuration with custom table name."""
    # Extract custom table name from migration config
    litestar_ext = None
    for ext in duckdb_migration_config_with_dict.migration_config["include_extensions"]:
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            litestar_ext = ext
            break

    table_name = litestar_ext["session_table"] if litestar_ext else "litestar_sessions"
    return SQLSpecSessionConfig(key="duckdb-custom", max_age=3600, table_name=table_name)


@pytest.fixture
def session_backend_custom(session_backend_config_custom: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with custom configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_custom)


@pytest.fixture
def session_store(duckdb_migration_config: DuckDBConfig) -> SQLSpecSessionStore:
    """Create a session store using migrated config."""
    # Apply migrations to create the session table
    commands = SyncMigrationCommands(duckdb_migration_config)
    commands.init(duckdb_migration_config.migration_config["script_location"], package=False)
    commands.upgrade()

    return SQLSpecSessionStore(config=duckdb_migration_config, table_name="litestar_sessions")


@pytest.fixture
def session_config() -> SQLSpecSessionConfig:
    """Create a session config."""
    return SQLSpecSessionConfig(table_name="litestar_sessions", store="sessions", max_age=3600)


@pytest.fixture
def litestar_app(session_config: SQLSpecSessionConfig, session_store: SQLSpecSessionStore) -> Litestar:
    """Create a Litestar app with session middleware for testing."""

    @get("/session/set/{key:str}")
    async def set_session_value(request: Any, key: str) -> dict:
        """Set a session value."""
        value = request.query_params.get("value", "default")
        request.session[key] = value
        return {"status": "set", "key": key, "value": value}

    @get("/session/get/{key:str}")
    async def get_session_value(request: Any, key: str) -> dict:
        """Get a session value."""
        value = request.session.get(key)
        return {"key": key, "value": value}

    @post("/session/bulk")
    async def set_bulk_session(request: Any) -> dict:
        """Set multiple session values."""
        data = await request.json()
        for key, value in data.items():
            request.session[key] = value
        return {"status": "bulk set", "count": len(data)}

    @get("/session/all")
    async def get_all_session(request: Any) -> dict:
        """Get all session data."""
        return dict(request.session)

    @post("/session/clear")
    async def clear_session(request: Any) -> dict:
        """Clear all session data."""
        request.session.clear()
        return {"status": "cleared"}

    @post("/session/key/{key:str}/delete")
    async def delete_session_key(request: Any, key: str) -> dict:
        """Delete a specific session key."""
        if key in request.session:
            del request.session[key]
            return {"status": "deleted", "key": key}
        return {"status": "not found", "key": key}

    @get("/counter")
    async def counter(request: Any) -> dict:
        """Increment a counter in session."""
        count = request.session.get("count", 0)
        count += 1
        request.session["count"] = count
        return {"count": count}

    @put("/user/profile")
    async def set_user_profile(request: Any) -> dict:
        """Set user profile data."""
        profile = await request.json()
        request.session["profile"] = profile
        return {"status": "profile set", "profile": profile}

    @get("/user/profile")
    async def get_user_profile(request: Any) -> dict:
        """Get user profile data."""
        profile = request.session.get("profile")
        if not profile:
            return {"error": "No profile found"}, HTTP_404_NOT_FOUND
        return {"profile": profile}

    # Register the store in the app
    stores = StoreRegistry()
    stores.register("sessions", session_store)

    return Litestar(
        route_handlers=[
            set_session_value,
            get_session_value,
            set_bulk_session,
            get_all_session,
            clear_session,
            delete_session_key,
            counter,
            set_user_profile,
            get_user_profile,
        ],
        middleware=[session_config.middleware],
        stores=stores,
    )
