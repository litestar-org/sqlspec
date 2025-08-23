"""Shared fixtures for Litestar extension tests with DuckDB."""

import tempfile
from pathlib import Path
from typing import Any

import pytest
from litestar import Litestar, get, post, put
from litestar.status_codes import HTTP_404_NOT_FOUND
from litestar.stores.registry import StoreRegistry

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.litestar import SQLSpecSessionConfig, SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands


@pytest.fixture
def migrated_config() -> DuckDBConfig:
    """Apply migrations to the config."""
    tmpdir = tempfile.mkdtemp()
    db_path = Path(tmpdir) / "test.duckdb"
    migration_dir = Path(tmpdir) / "migrations"

    # Create a separate config for migrations to avoid connection issues
    migration_config = DuckDBConfig(
        pool_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": "test_migrations",
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
            "version_table_name": "test_migrations",
            "include_extensions": ["litestar"],
        },
    )


@pytest.fixture
def session_store(migrated_config: DuckDBConfig) -> SQLSpecSessionStore:
    """Create a session store using the migrated config."""
    return SQLSpecSessionStore(config=migrated_config, table_name="litestar_sessions")


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
