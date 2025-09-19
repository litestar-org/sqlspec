"""Shared fixtures for Litestar extension tests with aiosqlite."""

import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend, SQLSpecSessionConfig, SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands


@pytest.fixture
async def aiosqlite_migration_config(request: pytest.FixtureRequest) -> AsyncGenerator[AiosqliteConfig, None]:
    """Create aiosqlite configuration with migration support using string format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sessions.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_aiosqlite_{abs(hash(request.node.nodeid)) % 1000000}"

        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": ["litestar"],  # Simple string format
            },
        )
        yield config
        await config.close_pool()


@pytest.fixture
async def aiosqlite_migration_config_with_dict(request: pytest.FixtureRequest) -> AsyncGenerator[AiosqliteConfig, None]:
    """Create aiosqlite configuration with migration support using dict format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sessions.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_aiosqlite_dict_{abs(hash(request.node.nodeid)) % 1000000}"

        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": [
                    {"name": "litestar", "session_table": "custom_sessions"}
                ],  # Dict format with custom table name
            },
        )
        yield config
        await config.close_pool()


@pytest.fixture
async def aiosqlite_migration_config_mixed(request: pytest.FixtureRequest) -> AsyncGenerator[AiosqliteConfig, None]:
    """Create aiosqlite configuration with only litestar extension to test migration robustness."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sessions.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_aiosqlite_mixed_{abs(hash(request.node.nodeid)) % 1000000}"

        config = AiosqliteConfig(
            pool_config={"database": str(db_path)},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": [
                    "litestar"  # String format - will use default table name
                ],
            },
        )
        yield config
        await config.close_pool()


@pytest.fixture
async def session_store_default(aiosqlite_migration_config: AiosqliteConfig) -> SQLSpecSessionStore:
    """Create a session store with default table name."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(aiosqlite_migration_config)
    await commands.init(aiosqlite_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the default migrated table
    return SQLSpecSessionStore(
        aiosqlite_migration_config,
        table_name="litestar_sessions",  # Default table name
    )


@pytest.fixture
def session_backend_config_default() -> SQLSpecSessionConfig:
    """Create session backend configuration with default table name."""
    return SQLSpecSessionConfig(key="aiosqlite-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def session_backend_default(session_backend_config_default: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with default configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_default)


@pytest.fixture
async def session_store_custom(aiosqlite_migration_config_with_dict: AiosqliteConfig) -> SQLSpecSessionStore:
    """Create a session store with custom table name."""
    # Apply migrations to create the session table with custom name
    commands = AsyncMigrationCommands(aiosqlite_migration_config_with_dict)
    await commands.init(aiosqlite_migration_config_with_dict.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the custom migrated table
    return SQLSpecSessionStore(
        aiosqlite_migration_config_with_dict,
        table_name="custom_sessions",  # Custom table name from config
    )


@pytest.fixture
def session_backend_config_custom() -> SQLSpecSessionConfig:
    """Create session backend configuration with custom table name."""
    return SQLSpecSessionConfig(key="aiosqlite-custom", max_age=3600, table_name="custom_sessions")


@pytest.fixture
def session_backend_custom(session_backend_config_custom: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with custom configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_custom)
