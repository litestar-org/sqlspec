"""Shared fixtures for Litestar extension tests with SQLite."""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend, SQLSpecSessionConfig, SQLSpecSyncSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.utils.sync_tools import async_


@pytest.fixture
def sqlite_migration_config(request: pytest.FixtureRequest) -> Generator[SqliteConfig, None, None]:
    """Create SQLite configuration with migration support using string format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sessions.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_sqlite_{abs(hash(request.node.nodeid)) % 1000000}"

        config = SqliteConfig(
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
def sqlite_migration_config_with_dict(request: pytest.FixtureRequest) -> Generator[SqliteConfig, None, None]:
    """Create SQLite configuration with migration support using dict format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sessions.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_sqlite_dict_{abs(hash(request.node.nodeid)) % 1000000}"

        config = SqliteConfig(
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
        if config.pool_instance:
            config.close_pool()


@pytest.fixture
def sqlite_migration_config_mixed(request: pytest.FixtureRequest) -> Generator[SqliteConfig, None, None]:
    """Create SQLite configuration with mixed extension formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "sessions.db"
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_sqlite_mixed_{abs(hash(request.node.nodeid)) % 1000000}"

        config = SqliteConfig(
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
def session_store_default(sqlite_migration_config: SqliteConfig) -> SQLSpecSyncSessionStore:
    """Create a session store with default table name."""

    # Apply migrations to create the session table
    def apply_migrations() -> None:
        commands = SyncMigrationCommands(sqlite_migration_config)
        commands.init(sqlite_migration_config.migration_config["script_location"], package=False)
        commands.upgrade()

    # Run migrations
    async_(apply_migrations)()

    # Create store using the default migrated table
    return SQLSpecSyncSessionStore(
        sqlite_migration_config,
        table_name="litestar_sessions",  # Default table name
    )


@pytest.fixture
def session_backend_config_default() -> SQLSpecSessionConfig:
    """Create session backend configuration with default table name."""
    return SQLSpecSessionConfig(key="sqlite-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def session_backend_default(session_backend_config_default: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with default configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_default)


@pytest.fixture
def session_store_custom(sqlite_migration_config_with_dict: SqliteConfig) -> SQLSpecSyncSessionStore:
    """Create a session store with custom table name."""

    # Apply migrations to create the session table with custom name
    def apply_migrations() -> None:
        commands = SyncMigrationCommands(sqlite_migration_config_with_dict)
        commands.init(sqlite_migration_config_with_dict.migration_config["script_location"], package=False)
        commands.upgrade()

    # Run migrations
    async_(apply_migrations)()

    # Create store using the custom migrated table
    return SQLSpecSyncSessionStore(
        sqlite_migration_config_with_dict,
        table_name="custom_sessions",  # Custom table name from config
    )


@pytest.fixture
def session_backend_config_custom() -> SQLSpecSessionConfig:
    """Create session backend configuration with custom table name."""
    return SQLSpecSessionConfig(key="sqlite-custom", max_age=3600, table_name="custom_sessions")


@pytest.fixture
def session_backend_custom(session_backend_config_custom: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with custom configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_custom)


@pytest.fixture
def session_store(sqlite_migration_config: SqliteConfig) -> SQLSpecSyncSessionStore:
    """Create a session store using migrated config."""

    # Apply migrations to create the session table
    def apply_migrations() -> None:
        commands = SyncMigrationCommands(sqlite_migration_config)
        commands.init(sqlite_migration_config.migration_config["script_location"], package=False)
        commands.upgrade()

    # Run migrations
    async_(apply_migrations)()

    return SQLSpecSyncSessionStore(config=sqlite_migration_config, table_name="litestar_sessions")


@pytest.fixture
def session_config() -> SQLSpecSessionConfig:
    """Create a session config."""
    return SQLSpecSessionConfig(key="session", store="sessions", max_age=3600)
