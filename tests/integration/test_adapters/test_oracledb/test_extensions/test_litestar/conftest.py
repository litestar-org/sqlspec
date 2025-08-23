"""Shared fixtures for Litestar extension tests with OracleDB."""

import tempfile
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest

from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.extensions.litestar.session import SQLSpecSessionBackend, SQLSpecSessionConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands


@pytest.fixture
async def oracle_async_migration_config(
    oracle_async_config: OracleAsyncConfig,
) -> AsyncGenerator[OracleAsyncConfig, None]:
    """Create Oracle async configuration with migration support using string format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create new config with migration settings
        config = OracleAsyncConfig(
            pool_config=oracle_async_config.pool_config,
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],  # Simple string format
            },
        )
        yield config
        await config.close_pool()


@pytest.fixture
def oracle_sync_migration_config(oracle_sync_config: OracleSyncConfig) -> Generator[OracleSyncConfig, None, None]:
    """Create Oracle sync configuration with migration support using string format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create new config with migration settings
        config = OracleSyncConfig(
            pool_config=oracle_sync_config.pool_config,
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],  # Simple string format
            },
        )
        yield config
        config.close_pool()


@pytest.fixture
async def oracle_async_migration_config_with_dict(
    oracle_async_config: OracleAsyncConfig,
) -> AsyncGenerator[OracleAsyncConfig, None]:
    """Create Oracle async configuration with migration support using dict format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = OracleAsyncConfig(
            pool_config=oracle_async_config.pool_config,
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": [
                    {"name": "litestar", "session_table": "custom_sessions"}
                ],  # Dict format with custom table name
            },
        )
        yield config
        await config.close_pool()


@pytest.fixture
def oracle_sync_migration_config_with_dict(
    oracle_sync_config: OracleSyncConfig,
) -> Generator[OracleSyncConfig, None, None]:
    """Create Oracle sync configuration with migration support using dict format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = OracleSyncConfig(
            pool_config=oracle_sync_config.pool_config,
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": [
                    {"name": "litestar", "session_table": "custom_sessions"}
                ],  # Dict format with custom table name
            },
        )
        yield config
        config.close_pool()


@pytest.fixture
async def oracle_async_migration_config_mixed(
    oracle_async_config: OracleAsyncConfig,
) -> AsyncGenerator[OracleAsyncConfig, None]:
    """Create Oracle async configuration with mixed extension formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = OracleAsyncConfig(
            pool_config=oracle_async_config.pool_config,
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": [
                    "litestar",  # String format - will use default table name
                    {"name": "other_ext", "option": "value"},  # Dict format for hypothetical extension
                ],
            },
        )
        yield config
        await config.close_pool()


@pytest.fixture
def oracle_sync_migration_config_mixed(oracle_sync_config: OracleSyncConfig) -> Generator[OracleSyncConfig, None, None]:
    """Create Oracle sync configuration with mixed extension formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = OracleSyncConfig(
            pool_config=oracle_sync_config.pool_config,
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": [
                    "litestar",  # String format - will use default table name
                    {"name": "other_ext", "option": "value"},  # Dict format for hypothetical extension
                ],
            },
        )
        yield config
        config.close_pool()


@pytest.fixture
async def oracle_async_session_store_default(
    oracle_async_migration_config: OracleAsyncConfig,
) -> SQLSpecSessionStore:
    """Create an async session store with default table name."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(oracle_async_migration_config)
    await commands.init(oracle_async_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the default migrated table
    return SQLSpecSessionStore(
        oracle_async_migration_config,
        table_name="litestar_sessions",  # Default table name
    )


@pytest.fixture
def oracle_async_session_backend_config_default() -> SQLSpecSessionConfig:
    """Create async session backend configuration with default table name."""
    return SQLSpecSessionConfig(key="oracle-async-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def oracle_async_session_backend_default(oracle_async_session_backend_config_default: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create async session backend with default configuration."""
    return SQLSpecSessionBackend(config=oracle_async_session_backend_config_default)


@pytest.fixture
def oracle_sync_session_store_default(oracle_sync_migration_config: OracleSyncConfig) -> SQLSpecSessionStore:
    """Create a sync session store with default table name."""
    # Apply migrations to create the session table
    commands = SyncMigrationCommands(oracle_sync_migration_config)
    commands.init(oracle_sync_migration_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Create store using the default migrated table
    return SQLSpecSessionStore(
        oracle_sync_migration_config,
        table_name="litestar_sessions",  # Default table name
    )


@pytest.fixture
def oracle_sync_session_backend_config_default() -> SQLSpecSessionConfig:
    """Create sync session backend configuration with default table name."""
    return SQLSpecSessionConfig(key="oracle-sync-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def oracle_sync_session_backend_default(oracle_sync_session_backend_config_default: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create sync session backend with default configuration."""
    return SQLSpecSessionBackend(config=oracle_sync_session_backend_config_default)


@pytest.fixture
async def oracle_async_session_store_custom(
    oracle_async_migration_config_with_dict: OracleAsyncConfig,
) -> SQLSpecSessionStore:
    """Create an async session store with custom table name."""
    # Apply migrations to create the session table with custom name
    commands = AsyncMigrationCommands(oracle_async_migration_config_with_dict)
    await commands.init(oracle_async_migration_config_with_dict.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the custom migrated table
    return SQLSpecSessionStore(
        oracle_async_migration_config_with_dict,
        table_name="custom_sessions",  # Custom table name from config
    )


@pytest.fixture
def oracle_async_session_backend_config_custom() -> SQLSpecSessionConfig:
    """Create async session backend configuration with custom table name."""
    return SQLSpecSessionConfig(key="oracle-async-custom", max_age=3600, table_name="custom_sessions")


@pytest.fixture
def oracle_async_session_backend_custom(oracle_async_session_backend_config_custom: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create async session backend with custom configuration."""
    return SQLSpecSessionBackend(config=oracle_async_session_backend_config_custom)


@pytest.fixture
def oracle_sync_session_store_custom(
    oracle_sync_migration_config_with_dict: OracleSyncConfig,
) -> SQLSpecSessionStore:
    """Create a sync session store with custom table name."""
    # Apply migrations to create the session table with custom name
    commands = SyncMigrationCommands(oracle_sync_migration_config_with_dict)
    commands.init(oracle_sync_migration_config_with_dict.migration_config["script_location"], package=False)
    commands.upgrade()

    # Create store using the custom migrated table
    return SQLSpecSessionStore(
        oracle_sync_migration_config_with_dict,
        table_name="custom_sessions",  # Custom table name from config
    )


@pytest.fixture
def oracle_sync_session_backend_config_custom() -> SQLSpecSessionConfig:
    """Create sync session backend configuration with custom table name."""
    return SQLSpecSessionConfig(key="oracle-sync-custom", max_age=3600, table_name="custom_sessions")


@pytest.fixture
def oracle_sync_session_backend_custom(oracle_sync_session_backend_config_custom: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create sync session backend with custom configuration."""
    return SQLSpecSessionBackend(config=oracle_sync_session_backend_config_custom)
