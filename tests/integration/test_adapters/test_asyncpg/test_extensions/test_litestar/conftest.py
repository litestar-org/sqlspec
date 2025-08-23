"""Shared fixtures for Litestar extension tests with asyncpg."""

import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from secrets import token_bytes

import pytest

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend, SQLSpecSessionConfig, SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands


@pytest.fixture
async def asyncpg_migration_config() -> AsyncGenerator[AsyncpgConfig, None]:
    """Create asyncpg configuration with migration support using string format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = AsyncpgConfig(
            pool_config={
                "dsn": "postgresql://postgres:postgres@localhost:5432/postgres",
                "min_size": 2,
                "max_size": 10,
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],  # Simple string format
            },
        )
        yield config
        await config.close_pool()


@pytest.fixture
async def asyncpg_migration_config_with_dict() -> AsyncGenerator[AsyncpgConfig, None]:
    """Create asyncpg configuration with migration support using dict format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = AsyncpgConfig(
            pool_config={
                "dsn": "postgresql://postgres:postgres@localhost:5432/postgres",
                "min_size": 2,
                "max_size": 10,
            },
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
async def asyncpg_migration_config_mixed() -> AsyncGenerator[AsyncpgConfig, None]:
    """Create asyncpg configuration with mixed extension formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = AsyncpgConfig(
            pool_config={
                "dsn": "postgresql://postgres:postgres@localhost:5432/postgres",
                "min_size": 2,
                "max_size": 10,
            },
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
async def session_store_default(asyncpg_migration_config: AsyncpgConfig) -> SQLSpecSessionStore:
    """Create a session store with default table name."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(asyncpg_migration_config)
    await commands.init(asyncpg_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the default migrated table
    return SQLSpecSessionStore(
        asyncpg_migration_config,
        table_name="litestar_sessions",  # Default table name
    )


@pytest.fixture
def session_backend_config_default() -> SQLSpecSessionConfig:
    """Create session backend configuration with default table name."""
    return SQLSpecSessionConfig(key="asyncpg-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def session_backend_default(session_backend_config_default: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with default configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_default)


@pytest.fixture
async def session_store_custom(asyncpg_migration_config_with_dict: AsyncpgConfig) -> SQLSpecSessionStore:
    """Create a session store with custom table name."""
    # Apply migrations to create the session table with custom name
    commands = AsyncMigrationCommands(asyncpg_migration_config_with_dict)
    await commands.init(asyncpg_migration_config_with_dict.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the custom migrated table
    return SQLSpecSessionStore(
        asyncpg_migration_config_with_dict,
        table_name="custom_sessions",  # Custom table name from config
    )


@pytest.fixture
def session_backend_config_custom() -> SQLSpecSessionConfig:
    """Create session backend configuration with custom table name."""
    return SQLSpecSessionConfig(key="asyncpg-custom", max_age=3600, table_name="custom_sessions")


@pytest.fixture
def session_backend_custom(session_backend_config_custom: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with custom configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_custom)


@pytest.fixture
async def session_store(asyncpg_migration_config: AsyncpgConfig) -> SQLSpecSessionStore:
    """Create a session store using migrated config."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(asyncpg_migration_config)
    await commands.init(asyncpg_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    return SQLSpecSessionStore(config=asyncpg_migration_config, table_name="litestar_sessions")


@pytest.fixture
async def session_config() -> SQLSpecSessionConfig:
    """Create a session config."""
    return SQLSpecSessionConfig(key="session", secret=token_bytes(16), store="sessions", max_age=3600)
