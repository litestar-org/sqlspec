"""Shared fixtures for Litestar extension tests with psqlpy."""

import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from secrets import token_bytes
from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.extensions.litestar import SQLSpecSessionBackend, SQLSpecSessionConfig, SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService


@pytest.fixture
async def psqlpy_migration_config(postgres_service: "PostgresService") -> AsyncGenerator[PsqlpyConfig, None]:
    """Create psqlpy configuration with migration support using string format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        dsn = f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"

        config = PsqlpyConfig(
            pool_config={"dsn": dsn, "max_db_pool_size": 5},
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],  # Simple string format
            },
        )
        yield config
        await config.close_pool()


@pytest.fixture
async def psqlpy_migration_config_with_dict(postgres_service: "PostgresService") -> AsyncGenerator[PsqlpyConfig, None]:
    """Create psqlpy configuration with migration support using dict format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        dsn = f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"

        config = PsqlpyConfig(
            pool_config={"dsn": dsn, "max_db_pool_size": 5},
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
async def psqlpy_migration_config_mixed(postgres_service: "PostgresService") -> AsyncGenerator[PsqlpyConfig, None]:
    """Create psqlpy configuration with mixed extension formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        dsn = f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"

        config = PsqlpyConfig(
            pool_config={"dsn": dsn, "max_db_pool_size": 5},
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
async def session_store_default(psqlpy_migration_config: PsqlpyConfig) -> SQLSpecSessionStore:
    """Create a session store with default table name."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(psqlpy_migration_config)
    await commands.init(psqlpy_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the default migrated table
    return SQLSpecSessionStore(
        psqlpy_migration_config,
        table_name="litestar_sessions",  # Default table name
    )


@pytest.fixture
def session_backend_config_default() -> SQLSpecSessionConfig:
    """Create session backend configuration with default table name."""
    return SQLSpecSessionConfig(key="psqlpy-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def session_backend_default(session_backend_config_default: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with default configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_default)


@pytest.fixture
async def session_store_custom(psqlpy_migration_config_with_dict: PsqlpyConfig) -> SQLSpecSessionStore:
    """Create a session store with custom table name."""
    # Apply migrations to create the session table with custom name
    commands = AsyncMigrationCommands(psqlpy_migration_config_with_dict)
    await commands.init(psqlpy_migration_config_with_dict.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the custom migrated table
    return SQLSpecSessionStore(
        psqlpy_migration_config_with_dict,
        table_name="custom_sessions",  # Custom table name from config
    )


@pytest.fixture
def session_backend_config_custom() -> SQLSpecSessionConfig:
    """Create session backend configuration with custom table name."""
    return SQLSpecSessionConfig(key="psqlpy-custom", max_age=3600, table_name="custom_sessions")


@pytest.fixture
def session_backend_custom(session_backend_config_custom: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with custom configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_custom)


@pytest.fixture
async def migrated_config(psqlpy_migration_config: PsqlpyConfig) -> PsqlpyConfig:
    """Apply migrations once and return the config."""
    commands = AsyncMigrationCommands(psqlpy_migration_config)
    await commands.init(psqlpy_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()
    return psqlpy_migration_config


@pytest.fixture
async def session_store(migrated_config: PsqlpyConfig) -> SQLSpecSessionStore:
    """Create a session store using migrated config."""
    return SQLSpecSessionStore(config=migrated_config, table_name="litestar_sessions")


@pytest.fixture
async def session_config() -> SQLSpecSessionConfig:
    """Create a session config."""
    return SQLSpecSessionConfig(key="session", secret=token_bytes(16), store="sessions", max_age=3600)
