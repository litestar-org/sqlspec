"""Shared fixtures for Litestar extension tests with psycopg."""

import tempfile
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.extensions.litestar.session import SQLSpecSessionBackend, SQLSpecSessionConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands


@pytest.fixture
def psycopg_sync_migration_config(postgres_service: PostgresService) -> "Generator[PsycopgSyncConfig, None, None]":
    """Create psycopg sync configuration with migration support."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = PsycopgSyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],  # Include litestar extension migrations
            },
        )
        yield config

        if config.pool_instance:
            config.close_pool()


@pytest.fixture
async def psycopg_async_migration_config(postgres_service: PostgresService) -> AsyncGenerator[PsycopgAsyncConfig, None]:
    """Create psycopg async configuration with migration support."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        config = PsycopgAsyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": "sqlspec_migrations",
                "include_extensions": ["litestar"],  # Include litestar extension migrations
            },
        )
        yield config
        await config.close_pool()


@pytest.fixture
def psycopg_sync_migrated_config(psycopg_sync_migration_config: PsycopgSyncConfig) -> PsycopgSyncConfig:
    """Apply migrations and return sync config."""
    commands = SyncMigrationCommands(psycopg_sync_migration_config)
    commands.init(psycopg_sync_migration_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Close migration pool after running migrations
    if psycopg_sync_migration_config.pool_instance:
        psycopg_sync_migration_config.close_pool()

    return psycopg_sync_migration_config


@pytest.fixture
async def psycopg_async_migrated_config(psycopg_async_migration_config: PsycopgAsyncConfig) -> PsycopgAsyncConfig:
    """Apply migrations and return async config."""
    commands = AsyncMigrationCommands(psycopg_async_migration_config)
    await commands.init(psycopg_async_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Close migration pool after running migrations
    if psycopg_async_migration_config.pool_instance:
        await psycopg_async_migration_config.close_pool()

    return psycopg_async_migration_config


@pytest.fixture
def sync_session_store(psycopg_sync_migrated_config: PsycopgSyncConfig) -> SQLSpecSessionStore:
    """Create a sync session store with default table name."""
    return SQLSpecSessionStore(
        psycopg_sync_migrated_config,
        table_name="litestar_sessions",  # Default table name
    )


@pytest.fixture
def sync_session_backend_config() -> SQLSpecSessionConfig:
    """Create sync session backend configuration."""
    return SQLSpecSessionConfig(key="psycopg-sync-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def sync_session_backend(sync_session_backend_config: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create sync session backend."""
    return SQLSpecSessionBackend(config=sync_session_backend_config)


@pytest.fixture
async def async_session_store(psycopg_async_migrated_config: PsycopgAsyncConfig) -> SQLSpecSessionStore:
    """Create an async session store with default table name."""
    return SQLSpecSessionStore(
        psycopg_async_migrated_config,
        table_name="litestar_sessions",  # Default table name
    )


@pytest.fixture
def async_session_backend_config() -> SQLSpecSessionConfig:
    """Create async session backend configuration."""
    return SQLSpecSessionConfig(key="psycopg-async-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def async_session_backend(async_session_backend_config: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create async session backend."""
    return SQLSpecSessionBackend(config=async_session_backend_config)
