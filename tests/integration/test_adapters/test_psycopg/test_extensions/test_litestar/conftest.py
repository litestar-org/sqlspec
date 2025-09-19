"""Shared fixtures for Litestar extension tests with psycopg."""

import tempfile
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from sqlspec.adapters.psycopg import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.extensions.litestar.session import SQLSpecSessionBackend, SQLSpecSessionConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands

if TYPE_CHECKING:
    from pytest_databases.docker.postgres import PostgresService


@pytest.fixture
def psycopg_sync_migration_config(
    postgres_service: "PostgresService", request: pytest.FixtureRequest
) -> "Generator[PsycopgSyncConfig, None, None]":
    """Create psycopg sync configuration with migration support."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_psycopg_sync_{abs(hash(request.node.nodeid)) % 1000000}"

        config = PsycopgSyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": [
                    {"name": "litestar", "session_table": "litestar_sessions_psycopg_sync"}
                ],  # Unique table for psycopg sync
            },
        )
        yield config

        # Cleanup: drop test tables and close pool
        try:
            with config.provide_session() as driver:
                driver.execute("DROP TABLE IF EXISTS litestar_sessions_psycopg_sync")
                driver.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass  # Ignore cleanup errors

        if config.pool_instance:
            config.close_pool()


@pytest.fixture
async def psycopg_async_migration_config(
    postgres_service: "PostgresService", request: pytest.FixtureRequest
) -> AsyncGenerator[PsycopgAsyncConfig, None]:
    """Create psycopg async configuration with migration support."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_psycopg_async_{abs(hash(request.node.nodeid)) % 1000000}"

        config = PsycopgAsyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": [
                    {"name": "litestar", "session_table": "litestar_sessions_psycopg_async"}
                ],  # Unique table for psycopg async
            },
        )
        yield config

        # Cleanup: drop test tables and close pool
        try:
            async with config.provide_session() as driver:
                await driver.execute("DROP TABLE IF EXISTS litestar_sessions_psycopg_async")
                await driver.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass  # Ignore cleanup errors

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
    """Create a sync session store with unique table name."""
    return SQLSpecSessionStore(
        psycopg_sync_migrated_config,
        table_name="litestar_sessions_psycopg_sync",  # Unique table name for psycopg sync
    )


@pytest.fixture
def sync_session_backend_config() -> SQLSpecSessionConfig:
    """Create sync session backend configuration."""
    return SQLSpecSessionConfig(key="psycopg-sync-session", max_age=3600, table_name="litestar_sessions_psycopg_sync")


@pytest.fixture
def sync_session_backend(sync_session_backend_config: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create sync session backend."""
    return SQLSpecSessionBackend(config=sync_session_backend_config)


@pytest.fixture
async def async_session_store(psycopg_async_migrated_config: PsycopgAsyncConfig) -> SQLSpecSessionStore:
    """Create an async session store with unique table name."""
    return SQLSpecSessionStore(
        psycopg_async_migrated_config,
        table_name="litestar_sessions_psycopg_async",  # Unique table name for psycopg async
    )


@pytest.fixture
def async_session_backend_config() -> SQLSpecSessionConfig:
    """Create async session backend configuration."""
    return SQLSpecSessionConfig(key="psycopg-async-session", max_age=3600, table_name="litestar_sessions_psycopg_async")


@pytest.fixture
def async_session_backend(async_session_backend_config: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create async session backend."""
    return SQLSpecSessionBackend(config=async_session_backend_config)


@pytest.fixture
def psycopg_sync_migration_config_with_dict(
    postgres_service: "PostgresService", request: pytest.FixtureRequest
) -> Generator[PsycopgSyncConfig, None, None]:
    """Create psycopg sync configuration with migration support using dict format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique names for test isolation
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_psycopg_sync_dict_{table_suffix}"
        session_table = f"custom_sessions_sync_{table_suffix}"

        config = PsycopgSyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": migration_table,
                "include_extensions": [
                    {"name": "litestar", "session_table": session_table}
                ],  # Dict format with custom table name
            },
        )
        yield config

        # Cleanup: drop test tables and close pool
        try:
            with config.provide_session() as driver:
                driver.execute(f"DROP TABLE IF EXISTS {session_table}")
                driver.execute(f"DROP TABLE IF EXISTS {migration_table}")
        except Exception:
            pass  # Ignore cleanup errors

        if config.pool_instance:
            config.close_pool()


@pytest.fixture
async def psycopg_async_migration_config_with_dict(
    postgres_service: "PostgresService", request: pytest.FixtureRequest
) -> AsyncGenerator[PsycopgAsyncConfig, None]:
    """Create psycopg async configuration with migration support using dict format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique names for test isolation
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        table_suffix = f"{worker_id}_{abs(hash(request.node.nodeid)) % 100000}"
        migration_table = f"sqlspec_migrations_psycopg_async_dict_{table_suffix}"
        session_table = f"custom_sessions_async_{table_suffix}"

        config = PsycopgAsyncConfig(
            pool_config={
                "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": migration_table,
                "include_extensions": [
                    {"name": "litestar", "session_table": session_table}
                ],  # Dict format with custom table name
            },
        )
        yield config

        # Cleanup: drop test tables and close pool
        try:
            async with config.provide_session() as driver:
                await driver.execute(f"DROP TABLE IF EXISTS {session_table}")
                await driver.execute(f"DROP TABLE IF EXISTS {migration_table}")
        except Exception:
            pass  # Ignore cleanup errors

        await config.close_pool()


@pytest.fixture
def sync_session_store_custom(psycopg_sync_migration_config_with_dict: PsycopgSyncConfig) -> SQLSpecSessionStore:
    """Create a sync session store with custom table name."""
    # Apply migrations to create the session table with custom name
    commands = SyncMigrationCommands(psycopg_sync_migration_config_with_dict)
    commands.init(psycopg_sync_migration_config_with_dict.migration_config["script_location"], package=False)
    commands.upgrade()

    # Close migration pool after running migrations
    if psycopg_sync_migration_config_with_dict.pool_instance:
        psycopg_sync_migration_config_with_dict.close_pool()

    # Extract session table name from config
    session_table_name = "custom_sessions"
    for ext in psycopg_sync_migration_config_with_dict.migration_config.get("include_extensions", []):
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "custom_sessions")
            break

    # Create store using the custom migrated table
    return SQLSpecSessionStore(psycopg_sync_migration_config_with_dict, table_name=session_table_name)


@pytest.fixture
async def async_session_store_custom(
    psycopg_async_migration_config_with_dict: PsycopgAsyncConfig,
) -> SQLSpecSessionStore:
    """Create an async session store with custom table name."""
    # Apply migrations to create the session table with custom name
    commands = AsyncMigrationCommands(psycopg_async_migration_config_with_dict)
    await commands.init(psycopg_async_migration_config_with_dict.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Close migration pool after running migrations
    if psycopg_async_migration_config_with_dict.pool_instance:
        await psycopg_async_migration_config_with_dict.close_pool()

    # Extract session table name from config
    session_table_name = "custom_sessions"
    for ext in psycopg_async_migration_config_with_dict.migration_config.get("include_extensions", []):
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "custom_sessions")
            break

    # Create store using the custom migrated table
    return SQLSpecSessionStore(psycopg_async_migration_config_with_dict, table_name=session_table_name)
