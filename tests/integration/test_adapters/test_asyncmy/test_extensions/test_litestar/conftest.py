"""Shared fixtures for Litestar extension tests with asyncmy."""

import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from pytest_databases.docker.mysql import MySQLService

from sqlspec.adapters.asyncmy.config import AsyncmyConfig
from sqlspec.extensions.litestar.session import SQLSpecSessionBackend, SQLSpecSessionConfig
from sqlspec.extensions.litestar.store import SQLSpecAsyncSessionStore
from sqlspec.migrations.commands import AsyncMigrationCommands


@pytest.fixture
async def asyncmy_migration_config(
    mysql_service: MySQLService, request: pytest.FixtureRequest
) -> AsyncGenerator[AsyncmyConfig, None]:
    """Create asyncmy configuration with migration support using string format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_asyncmy_{abs(hash(request.node.nodeid)) % 1000000}"

        config = AsyncmyConfig(
            pool_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "autocommit": True,
                "minsize": 1,
                "maxsize": 5,
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": ["litestar"],  # Simple string format
            },
        )
        yield config
        await config.close_pool()


@pytest.fixture
async def asyncmy_migration_config_with_dict(
    mysql_service: MySQLService, request: pytest.FixtureRequest
) -> AsyncGenerator[AsyncmyConfig, None]:
    """Create asyncmy configuration with migration support using dict format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_asyncmy_dict_{abs(hash(request.node.nodeid)) % 1000000}"

        config = AsyncmyConfig(
            pool_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "autocommit": True,
                "minsize": 1,
                "maxsize": 5,
            },
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
async def asyncmy_migration_config_mixed(
    mysql_service: MySQLService, request: pytest.FixtureRequest
) -> AsyncGenerator[AsyncmyConfig, None]:
    """Create asyncmy configuration with mixed extension formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_asyncmy_mixed_{abs(hash(request.node.nodeid)) % 1000000}"

        config = AsyncmyConfig(
            pool_config={
                "host": mysql_service.host,
                "port": mysql_service.port,
                "user": mysql_service.user,
                "password": mysql_service.password,
                "database": mysql_service.db,
                "autocommit": True,
                "minsize": 1,
                "maxsize": 5,
            },
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
        await config.close_pool()


@pytest.fixture
async def session_store_default(asyncmy_migration_config: AsyncmyConfig) -> SQLSpecAsyncSessionStore:
    """Create a session store with default table name."""
    # Apply migrations to create the session table
    commands = AsyncMigrationCommands(asyncmy_migration_config)
    await commands.init(asyncmy_migration_config.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the default migrated table
    return SQLSpecAsyncSessionStore(
        asyncmy_migration_config,
        table_name="litestar_sessions",  # Default table name
    )


@pytest.fixture
def session_backend_config_default() -> SQLSpecSessionConfig:
    """Create session backend configuration with default table name."""
    return SQLSpecSessionConfig(key="asyncmy-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def session_backend_default(session_backend_config_default: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with default configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_default)


@pytest.fixture
async def session_store_custom(asyncmy_migration_config_with_dict: AsyncmyConfig) -> SQLSpecAsyncSessionStore:
    """Create a session store with custom table name."""
    # Apply migrations to create the session table with custom name
    commands = AsyncMigrationCommands(asyncmy_migration_config_with_dict)
    await commands.init(asyncmy_migration_config_with_dict.migration_config["script_location"], package=False)
    await commands.upgrade()

    # Create store using the custom migrated table
    return SQLSpecAsyncSessionStore(
        asyncmy_migration_config_with_dict,
        table_name="custom_sessions",  # Custom table name from config
    )


@pytest.fixture
def session_backend_config_custom() -> SQLSpecSessionConfig:
    """Create session backend configuration with custom table name."""
    return SQLSpecSessionConfig(key="asyncmy-custom", max_age=3600, table_name="custom_sessions")


@pytest.fixture
def session_backend_custom(session_backend_config_custom: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with custom configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_custom)
