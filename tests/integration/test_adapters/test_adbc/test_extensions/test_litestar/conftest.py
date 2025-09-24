"""Shared fixtures for Litestar extension tests with ADBC adapter.

This module provides fixtures for testing the integration between SQLSpec's ADBC adapter
and Litestar's session middleware. ADBC is a sync-only adapter that provides Arrow-native
database connectivity across multiple database backends.
"""

import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.adbc.config import AdbcConfig
from sqlspec.extensions.litestar import SQLSpecSessionStore
from sqlspec.extensions.litestar.session import SQLSpecSessionConfig
from sqlspec.migrations.commands import SyncMigrationCommands


@pytest.fixture
def adbc_migration_config(
    postgres_service: PostgresService, request: pytest.FixtureRequest
) -> Generator[AdbcConfig, None, None]:
    """Create ADBC configuration with migration support using string format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter, worker ID, and test node ID for pytest-xdist
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        test_id = abs(hash(request.node.nodeid)) % 100000
        table_name = f"sqlspec_migrations_adbc_{worker_id}_{test_id}"

        config = AdbcConfig(
            connection_config={
                "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": [
                    {"name": "litestar", "session_table": f"litestar_sessions_adbc_{worker_id}_{test_id}"}
                ],  # Unique table for ADBC with worker ID
            },
        )
        yield config


@pytest.fixture
def adbc_migration_config_with_dict(
    postgres_service: PostgresService, request: pytest.FixtureRequest
) -> Generator[AdbcConfig, None, None]:
    """Create ADBC configuration with migration support using dict format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter, worker ID, and test node ID for pytest-xdist
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        test_id = abs(hash(request.node.nodeid)) % 100000
        table_name = f"sqlspec_migrations_adbc_dict_{worker_id}_{test_id}"

        config = AdbcConfig(
            connection_config={
                "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": [
                    {"name": "litestar", "session_table": f"custom_adbc_sessions_{worker_id}_{test_id}"}
                ],  # Dict format with custom table name and worker ID
            },
        )
        yield config


@pytest.fixture
def adbc_migration_config_mixed(
    postgres_service: PostgresService, request: pytest.FixtureRequest
) -> Generator[AdbcConfig, None, None]:
    """Create ADBC configuration with mixed extension formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter, worker ID, and test node ID for pytest-xdist
        worker_id = getattr(request.config, "workerinput", {}).get("workerid", "master")
        test_id = abs(hash(request.node.nodeid)) % 100000
        table_name = f"sqlspec_migrations_adbc_mixed_{worker_id}_{test_id}"

        config = AdbcConfig(
            connection_config={
                "uri": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
                "driver_name": "postgresql",
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": [
                    {
                        "name": "litestar",
                        "session_table": f"litestar_sessions_adbc_{worker_id}_{test_id}",
                    },  # Unique table for ADBC with worker ID
                    {"name": "other_ext", "option": "value"},  # Dict format for hypothetical extension
                ],
            },
        )
        yield config


@pytest.fixture
def session_backend_default(adbc_migration_config: AdbcConfig) -> SQLSpecSessionStore:
    """Create a session backend with default table name for ADBC (sync)."""
    # Apply migrations to create the session table
    commands = SyncMigrationCommands(adbc_migration_config)
    commands.init(adbc_migration_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Extract the unique table name from the config
    session_table_name = "litestar_sessions_adbc"
    for ext in adbc_migration_config.migration_config.get("include_extensions", []):
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "litestar_sessions_adbc")
            break

    # Create session store using the migrated table with unique name
    return SQLSpecSessionStore(config=adbc_migration_config, table_name=session_table_name)


@pytest.fixture
def session_backend_custom(adbc_migration_config_with_dict: AdbcConfig) -> SQLSpecSessionStore:
    """Create a session backend with custom table name for ADBC (sync)."""
    # Apply migrations to create the session table with custom name
    commands = SyncMigrationCommands(adbc_migration_config_with_dict)
    commands.init(adbc_migration_config_with_dict.migration_config["script_location"], package=False)
    commands.upgrade()

    # Extract the unique table name from the config
    session_table_name = "custom_adbc_sessions"
    for ext in adbc_migration_config_with_dict.migration_config.get("include_extensions", []):
        if isinstance(ext, dict) and ext.get("name") == "litestar":
            session_table_name = ext.get("session_table", "custom_adbc_sessions")
            break

    # Create session store using the custom migrated table with unique name
    return SQLSpecSessionStore(config=adbc_migration_config_with_dict, table_name=session_table_name)


@pytest.fixture
def session_config_default() -> SQLSpecSessionConfig:
    """Create a session configuration with default settings for ADBC."""
    return SQLSpecSessionConfig(
        table_name="litestar_sessions",
        store="sessions",  # This will be the key in the stores registry
        max_age=3600,
    )


@pytest.fixture
def session_config_custom() -> SQLSpecSessionConfig:
    """Create a session configuration with custom settings for ADBC."""
    return SQLSpecSessionConfig(
        table_name="custom_adbc_sessions",
        store="sessions",  # This will be the key in the stores registry
        max_age=3600,
    )
