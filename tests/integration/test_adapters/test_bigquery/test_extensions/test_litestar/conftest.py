"""Shared fixtures for Litestar extension tests with BigQuery."""

import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials

from sqlspec.adapters.bigquery.config import BigQueryConfig
from sqlspec.extensions.litestar.session import SQLSpecSessionBackend, SQLSpecSessionConfig
from sqlspec.extensions.litestar.store import SQLSpecSessionStore
from sqlspec.migrations.commands import SyncMigrationCommands

if TYPE_CHECKING:
    from pytest_databases.docker.bigquery import BigQueryService


@pytest.fixture
def bigquery_migration_config(
    bigquery_service: "BigQueryService",
    table_schema_prefix: str,
    request: pytest.FixtureRequest,
) -> Generator[BigQueryConfig, None, None]:
    """Create BigQuery configuration with migration support using string format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_bigquery_{abs(hash(request.node.nodeid)) % 1000000}"

        config = BigQueryConfig(
            connection_config={
                "project": bigquery_service.project,
                "dataset_id": table_schema_prefix,
                "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
                "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
            },
            migration_config={
                "script_location": str(migration_dir),
                "version_table_name": table_name,
                "include_extensions": ["litestar"],  # Simple string format
            },
        )
        yield config


@pytest.fixture
def bigquery_migration_config_with_dict(
    bigquery_service: "BigQueryService",
    table_schema_prefix: str,
    request: pytest.FixtureRequest,
) -> Generator[BigQueryConfig, None, None]:
    """Create BigQuery configuration with migration support using dict format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_bigquery_dict_{abs(hash(request.node.nodeid)) % 1000000}"

        config = BigQueryConfig(
            connection_config={
                "project": bigquery_service.project,
                "dataset_id": table_schema_prefix,
                "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
                "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
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


@pytest.fixture
def bigquery_migration_config_mixed(
    bigquery_service: "BigQueryService",
    table_schema_prefix: str,
    request: pytest.FixtureRequest,
) -> Generator[BigQueryConfig, None, None]:
    """Create BigQuery configuration with mixed extension formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migration_dir = Path(temp_dir) / "migrations"
        migration_dir.mkdir(parents=True, exist_ok=True)

        # Create unique version table name using adapter and test node ID
        table_name = f"sqlspec_migrations_bigquery_mixed_{abs(hash(request.node.nodeid)) % 1000000}"

        config = BigQueryConfig(
            connection_config={
                "project": bigquery_service.project,
                "dataset_id": table_schema_prefix,
                "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
                "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
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


@pytest.fixture
def session_store_default(bigquery_migration_config: BigQueryConfig) -> SQLSpecSessionStore:
    """Create a session store with default table name."""
    # Apply migrations to create the session table
    commands = SyncMigrationCommands(bigquery_migration_config)
    commands.init(bigquery_migration_config.migration_config["script_location"], package=False)
    commands.upgrade()

    # Create store using the default migrated table
    return SQLSpecSessionStore(
        bigquery_migration_config,
        table_name="litestar_sessions",  # Default table name
    )


@pytest.fixture
def session_backend_config_default() -> SQLSpecSessionConfig:
    """Create session backend configuration with default table name."""
    return SQLSpecSessionConfig(key="bigquery-session", max_age=3600, table_name="litestar_sessions")


@pytest.fixture
def session_backend_default(session_backend_config_default: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with default configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_default)


@pytest.fixture
def session_store_custom(bigquery_migration_config_with_dict: BigQueryConfig) -> SQLSpecSessionStore:
    """Create a session store with custom table name."""
    # Apply migrations to create the session table with custom name
    commands = SyncMigrationCommands(bigquery_migration_config_with_dict)
    commands.init(bigquery_migration_config_with_dict.migration_config["script_location"], package=False)
    commands.upgrade()

    # Create store using the custom migrated table
    return SQLSpecSessionStore(
        bigquery_migration_config_with_dict,
        table_name="custom_sessions",  # Custom table name from config
    )


@pytest.fixture
def session_backend_config_custom() -> SQLSpecSessionConfig:
    """Create session backend configuration with custom table name."""
    return SQLSpecSessionConfig(key="bigquery-custom", max_age=3600, table_name="custom_sessions")


@pytest.fixture
def session_backend_custom(session_backend_config_custom: SQLSpecSessionConfig) -> SQLSpecSessionBackend:
    """Create session backend with custom configuration."""
    return SQLSpecSessionBackend(config=session_backend_config_custom)
