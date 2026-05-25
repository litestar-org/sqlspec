"""Integration tests for BigQuery migration schema workflow."""

import os
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud.bigquery import Dataset
from google.cloud.exceptions import NotFound

from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import SyncMigrationCommands

if TYPE_CHECKING:
    from pytest_databases.docker.bigquery import BigQueryService

BIGQUERY_ENABLED = os.environ.get("CI") == "true" or os.environ.get("SQLSPEC_ENABLE_BIGQUERY_TESTS") == "1"

pytestmark = [
    pytest.mark.xdist_group("bigquery"),
    pytest.mark.skipif(
        not BIGQUERY_ENABLED,
        reason="BigQuery emulator is optional locally; set SQLSPEC_ENABLE_BIGQUERY_TESTS=1 to enable",
    ),
]


def _bigquery_identifier(prefix: str) -> str:
    """Return a generated BigQuery identifier."""
    return f"{prefix}_{uuid4().hex[:8]}"


def _write_bigquery_unqualified_table_migration(migration_dir: Path, table_name: str) -> None:
    migration_content = f'''"""Create an unqualified BigQuery table."""


def up():
    """Create an unqualified table."""
    return ["""
        CREATE TABLE {table_name} (
            id INT64,
            name STRING NOT NULL
        )
    """]


def down():
    """Drop the unqualified table."""
    return ["DROP TABLE IF EXISTS {table_name}"]
'''
    (migration_dir / "0001_create_unqualified_table.py").write_text(migration_content)


def _bigquery_config(bigquery_service: "BigQueryService", *, migration_config: dict[str, object]) -> BigQueryConfig:
    return BigQueryConfig(
        connection_config={
            "project": bigquery_service.project,
            "dataset_id": bigquery_service.dataset,
            "client_options": ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
            "credentials": AnonymousCredentials(),  # type: ignore[no-untyped-call]
        },
        migration_config=migration_config,
    )


def _create_dataset(config: BigQueryConfig, dataset_name: str) -> None:
    with config.provide_session() as driver:
        project = str(driver.connection.project)
        driver.connection.create_dataset(Dataset(f"{project}.{dataset_name}"), exists_ok=True)


def _drop_dataset(config: BigQueryConfig, dataset_name: str) -> None:
    if config.connection_instance is None:
        return
    with config.provide_session() as driver:
        project = str(driver.connection.project)
        driver.connection.delete_dataset(f"{project}.{dataset_name}", delete_contents=True, not_found_ok=True)


def _bigquery_table_exists(config: BigQueryConfig, dataset_name: str, table_name: str) -> bool:
    with config.provide_session() as driver:
        project = str(driver.connection.project)
        try:
            driver.connection.get_table(f"{project}.{dataset_name}.{table_name}")
        except NotFound:
            return False
        return True


def test_bigquery_migration_default_schema_applies_to_ddl(tmp_path: Path, bigquery_service: "BigQueryService") -> None:
    """BigQuery migrations run unqualified DDL in the configured default dataset."""
    dataset_name = _bigquery_identifier("dataset")
    table_name = _bigquery_identifier("table")
    version_table = _bigquery_identifier("versions")
    migration_dir = tmp_path / "migrations"

    config = _bigquery_config(
        bigquery_service,
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": version_table,
            "default_schema": dataset_name,
        },
    )

    try:
        _create_dataset(config, dataset_name)

        commands = SyncMigrationCommands(config)
        commands.init(str(migration_dir), package=True)
        _write_bigquery_unqualified_table_migration(migration_dir, table_name)
        commands.upgrade()

        assert _bigquery_table_exists(config, dataset_name, table_name)
        assert _bigquery_table_exists(config, dataset_name, version_table)
    finally:
        _drop_dataset(config, dataset_name)


def test_bigquery_migration_tracker_lives_in_configured_schema(
    tmp_path: Path, bigquery_service: "BigQueryService"
) -> None:
    """BigQuery stores the tracker table in version_table_schema when configured."""
    tracker_dataset = _bigquery_identifier("tracker")
    table_name = _bigquery_identifier("table")
    version_table = _bigquery_identifier("versions")
    migration_dir = tmp_path / "migrations"

    config = _bigquery_config(
        bigquery_service,
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": version_table,
            "version_table_schema": tracker_dataset,
        },
    )

    try:
        _create_dataset(config, tracker_dataset)

        commands = SyncMigrationCommands(config)
        commands.init(str(migration_dir), package=True)
        _write_bigquery_unqualified_table_migration(migration_dir, table_name)
        commands.upgrade()

        assert _bigquery_table_exists(config, bigquery_service.dataset, table_name)
        assert _bigquery_table_exists(config, tracker_dataset, version_table)
        assert not _bigquery_table_exists(config, bigquery_service.dataset, version_table)
    finally:
        _drop_dataset(config, tracker_dataset)


def test_bigquery_migration_separable_tracker_and_default_schema(
    tmp_path: Path, bigquery_service: "BigQueryService"
) -> None:
    """BigQuery supports separate datasets for migrated DDL and the tracker table."""
    default_dataset = _bigquery_identifier("default")
    tracker_dataset = _bigquery_identifier("tracker")
    table_name = _bigquery_identifier("table")
    version_table = _bigquery_identifier("versions")
    migration_dir = tmp_path / "migrations"

    config = _bigquery_config(
        bigquery_service,
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": version_table,
            "default_schema": default_dataset,
            "version_table_schema": tracker_dataset,
        },
    )

    try:
        _create_dataset(config, default_dataset)
        _create_dataset(config, tracker_dataset)

        commands = SyncMigrationCommands(config)
        commands.init(str(migration_dir), package=True)
        _write_bigquery_unqualified_table_migration(migration_dir, table_name)
        commands.upgrade()

        assert _bigquery_table_exists(config, default_dataset, table_name)
        assert _bigquery_table_exists(config, tracker_dataset, version_table)
        assert not _bigquery_table_exists(config, default_dataset, version_table)
    finally:
        _drop_dataset(config, default_dataset)
        _drop_dataset(config, tracker_dataset)


def test_bigquery_migration_missing_schema_fails_fast(tmp_path: Path, bigquery_service: "BigQueryService") -> None:
    """BigQuery validates the default dataset before creating tracker tables or applying DDL."""
    missing_dataset = _bigquery_identifier("missing")
    table_name = _bigquery_identifier("table")
    version_table = _bigquery_identifier("versions")
    migration_dir = tmp_path / "migrations"

    config = _bigquery_config(
        bigquery_service,
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": version_table,
            "default_schema": missing_dataset,
        },
    )
    commands = SyncMigrationCommands(config)
    commands.init(str(migration_dir), package=True)
    _write_bigquery_unqualified_table_migration(migration_dir, table_name)

    with pytest.raises(MigrationError, match=f"Configured schema '{missing_dataset}' does not exist"):
        commands.upgrade()

    assert not _bigquery_table_exists(config, bigquery_service.dataset, table_name)
    assert not _bigquery_table_exists(config, bigquery_service.dataset, version_table)
