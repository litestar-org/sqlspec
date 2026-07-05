"""Integration tests for DuckDB default-schema migration behavior.

The shared init/upgrade/downgrade/current/error migration lifecycle is covered by
tests/integration/adapters/contracts/test_migrations_contract.py. This module keeps the
DuckDB-specific default_schema / version_table_schema behavior that is not portable across
the contract matrix.
"""

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import SyncMigrationCommands

pytestmark = pytest.mark.xdist_group("duckdb")


def _duckdb_identifier(prefix: str) -> str:
    """Return a generated DuckDB identifier."""
    return f"{prefix}_{uuid4().hex[:8]}"


def _write_unqualified_table_migration(migration_dir: Path, table_name: str) -> None:
    migration_content = f'''"""Create an unqualified DuckDB table."""


def up():
    """Create an unqualified table."""
    return ["""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL
        )
    """]


def down():
    """Drop the unqualified table."""
    return ["DROP TABLE IF EXISTS {table_name}"]
'''
    (migration_dir / "0001_create_unqualified_table.py").write_text(migration_content)


def _duckdb_table_exists(driver: Any, schema: str, table_name: str) -> bool:
    result = driver.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", (schema, table_name)
    )
    return bool(result.data)


def test_duckdb_migration_default_schema_applies_to_ddl(tmp_path: Path) -> None:
    """DuckDB migrations run unqualified DDL in the configured default schema."""
    schema = _duckdb_identifier("schema")
    table_name = _duckdb_identifier("table")
    version_table = _duckdb_identifier("versions")
    migration_dir = tmp_path / "migrations"
    db_path = tmp_path / "test.duckdb"

    config = DuckDBConfig(
        connection_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": version_table,
            "default_schema": schema,
        },
    )
    commands = SyncMigrationCommands(config)

    try:
        with config.provide_session() as driver:
            driver.execute(f"CREATE SCHEMA {schema}")

        commands.init(str(migration_dir), package=True)
        _write_unqualified_table_migration(migration_dir, table_name)
        commands.upgrade()

        with config.provide_session() as driver:
            assert _duckdb_table_exists(driver, schema, table_name)
            assert _duckdb_table_exists(driver, schema, version_table)
    finally:
        if config.connection_instance:
            config.close_pool()


def test_duckdb_migration_separable_tracker_and_default_schema(tmp_path: Path) -> None:
    """DuckDB supports separate schemas for migrated DDL and the tracker table."""
    default_schema = _duckdb_identifier("default_schema")
    tracker_schema = _duckdb_identifier("tracker_schema")
    table_name = _duckdb_identifier("table")
    version_table = _duckdb_identifier("versions")
    migration_dir = tmp_path / "migrations"
    db_path = tmp_path / "test.duckdb"

    config = DuckDBConfig(
        connection_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": version_table,
            "default_schema": default_schema,
            "version_table_schema": tracker_schema,
        },
    )
    commands = SyncMigrationCommands(config)

    try:
        with config.provide_session() as driver:
            driver.execute(f"CREATE SCHEMA {default_schema}")
            driver.execute(f"CREATE SCHEMA {tracker_schema}")

        commands.init(str(migration_dir), package=True)
        _write_unqualified_table_migration(migration_dir, table_name)
        commands.upgrade()

        with config.provide_session() as driver:
            assert _duckdb_table_exists(driver, default_schema, table_name)
            assert _duckdb_table_exists(driver, tracker_schema, version_table)
            assert not _duckdb_table_exists(driver, default_schema, version_table)
    finally:
        if config.connection_instance:
            config.close_pool()


def test_duckdb_migration_tracker_lives_in_configured_schema(tmp_path: Path) -> None:
    """DuckDB stores the tracker table in version_table_schema when configured."""
    tracker_schema = _duckdb_identifier("tracker_schema")
    table_name = _duckdb_identifier("table")
    version_table = _duckdb_identifier("versions")
    migration_dir = tmp_path / "migrations"
    db_path = tmp_path / "test.duckdb"

    config = DuckDBConfig(
        connection_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": version_table,
            "version_table_schema": tracker_schema,
        },
    )
    commands = SyncMigrationCommands(config)

    try:
        with config.provide_session() as driver:
            driver.execute(f"CREATE SCHEMA {tracker_schema}")

        commands.init(str(migration_dir), package=True)
        _write_unqualified_table_migration(migration_dir, table_name)
        commands.upgrade()

        with config.provide_session() as driver:
            assert _duckdb_table_exists(driver, "main", table_name)
            assert _duckdb_table_exists(driver, tracker_schema, version_table)
            assert not _duckdb_table_exists(driver, "main", version_table)
    finally:
        if config.connection_instance:
            config.close_pool()


def test_duckdb_migration_missing_schema_fails_fast(tmp_path: Path) -> None:
    """DuckDB validates the default schema before creating tracker tables or applying DDL."""
    schema = _duckdb_identifier("missing_schema")
    table_name = _duckdb_identifier("missing_table")
    version_table = _duckdb_identifier("missing_versions")
    migration_dir = tmp_path / "migrations"
    db_path = tmp_path / "test.duckdb"

    config = DuckDBConfig(
        connection_config={"database": str(db_path)},
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": version_table,
            "default_schema": schema,
        },
    )
    commands = SyncMigrationCommands(config)

    try:
        commands.init(str(migration_dir), package=True)
        _write_unqualified_table_migration(migration_dir, table_name)

        with pytest.raises(MigrationError, match=f"Configured schema '{schema}' does not exist"):
            commands.upgrade()

        with config.provide_session() as driver:
            assert not _duckdb_table_exists(driver, "main", version_table)
            assert not _duckdb_table_exists(driver, "main", table_name)
    finally:
        if config.connection_instance:
            config.close_pool()
