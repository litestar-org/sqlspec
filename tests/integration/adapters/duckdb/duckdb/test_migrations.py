"""Integration tests for DuckDB migration session schema behaviour."""

from pathlib import Path

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig
from sqlspec.exceptions import UniqueViolationError
from sqlspec.migrations.commands import SyncMigrationCommands

_TABLES_SQL = """
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
"""


def _config(tmp_path: Path, **migration_options: str) -> DuckDBConfig:
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    return DuckDBConfig(
        connection_config={"database": str(tmp_path / "app.duckdb")},
        migration_config={"script_location": str(migration_dir), **migration_options},
    )


def _write_migration(tmp_path: Path, version: str, up_sql: str, *, directive: str | None = None) -> Path:
    migration_file = tmp_path / "migrations" / f"{version}_step.sql"
    header = f"-- schema: {directive}\n" if directive else ""
    migration_file.write_text(
        f"{header}-- name: migrate-{version}-up\n{up_sql}\n\n-- name: migrate-{version}-down\nSELECT 1;\n"
    )
    return migration_file


def _tables(config: DuckDBConfig) -> set[tuple[str, str]]:
    with config.provide_session() as driver:
        return {(row["table_schema"], row["table_name"]) for row in driver.select(_TABLES_SQL)}


def test_duckdb_directive_does_not_leak_into_later_migrations(tmp_path: Path) -> None:
    """A directive stays on its own migration, so later work and the tracker stay in the original schema."""
    config = _config(tmp_path)
    _write_migration(tmp_path, "0001", "CREATE TABLE items (id INTEGER);", directive="tenant")
    _write_migration(tmp_path, "0002", "CREATE TABLE plain (id INTEGER);")
    try:
        with config.provide_session() as driver:
            driver.execute_script("CREATE SCHEMA tenant")

        commands = SyncMigrationCommands(config)
        commands.upgrade()
        current = commands.current()
        commands.upgrade()

        assert current == "0002"
        assert _tables(config) == {("tenant", "items"), ("main", "plain"), ("main", "ddl_migrations")}
        with config.provide_session() as driver:
            assert driver.select_value("SELECT current_schema()") == "main"
    finally:
        config.close_pool()


def test_duckdb_default_schema_restores_next_session(tmp_path: Path) -> None:
    """A configured default schema applies to the migration and is gone in the next session."""
    config = _config(tmp_path, default_schema="tenant")
    _write_migration(tmp_path, "0001", "CREATE TABLE items (id INTEGER);")
    try:
        with config.provide_session() as driver:
            driver.execute_script("CREATE SCHEMA tenant")

        SyncMigrationCommands(config).upgrade()

        assert ("tenant", "items") in _tables(config)
        with config.provide_session() as driver:
            assert driver.select_value("SELECT current_schema()") == "main"
            assert driver.select_value("SELECT current_setting('search_path')") == ""
    finally:
        config.close_pool()


@pytest.mark.parametrize(("search_path", "expected"), [(None, ""), ("t,u", "t,u")], ids=["empty", "list"])
def test_duckdb_reset_restores_real_search_path(tmp_path: Path, search_path: str | None, expected: str) -> None:
    """Reset returns a real connection to the exact search path it held before the switch."""
    config = _config(tmp_path)
    try:
        with config.provide_session() as driver:
            driver.execute_script("CREATE SCHEMA tenant; CREATE SCHEMA t; CREATE SCHEMA u")
            if search_path is not None:
                driver.execute_script(f"SET search_path = '{search_path}'")

            driver.set_migration_session_schema("tenant")
            assert driver.select_value("SELECT current_schema()") == "tenant"
            driver.reset_migration_session_schema()

            assert driver.select_value("SELECT current_setting('search_path')") == expected
    finally:
        config.close_pool()


def test_duckdb_failed_migration_surfaces_constraint_error(tmp_path: Path) -> None:
    """A duplicate-key migration raises its own error, rolls back, and restores the schema."""
    config = _config(tmp_path, default_schema="tenant")
    migration_file = _write_migration(
        tmp_path,
        "0001",
        "CREATE TABLE main.dup (id INTEGER PRIMARY KEY);\nINSERT INTO main.dup VALUES (1);\nINSERT INTO main.dup VALUES (1);",
    )
    try:
        with config.provide_session() as driver:
            driver.execute_script("CREATE SCHEMA tenant")

        commands = SyncMigrationCommands(config)
        migration = commands.runner.load_migration(migration_file, "0001")
        with config.provide_session() as driver:
            with pytest.raises(UniqueViolationError):
                commands.runner.execute_upgrade(driver, migration, use_transaction=True)
            assert driver.select_value("SELECT current_schema()") == "main"

        assert ("main", "dup") not in _tables(config)
    finally:
        config.close_pool()
