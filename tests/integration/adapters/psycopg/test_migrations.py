"""Integration tests for Psycopg default-schema migration behavior.

The shared init/upgrade/downgrade/current/error migration lifecycle is covered by
tests/integration/adapters/contracts/test_migrations_contract.py. This module keeps the
PostgreSQL default_schema / version_table_schema behavior that is not portable across the
contract matrix.
"""

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig
from sqlspec.adapters.psycopg.config import PsycopgSyncConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.utils.text import quote_identifier


def unique_identifier(prefix: str) -> str:
    """Return a short PostgreSQL-safe identifier for integration tests."""
    return f"{prefix}_{uuid4().hex[:10]}"


def create_schema_sql(schema: str) -> str:
    """Return PostgreSQL CREATE SCHEMA SQL for a trusted test identifier."""
    return f"CREATE SCHEMA {quote_identifier(schema)}"


def drop_schema_sql(schema: str) -> str:
    """Return PostgreSQL DROP SCHEMA SQL for a trusted test identifier."""
    return f"DROP SCHEMA IF EXISTS {quote_identifier(schema)} CASCADE"


def _table_exists_sql(style: str) -> str:
    placeholders = "%s AND table_name = %s" if style == "pyformat" else "$1 AND table_name = $2"
    return f"SELECT 1 FROM information_schema.tables WHERE table_schema = {placeholders}"


def sync_table_exists(driver: Any, schema: str, table_name: str, *, style: str = "pyformat") -> bool:
    """Return whether the table exists using a sync SQLSpec driver."""
    result = driver.execute(_table_exists_sql(style), (schema, table_name))
    return bool(result.data)


async def async_table_exists(driver: Any, schema: str, table_name: str, *, style: str = "pyformat") -> bool:
    """Return whether the table exists using an async SQLSpec driver."""
    result = await driver.execute(_table_exists_sql(style), (schema, table_name))
    return bool(result.data)


def write_unqualified_table_migration(migration_dir: Path, table_name: str) -> None:
    """Write a Python migration that creates an unqualified table."""
    (migration_dir / "0001_create_unqualified_table.py").write_text(
        f'''"""Create an unqualified table."""


def up():
    """Create an unqualified table."""
    return ["""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )
    """]


def down():
    """Drop the unqualified table."""
    return ["DROP TABLE IF EXISTS {table_name}"]
'''
    )


pytestmark = pytest.mark.xdist_group("postgres")


def test_psycopg_sync_migration_default_schema_applies_to_ddl(
    tmp_path: Path, postgres_service: "PostgresService"
) -> None:
    """Psycopg sync migrations run unqualified DDL in the configured default schema."""
    schema = unique_identifier("psycopg_sync_default")
    table_name = unique_identifier("psycopg_sync_table")
    version_table = unique_identifier("psycopg_sync_versions")
    migration_dir = tmp_path / "migrations"

    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": version_table,
            "default_schema": schema,
        },
    )
    commands = SyncMigrationCommands(config)

    try:
        with config.provide_session() as driver:
            driver.execute_script(create_schema_sql(schema))
            driver.commit()

        commands.init(str(migration_dir), package=True)
        write_unqualified_table_migration(migration_dir, table_name)
        commands.upgrade()

        with config.provide_session() as driver:
            assert sync_table_exists(driver, schema, table_name, style="pyformat")
            assert not sync_table_exists(driver, "public", table_name, style="pyformat")
            assert sync_table_exists(driver, schema, version_table, style="pyformat")
    finally:
        with config.provide_session() as driver:
            driver.execute_script(drop_schema_sql(schema))
            driver.commit()
        if config.connection_instance:
            config.close_pool()


async def test_psycopg_async_migration_default_schema_applies_to_ddl(
    tmp_path: Path, postgres_service: "PostgresService"
) -> None:
    """Psycopg async migrations run unqualified DDL in the configured default schema."""
    schema = unique_identifier("psycopg_async_default")
    table_name = unique_identifier("psycopg_async_table")
    version_table = unique_identifier("psycopg_async_versions")
    migration_dir = tmp_path / "migrations"

    config = PsycopgAsyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": version_table,
            "default_schema": schema,
        },
    )
    commands = AsyncMigrationCommands(config)

    try:
        async with config.provide_session() as driver:
            await driver.execute_script(create_schema_sql(schema))

        await commands.init(str(migration_dir), package=True)
        write_unqualified_table_migration(migration_dir, table_name)
        await commands.upgrade()

        async with config.provide_session() as driver:
            assert await async_table_exists(driver, schema, table_name, style="pyformat")
            assert not await async_table_exists(driver, "public", table_name, style="pyformat")
            assert await async_table_exists(driver, schema, version_table, style="pyformat")
    finally:
        async with config.provide_session() as driver:
            await driver.execute_script(drop_schema_sql(schema))
        if config.connection_instance:
            await config.close_pool()


def test_psycopg_migration_separable_tracker_and_default_schema(
    tmp_path: Path, postgres_service: "PostgresService"
) -> None:
    """Psycopg supports separate schemas for migrated DDL and the tracker table."""
    default_schema = unique_identifier("psycopg_default")
    tracker_schema = unique_identifier("psycopg_tracker")
    table_name = unique_identifier("psycopg_table")
    version_table = unique_identifier("psycopg_versions")
    migration_dir = tmp_path / "migrations"

    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
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
            driver.execute_script(create_schema_sql(default_schema))
            driver.execute_script(create_schema_sql(tracker_schema))
            driver.commit()

        commands.init(str(migration_dir), package=True)
        write_unqualified_table_migration(migration_dir, table_name)
        commands.upgrade()

        with config.provide_session() as driver:
            assert sync_table_exists(driver, default_schema, table_name, style="pyformat")
            assert sync_table_exists(driver, tracker_schema, version_table, style="pyformat")
            assert not sync_table_exists(driver, default_schema, version_table, style="pyformat")
    finally:
        with config.provide_session() as driver:
            driver.execute_script(drop_schema_sql(default_schema))
            driver.execute_script(drop_schema_sql(tracker_schema))
            driver.commit()
        if config.connection_instance:
            config.close_pool()


def test_psycopg_migration_missing_schema_fails_fast(tmp_path: Path, postgres_service: "PostgresService") -> None:
    """Psycopg validates the default schema before creating tracker tables or applying DDL."""
    schema = unique_identifier("psycopg_missing")
    table_name = unique_identifier("psycopg_table")
    version_table = unique_identifier("psycopg_versions")
    migration_dir = tmp_path / "migrations"

    config = PsycopgSyncConfig(
        connection_config={
            "conninfo": f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
        },
        migration_config={
            "script_location": str(migration_dir),
            "version_table_name": version_table,
            "default_schema": schema,
        },
    )
    commands = SyncMigrationCommands(config)

    try:
        commands.init(str(migration_dir), package=True)
        write_unqualified_table_migration(migration_dir, table_name)

        with pytest.raises(MigrationError, match=f"Configured schema '{schema}' does not exist"):
            commands.upgrade()

        with config.provide_session() as driver:
            assert not sync_table_exists(driver, "public", version_table, style="pyformat")
            assert not sync_table_exists(driver, "public", table_name, style="pyformat")
    finally:
        if config.connection_instance:
            config.close_pool()
