"""Integration tests for Psycopg default-schema migration behavior.

The shared init/upgrade/downgrade/current/error migration lifecycle is covered by
tests/integration/adapters/contracts/test_migrations_contract.py. This module keeps the
PostgreSQL default_schema / version_table_schema behavior that is not portable across the
contract matrix.
"""

from pathlib import Path

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.psycopg import PsycopgAsyncConfig
from sqlspec.adapters.psycopg.config import PsycopgSyncConfig
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from tests.integration.adapters._postgres_migration_schema import (
    async_table_exists,
    create_schema_sql,
    drop_schema_sql,
    sync_table_exists,
    unique_identifier,
    write_unqualified_table_migration,
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
