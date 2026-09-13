"""Upgrade coverage for installations that applied ADK 0001 before memory support."""

import importlib

import pytest

from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.migrations.context import MigrationContext

migration = importlib.import_module("sqlspec.extensions.adk.migrations.0002_create_mssql_python_memory")


@pytest.mark.anyio
async def test_existing_mssql_installation_receives_memory_table_and_indexes() -> None:
    config = MssqlPythonConfig(extension_config={"adk": {"memory_table": "custom_memory"}})
    statements = await migration.up(MigrationContext(config=config))

    assert len(statements) == 5
    assert "CREATE TABLE [dbo].[custom_memory]" in statements[0]
    assert all("IF NOT EXISTS" in statement for statement in statements)
    assert all("OBJECT_ID(N'[dbo].[custom_memory]')" in statement for statement in statements[1:])
    assert await migration.down(MigrationContext(config=config)) == []


@pytest.mark.anyio
async def test_disabled_mssql_memory_migration_does_not_create_tables() -> None:
    config = MssqlPythonConfig(extension_config={"adk": {"enable_memory": False}})
    assert await migration.up(MigrationContext(config=config)) == []


@pytest.mark.anyio
async def test_other_adapters_are_unchanged_by_mssql_memory_upgrade() -> None:
    config = SqliteConfig(connection_config={"database": ":memory:"}, extension_config={"adk": {}})
    assert await migration.up(MigrationContext(config=config)) == []


@pytest.mark.anyio
async def test_memory_upgrade_requires_database_config() -> None:
    from sqlspec.exceptions import SQLSpecError

    with pytest.raises(SQLSpecError, match="context must have a config"):
        await migration.up()
