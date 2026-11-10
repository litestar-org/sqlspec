"""Upgrade coverage for installations that applied ADK 0001 before memory support."""

import importlib
import re
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.arrow_odbc import ArrowOdbcConfig
from sqlspec.adapters.arrow_odbc.adk import ArrowOdbcADKMemoryStore
from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.mssql_python.adk import MssqlPythonADKMemoryStore
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import StatementConfig
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


@pytest.mark.anyio
async def test_external_config_with_mssql_adapter_name_runs_memory_upgrade() -> None:
    class ExternalMssqlConfig:
        adapter_name = "mssql_python"
        is_async = False
        extension_config = {"adk": {"memory_table": "ext_memory"}}

    ExternalMssqlConfig.__module__ = "myapp.database.config"
    statements = await migration.up(MigrationContext(config=ExternalMssqlConfig()))
    assert len(statements) == 5
    assert "CREATE TABLE [dbo].[ext_memory]" in statements[0]


def _extract_index_names(statements: list[str]) -> set[str]:
    names: set[str] = set()
    for statement in statements:
        match = re.search(r"CREATE INDEX \[([^\]]+)\]", statement)
        if match is not None:
            names.add(match.group(1))
    return names


@pytest.mark.anyio
async def test_mssql_reconcile_memory_indexes_match_numbered_upgrade() -> None:
    """Native SQL Server and Arrow ODBC memory companion indexes match 0002 numbered upgrade output."""
    mssql_cfg = MssqlPythonConfig(extension_config={"adk": {"memory_table": "custom_memory"}})
    arrow_cfg = ArrowOdbcConfig(
        connection_config={"connection_string": "Driver=x"},
        statement_config=StatementConfig(dialect="mssql"),
        extension_config={"adk": {"memory_table": "custom_memory"}},
    )

    numbered_statements = await migration.up(MigrationContext(config=mssql_cfg))
    numbered_names = _extract_index_names(numbered_statements[1:])

    mssql_driver = MagicMock()
    mssql_driver.data_dictionary.get_indexes.return_value = []
    MssqlPythonADKMemoryStore(mssql_cfg).reconcile_companion_objects_sync(mssql_driver)
    mssql_names = _extract_index_names([str(call.args[0]) for call in mssql_driver.execute.call_args_list])

    arrow_driver = MagicMock()
    arrow_driver.data_dictionary.get_indexes.return_value = []
    ArrowOdbcADKMemoryStore(arrow_cfg).reconcile_companion_objects_sync(arrow_driver)
    arrow_names = _extract_index_names([str(call.args[0]) for call in arrow_driver.execute.call_args_list])

    expected = {
        "idx_custom_memory_app_scope_user_time",
        "idx_custom_memory_scope",
        "idx_custom_memory_session",
        "idx_custom_memory_timestamp",
    }
    assert numbered_names == expected
    assert mssql_names == numbered_names
    assert arrow_names == numbered_names
