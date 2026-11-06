"""Integration tests for mssql-python transaction persistence."""

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pytest_databases.docker.mssql import MSSQLService

from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.exceptions import SQLSpecError
from sqlspec.migrations.commands import SyncMigrationCommands
from tests.integration.fixtures.mssql import _mssql_python_connection_config

pytestmark = [pytest.mark.mssql_python, pytest.mark.xdist_group("mssql")]

_DROP_TABLE_SQL = "DROP TABLE IF EXISTS sqlspec_mssql_python_transactions"
_CREATE_TABLE_SQL = "CREATE TABLE sqlspec_mssql_python_transactions (id INT PRIMARY KEY, value VARCHAR(50) NOT NULL)"
_INSERT_SQL = "INSERT INTO sqlspec_mssql_python_transactions (id, value) VALUES (?, ?)"
_COUNT_SQL = "SELECT COUNT(*) FROM sqlspec_mssql_python_transactions WHERE id = ?"


def test_mssql_python_transactions_persist_and_restore_autocommit(mssql_service: MSSQLService) -> None:
    """Commit and rollback should use DBAPI-owned transactions visible across connections."""
    manual_config = MssqlPythonConfig(
        connection_config=_mssql_python_connection_config(mssql_service, autocommit=False)
    )
    autocommit_config = MssqlPythonConfig(connection_config=_mssql_python_connection_config(mssql_service))
    try:
        with manual_config.provide_session() as setup:
            setup.execute_script(_DROP_TABLE_SQL)
            setup.execute_script(_CREATE_TABLE_SQL)
            setup.commit()

        with manual_config.provide_session() as writer, manual_config.provide_session() as observer:
            writer.begin()
            writer.execute(_INSERT_SQL, (1, "committed"))
            writer.commit()

            assert observer.select_value(_COUNT_SQL, (1,)) == 1
            observer.commit()

            writer.begin()
            writer.execute(_INSERT_SQL, (2, "rolled-back"))
            writer.rollback()

            assert observer.select_value(_COUNT_SQL, (2,)) == 0
            observer.commit()

        with autocommit_config.provide_session() as writer, manual_config.provide_session() as observer:
            assert writer.connection.autocommit is True
            writer.begin()
            assert writer.connection.autocommit is False
            writer.execute(_INSERT_SQL, (3, "autocommit-restored"))
            writer.commit()
            assert writer.connection.autocommit is True

            assert observer.select_value(_COUNT_SQL, (3,)) == 1
            observer.commit()
    finally:
        try:
            with manual_config.provide_session() as cleanup:
                cleanup.execute_script(_DROP_TABLE_SQL)
                cleanup.commit()
        finally:
            manual_config.close_pool()
            autocommit_config.close_pool()


@pytest.mark.parametrize("autocommit", [False, True], ids=["autocommit_off", "autocommit_on"])
def test_mssql_python_failed_migration_leaves_no_partial_work(
    tmp_path: Path, mssql_python_migration_connection_config: dict[str, Any], autocommit: bool
) -> None:
    """A failed transactional migration persists no work and restores the user's default schema."""
    suffix = uuid4().hex[:8]
    schema = f"mig_{suffix}"
    table = f"partial_{suffix}"
    migration_file = tmp_path / "0001_fails.py"
    migration_file.write_text(
        f'''"""Fail after creating and populating a table."""


def up():
    """Create a table, then reference a missing one."""
    return ["CREATE TABLE dbo.{table} (id INT)", "INSERT INTO dbo.{table} VALUES (1)", "SELECT * FROM dbo.missing_{table}"]


def down():
    """Drop the table."""
    return ["DROP TABLE dbo.{table}"]
'''
    )
    connection_config = dict(mssql_python_migration_connection_config)
    connection_config["autocommit"] = autocommit
    config = MssqlPythonConfig(
        connection_config=connection_config,
        migration_config={
            "script_location": str(tmp_path),
            "default_schema": schema,
            "version_table_name": f"v{table}",
        },
    )
    try:
        with config.provide_session() as driver:
            driver.execute_script(f"CREATE SCHEMA {schema}")
            driver.commit()

        commands = SyncMigrationCommands(config)
        migration = commands.runner.load_migration(migration_file, "0001")
        with config.provide_session() as driver, pytest.raises(SQLSpecError, match="Base table or view not found"):
            commands.runner.execute_upgrade(driver, migration, use_transaction=True)

        with config.provide_session() as driver:
            assert driver.select_value(f"SELECT COUNT(*) FROM sys.tables WHERE name = '{table}'") == 0
            assert driver.select_value("SELECT SCHEMA_NAME()") == "dbo"
    finally:
        with config.provide_session() as driver:
            driver.execute_script(f"DROP TABLE IF EXISTS dbo.{table}")
            driver.execute_script(f"DROP SCHEMA IF EXISTS {schema}")
            driver.commit()
        config.close_pool()
