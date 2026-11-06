"""Caller-owned pymssql transaction boundaries around statement stacks."""

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from sqlspec import StatementStack
from sqlspec.adapters.pymssql import PymssqlConfig
from sqlspec.exceptions import SQLParsingError, StackExecutionError
from sqlspec.migrations.commands import SyncMigrationCommands

pytestmark = [pytest.mark.pymssql, pytest.mark.xdist_group("mssql")]


@pytest.mark.parametrize("fails", [False, True])
def test_execute_stack_preserves_caller_transaction(pymssql_connection_config: dict[str, Any], fails: bool) -> None:
    config = PymssqlConfig(connection_config=pymssql_connection_config)
    try:
        with config.provide_session() as driver:
            driver.execute_script("DROP TABLE IF EXISTS sqlspec_pymssql_stack_txn")
            driver.execute_script("CREATE TABLE sqlspec_pymssql_stack_txn (id INT PRIMARY KEY)")
            driver.commit()
            try:
                driver.begin()
                driver.execute("INSERT INTO sqlspec_pymssql_stack_txn (id) VALUES (?)", (1,))
                transaction_count = driver.select_value("SELECT @@TRANCOUNT")
                stack = StatementStack().push_execute("INSERT INTO sqlspec_pymssql_stack_txn (id) VALUES (?)", (2,))
                if fails:
                    stack = stack.push_execute("INSERT INTO sqlspec_pymssql_stack_txn (id) VALUES (?)", (2,))
                    with pytest.raises(StackExecutionError):
                        driver.execute_stack(stack)
                else:
                    results = driver.execute_stack(stack)
                    assert len(results) == 1
                    assert results[0].rows_affected == 1
                assert driver.select_value("SELECT @@TRANCOUNT") == transaction_count
                assert driver.select_value("SELECT COUNT(*) FROM sqlspec_pymssql_stack_txn") == 2
                driver.rollback()
                assert driver.select_value("SELECT COUNT(*) FROM sqlspec_pymssql_stack_txn") == 0
            finally:
                driver.rollback()
                driver.execute_script("DROP TABLE IF EXISTS sqlspec_pymssql_stack_txn")
                driver.commit()
    finally:
        config.close_pool()


@pytest.mark.parametrize("autocommit", [None, True], ids=["default", "autocommit"])
def test_pymssql_failed_migration_leaves_no_partial_work(
    tmp_path: Path, mssql_migration_connection_config: dict[str, Any], autocommit: bool | None
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
    connection_config = dict(mssql_migration_connection_config)
    if autocommit is not None:
        connection_config["autocommit"] = autocommit
    config = PymssqlConfig(
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
        with config.provide_session() as driver, pytest.raises(SQLParsingError, match="Invalid object name"):
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
