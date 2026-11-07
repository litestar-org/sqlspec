"""Shared body for failed-migration acceptance across the SQL Server drivers."""

from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from sqlspec.migrations.commands import SyncMigrationCommands


def assert_failed_migration_leaves_no_partial_work(
    tmp_path: Path,
    config_type: Any,
    connection_config: "dict[str, Any]",
    expected_error: "type[Exception]",
    error_match: str,
) -> None:
    """Run a migration that fails after creating a table, then check that nothing persisted.

    Args:
        tmp_path: Directory for the migration file.
        config_type: SQL Server configuration class under test.
        connection_config: Connection parameters for the migration login.
        expected_error: Exception type the driver maps the failure to.
        error_match: Pattern the exception message must match.
    """
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
    config = config_type(
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
        with config.provide_session() as driver, pytest.raises(expected_error, match=error_match):
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
