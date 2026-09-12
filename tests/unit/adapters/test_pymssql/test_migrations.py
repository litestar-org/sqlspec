"""Unit tests for the pymssql migration tracker."""

from typing import Any, cast

from sqlspec.adapters.pymssql.config import PymssqlConfig
from sqlspec.adapters.pymssql.migrations import PymssqlSyncMigrationTracker, _split_schema_table


class FakeSyncMigrationDriver:
    """Minimal sync driver for migration tracker tests."""

    def __init__(self) -> None:
        self.scripts: list[str] = []
        self.commits = 0

    def execute_script(self, statement: str) -> None:
        self.scripts.append(statement)

    def select(self, _statement: Any, **_kwargs: Any) -> list[dict[str, str]]:
        return [{"column_name": "version_num"}, {"column_name": "version_type"}]

    def commit(self) -> None:
        self.commits += 1


def test_sync_tracker_uses_tsql_idempotent_create_table() -> None:
    """The sync tracker should create the version table with T-SQL-safe DDL."""
    driver = FakeSyncMigrationDriver()
    tracker = PymssqlSyncMigrationTracker(version_table_name="__migrations_test")

    tracker.ensure_tracking_table(cast("Any", driver))

    ddl = "\n".join(driver.scripts)
    assert "IF NOT EXISTS (SELECT 1 FROM sys.tables" in ddl
    assert "SCHEMA_ID('dbo')" in ddl
    assert "NVARCHAR(32)" in ddl
    assert "DATETIME2(6)" in ddl
    assert "TIMESTAMP" not in ddl
    assert driver.commits >= 1


def test_pymssql_configs_use_tsql_migration_trackers() -> None:
    """The sync config should use the MSSQL migration tracker type."""
    assert PymssqlConfig.migration_tracker_type is PymssqlSyncMigrationTracker


def test_split_schema_table_preserves_bracket_quoted_dots() -> None:
    assert _split_schema_table("[dbo.schema].[migration.table]") == ("dbo.schema", "migration.table")
