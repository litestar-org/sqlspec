"""Unit tests for the pymssql migration tracker."""

from typing import Any, cast

from sqlspec.adapters.pymssql.config import PymssqlConfig
from sqlspec.adapters.pymssql.core import split_schema_and_table
from sqlspec.adapters.pymssql.migrations import PymssqlSyncMigrationTracker


class FakeSyncMigrationDriver:
    """Minimal sync driver for migration tracker tests."""

    def __init__(self, driver_features: "dict[str, Any] | None" = None) -> None:
        self.scripts: list[str] = []
        self.commits = 0
        if driver_features is not None:
            self.driver_features = driver_features

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
    assert "CREATE TABLE [__migrations_test]" in ddl
    assert "NVARCHAR(32)" in ddl
    assert "DATETIME2(6)" in ddl
    assert "TIMESTAMP" not in ddl
    assert driver.commits >= 1


def test_pymssql_configs_use_tsql_migration_trackers() -> None:
    """The sync config should use the MSSQL migration tracker type."""
    assert PymssqlConfig.migration_tracker_type is PymssqlSyncMigrationTracker


def test_split_schema_table_preserves_bracket_quoted_dots() -> None:
    assert split_schema_and_table("[dbo.schema].[migration.table]") == ("dbo.schema", "migration.table")


def test_add_column_statement_text_quotes_identifiers_and_guards_not_null() -> None:
    """_add_column_statement_text should bracket-quote identifiers and only emit NOT NULL when a default exists."""
    tracker = PymssqlSyncMigrationTracker(version_table_name="custom.migrations")
    ddl_no_default = tracker._add_column_statement_text("execution_sequence")
    ddl_with_default = tracker._add_column_statement_text("applied_at")

    assert ddl_no_default == "ALTER TABLE [custom].[migrations] ADD [execution_sequence] INT NULL;"
    assert (
        ddl_with_default
        == "ALTER TABLE [custom].[migrations] ADD [applied_at] DATETIME2(6) DEFAULT SYSUTCDATETIME() NOT NULL;"
    )


def test_commit_if_needed_respects_autocommit_driver_features() -> None:
    """_commit_if_needed should skip manual commit when autocommit is enabled in driver_features."""
    driver = FakeSyncMigrationDriver(driver_features={"autocommit": True})
    tracker = PymssqlSyncMigrationTracker(version_table_name="__migrations_test")

    tracker.ensure_tracking_table(cast("Any", driver))

    assert driver.commits == 0
