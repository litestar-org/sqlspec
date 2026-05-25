"""Unit tests for the mssql_python migration tracker."""

from typing import Any, cast

import pytest

from sqlspec.adapters.mssql_python.config import MssqlPythonAsyncConfig, MssqlPythonConfig
from sqlspec.adapters.mssql_python.migrations import MssqlPythonAsyncMigrationTracker, MssqlPythonSyncMigrationTracker


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


class FakeAsyncMigrationDriver:
    """Minimal async driver for migration tracker tests."""

    def __init__(self) -> None:
        self.scripts: list[str] = []
        self.commits = 0

    async def execute_script(self, statement: str) -> None:
        self.scripts.append(statement)

    async def select(self, _statement: Any, **_kwargs: Any) -> list[dict[str, str]]:
        return [{"column_name": "version_num"}, {"column_name": "version_type"}]

    async def commit(self) -> None:
        self.commits += 1


def test_sync_tracker_uses_tsql_idempotent_create_table() -> None:
    """The sync tracker should create the version table with T-SQL-safe DDL."""
    driver = FakeSyncMigrationDriver()
    tracker = MssqlPythonSyncMigrationTracker(version_table_name="__migrations_test")

    tracker.ensure_tracking_table(cast("Any", driver))

    ddl = "\n".join(driver.scripts)
    assert "IF NOT EXISTS (SELECT 1 FROM sys.tables" in ddl
    assert "SCHEMA_ID('dbo')" in ddl
    assert "NVARCHAR(32)" in ddl
    assert "DATETIME2(6)" in ddl
    assert "TIMESTAMP" not in ddl
    assert driver.commits >= 1


@pytest.mark.anyio
async def test_async_tracker_uses_tsql_idempotent_create_table() -> None:
    """The async tracker should use the same T-SQL-safe DDL."""
    driver = FakeAsyncMigrationDriver()
    tracker = MssqlPythonAsyncMigrationTracker(version_table_name="__migrations_test")

    await tracker.ensure_tracking_table(cast("Any", driver))

    ddl = "\n".join(driver.scripts)
    assert "IF NOT EXISTS (SELECT 1 FROM sys.tables" in ddl
    assert "NVARCHAR(MAX)" in ddl
    assert "DATETIME2(6)" in ddl
    assert driver.commits >= 1


def test_mssql_python_configs_use_tsql_migration_trackers() -> None:
    """Sync and async configs should use the MSSQL migration tracker types."""
    assert MssqlPythonConfig.migration_tracker_type is MssqlPythonSyncMigrationTracker
    assert MssqlPythonAsyncConfig.migration_tracker_type is MssqlPythonAsyncMigrationTracker
