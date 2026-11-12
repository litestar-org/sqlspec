"""Public Snowflake ledger operations against a DB-API boundary implementation."""

import sqlite3
from typing import Any

import pytest

from sqlspec.adapters.snowflake import SnowflakeDriver, SnowflakeSyncMigrationTracker


class LedgerCursor:
    """Execute standard ledger SQL locally and expose Snowflake catalog rows."""

    def __init__(self, connection: "LedgerConnection") -> None:
        self.connection = connection
        self.cursor = connection.database.cursor()
        self.description: list[tuple[Any, ...]] | None = None
        self.rowcount = -1
        self.rows: list[Any] | None = None

    def execute(self, statement: str, parameters: Any = None, **kwargs: Any) -> None:
        self.connection.statements.append(statement)
        upper = statement.upper()
        if "INFORMATION_SCHEMA.TABLES" in upper:
            tables = self.connection.database.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            self.description = [("table_name", 2, None, None, None, None, False)]
            self.rows = tables
            self.rowcount = len(tables)
            return
        if "INFORMATION_SCHEMA.COLUMNS" in upper:
            table = self.connection.database.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchone()[
                0
            ]
            columns = self.connection.database.execute(
                'PRAGMA table_info("' + table.replace('"', '""') + '")'
            ).fetchall()
            names = ["column_name", "data_type", "is_nullable", "ordinal_position", "column_default"]
            self.description = [(name, 2, None, None, None, None, False) for name in names]
            self.rows = [(column[1], column[2], not column[3], column[0] + 1, column[4]) for column in columns]
            self.rowcount = len(columns)
            return
        self.cursor.execute(statement.replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP"), parameters or ())
        self.description = (
            [(column[0], 2, None, None, None, None, True) for column in self.cursor.description]
            if self.cursor.description
            else None
        )
        self.rowcount = self.cursor.rowcount
        self.rows = None

    def fetchall(self) -> list[Any]:
        if self.rows is not None:
            rows, self.rows = self.rows, []
            return rows
        return self.cursor.fetchall()

    def close(self) -> None:
        self.cursor.close()


class LedgerConnection:
    """Local DB-API collaborator preserving the actual emitted ledger SQL."""

    def __init__(self) -> None:
        self.database = sqlite3.connect(":memory:")
        self.statements: list[str] = []

    def cursor(self) -> LedgerCursor:
        return LedgerCursor(self)

    def commit(self) -> None:
        self.database.commit()

    def rollback(self) -> None:
        self.database.rollback()


@pytest.mark.parametrize(
    ("schema", "expected"), [(None, None), ("analytics", '"ANALYTICS"'), ('"MixedSchema"', '"MixedSchema"')]
)
def test_tracker_catalog_schema_preserves_explicit_case(schema: str | None, expected: str | None) -> None:
    tracker = SnowflakeSyncMigrationTracker(version_table_name='"MixedLedger"', version_table_schema=schema)
    assert tracker.catalog_schema_name() == expected


@pytest.mark.parametrize(
    ("table_name", "physical_name"), [('"MixedLedger"', '"MixedLedger"'), ("ddl_migrations", '"DDL_MIGRATIONS"')]
)
def test_native_ledger_records_reads_updates_and_removes_versions(table_name: str, physical_name: str) -> None:
    connection = LedgerConnection()
    driver = SnowflakeDriver(connection)
    tracker = SnowflakeSyncMigrationTracker(version_table_name=table_name)
    try:
        tracker.ensure_tracking_table(driver)
        assert tracker.get_current_version(driver) is None
        tracker.record_migration(driver, "0001", "first", 12, "checksum-a")
        tracker.record_migration(driver, "0002", "squashed", 15, "checksum-b", replaces=["0000"])
        assert tracker.get_current_version(driver) == "0002"
        applied = tracker.get_applied_migrations(driver)
        assert [(row["version_num"], row["execution_sequence"], row["description"]) for row in applied] == [
            ("0001", 1, "first"),
            ("0002", 2, "squashed"),
        ]
        assert applied[1]["replaces"] == "0000"
        assert tracker.is_squash_already_applied(driver, "0004", ["0001", "0002"])
        tracker.update_version_record(driver, "0002", "0003")
        assert tracker.get_current_version(driver) == "0003"
        tracker.remove_migration(driver, "0003")
        assert tracker.get_current_version(driver) == "0001"
        assert connection.statements[0].startswith(f"CREATE TABLE IF NOT EXISTS {physical_name}")
    finally:
        connection.database.close()
