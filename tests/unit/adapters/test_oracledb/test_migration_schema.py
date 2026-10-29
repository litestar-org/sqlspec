"""Unit coverage for Oracle migration schema hooks."""

from typing import Any

import pytest

from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.adapters.oracledb.migrations import OracleAsyncMigrationTracker, OracleSyncMigrationTracker
from sqlspec.exceptions import MigrationError


class FakeSyncCursor:
    def __init__(self, schema_exists: bool = True) -> None:
        self.schema_exists = schema_exists
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any | None = None) -> None:
        self.executed.append((sql, parameters))

    def fetchone(self) -> tuple[int] | None:
        return (1,) if self.schema_exists else None

    def close(self) -> None:
        return None


class FakeSyncConnection:
    def __init__(self, cursor: FakeSyncCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeSyncCursor:
        return self._cursor


class FakeAsyncCursor:
    def __init__(self, schema_exists: bool = True) -> None:
        self.schema_exists = schema_exists
        self.executed: list[tuple[str, Any]] = []

    async def execute(self, sql: str, parameters: Any | None = None) -> None:
        self.executed.append((sql, parameters))

    async def fetchone(self) -> tuple[int] | None:
        return (1,) if self.schema_exists else None

    def close(self) -> None:
        return None


class FakeAsyncConnection:
    def __init__(self, cursor: FakeAsyncCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeAsyncCursor:
        return self._cursor


def test_oracle_sync_migration_schema_hooks() -> None:
    cursor = FakeSyncCursor()
    driver = OracleSyncDriver(FakeSyncConnection(cursor))  # type: ignore[arg-type]

    driver.set_migration_session_schema("TENANT")
    assert driver.has_schema("TENANT") is True

    assert cursor.executed == [
        ('ALTER SESSION SET CURRENT_SCHEMA = "TENANT"', None),
        ("SELECT 1 FROM ALL_USERS WHERE USERNAME = :schema_name", {"schema_name": "TENANT"}),
    ]
    assert OracleSyncConfig.supports_migration_schemas is True


def test_oracle_sync_preserves_mixed_case_identifier() -> None:
    cursor = FakeSyncCursor()
    driver = OracleSyncDriver(FakeSyncConnection(cursor))  # type: ignore[arg-type]

    driver.set_migration_session_schema("MixedCase")
    assert driver.has_schema("MixedCase") is True

    assert cursor.executed == [
        ('ALTER SESSION SET CURRENT_SCHEMA = "MixedCase"', None),
        ("SELECT 1 FROM ALL_USERS WHERE USERNAME = :schema_name", {"schema_name": "MixedCase"}),
    ]


def test_oracle_sync_normalizes_lowercase_identifier() -> None:
    cursor = FakeSyncCursor()
    driver = OracleSyncDriver(FakeSyncConnection(cursor))  # type: ignore[arg-type]

    driver.set_migration_session_schema("tenant")
    assert driver.has_schema("tenant") is True

    assert cursor.executed == [
        ('ALTER SESSION SET CURRENT_SCHEMA = "TENANT"', None),
        ("SELECT 1 FROM ALL_USERS WHERE USERNAME = :schema_name", {"schema_name": "TENANT"}),
    ]


@pytest.mark.anyio
async def test_oracle_async_migration_schema_hooks() -> None:
    cursor = FakeAsyncCursor()
    driver = OracleAsyncDriver(FakeAsyncConnection(cursor))  # type: ignore[arg-type]

    await driver.set_migration_session_schema("TENANT")
    assert await driver.has_schema("TENANT") is True

    assert cursor.executed == [
        ('ALTER SESSION SET CURRENT_SCHEMA = "TENANT"', None),
        ("SELECT 1 FROM ALL_USERS WHERE USERNAME = :schema_name", {"schema_name": "TENANT"}),
    ]
    assert OracleAsyncConfig.supports_migration_schemas is True


@pytest.mark.anyio
async def test_oracle_async_has_schema_returns_false_for_missing_user() -> None:
    cursor = FakeAsyncCursor(schema_exists=False)
    driver = OracleAsyncDriver(FakeAsyncConnection(cursor))  # type: ignore[arg-type]

    assert await driver.has_schema("missing") is False


class RecordingResult:
    def __init__(self, data: list[dict[str, Any]]) -> None:
        self.data = data

    def get_data(self) -> list[dict[str, Any]]:
        return self.data


class RecordingSyncDriver:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.statements: list[str] = []
        self.rows = rows if rows is not None else [{"next_seq": 4}]
        self.driver_features: dict[str, Any] = {}
        self.commits = 0

    def execute(self, statement: Any) -> RecordingResult:
        self.statements.append(statement.build(dialect="oracle").sql)
        return RecordingResult(self.rows)

    def commit(self) -> None:
        self.commits += 1


class RecordingAsyncDriver(RecordingSyncDriver):
    async def execute(self, statement: Any) -> RecordingResult:  # type: ignore[override]
        return RecordingSyncDriver.execute(self, statement)

    async def commit(self) -> None:  # type: ignore[override]
        self.commits += 1


def test_oracle_sync_tracker_records_replaced_versions_for_squashed_migrations() -> None:
    """Squashed migrations recorded on Oracle persist the replaced versions in the replaces column."""
    tracker = OracleSyncMigrationTracker()
    driver = RecordingSyncDriver()

    tracker.record_migration(driver, "0001", "release", 12, "abc", replaces=["0001", "0002"])  # type: ignore[arg-type]
    tracker.record_migration(driver, "0002", "plain", 3, "def")  # type: ignore[arg-type]

    squashed_insert, plain_insert = driver.statements[1], driver.statements[3]
    assert squashed_insert.startswith('INSERT INTO "DDL_MIGRATIONS"')
    assert "  replaces\n" in squashed_insert
    assert ":replaces" in squashed_insert
    assert "replaces" not in plain_insert
    assert driver.commits == 2


@pytest.mark.anyio
async def test_oracle_async_tracker_records_replaced_versions_for_squashed_migrations() -> None:
    """The async Oracle tracker records replaced versions exactly like the sync tracker."""
    tracker = OracleAsyncMigrationTracker()
    driver = RecordingAsyncDriver()

    await tracker.record_migration(driver, "0001", "release", 12, "abc", replaces=["0001", "0002"])  # type: ignore[arg-type]

    assert driver.statements[1].startswith('INSERT INTO "DDL_MIGRATIONS"')
    assert ":replaces" in driver.statements[1]


def test_oracle_tracker_schema_and_queries_include_replaces_column() -> None:
    """The Oracle tracking table DDL, applied query, and squash reconciliation support replaces."""
    tracker = OracleSyncMigrationTracker()

    assert "replaces" in {column.name.lower() for column in tracker._tracking_table_ddl().columns}  # pyright: ignore[reportPrivateUsage]
    assert "REPLACES AS replaces" in tracker._applied_migrations_query().build(dialect="oracle").sql  # pyright: ignore[reportPrivateUsage]
    assert tracker._detect_missing_columns({"VERSION_NUM", "APPLIED_BY"}) >= {"replaces"}  # pyright: ignore[reportPrivateUsage]

    driver = RecordingSyncDriver(rows=[{"version_num": "0001"}, {"version_num": "0002"}])
    assert tracker.is_squash_already_applied(driver, "0001", ["0001", "0002"]) is True  # type: ignore[arg-type]
    assert driver.statements[0].startswith("SELECT\n  VERSION_NUM AS version_num")

    partial = RecordingSyncDriver(rows=[{"version_num": "0001"}])
    with pytest.raises(MigrationError, match="Partial squash state"):
        tracker.is_squash_already_applied(partial, "0001", ["0001", "0002"])  # type: ignore[arg-type]
