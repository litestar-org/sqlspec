"""Unit coverage for Oracle migration schema hooks."""

from typing import Any

import pytest

from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.adapters.oracledb.migrations import OracleAsyncMigrationTracker, OracleSyncMigrationTracker
from sqlspec.exceptions import MigrationError

CAPTURE = ("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL", None)

RESET_CASES = [
    pytest.param("APP_USER", '"APP_USER"', id="upper"),
    pytest.param("MixedCase", '"MixedCase"', id="mixed"),
    pytest.param("lower_owner", '"lower_owner"', id="lower"),
    pytest.param('A"B', '"A""B"', id="quote"),
]


class FakeSyncCursor:
    def __init__(self, schema_exists: bool = True, current_schema: str = "APP_USER") -> None:
        self.schema_exists = schema_exists
        self.current_schema = current_schema
        self.executed: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any | None = None) -> None:
        self.executed.append((sql, parameters))

    def fetchone(self) -> tuple[Any] | None:
        if "SYS_CONTEXT" in self.executed[-1][0]:
            return (self.current_schema,)
        return (1,) if self.schema_exists else None

    def close(self) -> None:
        return None


class FakeSyncConnection:
    def __init__(self, cursor: FakeSyncCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> FakeSyncCursor:
        return self._cursor


class FakeAsyncCursor:
    def __init__(self, schema_exists: bool = True, current_schema: str = "APP_USER") -> None:
        self.schema_exists = schema_exists
        self.current_schema = current_schema
        self.executed: list[tuple[str, Any]] = []

    async def execute(self, sql: str, parameters: Any | None = None) -> None:
        self.executed.append((sql, parameters))

    async def fetchone(self) -> tuple[Any] | None:
        if "SYS_CONTEXT" in self.executed[-1][0]:
            return (self.current_schema,)
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
        CAPTURE,
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
        CAPTURE,
        ('ALTER SESSION SET CURRENT_SCHEMA = "MixedCase"', None),
        ("SELECT 1 FROM ALL_USERS WHERE USERNAME = :schema_name", {"schema_name": "MixedCase"}),
    ]


def test_oracle_sync_normalizes_lowercase_identifier() -> None:
    cursor = FakeSyncCursor()
    driver = OracleSyncDriver(FakeSyncConnection(cursor))  # type: ignore[arg-type]

    driver.set_migration_session_schema("tenant")
    assert driver.has_schema("tenant") is True

    assert cursor.executed == [
        CAPTURE,
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
        CAPTURE,
        ('ALTER SESSION SET CURRENT_SCHEMA = "TENANT"', None),
        ("SELECT 1 FROM ALL_USERS WHERE USERNAME = :schema_name", {"schema_name": "TENANT"}),
    ]
    assert OracleAsyncConfig.supports_migration_schemas is True


@pytest.mark.anyio
async def test_oracle_async_has_schema_returns_false_for_missing_user() -> None:
    cursor = FakeAsyncCursor(schema_exists=False)
    driver = OracleAsyncDriver(FakeAsyncConnection(cursor))  # type: ignore[arg-type]

    assert await driver.has_schema("missing") is False


@pytest.mark.parametrize(("current_schema", "quoted_schema"), RESET_CASES)
def test_oracle_sync_reset_restores_captured_schema(current_schema: str, quoted_schema: str) -> None:
    cursor = FakeSyncCursor(current_schema=current_schema)
    driver = OracleSyncDriver(FakeSyncConnection(cursor))  # type: ignore[arg-type]

    driver.set_migration_session_schema("TENANT")
    driver.reset_migration_session_schema()

    assert cursor.executed[-1] == (f"ALTER SESSION SET CURRENT_SCHEMA = {quoted_schema}", None)


@pytest.mark.anyio
@pytest.mark.parametrize(("current_schema", "quoted_schema"), RESET_CASES)
async def test_oracle_async_reset_restores_captured_schema(current_schema: str, quoted_schema: str) -> None:
    cursor = FakeAsyncCursor(current_schema=current_schema)
    driver = OracleAsyncDriver(FakeAsyncConnection(cursor))  # type: ignore[arg-type]

    await driver.set_migration_session_schema("TENANT")
    await driver.reset_migration_session_schema()

    assert cursor.executed[-1] == (f"ALTER SESSION SET CURRENT_SCHEMA = {quoted_schema}", None)


def test_oracle_sync_second_switch_keeps_first_capture() -> None:
    cursor = FakeSyncCursor()
    driver = OracleSyncDriver(FakeSyncConnection(cursor))  # type: ignore[arg-type]

    driver.set_migration_session_schema("ONE")
    driver.set_migration_session_schema("TWO")
    driver.reset_migration_session_schema()

    assert cursor.executed.count(CAPTURE) == 1
    assert cursor.executed[-1] == ('ALTER SESSION SET CURRENT_SCHEMA = "APP_USER"', None)


@pytest.mark.anyio
async def test_oracle_async_second_switch_keeps_first_capture() -> None:
    cursor = FakeAsyncCursor()
    driver = OracleAsyncDriver(FakeAsyncConnection(cursor))  # type: ignore[arg-type]

    await driver.set_migration_session_schema("ONE")
    await driver.set_migration_session_schema("TWO")
    await driver.reset_migration_session_schema()

    assert cursor.executed.count(CAPTURE) == 1
    assert cursor.executed[-1] == ('ALTER SESSION SET CURRENT_SCHEMA = "APP_USER"', None)


def test_oracle_sync_reset_without_switch_is_noop() -> None:
    cursor = FakeSyncCursor()
    driver = OracleSyncDriver(FakeSyncConnection(cursor))  # type: ignore[arg-type]

    driver.reset_migration_session_schema()
    assert cursor.executed == []

    driver.set_migration_session_schema("TENANT")
    driver.reset_migration_session_schema()
    statements_after_first_reset = list(cursor.executed)
    driver.reset_migration_session_schema()

    assert cursor.executed == statements_after_first_reset


@pytest.mark.anyio
async def test_oracle_async_reset_without_switch_is_noop() -> None:
    cursor = FakeAsyncCursor()
    driver = OracleAsyncDriver(FakeAsyncConnection(cursor))  # type: ignore[arg-type]

    await driver.reset_migration_session_schema()
    assert cursor.executed == []

    await driver.set_migration_session_schema("TENANT")
    await driver.reset_migration_session_schema()
    statements_after_first_reset = list(cursor.executed)
    await driver.reset_migration_session_schema()

    assert cursor.executed == statements_after_first_reset


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
