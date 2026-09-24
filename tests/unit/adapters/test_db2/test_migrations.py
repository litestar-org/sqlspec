"""Tests for the Db2 migration trackers and migration schema hooks.

Tracker and schema-hook behaviors run in both driver modes through ``db2_mode``.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2SyncConfig
from sqlspec.adapters.db2.core import TABLE_EXISTS_SQL, build_set_schema_sql, split_db2_table_name
from sqlspec.adapters.db2.migrations import Db2AsyncMigrationTracker, Db2SyncMigrationTracker
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.observability import resolve_db_system
from tests.unit.adapters.test_db2._fakes import DriverMode, FakeDb2Connection, FakeDb2Cursor

pytestmark = pytest.mark.anyio

EXPECTED_TRACKING_DDL = """CREATE TABLE ddl_migrations (
  version_num VARCHAR(32) NOT NULL PRIMARY KEY,
  version_type VARCHAR(16),
  execution_sequence INTEGER,
  description CLOB,
  applied_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP,
  execution_time_ms INTEGER,
  checksum VARCHAR(64),
  applied_by VARCHAR(255),
  replaces CLOB
)"""


class _ProbeDriver:
    """Driver stand-in that answers the tracking-table probe from a scripted sequence or a catalog.

    With a catalog, the probe finds the table when its bound ``(schema, table)`` pair is listed.
    """

    def __init__(self, probe_results: "list[Any]", catalog: "set[tuple[str | None, str]] | None" = None) -> None:
        self.driver_features: dict[str, Any] = {}
        self.probe_results = probe_results
        self.catalog = catalog
        self.probes: list[tuple[str, Any]] = []
        self.executed: list[Any] = []
        self.commits = 0

    def select_value_or_none(self, statement: str, parameters: Any) -> Any:
        self.probes.append((statement, parameters))
        if self.catalog is not None:
            return 1 if tuple(parameters) in self.catalog else None
        return self.probe_results.pop(0)

    def execute(self, statement: Any) -> None:
        self.executed.append(statement)

    def commit(self) -> None:
        self.commits += 1


class _AsyncProbeDriver(_ProbeDriver):
    """Async form of ``_ProbeDriver`` for the async tracker."""

    async def select_value_or_none(self, statement: str, parameters: Any) -> Any:  # type: ignore[override]
        return super().select_value_or_none(statement, parameters)

    async def execute(self, statement: Any) -> None:  # type: ignore[override]
        super().execute(statement)

    async def commit(self) -> None:  # type: ignore[override]
        super().commit()


def _probe_driver(
    db2_mode: DriverMode, probe_results: "list[Any]", catalog: "set[tuple[str | None, str]] | None" = None
) -> _ProbeDriver:
    driver_class = _AsyncProbeDriver if db2_mode.is_async else _ProbeDriver
    return driver_class(probe_results, catalog)


def _stub_schema_migration(monkeypatch: pytest.MonkeyPatch, tracker: Any, db2_mode: DriverMode) -> "list[object]":
    """Replace the tracker's column migration with a recorder of the drivers it receives.

    Returns:
        The list that receives each driver.
    """
    calls: list[object] = []

    def record(self: Any, driver: Any) -> Any:
        calls.append(driver)
        return db2_mode.call(lambda: None) if db2_mode.is_async else None

    monkeypatch.setattr(type(tracker), "_migrate_schema_if_needed", record)
    return calls


def _recording_connection(rows: "list[Any]") -> FakeDb2Connection:
    """Build a connection whose every cursor starts with the given rows.

    Returns:
        The fake connection.
    """
    return FakeDb2Connection(lambda: FakeDb2Cursor(rows=list(rows)))


def _executed_sql(connection: FakeDb2Connection) -> "list[str]":
    return [sql for cursor in connection.cursors for sql, _ in cursor.executed]


async def test_tracking_table_ddl_is_valid_db2(db2_mode: DriverMode) -> None:
    """The tracking table renders Db2 DDL with a NOT NULL key, a Db2 timestamp default and CLOB text."""
    rendered = db2_mode.tracker("ddl_migrations")._tracking_table_ddl().build(dialect="db2").sql

    assert rendered == EXPECTED_TRACKING_DDL
    assert "IF NOT EXISTS" not in rendered
    assert '"' not in rendered
    assert "CURRENT_TIMESTAMP(" not in rendered


async def test_ensure_tracking_table_creates_only_when_absent(
    db2_mode: DriverMode, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CREATE TABLE runs only when the catalog probe finds no tracking table."""
    tracker = db2_mode.tracker("ddl_migrations")
    schema_checks = _stub_schema_migration(monkeypatch, tracker, db2_mode)
    driver = _probe_driver(db2_mode, [None, 1])

    await db2_mode.call(tracker.ensure_tracking_table, driver)
    await db2_mode.call(tracker.ensure_tracking_table, driver)

    assert len(driver.executed) == 1
    assert driver.executed[0].build(dialect="db2").sql == EXPECTED_TRACKING_DDL
    assert driver.commits == 1
    assert [statement for statement, _ in driver.probes] == [TABLE_EXISTS_SQL, TABLE_EXISTS_SQL]
    assert len(schema_checks) == 2


@pytest.mark.parametrize(
    ("table_name", "expected"),
    [
        ("app.ddl_migrations", ("APP", "DDL_MIGRATIONS")),
        ("ddl_migrations", (None, "DDL_MIGRATIONS")),
        ("AppMigrations", (None, "APPMIGRATIONS")),
        ("App.Tracker", ("APP", "TRACKER")),
    ],
    ids=["qualified", "unqualified", "mixed-case", "mixed-case-qualified"],
)
async def test_probe_binds_catalog_folded_schema_and_table(
    db2_mode: DriverMode, table_name: str, expected: "tuple[str | None, str]"
) -> None:
    """The probe binds the upper-folded names Db2 stores for the tracker's unquoted DDL."""
    driver = _probe_driver(db2_mode, [1])

    await db2_mode.call(db2_mode.tracker(table_name).ensure_tracking_table, driver)

    assert driver.probes[0][1] == expected
    assert driver.executed == []


async def test_mixed_case_tracking_table_is_created_once(db2_mode: DriverMode, monkeypatch: pytest.MonkeyPatch) -> None:
    """A mixed-case tracking table found under its folded catalog name is not created again."""
    tracker = db2_mode.tracker("AppMigrations")
    _stub_schema_migration(monkeypatch, tracker, db2_mode)
    driver = _probe_driver(db2_mode, [], catalog={(None, "APPMIGRATIONS")})

    await db2_mode.call(tracker.ensure_tracking_table, driver)
    await db2_mode.call(tracker.ensure_tracking_table, driver)

    assert driver.executed == []
    assert len(driver.probes) == 2


@pytest.mark.parametrize(
    ("name", "expected"), [("", (None, "")), ("a.b.c", ("B", "C")), ('app."My.Table"', ("APP", "My.Table"))]
)
def test_split_db2_table_name(name: str, expected: "tuple[str | None, str]") -> None:
    """Qualified names split on unquoted dots, keeping the last two parts."""
    assert split_db2_table_name(name) == expected


async def test_probe_binds_configured_schema(db2_mode: DriverMode) -> None:
    """A schema passed separately from the table name is bound the same way."""
    driver = _probe_driver(db2_mode, [1])

    await db2_mode.call(db2_mode.tracker("ddl_migrations", "app").ensure_tracking_table, driver)

    assert driver.probes[0][1] == ("APP", "DDL_MIGRATIONS")


async def test_record_migration_binds_utc_applied_at(db2_mode: DriverMode) -> None:
    """Recording a migration binds applied_at as a naive UTC timestamp."""
    tracker = db2_mode.tracker("ddl_migrations")

    statement = tracker._record_migration_statement("0001", "sequential", 1, "init", 12, "abc", "me").build(
        dialect="db2"
    )

    applied_at = statement.parameters["applied_at"]
    assert applied_at.tzinfo is None
    assert abs(applied_at - datetime.now(timezone.utc).replace(tzinfo=None)) < timedelta(seconds=1)
    assert statement.parameters["version_num"] == "0001"
    assert statement.parameters["applied_by"] == "me"


async def test_record_squashed_migration_binds_utc_applied_at(db2_mode: DriverMode) -> None:
    """Recording a squashed migration binds applied_at as naive UTC and keeps the replaced versions."""
    tracker = db2_mode.tracker("ddl_migrations")

    statement = tracker._record_squashed_migration_statement(
        "0003", "sequential", 3, "squash", 0, "abc", "me", "0001,0002"
    ).build(dialect="db2")

    applied_at = statement.parameters["applied_at"]
    assert applied_at.tzinfo is None
    assert abs(applied_at - datetime.now(timezone.utc).replace(tzinfo=None)) < timedelta(seconds=1)
    assert statement.parameters["replaces"] == "0001,0002"


async def test_set_and_reset_migration_session_schema(db2_mode: DriverMode) -> None:
    """The session schema is switched after capturing the current one and restored exactly."""
    connection = _recording_connection([("DB2INST1",)])
    driver = db2_mode.driver(connection)

    await db2_mode.call(driver.set_migration_session_schema, "app")
    await db2_mode.call(driver.set_migration_session_schema, "other")
    await db2_mode.call(driver.reset_migration_session_schema)
    await db2_mode.call(driver.reset_migration_session_schema)

    assert _executed_sql(connection) == [
        "VALUES CURRENT SCHEMA",
        'SET SCHEMA "APP"',
        'SET SCHEMA "OTHER"',
        'SET SCHEMA "DB2INST1"',
    ]
    assert all(cursor.closed for cursor in connection.cursors)


async def test_reset_restores_case_sensitive_schema(db2_mode: DriverMode) -> None:
    """A captured lowercase schema is restored without case folding."""
    connection = _recording_connection([("app",)])
    driver = db2_mode.driver(connection)

    await db2_mode.call(driver.set_migration_session_schema, "OTHER")
    await db2_mode.call(driver.reset_migration_session_schema)

    assert _executed_sql(connection)[-1] == 'SET SCHEMA "app"'


def test_set_schema_quotes_identifier() -> None:
    """Schema names fold like unquoted Db2 identifiers and embedded quotes are doubled."""
    assert build_set_schema_sql('a"b') == 'SET SCHEMA "A""B"'
    assert build_set_schema_sql("MixedCase") == 'SET SCHEMA "MixedCase"'
    assert build_set_schema_sql('"app"') == 'SET SCHEMA "app"'


@pytest.mark.parametrize("schema", ["", "   ", '""'])
def test_set_schema_rejects_empty_names(schema: str) -> None:
    """An empty schema name is a configuration error."""
    with pytest.raises(ImproperConfigurationError):
        build_set_schema_sql(schema)


@pytest.mark.parametrize(("rows", "expected"), [([(1,)], True), ([], False)])
async def test_has_schema(db2_mode: DriverMode, rows: "list[Any]", expected: bool) -> None:
    """Schema existence is read from SYSCAT.SCHEMATA with a bound, folded name."""
    connection = _recording_connection(rows)
    driver = db2_mode.driver(connection)

    assert await db2_mode.call(driver.has_schema, "app") is expected
    assert connection.cursors[0].executed == [("SELECT 1 FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = ?", ("APP",))]


def test_configs_use_db2_trackers() -> None:
    """Each Db2 config tracks migrations with the Db2 tracker of its mode."""
    assert Db2SyncConfig.migration_tracker_type is Db2SyncMigrationTracker
    assert Db2AsyncConfig.migration_tracker_type is Db2AsyncMigrationTracker


@pytest.mark.parametrize("driver_name", ["Db2SyncDriver", "Db2AsyncDriver"])
def test_db2_drivers_resolve_db_system(driver_name: str) -> None:
    """Db2 drivers report the Db2 database system in observability records."""
    assert resolve_db_system(driver_name) == "db2"
