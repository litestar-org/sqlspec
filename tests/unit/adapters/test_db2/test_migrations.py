"""Tests for the Db2 migration tracker and migration schema hooks."""

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from sqlspec.adapters.db2.config import Db2SyncConfig
from sqlspec.adapters.db2.core import TABLE_EXISTS_SQL, build_set_schema_sql, split_db2_table_name
from sqlspec.adapters.db2.driver import Db2SyncDriver
from sqlspec.adapters.db2.migrations import Db2SyncMigrationTracker
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.observability import resolve_db_system
from tests.unit.adapters.test_db2._fakes import FakeDb2Connection, FakeDb2Cursor

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
    """Driver stand-in that answers the tracking-table probe from a scripted sequence."""

    def __init__(self, probe_results: "list[Any]") -> None:
        self.driver_features: dict[str, Any] = {}
        self.probe_results = probe_results
        self.probes: list[tuple[str, Any]] = []
        self.executed: list[Any] = []
        self.commits = 0

    def select_value_or_none(self, statement: str, parameters: Any) -> Any:
        self.probes.append((statement, parameters))
        return self.probe_results.pop(0)

    def execute(self, statement: Any) -> None:
        self.executed.append(statement)

    def commit(self) -> None:
        self.commits += 1


def _recording_connection(rows: "list[Any]") -> FakeDb2Connection:
    """Build a connection whose every cursor starts with the given rows.

    Returns:
        The fake connection.
    """
    return FakeDb2Connection(lambda: FakeDb2Cursor(rows=list(rows)))


def _executed_sql(connection: FakeDb2Connection) -> "list[str]":
    return [sql for cursor in connection.cursors for sql, _ in cursor.executed]


def test_tracking_table_ddl_is_valid_db2() -> None:
    """The tracking table renders Db2 DDL with a NOT NULL key, a Db2 timestamp default and CLOB text."""
    rendered = Db2SyncMigrationTracker("ddl_migrations")._tracking_table_ddl().build(dialect="db2").sql

    assert rendered == EXPECTED_TRACKING_DDL
    assert "IF NOT EXISTS" not in rendered
    assert '"' not in rendered
    assert "CURRENT_TIMESTAMP(" not in rendered


def test_ensure_tracking_table_creates_only_when_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """CREATE TABLE runs only when the catalog probe finds no tracking table."""
    schema_checks: list[object] = []
    monkeypatch.setattr(
        Db2SyncMigrationTracker, "_migrate_schema_if_needed", lambda self, driver: schema_checks.append(driver)
    )
    tracker = Db2SyncMigrationTracker("ddl_migrations")
    driver = _ProbeDriver([None, 1])

    tracker.ensure_tracking_table(driver)  # type: ignore[arg-type]
    tracker.ensure_tracking_table(driver)  # type: ignore[arg-type]

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
        ('"App"."Tracker"', ("App", "Tracker")),
    ],
)
def test_probe_binds_normalized_schema_and_table(table_name: str, expected: "tuple[str | None, str]") -> None:
    """The probe binds catalog-folded names and leaves the schema to CURRENT SCHEMA when unqualified."""
    driver = _ProbeDriver([1])

    Db2SyncMigrationTracker(table_name).ensure_tracking_table(driver)  # type: ignore[arg-type]

    assert driver.probes[0][1] == expected
    assert driver.executed == []


@pytest.mark.parametrize(
    ("name", "expected"), [("", (None, "")), ("a.b.c", ("B", "C")), ('app."My.Table"', ("APP", "My.Table"))]
)
def test_split_db2_table_name(name: str, expected: "tuple[str | None, str]") -> None:
    """Qualified names split on unquoted dots, keeping the last two parts."""
    assert split_db2_table_name(name) == expected


def test_probe_binds_configured_schema() -> None:
    """A schema passed separately from the table name is bound the same way."""
    driver = _ProbeDriver([1])

    Db2SyncMigrationTracker("ddl_migrations", "app").ensure_tracking_table(driver)  # type: ignore[arg-type]

    assert driver.probes[0][1] == ("APP", "DDL_MIGRATIONS")


def test_record_migration_binds_utc_applied_at() -> None:
    """Recording a migration binds applied_at as a naive UTC timestamp."""
    tracker = Db2SyncMigrationTracker("ddl_migrations")

    statement = tracker._record_migration_statement("0001", "sequential", 1, "init", 12, "abc", "me").build(
        dialect="db2"
    )

    applied_at = statement.parameters["applied_at"]
    assert applied_at.tzinfo is None
    assert abs(applied_at - datetime.now(timezone.utc).replace(tzinfo=None)) < timedelta(seconds=1)
    assert statement.parameters["version_num"] == "0001"
    assert statement.parameters["applied_by"] == "me"


def test_record_squashed_migration_binds_utc_applied_at() -> None:
    """Recording a squashed migration binds applied_at as naive UTC and keeps the replaced versions."""
    tracker = Db2SyncMigrationTracker("ddl_migrations")

    statement = tracker._record_squashed_migration_statement(
        "0003", "sequential", 3, "squash", 0, "abc", "me", "0001,0002"
    ).build(dialect="db2")

    applied_at = statement.parameters["applied_at"]
    assert applied_at.tzinfo is None
    assert abs(applied_at - datetime.now(timezone.utc).replace(tzinfo=None)) < timedelta(seconds=1)
    assert statement.parameters["replaces"] == "0001,0002"


def test_set_and_reset_migration_session_schema() -> None:
    """The session schema is switched after capturing the current one and restored exactly."""
    connection = _recording_connection([("DB2INST1",)])
    driver = Db2SyncDriver(connection)

    driver.set_migration_session_schema("app")
    driver.set_migration_session_schema("other")
    driver.reset_migration_session_schema()
    driver.reset_migration_session_schema()

    assert _executed_sql(connection) == [
        "VALUES CURRENT SCHEMA",
        'SET SCHEMA "APP"',
        'SET SCHEMA "OTHER"',
        'SET SCHEMA "DB2INST1"',
    ]


def test_reset_restores_case_sensitive_schema() -> None:
    """A captured lowercase schema is restored without case folding."""
    connection = _recording_connection([("app",)])
    driver = Db2SyncDriver(connection)

    driver.set_migration_session_schema("OTHER")
    driver.reset_migration_session_schema()

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
def test_has_schema(rows: "list[Any]", expected: bool) -> None:
    """Schema existence is read from SYSCAT.SCHEMATA with a bound, folded name."""
    connection = _recording_connection(rows)
    driver = Db2SyncDriver(connection)

    assert driver.has_schema("app") is expected
    assert connection.cursors[0].executed == [("SELECT 1 FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = ?", ("APP",))]


def test_config_uses_db2_tracker() -> None:
    """The sync config tracks migrations with the Db2 tracker."""
    assert Db2SyncConfig.migration_tracker_type is Db2SyncMigrationTracker


def test_db2_drivers_resolve_db_system() -> None:
    """Db2 drivers report the Db2 database system in observability records."""
    assert resolve_db_system("Db2SyncDriver") == "db2"
