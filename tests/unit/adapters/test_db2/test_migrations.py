"""Tests for the Db2 migration trackers and migration schema hooks.

Tracker and schema-hook behaviors run in both driver modes through ``db2_mode``.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from sqlspec.adapters.db2.config import Db2AsyncConfig, Db2SyncConfig
from sqlspec.adapters.db2.core import TABLE_EXISTS_SQL, build_set_schema_sql, split_db2_table_name
from sqlspec.adapters.db2.data_dictionary import Db2AsyncDataDictionary, Db2SyncDataDictionary
from sqlspec.adapters.db2.migrations import Db2AsyncMigrationTracker, Db2SyncMigrationTracker
from sqlspec.exceptions import ImproperConfigurationError, PermissionDeniedError
from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands
from sqlspec.observability import resolve_db_system
from tests.unit.adapters.test_db2._fakes import DriverMode, FakeDb2Connection, FakeDb2Cursor, db2_error

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


_TRACKING_COLUMNS = (
    "version_num",
    "version_type",
    "execution_sequence",
    "description",
    "applied_at",
    "execution_time_ms",
    "checksum",
    "applied_by",
    "replaces",
)


class _ProbeDriver:
    """Driver stand-in that answers the tracking-table probe from a scripted sequence or a catalog.

    With a catalog, the probe finds the table when its bound ``(schema, table)`` pair is listed, and an
    executed CREATE TABLE stores its rendered name the way Db2 does, folding unquoted parts to uppercase and
    rejecting a name that already exists with ``SQL0601N``.
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
        if self.catalog is None:
            return
        rendered_sql = statement.build(dialect="db2").sql
        if not rendered_sql.startswith("CREATE TABLE "):
            return
        rendered_name = rendered_sql.removeprefix("CREATE TABLE ").split(" (", 1)[0]
        cataloged_name = split_db2_table_name(rendered_name)
        if cataloged_name in self.catalog:
            msg = f"SQL0601N {rendered_name} already exists"
            raise RuntimeError(msg)
        self.catalog.add(cataloged_name)

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


class _CatalogDriver(_ProbeDriver):
    """Probe driver whose real data dictionary reads a catalog keyed by exact ``(schema, table)`` names."""

    def __init__(self, catalog: "set[tuple[str | None, str]]") -> None:
        super().__init__([], catalog)
        self.data_dictionary: Any = Db2SyncDataDictionary()
        self.lookups: list[tuple[str | None, str | None]] = []

    def select(self, _statement: Any, *, schema_name: Any, table_name: Any, schema_type: Any = None) -> "list[Any]":
        self.lookups.append((schema_name, table_name))
        catalog = self.catalog or set()
        if schema_type is not None:
            return [{"schema_name": schema, "table_name": table} for schema, table in catalog if schema == schema_name]
        if (schema_name, table_name) not in catalog:
            return []
        return [{"column_name": column} for column in _TRACKING_COLUMNS]


class _AsyncCatalogDriver(_CatalogDriver):
    """Async form of ``_CatalogDriver`` for the async tracker and commands."""

    def __init__(self, catalog: "set[tuple[str | None, str]]") -> None:
        super().__init__(catalog)
        self.data_dictionary = Db2AsyncDataDictionary()

    async def select_value_or_none(self, statement: str, parameters: Any) -> Any:  # type: ignore[override]
        return super().select_value_or_none(statement, parameters)

    async def execute(self, statement: Any) -> None:  # type: ignore[override]
        super().execute(statement)

    async def commit(self) -> None:  # type: ignore[override]
        super().commit()

    async def select(self, _statement: Any, **kwargs: Any) -> "list[Any]":  # type: ignore[override]
        return super().select(_statement, **kwargs)


def _catalog_driver(db2_mode: DriverMode, catalog: "set[tuple[str | None, str]]") -> _CatalogDriver:
    driver_class = _AsyncCatalogDriver if db2_mode.is_async else _CatalogDriver
    return driver_class(catalog)


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
        ('"QuotedMigrations"', (None, "QuotedMigrations")),
        ('app."QuotedMigrations"', ("APP", "QuotedMigrations")),
    ],
    ids=[
        "qualified",
        "unqualified",
        "mixed-case",
        "mixed-case-qualified",
        "quoted-mixed-case",
        "qualified-quoted-mixed-case",
    ],
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
    ("table_name", "table_schema", "rendered_name", "cataloged_name"),
    [
        ('"App"."Tracker"', None, '"App"."Tracker"', ("App", "Tracker")),
        ('"app"."tracker"', None, '"app"."tracker"', ("app", "tracker")),
        ('"QuotedMigrations"', None, '"QuotedMigrations"', (None, "QuotedMigrations")),
        ('app."QuotedMigrations"', None, 'app."QuotedMigrations"', ("APP", "QuotedMigrations")),
        ('"App".tracker', None, '"App".tracker', ("App", "TRACKER")),
        ("tracker", '"App"', '"App".tracker', ("App", "TRACKER")),
    ],
    ids=[
        "quoted-schema-and-table",
        "quoted-lowercase-schema-and-table",
        "quoted-table",
        "quoted-table-only",
        "quoted-schema-only",
        "quoted-schema-option",
    ],
)
async def test_quoted_tracking_table_is_created_once(
    db2_mode: DriverMode,
    monkeypatch: pytest.MonkeyPatch,
    table_name: str,
    table_schema: "str | None",
    rendered_name: str,
    cataloged_name: "tuple[str | None, str]",
) -> None:
    """DDL keeps the quoting the probe binds, so the second probe finds the table the first run created."""
    tracker = db2_mode.tracker(table_name, table_schema)
    _stub_schema_migration(monkeypatch, tracker, db2_mode)
    driver = _probe_driver(db2_mode, [], catalog=set())

    await db2_mode.call(tracker.ensure_tracking_table, driver)
    await db2_mode.call(tracker.ensure_tracking_table, driver)

    assert len(driver.executed) == 1
    assert driver.executed[0].build(dialect="db2").sql.startswith(f"CREATE TABLE {rendered_name} (")
    assert driver.catalog == {cataloged_name}


CATALOG_NAME_CASES = [
    pytest.param('"app"."tracker"', None, ("app", "tracker"), id="quoted-lowercase-schema-and-table"),
    pytest.param('"App"."Tracker"', None, ("App", "Tracker"), id="quoted-mixed-case"),
    pytest.param("tracker", '"app"', ("app", "TRACKER"), id="quoted-lowercase-schema-option"),
    pytest.param("App.Tracker", None, ("APP", "TRACKER"), id="unquoted-mixed-case"),
    pytest.param("app.tracker", None, ("APP", "TRACKER"), id="unquoted-lowercase"),
]


@pytest.mark.parametrize(("table_name", "table_schema", "cataloged_name"), CATALOG_NAME_CASES)
async def test_tracking_table_columns_are_read_by_exact_catalog_name(
    db2_mode: DriverMode, table_name: str, table_schema: "str | None", cataloged_name: "tuple[str, str]"
) -> None:
    """The column check finds the table under its exact catalog name, so it adds no columns."""
    tracker = db2_mode.tracker(table_name, table_schema)
    driver = _catalog_driver(db2_mode, set())

    await db2_mode.call(tracker.ensure_tracking_table, driver)

    assert driver.catalog == {cataloged_name}
    assert driver.lookups == [cataloged_name]
    assert len(driver.executed) == 1


@pytest.mark.parametrize(("table_name", "table_schema", "cataloged_name"), CATALOG_NAME_CASES)
async def test_read_applied_migrations_finds_table_by_exact_catalog_name(
    db2_mode: DriverMode,
    monkeypatch: pytest.MonkeyPatch,
    table_name: str,
    table_schema: "str | None",
    cataloged_name: "tuple[str, str]",
) -> None:
    """Applied records are read when the tracking table exists under its exact catalog schema."""
    migration_config = {"version_table_name": table_name, "script_location": "migrations"}
    if table_schema:
        migration_config["version_table_schema"] = table_schema
    config = db2_mode.config(connection_config={"database": "x"}, migration_config=migration_config)
    commands: Any = (AsyncMigrationCommands if db2_mode.is_async else SyncMigrationCommands)(config)
    applied = [{"version_num": "0001"}]

    def read_applied(_driver: Any) -> Any:
        return db2_mode.call(lambda: applied) if db2_mode.is_async else applied

    monkeypatch.setattr(type(commands.tracker), "get_applied_migrations", lambda _self, driver: read_applied(driver))
    driver = _catalog_driver(db2_mode, {cataloged_name})

    assert await db2_mode.call(commands._read_applied_migrations, driver) == applied
    assert driver.lookups[0][0] == cataloged_name[0]


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
    assert abs(applied_at - datetime.now(UTC).replace(tzinfo=None)) < timedelta(seconds=1)
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
    assert abs(applied_at - datetime.now(UTC).replace(tzinfo=None)) < timedelta(seconds=1)
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


async def test_migration_schema_operations_translate_exceptions(db2_mode: DriverMode) -> None:
    """Migration schema operations map native driver errors to SQLSpec exceptions."""
    error = db2_error(sqlcode=-551, sqlstate="42501", text="permission denied")
    connection = FakeDb2Connection(cursors=lambda: FakeDb2Cursor(error=error))
    driver = db2_mode.driver(connection)

    with pytest.raises(PermissionDeniedError):
        await db2_mode.call(driver.has_schema, "app")

    with pytest.raises(PermissionDeniedError):
        await db2_mode.call(driver.set_migration_session_schema, "app")


def test_configs_use_db2_trackers() -> None:
    """Each Db2 config tracks migrations with the Db2 tracker of its mode."""
    assert Db2SyncConfig.migration_tracker_type is Db2SyncMigrationTracker
    assert Db2AsyncConfig.migration_tracker_type is Db2AsyncMigrationTracker


@pytest.mark.parametrize("driver_name", ["Db2SyncDriver", "Db2AsyncDriver"])
def test_db2_drivers_resolve_db_system(driver_name: str) -> None:
    """Db2 drivers report the Db2 database system in observability records."""
    assert resolve_db_system(driver_name) == "db2"
