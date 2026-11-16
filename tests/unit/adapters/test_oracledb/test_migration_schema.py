"""Unit coverage for Oracle migration schema hooks."""

from typing import Any

import pytest

from sqlspec.adapters.oracledb.config import OracleAsyncConfig, OracleSyncConfig
from sqlspec.adapters.oracledb.driver import OracleAsyncDriver, OracleSyncDriver
from sqlspec.adapters.oracledb.migrations import OracleAsyncMigrationTracker, OracleSyncMigrationTracker
from sqlspec.exceptions import ImproperConfigurationError, MigrationError

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


class _OracleMode:
    """Build the sync or async driver and fake cursor a shared test body exercises."""

    def __init__(self, is_async: bool) -> None:
        self.is_async = is_async

    def session(self, current_schema: str = "APP_USER") -> "tuple[Any, Any]":
        if self.is_async:
            async_cursor = FakeAsyncCursor(current_schema=current_schema)
            return OracleAsyncDriver(FakeAsyncConnection(async_cursor)), async_cursor  # type: ignore[arg-type]
        sync_cursor = FakeSyncCursor(current_schema=current_schema)
        return OracleSyncDriver(FakeSyncConnection(sync_cursor)), sync_cursor  # type: ignore[arg-type]

    async def call(self, function: Any, *args: Any) -> None:
        result = function(*args)
        if self.is_async:
            await result


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def oracle_mode(request: pytest.FixtureRequest) -> _OracleMode:
    return _OracleMode(bool(request.param))


@pytest.mark.anyio
@pytest.mark.parametrize(("current_schema", "quoted_schema"), RESET_CASES)
async def test_oracle_reset_restores_captured_schema(
    oracle_mode: _OracleMode, current_schema: str, quoted_schema: str
) -> None:
    driver, cursor = oracle_mode.session(current_schema)

    await oracle_mode.call(driver.set_migration_session_schema, "TENANT")
    await oracle_mode.call(driver.reset_migration_session_schema)

    assert cursor.executed[-1] == (f"ALTER SESSION SET CURRENT_SCHEMA = {quoted_schema}", None)


@pytest.mark.anyio
async def test_oracle_second_switch_keeps_first_capture(oracle_mode: _OracleMode) -> None:
    driver, cursor = oracle_mode.session()

    await oracle_mode.call(driver.set_migration_session_schema, "ONE")
    await oracle_mode.call(driver.set_migration_session_schema, "TWO")
    await oracle_mode.call(driver.reset_migration_session_schema)

    assert cursor.executed.count(CAPTURE) == 1
    assert cursor.executed[-1] == ('ALTER SESSION SET CURRENT_SCHEMA = "APP_USER"', None)


@pytest.mark.anyio
async def test_oracle_reset_without_switch_is_noop(oracle_mode: _OracleMode) -> None:
    driver, cursor = oracle_mode.session()

    await oracle_mode.call(driver.reset_migration_session_schema)
    assert cursor.executed == []

    await oracle_mode.call(driver.set_migration_session_schema, "TENANT")
    await oracle_mode.call(driver.reset_migration_session_schema)
    statements_after_first_reset = list(cursor.executed)
    await oracle_mode.call(driver.reset_migration_session_schema)

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


class _ScriptRecordingDataDictionary:
    def get_columns(self, _driver: Any, _table: str, schema: str | None = None) -> list[dict[str, Any]]:
        _ = schema
        return [
            {"column_name": name}
            for name in (
                "VERSION_NUM",
                "VERSION_TYPE",
                "EXECUTION_SEQUENCE",
                "DESCRIPTION",
                "APPLIED_AT",
                "EXECUTION_TIME_MS",
                "CHECKSUM",
                "APPLIED_BY",
                "REPLACES",
            )
        ]


class _AsyncScriptRecordingDataDictionary:
    async def get_columns(self, _driver: Any, _table: str, schema: str | None = None) -> list[dict[str, Any]]:
        _ = schema
        return _ScriptRecordingDataDictionary().get_columns(_driver, _table, schema=schema)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_oracle_ensure_tracking_table_escapes_single_quotes_in_quoted_identifier(is_async: bool) -> None:
    """Quoted Oracle tracker table names containing apostrophes double single quotes inside EXECUTE IMMEDIATE."""
    scripts: list[str] = []
    if is_async:
        tracker_async = OracleAsyncMigrationTracker(version_table_name='"mig\'versions"')

        class _AsyncScriptDriver:
            data_dictionary = _AsyncScriptRecordingDataDictionary()

            async def execute_script(self, sql: str) -> None:
                scripts.append(sql)

            async def commit(self) -> None:
                return None

        await tracker_async.ensure_tracking_table(_AsyncScriptDriver())  # type: ignore[arg-type]
    else:
        tracker_sync = OracleSyncMigrationTracker(version_table_name='"mig\'versions"')

        class _SyncScriptDriver:
            data_dictionary = _ScriptRecordingDataDictionary()

            def execute_script(self, sql: str) -> None:
                scripts.append(sql)

            def commit(self) -> None:
                return None

        tracker_sync.ensure_tracking_table(_SyncScriptDriver())  # type: ignore[arg-type]

    assert len(scripts) == 1
    assert "CREATE TABLE \"mig''versions\" (" in scripts[0]


def test_oracle_ensure_tracking_table_rejects_malformed_mutated_version_table() -> None:
    """Mutated unquoted version_table with SQL delimiters raises ImproperConfigurationError before DDL execution."""
    tracker = OracleSyncMigrationTracker()
    tracker.version_table = "bad'; DROP TABLE x; --"
    with pytest.raises(ImproperConfigurationError):
        tracker.ensure_tracking_table(RecordingSyncDriver())  # type: ignore[arg-type]


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
async def test_oracle_add_missing_replaces_column_quotes_uppercase_identifier(is_async: bool) -> None:
    """Adding missing migration columns quotes uppercase Oracle identifiers so queries match."""
    executed: list[str] = []
    missing_replaces = [
        {"column_name": name}
        for name in (
            "VERSION_NUM",
            "VERSION_TYPE",
            "EXECUTION_SEQUENCE",
            "DESCRIPTION",
            "APPLIED_AT",
            "EXECUTION_TIME_MS",
            "CHECKSUM",
            "APPLIED_BY",
        )
    ]

    if is_async:
        tracker_async = OracleAsyncMigrationTracker()

        class _AsyncDataDictionary:
            async def get_columns(self, _driver: Any, _table: str, schema: str | None = None) -> list[dict[str, Any]]:
                _ = schema
                return missing_replaces

        class _AsyncAddColumnDriver:
            data_dictionary = _AsyncDataDictionary()

            async def execute_script(self, sql: str) -> None:
                _ = sql

            async def execute(self, sql: str) -> None:
                executed.append(sql)

            async def commit(self) -> None:
                return None

        await tracker_async.ensure_tracking_table(_AsyncAddColumnDriver())  # type: ignore[arg-type]
    else:
        tracker_sync = OracleSyncMigrationTracker()

        class _SyncDataDictionary:
            def get_columns(self, _driver: Any, _table: str, schema: str | None = None) -> list[dict[str, Any]]:
                _ = schema
                return missing_replaces

        class _SyncAddColumnDriver:
            data_dictionary = _SyncDataDictionary()

            def execute_script(self, sql: str) -> None:
                _ = sql

            def execute(self, sql: str) -> None:
                executed.append(sql)

            def commit(self) -> None:
                return None

        tracker_sync.ensure_tracking_table(_SyncAddColumnDriver())  # type: ignore[arg-type]

    assert len(executed) == 1
    assert 'ALTER TABLE "DDL_MIGRATIONS"' in executed[0]
    assert 'ADD "REPLACES" VARCHAR2(4000)' in executed[0]
    assert '"replaces"' not in executed[0]
