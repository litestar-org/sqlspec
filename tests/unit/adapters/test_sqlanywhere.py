"""Unit tests for the SAP SQL Anywhere (sqlanydb) adapter."""

from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.sqlanywhere import (
    DEFAULT_SQLANYWHERE_PORT,
    SqlanywhereConfig,
    SqlanywhereConnection,
    SqlanywhereConnectionPool,
    SqlanywhereCursor,
    SqlanywhereDataDictionary,
    SqlanywhereDriver,
    SqlanywhereExceptionHandler,
    SqlanywhereSyncDataDictionary,
    build_connection_config,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
)
from sqlspec.adapters.sqlanywhere._typing import SqlanywhereError
from sqlspec.adapters.sqlanywhere.adk import SqlanywhereADKConfig, SqlanywhereADKMemoryStore, SqlanywhereADKStore
from sqlspec.adapters.sqlanywhere.events import SqlanywhereEventQueueStore
from sqlspec.adapters.sqlanywhere.litestar import SqlanywhereStore
from sqlspec.adapters.sqlanywhere.migrations import SqlanywhereSyncMigrationTracker
from sqlspec.core import SQL, ParameterStyle
from sqlspec.data_dictionary.dialects.sqlanywhere import SqlanywhereVersionInfo
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    MissingDependencyError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    SerializationConflictError,
    SQLParsingError,
    UniqueViolationError,
)


class FakeSqlanydbError(Exception):
    """Fake sqlanydb Error for SQL Anywhere unit tests."""


class FakeCursor:
    """Minimal DB-API 2.0 cursor for SQL Anywhere unit tests."""

    def __init__(
        self, rows: "list[Any] | None" = None, description: "list[tuple[str, ...]] | None" = None, rowcount: int = -1
    ) -> None:
        self.rows = rows or []
        self.description = description
        self.rowcount = rowcount
        self.closed = False
        self.calls: list[tuple[str, Any]] = []
        self.many_calls: list[tuple[str, Any]] = []
        self.fetchmany_sizes: list[int] = []

    def execute(self, sql: str, parameters: Any = None) -> None:
        self.calls.append((sql, parameters))

    def executemany(self, sql: str, parameters: Any = None) -> None:
        self.many_calls.append((sql, parameters))

    def fetchall(self) -> "list[Any]":
        return self.rows

    def fetchmany(self, size: int) -> "list[Any]":
        self.fetchmany_sizes.append(size)
        chunk = self.rows[:size]
        self.rows = self.rows[size:]
        return chunk

    def fetchone(self) -> Any:
        return self.rows[0] if self.rows else None

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    """Minimal sqlanydb connection for SQL Anywhere unit tests."""

    def __init__(self, cursor: "FakeCursor | None" = None) -> None:
        self.cursor_obj = cursor or FakeCursor()
        self.closed = False
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, *args: Any, **kwargs: Any) -> FakeCursor:
        self.cursor_args = args
        self.cursor_kwargs = kwargs
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def test_build_connection_config_normalizes_friendly_keys_and_strips_pool_keys() -> None:
    """build_connection_config maps friendly keys into sqlanydb connection keys and strips pool parameters."""
    cfg = build_connection_config({
        "username": "dba",
        "password": "sql",
        "database": "demo",
        "server": "demo17",
        "database_file": "/var/sa/demo.db",
        "host": "sa.internal",
        "pool_min_size": 2,
        "pool_max_size": 8,
        "pool_timeout": 15.0,
        "pool_recycle_seconds": 600,
        "health_check_interval": 30.0,
        "extra": {"cset": "UTF-8", "ignored": None},
    })

    assert cfg["uid"] == "dba"
    assert "username" not in cfg
    assert "user" not in cfg
    assert cfg["pwd"] == "sql"
    assert "password" not in cfg
    assert cfg["dbn"] == "demo"
    assert "database" not in cfg
    assert cfg["eng"] == "demo17"
    assert "server" not in cfg
    assert cfg["dbf"] == "/var/sa/demo.db"
    assert "database_file" not in cfg
    assert cfg["host"] == f"sa.internal:{DEFAULT_SQLANYWHERE_PORT}"
    assert "port" not in cfg
    assert cfg["cset"] == "UTF-8"
    assert "pool_min_size" not in cfg
    assert "pool_max_size" not in cfg
    assert "pool_timeout" not in cfg
    assert "pool_recycle_seconds" not in cfg
    assert "health_check_interval" not in cfg
    assert "extra" not in cfg


def test_build_connection_config_combines_host_and_explicit_port() -> None:
    """build_connection_config combines host and explicit port into host:port."""
    cfg = build_connection_config({"user": "dba", "pwd": "sql", "host": "10.0.0.5", "port": 2640, "dsn": "SADemo"})
    assert cfg["uid"] == "dba"
    assert cfg["pwd"] == "sql"
    assert cfg["host"] == "10.0.0.5:2640"
    assert "port" not in cfg
    assert cfg["dsn"] == "SADemo"


def test_sqlanywhere_pool_checkout_release_and_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """SqlanywhereConnectionPool manages connection acquisition, release, and pool closure."""
    import sqlspec.adapters.sqlanywhere.pool as pool_module

    captured_kwargs: list[dict[str, Any]] = []
    fake_conn = FakeConnection(FakeCursor(rows=[(1,)], description=[("1",)]))

    class FakeSqlanydbModule:
        def connect(self, **kwargs: Any) -> FakeConnection:
            captured_kwargs.append(kwargs)
            return fake_conn

    monkeypatch.setattr(pool_module, "_load_sqlanydb_module", lambda: FakeSqlanydbModule())

    cfg = build_connection_config({"user": "dba", "password": "sql", "database": "demo", "host": "localhost"})
    pool = SqlanywhereConnectionPool(cfg, min_size=1, max_size=2)
    conn = pool.acquire()
    assert cast("object", conn) is fake_conn
    assert captured_kwargs[0]["uid"] == "dba"
    assert captured_kwargs[0]["pwd"] == "sql"
    assert captured_kwargs[0]["dbn"] == "demo"
    assert captured_kwargs[0]["host"] == "localhost:2638"

    pool.release(conn)
    assert pool.size() == 1
    pool.close()
    assert fake_conn.closed is True


def test_sqlanywhere_pool_raises_missing_dependency_when_sqlanydb_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """SqlanywhereConnectionPool raises MissingDependencyError when sqlanydb is not installed."""
    import sqlspec.adapters.sqlanywhere.pool as pool_module

    def _raise_import_error() -> Any:
        msg = "No module named 'sqlanydb'"
        raise ImportError(msg)

    monkeypatch.setattr(pool_module, "import_module", lambda name: _raise_import_error())

    pool = SqlanywhereConnectionPool({"uid": "dba", "pwd": "sql"})
    with pytest.raises(MissingDependencyError, match="sqlanydb"):
        pool.acquire()


@pytest.mark.parametrize(
    ("args", "expected_type"),
    [
        (("23505", -193, "Primary key for table 'users' is not unique"), UniqueViolationError),
        (("23505", -196, "Index 'ix_email' for table 'users' would not be unique"), UniqueViolationError),
        (("23503", -194, "No primary key value for foreign key 'fk_user'"), ForeignKeyViolationError),
        (("23503", -198, "Primary key for row in table 'users' is referenced"), ForeignKeyViolationError),
        (("23502", -195, "Column 'email' in table 'users' cannot be NULL"), NotNullViolationError),
        (("23506", -209, "Invalid value for column 'age'"), CheckViolationError),
        ((None, -1091, "Constraint 'chk_age' violated"), CheckViolationError),
        (("40001", -306, "Deadlock detected"), DeadlockError),
        ((None, -307, "All threads are blocked"), DeadlockError),
        ((None, -684, "Rollback occurred due to deadlock"), DeadlockError),
        ((None, -210, "User 'another' has the row in 'users' locked"), SerializationConflictError),
        (("42000", -131, "Syntax error near 'SELEC'"), SQLParsingError),
        (("42S02", -141, "Table 'missing_tbl' not found"), NotFoundError),
        (("42S22", -143, "Column 'missing_col' not found"), NotFoundError),
        (("08001", -100, "Database server not found"), DatabaseConnectionError),
        (("28000", -103, "Invalid user ID or password"), DatabaseConnectionError),
        ((None, -83, "Specified database not found"), DatabaseConnectionError),
        ((None, -308, "Connection was terminated"), DatabaseConnectionError),
        ((None, -832, "Connection error"), DatabaseConnectionError),
        ((None, -101, "Not connected to a database"), OperationalError),
        (("22018", -157, "Cannot convert 'abc' to a numeric"), DataError),
        (("22003", -158, "Value 999999 out of range for destination"), DataError),
        (("42501", -121, "Permission denied: you do not have permission"), PermissionDeniedError),
    ],
)
def test_create_mapped_exception_maps_sqlanywhere_sqlcode_and_sqlstate(
    args: tuple[Any, ...], expected_type: type[Exception]
) -> None:
    """create_mapped_exception maps SQL Anywhere negative SQLCODEs and SQLSTATEs to SQLSpec exceptions."""
    err = FakeSqlanydbError(*args)
    mapped = create_mapped_exception(err)
    assert isinstance(mapped, expected_type)


def test_sqlanywhere_driver_profile_and_statement_config() -> None:
    """Sqlanywhere driver profile uses QMARK execution and sqlanywhere dialect."""
    assert driver_profile.name == "sqlanywhere"
    assert driver_profile.default_style == ParameterStyle.QMARK
    assert driver_profile.default_execution_style == ParameterStyle.QMARK
    assert driver_profile.default_dialect == "sqlanywhere"
    assert default_statement_config.dialect == "sqlanywhere"


def test_sqlanywhere_cursor_context_manager_closes_cursor() -> None:
    """SqlanywhereCursor creates and closes the underlying DB-API cursor."""
    fake_cursor = FakeCursor()
    fake_conn = FakeConnection(fake_cursor)
    with SqlanywhereCursor(cast("SqlanywhereConnection", fake_conn)) as cursor:
        assert cursor is fake_cursor
    assert fake_cursor.closed is True


def test_sqlanywhere_exception_handler_defers_mapped_exception() -> None:
    """SqlanywhereExceptionHandler captures DB-API errors into pending_exception."""
    handler = SqlanywhereExceptionHandler()
    with handler:
        raise FakeSqlanydbError("23505", -193, "Primary key not unique")
    assert isinstance(handler.pending_exception, UniqueViolationError)


def test_sqlanywhere_driver_execute_many_script_and_transactions() -> None:
    """SqlanywhereDriver executes SELECT, DML, execute_many, execute_script, and transactions."""
    select_cursor = FakeCursor(rows=[(1, "Ada")], description=[("id",), ("name",)], rowcount=1)
    conn = FakeConnection(select_cursor)
    driver = SqlanywhereDriver(cast("SqlanywhereConnection", conn))

    select_result = driver.execute(SQL("SELECT id, name FROM DBA.users WHERE id = ?", 1))
    assert select_result.get_data() == [{"id": 1, "name": "Ada"}]
    assert select_cursor.calls[0][1] == (1,)

    dml_cursor = FakeCursor(rows=[], description=None, rowcount=2)
    conn.cursor_obj = dml_cursor
    many_result = driver.execute_many(SQL("INSERT INTO DBA.users (id, name) VALUES (?, ?)"), [(1, "Ada"), (2, "Grace")])
    assert many_result.rows_affected == 2
    assert len(dml_cursor.many_calls) == 1

    script_cursor = FakeCursor(rows=[], description=None, rowcount=1)
    conn.cursor_obj = script_cursor
    script_result = driver.execute_script("CREATE TABLE IF NOT EXISTS DBA.t1 (id INT); INSERT INTO DBA.t1 VALUES (1);")
    assert script_result.total_statements == 2

    driver.begin()
    driver.commit()
    assert conn.commits == 1

    driver.begin()
    driver.rollback()
    assert conn.rollbacks == 1


def test_sqlanywhere_driver_select_to_arrow() -> None:
    """SqlanywhereDriver.select_to_arrow streams rows into a PyArrow table."""
    cursor = FakeCursor(rows=[(1, "Ada"), (2, "Grace")], description=[("id",), ("name",)], rowcount=2)
    conn = FakeConnection(cursor)
    driver = SqlanywhereDriver(cast("SqlanywhereConnection", conn))

    arrow_result = driver.select_to_arrow("SELECT id, name FROM DBA.users")
    assert arrow_result.rows_affected == 2
    assert arrow_result.to_dict() == [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]


def test_sqlanywhere_data_dictionary_version_and_metadata() -> None:
    """SqlanywhereSyncDataDictionary parses version banners and builds table/column/index/FK metadata."""
    dd = SqlanywhereSyncDataDictionary()
    assert SqlanywhereDataDictionary is SqlanywhereSyncDataDictionary

    fake_driver = MagicMock(spec=SqlanywhereDriver)
    fake_driver.select_one_or_none.return_value = {"version_string": "17.0.11.7458"}
    fake_driver.select_value_or_none.return_value = "DBA"

    def _select_side_effect(query: Any, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        _ = (args, kwargs)
        sql_text = str(query)
        if "SYS.SYSTABCOL" in sql_text or "SYS.SYSCOLUMN" in sql_text:
            return [
                {
                    "schema_name": "DBA",
                    "table_name": "users",
                    "column_name": "id",
                    "data_type": "integer",
                    "is_nullable": 0,
                    "column_default": "autoincrement",
                    "ordinal_position": 1,
                    "max_length": 4,
                    "numeric_scale": 0,
                    "is_primary": 1,
                    "is_unique": 1,
                    "is_identity": 1,
                    "identity_generation": "BY DEFAULT",
                    "is_generated": 0,
                },
                {
                    "schema_name": "DBA",
                    "table_name": "users",
                    "column_name": "payload",
                    "data_type": "long nvarchar",
                    "is_nullable": 1,
                    "column_default": None,
                    "ordinal_position": 2,
                    "max_length": 2147483647,
                    "numeric_scale": 0,
                    "is_primary": 0,
                    "is_unique": 0,
                    "is_identity": 0,
                    "identity_generation": None,
                    "is_generated": 0,
                },
            ]
        if "SYS.SYSTAB" in sql_text and "SYS.SYSIDX" not in sql_text:
            return [{"schema_name": "DBA", "table_name": "users", "table_type": "BASE TABLE"}]
        return []

    fake_driver.select.side_effect = _select_side_effect

    version = dd.get_version(fake_driver)
    assert isinstance(version, SqlanywhereVersionInfo)
    assert (version.major, version.minor, version.patch, version.build_number) == (17, 0, 11, "7458")
    assert dd.get_feature_flag(fake_driver, "supports_cte") is True
    assert dd.get_feature_flag(fake_driver, "supports_merge") is True
    assert dd.get_optimal_type(fake_driver, "uuid") == "UNIQUEIDENTIFIER"
    assert dd.get_optimal_type(fake_driver, "blob") == "LONG BINARY"

    tables = dd.get_tables(fake_driver, schema="DBA")
    assert len(tables) == 1
    assert tables[0]["table_name"] == "users"

    columns = dd.get_columns(fake_driver, table="users", schema="DBA")
    assert len(columns) == 2
    assert columns[0]["identity_generation"] == "BY DEFAULT"
    assert columns[1]["data_type"] == "long nvarchar"


def test_sqlanywhere_migration_tracker_and_extension_stores() -> None:
    """SqlanywhereSyncMigrationTracker, SqlanywhereStore, SqlanywhereEventQueueStore, and SqlanywhereADKStore emit SQL Anywhere DDL."""
    tracker = SqlanywhereSyncMigrationTracker(version_table_name="DBA.ddl_migrations")
    ddl_text = tracker._idempotent_tracking_table_ddl_text()
    assert "CREATE TABLE IF NOT EXISTS" in ddl_text
    assert "VARCHAR(32)" in ddl_text
    assert "TIMESTAMP" in ddl_text
    assert "CURRENT UTC TIMESTAMP" in ddl_text
    assert "LONG VARCHAR" in ddl_text
    assert "NVARCHAR(MAX)" not in ddl_text

    config = SqlanywhereConfig(
        connection_config={"user": "dba", "password": "sql", "database": "demo"},
        extension_config={
            "litestar": {"session_table": "litestar_session", "manage_schema": False},
            "events": {"queue_table": "event_queue"},
            "adk": {
                "session_table": "adk_sessions",
                "events_table": "adk_events",
                "memory_table": "adk_memory",
                "owner_id_column": "tenant_id INTEGER NOT NULL REFERENCES DBA.tenants(id)",
            },
        },
    )

    litestar_store = SqlanywhereStore(config)
    create_litestar_sql = litestar_store._table_ddl()
    assert "CREATE TABLE IF NOT EXISTS" in create_litestar_sql
    assert "LONG BINARY" in create_litestar_sql
    assert "TIMESTAMP" in create_litestar_sql
    drop_litestar_sql = litestar_store._drop_table_sql()
    assert "DROP TABLE IF EXISTS" in drop_litestar_sql[0]

    event_store = SqlanywhereEventQueueStore(config)
    assert SqlanywhereEventQueueStore is SqlanywhereEventQueueStore
    event_statements = event_store.create_statements()
    assert "LONG VARCHAR" in event_statements[0]
    assert "TIMESTAMP" in event_statements[0]
    assert "CURRENT UTC TIMESTAMP" in event_statements[0]

    adk_store = SqlanywhereADKStore(config)
    assert isinstance(adk_store.config, SqlanywhereConfig)
    session_ddl = adk_store._sessions_table_ddl()
    events_ddl = adk_store._events_table_ddl()
    assert "CREATE TABLE IF NOT EXISTS" in session_ddl
    assert "LONG VARCHAR" in session_ddl
    assert "tenant_id INTEGER NOT NULL REFERENCES DBA.tenants(id)" in session_ddl
    assert "LONG VARCHAR" in events_ddl
    assert "LONG BINARY" in events_ddl
    assert "BIT NULL" in events_ddl

    memory_store = SqlanywhereADKMemoryStore(config)
    memory_ddl = memory_store._memory_table_ddl()
    assert "CREATE TABLE IF NOT EXISTS" in memory_ddl
    assert "LONG VARCHAR" in memory_ddl
    assert "TIMESTAMP" in memory_ddl
    assert isinstance(SqlanywhereADKConfig(), dict)


@pytest.mark.anyio
async def test_sqlanywhere_litestar_and_adk_stores_roundtrip_with_mocked_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SqlanywhereStore, SqlanywhereADKStore, and SqlanywhereADKMemoryStore execute operations cleanly."""
    config = SqlanywhereConfig(connection_config={"user": "dba", "password": "sql", "database": "demo"})
    litestar_store = SqlanywhereStore(config)

    monkeypatch.setattr(SqlanywhereStore, "_get", MagicMock(return_value=b"payload"))
    monkeypatch.setattr(SqlanywhereStore, "_set", MagicMock())
    monkeypatch.setattr(SqlanywhereStore, "_delete", MagicMock())
    monkeypatch.setattr(SqlanywhereStore, "_exists", MagicMock(return_value=True))
    monkeypatch.setattr(SqlanywhereStore, "_expires_in", MagicMock(return_value=90))
    monkeypatch.setattr(SqlanywhereStore, "_delete_expired", MagicMock(return_value=3))

    await litestar_store.set("sess-1", "payload", expires_in=90)
    assert await litestar_store.get("sess-1") == b"payload"
    assert await litestar_store.exists("sess-1") is True
    assert await litestar_store.expires_in("sess-1") == 90
    assert await litestar_store.delete_expired() == 3
    await litestar_store.delete("sess-1")

    captured: list[tuple[str, tuple[Any, ...]]] = []

    def _capture(_self: SqlanywhereADKStore, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        captured.append((sql, tuple(params)))
        return []

    monkeypatch.setattr(SqlanywhereADKStore, "_execute_fetchall", _capture)
    adk_store = SqlanywhereADKStore(config)
    adk_store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)
    sql, params = captured[0]
    normalized_sql = " ".join(sql.split())
    assert "ORDER BY create_time ASC, id ASC LIMIT ? OFFSET ?" in normalized_sql
    assert params == ("app", "u1", 10, 20)

    monkeypatch.setattr(SqlanywhereADKMemoryStore, "insert_memory_entries", MagicMock(return_value=1))
    memory_store = SqlanywhereADKMemoryStore(config)
    assert memory_store.insert_memory_entries([]) == 1


def test_sqlanywhere_cursor_handles_cursor_without_close() -> None:
    """SqlanywhereCursor safely handles raw cursors that lack a close() method."""

    class CursorWithoutClose:
        pass

    class ConnWithBareCursor:
        def cursor(self) -> CursorWithoutClose:
            return CursorWithoutClose()

    with SqlanywhereCursor(cast("SqlanywhereConnection", ConnWithBareCursor())) as cursor:
        assert isinstance(cursor, CursorWithoutClose)


def test_sqlanywhere_adk_store_handles_native_missing_table(monkeypatch: pytest.MonkeyPatch) -> None:
    """SqlanywhereADKStore.get_session returns None when native driver signals table missing."""
    config = SqlanywhereConfig(connection_config={"user": "dba", "password": "sql", "database": "demo"})
    store = SqlanywhereADKStore(config)

    class FakeSqlanydbError(SqlanywhereError):
        def __init__(self, sqlstate: str, sqlcode: int, message: str) -> None:
            super().__init__(message)
            self.sqlstate = sqlstate
            self.sqlcode = sqlcode

    def _table_missing(_self: Any, *_args: Any, **_kwargs: Any) -> None:
        raise FakeSqlanydbError("42S02", -141, "Table 'adk_session' not found")

    monkeypatch.setattr(SqlanywhereADKStore, "_execute_fetchone", _table_missing)
    assert store.get_session("app", "user", "sess") is None
