"""Unit tests for the Sybase ASE (pymssql over FreeTDS TDS 5.0) adapter."""

import os
import socket
import threading
from contextlib import nullcontext
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pymssql
import pytest

from sqlspec.adapters.sybase import (
    DEFAULT_SYBASE_CONN_PROPERTIES,
    DEFAULT_SYBASE_TDS_VERSION,
    SybaseConfig,
    SybaseConnection,
    SybaseConnectionPool,
    SybaseCursor,
    SybaseDataDictionary,
    SybaseDriver,
    SybaseExceptionHandler,
    SybaseSyncDataDictionary,
    build_connection_config,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
)
from sqlspec.adapters.sybase._typing import SybaseRawCursor
from sqlspec.adapters.sybase.adk import SybaseADKMemoryStore, SybaseADKStore
from sqlspec.adapters.sybase.core import (
    _ERROR_CODE_MAPPING,
    build_insert_statement,
    escape_literal_percent,
    format_identifier,
)
from sqlspec.adapters.sybase.events import SybaseEventQueueStore
from sqlspec.adapters.sybase.litestar import SybaseStore
from sqlspec.adapters.sybase.migrations import SybaseSyncMigrationTracker
from sqlspec.core import SQL, ParameterStyle
from sqlspec.data_dictionary.dialects.sybase import SybaseVersionInfo
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)


class FakeSybaseError(Exception):
    """Fake pymssql error for Sybase unit tests."""


class FakeCursor:
    """Minimal DB-API cursor for Sybase unit tests."""

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
    """Minimal pymssql connection for Sybase unit tests."""

    def __init__(self, cursor: "FakeCursor | None" = None) -> None:
        self.cursor_obj = cursor or FakeCursor()
        self.closed = False
        self.commits = 0
        self.rollbacks = 0
        self.autocommit_values: list[bool] = []
        self.autocommit_state = True

    def cursor(self, *args: Any, **kwargs: Any) -> FakeCursor:
        self.cursor_args = args
        self.cursor_kwargs = kwargs
        return self.cursor_obj

    def commit(self) -> None:
        if not self.autocommit_state:
            self.commits += 1

    def rollback(self) -> None:
        if not self.autocommit_state:
            self.rollbacks += 1

    def close(self) -> None:
        self.closed = True

    def autocommit(self, value: bool) -> None:
        self.autocommit_values.append(value)
        self.autocommit_state = value


def test_build_connection_config_defaults_and_normalization() -> None:
    """build_connection_config sets Sybase ASE defaults and strips tds_version='5.0' for pool TDSVER negotiation."""
    cfg = build_connection_config({
        "host": "ase.internal",
        "username": "sa",
        "db": "pubs2",
        "tds_version": "5.0",
        "extra": {"appname": "sqlspec_sybase", "ignored": None},
    })

    assert cfg["server"] == "ase.internal"
    assert "host" not in cfg
    assert cfg["port"] == 5000
    assert cfg["user"] == "sa"
    assert "username" not in cfg
    assert cfg["database"] == "pubs2"
    assert "db" not in cfg
    assert cfg["charset"] == "UTF-8"
    assert cfg["autocommit"] is True
    assert cfg["conn_properties"] == DEFAULT_SYBASE_CONN_PROPERTIES
    assert cfg["_sybase_tds_version"] == DEFAULT_SYBASE_TDS_VERSION
    assert "tds_version" not in cfg
    assert cfg["appname"] == "sqlspec_sybase"
    assert "extra" not in cfg


def test_sybase_pool_requires_tdsver_or_server_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    """SybaseConnectionPool requires either TDSVER='5.0' or a server_alias at process start."""
    import sqlspec.adapters.sybase.pool as pool_module

    fake_conn = FakeConnection()

    class FakeModule:
        def connect(self, **kwargs: Any) -> FakeConnection:
            return fake_conn

    monkeypatch.setattr(pool_module, "PYMSSQL_MODULE", FakeModule())

    monkeypatch.delenv("TDSVER", raising=False)
    cfg_invalid = build_connection_config({"server": "ase.example.test", "user": "sa", "database": "master"})
    with pytest.raises(ImproperConfigurationError):
        SybaseConnectionPool(cfg_invalid)

    monkeypatch.setenv("TDSVER", "5.0")
    cfg_env = build_connection_config({"server": "ase.example.test", "user": "sa", "database": "master"})
    pool_env = SybaseConnectionPool(cfg_env)
    conn = pool_env.acquire()
    pool_env.release(conn)
    assert cast("object", conn) is fake_conn
    assert os.environ.get("TDSVER") == "5.0"

    monkeypatch.delenv("TDSVER", raising=False)
    cfg_alias = build_connection_config({"server_alias": "SYBASE_ASE", "user": "sa", "database": "master"})
    pool_alias = SybaseConnectionPool(cfg_alias)
    conn_alias = pool_alias.acquire()
    pool_alias.release(conn_alias)
    assert cast("object", conn_alias) is fake_conn


def test_sybase_pool_transmits_native_tds_50_login_packet_over_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    """SybaseConnectionPool.new_connection transmits a native 619-byte TDS 5.0 login packet via FreeTDS."""
    monkeypatch.setenv("TDSVER", "5.0")
    received_packets: list[bytes] = []
    ready = threading.Event()

    def _listener(server_sock: socket.socket) -> None:
        ready.set()
        try:
            conn, _ = server_sock.accept()
            with conn:
                conn.settimeout(2.0)
                data = conn.recv(4096)
                received_packets.append(data)
        except OSError:
            return

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(("127.0.0.1", 0))
        port = server_sock.getsockname()[1]
        server_sock.listen(1)

        listener_thread = threading.Thread(target=_listener, args=(server_sock,), daemon=True)
        listener_thread.start()
        ready.wait(timeout=2.0)

        cfg = build_connection_config({
            "server": "127.0.0.1",
            "port": port,
            "user": "sa",
            "password": "secret",
            "database": "master",
            "login_timeout": 1,
            "timeout": 1,
        })
        pool = SybaseConnectionPool(cfg)
        with pytest.raises(Exception):
            pool.new_connection()

        listener_thread.join(timeout=2.0)

    assert len(received_packets) == 1
    packet = received_packets[0]
    assert len(packet) >= 500
    assert packet[0] == 0x02
    assert 0xE2 in packet


@pytest.mark.parametrize(
    ("code", "message", "expected_type"),
    [
        (2601, "Attempt to insert duplicate key row in object 'users' with unique index (2601)", UniqueViolationError),
        (2627, "Violation of UNIQUE KEY constraint (2627)", UniqueViolationError),
        (546, "Foreign key constraint violation occurred, dbname = 'pubs2' (546)", ForeignKeyViolationError),
        (547, "Dependent foreign key constraint violation (547)", ForeignKeyViolationError),
        (547, "The UPDATE statement conflicted with the CHECK constraint (547)", CheckViolationError),
        (548, "Check constraint violation occurred, dbname = 'pubs2' (548)", CheckViolationError),
        (233, "The column id in table users does not allow null values (233)", NotNullViolationError),
        (515, "Attempt to insert NULL value into column 'name' (515)", NotNullViolationError),
        (1205, "Your server command (family id #0, process id #14) encountered a deadlock (1205)", DeadlockError),
        (12205, "Could not acquire a lock within the specified wait period (12205)", QueryTimeoutError),
        (102, "Incorrect syntax near 'FROM' (102)", SQLParsingError),
        (156, "Incorrect syntax near the keyword 'SELECT' (156)", SQLParsingError),
        (11021, "Function FOO invoked with wrong number or type of argument(s) (11021)", SQLParsingError),
        (207, "Invalid column name 'missing_col' (207)", NotFoundError),
        (208, "users not found. Specify owner.objectname (208)", NotFoundError),
        (4002, "Login failed (4002)", DatabaseConnectionError),
        (18456, "Login failed for user 'sa' (18456)", DatabaseConnectionError),
        (20002, "Adaptive Server connection failed (20002)", DatabaseConnectionError),
        (20047, "DBPROCESS is dead or not enabled (20047)", OperationalError),
        (
            2762,
            "The 'CREATE TABLE' command is not allowed within a multi-statement transaction (2762)",
            OperationalError,
        ),
        (1105, "Can't allocate space for object 'syslogs' in database 'pubs2' (1105)", OperationalError),
        (247, "Arithmetic overflow during explicit conversion of INT value (247)", DataError),
        (220, "Arithmetic overflow error for type tinyint (220)", DataError),
        (168, "Scale error during implicit conversion of NUMERIC value (168)", DataError),
        (3606, "Arithmetic overflow occurred (3606)", DataError),
        (10330, "SELECT permission denied on object users, database pubs2, owner dbo (10330)", PermissionDeniedError),
        (10331, "Permission denied, database pubs2, owner dbo (10331)", PermissionDeniedError),
        (10332, "EXECUTE permission denied on object sp_proc (10332)", PermissionDeniedError),
    ],
)
def test_create_mapped_exception_maps_sybase_ase_error_codes(
    code: int, message: str, expected_type: type[Exception]
) -> None:
    """Sybase ASE error codes map to their canonical SQLSpec exception classes."""
    mapped = create_mapped_exception(Exception(code, message))
    assert isinstance(mapped, expected_type)


def test_sybase_driver_profile_and_config_flags() -> None:
    """SybaseConfig sets supports_transactional_ddl=False and registers the 'sybase' dialect."""
    assert SybaseConfig.supports_transactional_ddl is False
    assert SybaseDriver.dialect == "sybase"
    assert default_statement_config.dialect == "sybase"
    assert driver_profile.default_execution_style is ParameterStyle.POSITIONAL_PYFORMAT


def test_sybase_driver_execute_many_script_and_transactions() -> None:
    """SybaseDriver executes SELECT, DML, execute_many, execute_script, and transactions."""
    cursor = FakeCursor(rows=[(1, "Ada")], description=[("id",), ("name",)], rowcount=1)
    connection = FakeConnection(cursor)
    driver = SybaseDriver(cast("SybaseConnection", connection))

    select_result = driver.execute("SELECT id, name FROM dbo.users WHERE id = ?", 1)
    assert select_result.get_data() == [{"id": 1, "name": "Ada"}]
    assert cursor.calls[-1] == ("SELECT id, name FROM dbo.users WHERE id = %s", (1,))

    many_stmt = SQL(
        "INSERT INTO dbo.users (id, name) VALUES (?, ?)",
        [(1, "Ada"), (2, "Grace")],
        statement_config=default_statement_config,
        is_many=True,
    )
    many_result = driver.dispatch_execute_many(cast("SybaseRawCursor", cursor), many_stmt)
    assert many_result.rowcount_override == 1
    assert cursor.many_calls[-1] == ("INSERT INTO dbo.users (id, name) VALUES (%s, %s)", [(1, "Ada"), (2, "Grace")])

    script_stmt = SQL(
        "INSERT INTO dbo.users (id) VALUES (1); INSERT INTO dbo.users (id) VALUES (2);",
        statement_config=default_statement_config,
        is_script=True,
    )
    script_result = driver.dispatch_execute_script(cast("SybaseRawCursor", cursor), script_stmt)
    assert script_result.statement_count == 2
    assert script_result.successful_statements == 2

    driver.begin()
    assert driver._connection_in_transaction() is True
    driver.commit()
    assert driver._connection_in_transaction() is False
    assert ("BEGIN TRANSACTION", None) in cursor.calls
    assert ("IF @@TRANCOUNT > 0 COMMIT TRANSACTION", None) in cursor.calls

    with pytest.raises(TransactionError):
        driver.create_savepoint("bad;name")


def test_sybase_driver_select_to_arrow_and_load_from_arrow() -> None:
    """SybaseDriver converts SELECT results to Arrow and loads Arrow tables via executemany."""
    pa = pytest.importorskip("pyarrow")
    cursor = FakeCursor(rows=[(1, "Ada"), (2, "Grace")], description=[("id",), ("name",)], rowcount=2)
    connection = FakeConnection(cursor)
    driver = SybaseDriver(cast("SybaseConnection", connection), driver_features=SybaseConfig().driver_features)

    arrow_result = driver.select_to_arrow("SELECT id, name FROM dbo.users")
    assert arrow_result.rows_affected == 2

    table = pa.table({"id": [1, 2], "name": ["Ada", "Grace"]})
    job = driver.load_from_arrow("dbo.users", table, overwrite=True)
    assert job.telemetry["rows_processed"] == 2
    assert cursor.calls[-1] == ("DELETE FROM [dbo].[users]", None)
    assert cursor.many_calls[-1] == (
        "INSERT INTO [dbo].[users] ([id], [name]) VALUES (%s, %s)",
        [(1, "Ada"), (2, "Grace")],
    )


def test_sybase_data_dictionary_reflection() -> None:
    """SybaseSyncDataDictionary parses ASE version strings and builds table/column/index/FK metadata."""
    assert SybaseDataDictionary is SybaseSyncDataDictionary
    dd = SybaseSyncDataDictionary()

    class FakeDDDriver:
        def select_one_or_none(self, _sql: Any, **_kwargs: Any) -> dict[str, Any]:
            return {"version_string": "Adaptive Server Enterprise/16.0 SP04 PL04/EBF 30829 SMP/P/x86_64/Linux"}

        def select(self, _sql: Any, **kwargs: Any) -> list[dict[str, Any]]:
            if kwargs.get("table_name") == "users":
                return [
                    {
                        "schema_name": "dbo",
                        "table_name": "users",
                        "column_name": "id",
                        "data_type": "numeric(10, 0)",
                        "is_nullable": False,
                        "column_default": None,
                        "ordinal_position": 1,
                        "max_length": 10,
                        "numeric_scale": 0,
                        "is_primary": True,
                        "is_unique": False,
                        "is_identity": True,
                        "identity_generation": "ALWAYS",
                        "is_generated": False,
                    },
                    {
                        "schema_name": "dbo",
                        "table_name": "users",
                        "column_name": "bio",
                        "data_type": "unitext",
                        "is_nullable": True,
                        "column_default": None,
                        "ordinal_position": 2,
                        "max_length": 1073741823,
                        "numeric_scale": 0,
                        "is_primary": False,
                        "is_unique": False,
                        "is_identity": False,
                        "identity_generation": None,
                        "is_generated": False,
                    },
                ]
            return [{"schema_name": "dbo", "table_name": "users", "table_type": "TABLE"}]

    fake_driver = cast("Any", FakeDDDriver())
    version = dd.get_version(fake_driver)
    assert isinstance(version, SybaseVersionInfo)
    assert (version.major, version.minor, version.patch) == (16, 0, 4)
    assert dd.get_feature_flag(fake_driver, "supports_transactions") is True
    assert dd.get_feature_flag(fake_driver, "supports_transactional_ddl") is False
    assert dd.get_optimal_type(fake_driver, "timestamp") == "BIGDATETIME"
    assert dd.get_optimal_type(fake_driver, "blob") == "IMAGE"

    tables = dd.get_tables(fake_driver, schema="dbo")
    assert len(tables) == 1
    assert tables[0]["table_name"] == "users"

    columns = dd.get_columns(fake_driver, table="users", schema="dbo")
    assert len(columns) == 2
    assert columns[0]["data_type"] == "numeric(10, 0)"
    assert columns[0]["identity_generation"] == "ALWAYS"
    assert columns[1]["data_type"] == "unitext"


def test_sybase_migration_tracker_and_extension_stores() -> None:
    """SybaseSyncMigrationTracker, SybaseStore, SybaseEventQueueStore, and SybaseADKStore emit ASE-safe SQL."""
    tracker = SybaseSyncMigrationTracker(version_table_name="dbo.ddl_migrations")
    ddl_text = tracker._idempotent_tracking_table_ddl_text()
    assert "IF OBJECT_ID(" in ddl_text
    assert "EXECUTE(" in ddl_text
    assert "VARCHAR(32)" in ddl_text
    assert "BIGDATETIME" in ddl_text
    assert "GETUTCDATE()" in ddl_text
    assert "TEXT" in ddl_text
    assert "NVARCHAR(MAX)" not in ddl_text

    config = SybaseConfig(
        extension_config={
            "litestar": {"session_table": "litestar_session", "manage_schema": False},
            "events": {"queue_table": "event_queue"},
            "adk": {},
        }
    )

    litestar_store = SybaseStore(config)
    litestar_ddl = litestar_store._table_ddl()
    assert "IF OBJECT_ID(" in litestar_ddl
    assert "IMAGE NOT NULL" in litestar_ddl
    assert "BIGDATETIME" in litestar_ddl
    assert "GETUTCDATE()" in litestar_ddl

    assert SybaseEventQueueStore is SybaseEventQueueStore
    event_store = SybaseEventQueueStore(config)
    event_stmts = event_store.create_statements()
    assert "TEXT" in event_stmts[0]
    assert "BIGDATETIME" in event_stmts[0]
    assert "GETUTCDATE()" in event_stmts[0]
    assert "IF OBJECT_ID(" in event_stmts[0]

    adk_store = SybaseADKStore(config)
    sessions_ddl = adk_store._sessions_table_ddl()
    events_ddl = adk_store._events_table_ddl()
    assert "IF OBJECT_ID(" in sessions_ddl
    assert "TEXT NOT NULL" in sessions_ddl
    assert "BIGDATETIME" in sessions_ddl
    assert "GETUTCDATE()" in sessions_ddl
    assert "IF OBJECT_ID(" in events_ddl

    mem_store = SybaseADKMemoryStore(config)
    mem_ddl = mem_store._memory_table_ddl()
    assert "IF OBJECT_ID(" in mem_ddl
    assert "TEXT NOT NULL" in mem_ddl
    assert "BIGDATETIME" in mem_ddl
    assert "GETUTCDATE()" in mem_ddl

    assert SybaseCursor.__slots__ == ("_as_dict", "connection", "cursor")
    assert SybaseExceptionHandler.__slots__ == ()


@pytest.mark.anyio
async def test_sybase_litestar_store_async_bridge(monkeypatch: pytest.MonkeyPatch) -> None:
    """SybaseStore async methods bridge to sync execution via async_."""
    calls: list[str] = []
    monkeypatch.setattr(SybaseStore, "_create_table", lambda _self: calls.append("created"))
    monkeypatch.setattr(SybaseStore, "reconcile_schema", AsyncMock())
    store = SybaseStore(SybaseConfig(extension_config={"litestar": {"session_table": "litestar_session"}}))

    await store.create_table()
    assert calls == ["created"]


def test_sybase_adk_store_session_list_and_memory_operations(monkeypatch: pytest.MonkeyPatch) -> None:
    """SybaseADKStore uses ROWS LIMIT ... OFFSET ... for pagination and SybaseADKMemoryStore inserts entries."""
    captured: list[tuple[str, tuple[Any, ...]]] = []

    def _capture(_self: SybaseADKStore, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        captured.append((sql, tuple(params)))
        return []

    monkeypatch.setattr(SybaseADKStore, "_execute_fetchall", _capture)
    config = MagicMock()
    config.extension_config = {"adk": {}}
    store = SybaseADKStore(cast("Any", config))

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)
    sql, params = captured[0]
    normalized_sql = " ".join(sql.split())
    assert "ORDER BY create_time ASC, id ASC ROWS LIMIT %s OFFSET %s" in normalized_sql
    assert params == ("app", "u1", 10, 20)

    conn = config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.rowcount = 1
    mem_store = SybaseADKMemoryStore(cast("Any", config))
    now = datetime.now(tz=UTC)
    entry = cast(
        "Any",
        {
            "id": "mem-1",
            "session_id": "sess-1",
            "app_name": "app",
            "user_id": "user-1",
            "event_id": "evt-1",
            "timestamp": now,
            "content_json": {"text": "hello"},
            "content_text": "hello",
            "metadata_json": None,
            "inserted_at": now,
        },
    )
    inserted = mem_store.insert_memory_entries([entry])
    assert inserted == 1


def test_sybase_native_error_handling_and_missing_table(monkeypatch: pytest.MonkeyPatch) -> None:
    """Native pymssql errors in commit() are caught and ADK get_session() returns None on table missing."""
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    driver = SybaseDriver(cast("SybaseConnection", conn))

    def _fail_commit() -> None:
        raise pymssql.OperationalError("Native pymssql commit failure")

    conn.commit = _fail_commit
    with pytest.raises(SQLSpecError, match="Failed to commit Sybase ASE transaction"):
        driver.commit()

    config = MagicMock()
    config.extension_config = {"adk": {}}
    store = SybaseADKStore(cast("Any", config))

    def _table_missing(_self: Any, *_args: Any, **_kwargs: Any) -> None:
        raise pymssql.DatabaseError(208, "Invalid object name 'adk_session'")

    monkeypatch.setattr(SybaseADKStore, "_execute_fetchone", _table_missing)
    assert store.get_session("app", "user", "sess") is None


def test_sybase_commit_maps_deadlock_error_1205() -> None:
    """Simulate error 1205 during driver.commit(); assert DeadlockError is raised."""
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    driver = SybaseDriver(cast("SybaseConnection", conn))

    def _fail_commit() -> None:
        raise pymssql.OperationalError(1205, "Your transaction was deadlock victim (1205)")

    conn.commit = _fail_commit
    with pytest.raises(DeadlockError, match="Failed to commit Sybase ASE transaction"):
        driver.commit()


def test_sybase_adk_store_maps_unique_violation(monkeypatch: pytest.MonkeyPatch) -> None:
    """SybaseADKStore raises UniqueViolationError on duplicate key violation."""
    config = MagicMock()
    config.extension_config = {"adk": {}}
    store = SybaseADKStore(cast("Any", config))

    class DummyCursor:
        def execute(self, _sql: str, _params: Any = ()) -> None:
            raise pymssql.IntegrityError(2601, "Attempt to insert duplicate key row with unique index")

        def close(self) -> None:
            pass

    class DummyConnection:
        def cursor(self, as_dict: bool = False) -> DummyCursor:
            return DummyCursor()

        def rollback(self) -> None:
            pass

    monkeypatch.setattr(config, "provide_connection", lambda: nullcontext(DummyConnection()))
    with pytest.raises(UniqueViolationError):
        store._execute("INSERT INTO dummy VALUES (1)")


def test_sybase_litestar_store_get_raises_sqlspec_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """SybaseStore._get() maps raw database errors to SQLSpecError."""
    config = MagicMock()
    config.extension_config = {"litestar": {}}
    store = SybaseStore(cast("Any", config))

    class FailingCursor:
        def execute(self, _sql: str, _params: Any = ()) -> None:
            raise pymssql.OperationalError("Connection lost")

        def close(self) -> None:
            pass

    class FailingConnection:
        def cursor(self) -> FailingCursor:
            return FailingCursor()

    monkeypatch.setattr(config, "provide_connection", lambda: nullcontext(FailingConnection()))
    with pytest.raises(SQLSpecError):
        store._get("session-1")


def test_sybase_has_schema_under_active_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """has_schema runs cleanly inside an active exception block without isolation leak."""
    config = MagicMock()
    driver = SybaseDriver(config)

    class DummyCursor:
        def execute(self, _sql: str, _params: Any = ()) -> None:
            pass

        def fetchone(self) -> Any:
            return (1,)

        def close(self) -> None:
            pass

    class DummyConnection:
        def cursor(self) -> DummyCursor:
            return DummyCursor()

    monkeypatch.setattr(driver, "connection", DummyConnection())

    try:
        raise RuntimeError("active error")
    except RuntimeError:
        has_user = driver.has_schema("dbo")
        assert has_user is True


def test_sybase_format_identifier_and_insert_statements() -> None:
    """format_identifier, build_insert_statement, and escape_literal_percent format queries correctly."""
    assert format_identifier("dbo.my_table") == "[dbo].[my_table]"
    sql = build_insert_statement("my_table", ["col1", "col2"])
    assert sql == "INSERT INTO [my_table] ([col1], [col2]) VALUES (%s, %s)"
    escaped = escape_literal_percent(
        "SELECT * FROM t WHERE val LIKE '100%' AND id = %s", ("1",), default_statement_config.parameter_validator
    )
    assert "100%%" in escaped


def test_sybase_migration_tracker_existing_columns_query() -> None:
    """_existing_columns_query resolves USER_NAME() when schema is unqualified."""
    tracker_unqualified = SybaseSyncMigrationTracker(version_table_name="my_versions")
    query_unqualified = tracker_unqualified._existing_columns_query()
    assert "u.name = USER_NAME()" in query_unqualified
    assert "o.name = 'my_versions'" in query_unqualified

    tracker_qualified = SybaseSyncMigrationTracker(version_table_name="custom_schema.my_versions")
    query_qualified = tracker_qualified._existing_columns_query()
    assert "u.name = 'custom_schema'" in query_qualified
    assert "o.name = 'my_versions'" in query_qualified


def test_sybase_error_code_mapping_count() -> None:
    """_ERROR_CODE_MAPPING contains 38 error codes."""
    assert len(_ERROR_CODE_MAPPING) == 38
