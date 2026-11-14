"""Unit tests for the Sybase ASE (pymssql over FreeTDS TDS 5.0) adapter."""

import asyncio
import os
import socket
import threading
from collections.abc import Generator
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, cast
from unittest.mock import AsyncMock, MagicMock, Mock

import pymssql
import pytest

import sqlspec.adapters.sybase.pool as sybase_pool
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
from sqlspec.adapters.sybase.core import build_insert_statement, escape_literal_percent, format_identifier
from sqlspec.adapters.sybase.events import SybaseEventQueueStore
from sqlspec.adapters.sybase.litestar import SybaseStore
from sqlspec.adapters.sybase.migrations import SybaseSyncMigrationTracker
from sqlspec.core import SQL, ParameterStyle
from sqlspec.data_dictionary.dialects.sybase import SybaseVersionInfo
from sqlspec.driver._common import _DETACHED_CONNECTION
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    MigrationError,
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
from sqlspec.extensions.adk import StoredEvent
from sqlspec.extensions.adk.migrations.reconcile import reconcile as adk_reconcile
from sqlspec.extensions.events.migrations.reconcile import reconcile as events_reconcile
from sqlspec.extensions.litestar.migrations.reconcile import reconcile as litestar_reconcile
from sqlspec.migrations.commands import SyncMigrationCommands
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.runner import SyncMigrationRunner
from sqlspec.migrations.utils import validate_builtin_reconcile_adapter


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


@pytest.fixture
def recording_sybase_config(monkeypatch: pytest.MonkeyPatch) -> Generator[tuple[SybaseConfig, FakeCursor], None, None]:
    """Record native DB-API calls while using the real config, pool, driver and stores."""
    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    monkeypatch.setenv("TDSVER", "5.0")
    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=lambda **_kwargs: connection))
    config = SybaseConfig(extension_config={"adk": {}, "litestar": {"session_table": "sessions"}})
    try:
        yield config, cursor
    finally:
        config.close_pool()


@pytest.mark.parametrize(
    ("table", "object_name"), [("event_queue", "[event_queue]"), ("audit.event_queue", "[audit].[event_queue]")]
)
def test_sybase_event_queue_guards_resolve_same_owner(table: str, object_name: str) -> None:
    store = SybaseEventQueueStore(SybaseConfig(extension_config={"events": {"queue_table": table}}))

    create_table, create_index = store.create_statements()
    drop_table = store.drop_statements()[0]

    assert create_table.startswith(f"IF OBJECT_ID('{object_name}') IS NULL EXECUTE('CREATE TABLE {store.table_name} ")
    assert f"AND id = OBJECT_ID('{object_name}')) EXECUTE('CREATE INDEX " in create_index
    assert drop_table == f"IF OBJECT_ID('{object_name}') IS NOT NULL EXECUTE('DROP TABLE {store.table_name}');"


def test_sybase_tracking_table_guard_uses_native_object_id(
    recording_sybase_config: tuple[SybaseConfig, FakeCursor],
) -> None:
    config, cursor = recording_sybase_config
    with config.provide_session() as driver:
        SybaseSyncMigrationTracker(version_table_name="audit.ddl_migrations").ensure_tracking_table(driver)

    assert driver.connection is _DETACHED_CONNECTION
    assert cursor.calls[0][0].startswith(
        "IF OBJECT_ID('[audit].[ddl_migrations]') IS NULL EXECUTE('CREATE TABLE [audit].[ddl_migrations]"
    )


@pytest.mark.parametrize("body_raises", [False, True], ids=["clean-exit", "body-exception"])
def test_sybase_provide_session_detaches_driver_on_exit(
    recording_sybase_config: tuple[SybaseConfig, FakeCursor], body_raises: bool
) -> None:
    """SybaseConfig.provide_session detaches the yielded driver on clean and error exit."""
    config, _cursor = recording_sybase_config
    if body_raises:
        with pytest.raises(RuntimeError, match="sybase body error"), config.provide_session() as session:
            raise RuntimeError("sybase body error")
    else:
        with config.provide_session() as session:
            pass

    assert session.connection is _DETACHED_CONNECTION


def test_sybase_adk_lifecycle_uses_native_object_id(recording_sybase_config: tuple[SybaseConfig, FakeCursor]) -> None:
    config, cursor = recording_sybase_config
    sessions = SybaseADKStore(config)
    memory = SybaseADKMemoryStore(config)
    sessions.create_tables()
    memory.create_tables()
    sessions.drop_tables()
    memory.drop_tables()

    statements = [sql for sql, _params in cursor.calls]
    for table in ("adk_session", "adk_event", "adk_app_state", "adk_user_state", "adk_internal_metadata", "adk_memory"):
        reference = f"[dbo].[{table}]"
        assert any(
            statement.startswith(f"IF OBJECT_ID('{reference}') IS NULL EXECUTE('CREATE TABLE {reference}")
            for statement in statements
        )
        assert any(
            statement.startswith(f"IF OBJECT_ID('{reference}') IS NOT NULL EXECUTE('DROP TABLE {reference}')")
            for statement in statements
        )


@pytest.mark.anyio
async def test_sybase_litestar_create_uses_native_object_id(
    recording_sybase_config: tuple[SybaseConfig, FakeCursor],
) -> None:
    config, cursor = recording_sybase_config
    await SybaseStore(config).create_table()

    assert any(
        sql.startswith("IF OBJECT_ID('[dbo].[sessions]') IS NULL EXECUTE('CREATE TABLE [dbo].[sessions]")
        for sql, _params in cursor.calls
    )


@pytest.mark.parametrize(
    ("limit", "after"), [(None, None), (5, None), (5, datetime(2026, 1, 1, tzinfo=UTC)), (0, None)]
)
def test_sybase_adk_events_use_top_for_bounded_queries(
    recording_sybase_config: tuple[SybaseConfig, FakeCursor], limit: int | None, after: datetime | None
) -> None:
    config, cursor = recording_sybase_config
    assert SybaseADKStore(config).get_events("app", "user", "session", after_timestamp=after, limit=limit) == []
    if limit == 0:
        assert cursor.calls == []
        return

    statement, parameters = cursor.calls[-1]
    sql = " ".join(statement.split())
    prefix = "SELECT TOP %s e.id," if limit is not None else "SELECT e.id,"
    assert sql.startswith(prefix)
    assert (
        "JOIN [dbo].[adk_session] s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id" in sql
    )
    assert "WHERE s.app_name = %s AND s.user_id = %s AND s.id = %s" in sql
    assert sql.endswith("ORDER BY e.timestamp ASC")
    expected: tuple[Any, ...] = ("app", "user", "session")
    if limit is not None:
        expected = (limit, *expected)
    if after is not None:
        expected = (*expected, after)
        assert "AND e.timestamp > %s" in sql
    assert parameters == expected


@pytest.mark.parametrize(("limit", "offset"), [(None, None), (5, None), (5, 0), (5, 2), (0, None)])
def test_sybase_adk_session_pages_use_top_only_without_offset(
    recording_sybase_config: tuple[SybaseConfig, FakeCursor], limit: int | None, offset: int | None
) -> None:
    config, cursor = recording_sybase_config
    assert SybaseADKStore(config).list_sessions("app", "user", limit=limit, offset=offset) == []
    if limit == 0:
        assert cursor.calls == []
        return

    statement, parameters = cursor.calls[-1]
    sql = " ".join(statement.split())
    if limit is not None and not offset:
        assert sql.startswith("SELECT TOP %s id,")
        assert parameters == (limit, "app", "user")
    else:
        assert sql.startswith("SELECT id,")
        assert parameters == (("app", "user", limit, offset) if offset else ("app", "user"))
    ordering = "ORDER BY update_time DESC, id DESC"
    assert sql.endswith(f"{ordering} ROWS LIMIT %s OFFSET %s" if offset else ordering)


@pytest.mark.parametrize(
    ("scope", "scope_parameters"), [("all", ("app", "user")), ("user", ("app", "user")), ("app", ("app",))]
)
@pytest.mark.parametrize(("limit", "expected_limit"), [(7, 7), (None, 20), (0, 20)])
def test_sybase_memory_search_uses_top_and_preserves_scope_and_like_values(
    recording_sybase_config: tuple[SybaseConfig, FakeCursor],
    scope: Literal["all", "user", "app"],
    scope_parameters: tuple[str, ...],
    limit: int | None,
    expected_limit: int,
) -> None:
    config, cursor = recording_sybase_config
    assert (
        SybaseADKMemoryStore(config).search_entries(r"50%_[x]\\", "app", "user", limit=limit, scope_filter=scope) == []
    )
    statement, parameters = cursor.calls[-1]
    sql = " ".join(statement.split())
    assert sql.startswith("SELECT TOP %s id,")
    assert sql.endswith("ORDER BY timestamp DESC")
    assert parameters == (expected_limit, *scope_parameters, r"%50\%\_\[x]\\\\%")


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


@pytest.mark.parametrize("entry_point", ["config", "pool"])
@pytest.mark.parametrize("alias_key", ["freetds_server", "server_alias", "is_server_alias"])
def test_sybase_alias_connections_pass_only_native_kwargs(
    monkeypatch: pytest.MonkeyPatch, entry_point: str, alias_key: str
) -> None:
    """Both connection entry points preserve aliases and accepted native options."""
    monkeypatch.delenv("TDSVER", raising=False)
    captured: list[dict[str, Any]] = []
    connection = FakeConnection()

    def connect(
        *,
        server: str,
        user: str,
        database: str,
        port: int | str,
        charset: str,
        autocommit: bool,
        conn_properties: str,
        read_only: bool,
    ) -> FakeConnection:
        captured.append({
            "server": server,
            "user": user,
            "database": database,
            "port": port,
            "charset": charset,
            "autocommit": autocommit,
            "conn_properties": conn_properties,
            "read_only": read_only,
        })
        return connection

    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=connect))
    parameters = {
        alias_key: True if alias_key == "is_server_alias" else "SQLSPEC_ASE",
        "server": "SQLSPEC_ASE" if alias_key == "is_server_alias" else "ignored-host",
        "username": "sa",
        "db": "pubs2",
        "port": "5100",
        "read_only": True,
    }
    if entry_point == "config":
        config = SybaseConfig(connection_config=parameters)
        pool = config.provide_pool()
    else:
        pool = SybaseConnectionPool(parameters)
    try:
        with pool.get_connection() as acquired:
            assert cast("object", acquired) is connection
        assert captured == [
            {
                "server": "SQLSPEC_ASE",
                "user": "sa",
                "database": "pubs2",
                "port": "5100",
                "charset": "UTF-8",
                "autocommit": True,
                "conn_properties": DEFAULT_SYBASE_CONN_PROPERTIES,
                "read_only": True,
            }
        ]
    finally:
        pool.close()


@pytest.mark.parametrize("port_parameters", [{}, {"port": None}, {"port": ""}, {"port": 0}, {"port": "0"}])
def test_sybase_alias_requires_explicit_nonzero_port(
    monkeypatch: pytest.MonkeyPatch, port_parameters: dict[str, Any]
) -> None:
    monkeypatch.delenv("TDSVER", raising=False)
    config = SybaseConfig(connection_config={"server_alias": "SQLSPEC_ASE", **port_parameters})

    with pytest.raises(ImproperConfigurationError, match=r"explicit.*port"):
        config.provide_pool()


def test_sybase_alias_normalization_is_idempotent_and_keeps_session_overrides() -> None:
    parameters = {
        "extra": {"freetds_server": "SQLSPEC_ASE", "port": 5100, "username": "sa", "db": "pubs2"},
        "conn_properties": "SET TEXTSIZE 1000;",
        "autocommit": False,
    }
    normalized = build_connection_config(parameters)

    assert normalized["server"] == "SQLSPEC_ASE"
    assert normalized["port"] == 5100
    assert normalized["conn_properties"] == "SET TEXTSIZE 1000;"
    assert normalized["autocommit"] is False
    assert build_connection_config(normalized) == normalized
    assert "SET CHAINED OFF;" in DEFAULT_SYBASE_CONN_PROPERTIES


def test_sybase_nested_pool_checkouts_keep_outer_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TDSVER", "5.0")
    connection = FakeConnection()
    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=lambda **_kwargs: connection))
    pool = SybaseConnectionPool({"server": "ase.example.test"}, recycle_seconds=0, health_check_interval=0)
    try:
        with pool.get_connection() as outer:
            assert cast("object", outer) is connection
            assert pool.checked_out() == 1
            with pytest.raises(ValueError, match="inner failure"), pool.get_connection() as inner:
                assert inner is outer
                assert pool.checked_out() == 2
                raise ValueError("inner failure")
            assert connection.closed is False
            assert pool.checked_out() == 1
        assert connection.closed is False
        assert pool.checked_out() == 0
    finally:
        pool.close()
    assert connection.closed is True


def test_sybase_nested_pool_checkouts_are_thread_local(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TDSVER", "5.0")
    opened: list[FakeConnection] = []
    errors: list[BaseException] = []

    def connect(**_kwargs: Any) -> FakeConnection:
        connection = FakeConnection()
        opened.append(connection)
        return connection

    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=connect))
    pool = SybaseConnectionPool({"server": "ase.example.test"}, recycle_seconds=0, health_check_interval=0)

    def worker() -> None:
        try:
            with pool.get_connection() as outer:
                with pytest.raises(ValueError, match="worker failure"), pool.get_connection() as inner:
                    assert outer is inner
                    assert pool.checked_out() == 3
                    raise ValueError("worker failure")
                assert cast("FakeConnection", outer).closed is False
        except BaseException as exc:
            errors.append(exc)

    try:
        with pool.get_connection() as outer:
            thread = threading.Thread(target=worker, daemon=True)
            thread.start()
            thread.join(timeout=3)
            assert not thread.is_alive()
            assert errors == []
            assert len(opened) == 2
            assert pool.checked_out() == 1
            assert pool.size() == 1
            assert opened[1].closed is True
            assert cast("FakeConnection", outer).closed is False
        assert pool.checked_out() == 0
    finally:
        pool.close()


def test_sybase_pool_recovers_acquisition_failure_and_retires_outer_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TDSVER", "5.0")
    failed_connection = FakeConnection()
    clean_connection = FakeConnection()
    outcomes = [OSError("login timed out"), failed_connection, clean_connection]

    def connect(**_kwargs: Any) -> FakeConnection:
        outcome = outcomes.pop(0)
        if isinstance(outcome, OSError):
            raise outcome
        return outcome

    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=connect))
    pool = SybaseConnectionPool({"server": "ase.example.test"}, recycle_seconds=0, health_check_interval=0)
    try:
        with pytest.raises(OSError, match="login timed out"), pool.get_connection():
            pytest.fail("Failed acquisition must not enter the checkout body")
        assert pool.checked_out() == 0
        assert pool.size() == 0
        with pytest.raises(ValueError, match="outer failure"), pool.get_connection() as connection:
            assert cast("object", connection) is failed_connection
            with pool.get_connection() as nested:
                assert nested is connection
                raise ValueError("outer failure")
        assert failed_connection.closed is True
        assert pool.checked_out() == 0
        assert pool.size() == 0
        with pool.get_connection() as replacement:
            assert cast("object", replacement) is clean_connection
            assert clean_connection.closed is False
        assert pool.checked_out() == 0
    finally:
        pool.close()


def test_sybase_pool_discards_thread_connection_on_cancelled_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A forwarded cancellation must close and discard the thread-local connection."""
    monkeypatch.setenv("TDSVER", "5.0")
    c1 = FakeConnection()
    c2 = FakeConnection()
    connections = [c1, c2]

    def connect(**_kwargs: Any) -> FakeConnection:
        return connections.pop(0)

    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=connect))
    pool = SybaseConnectionPool({"server": "ase.example.test"}, recycle_seconds=0, health_check_interval=0)
    try:
        with pytest.raises(asyncio.CancelledError):
            with pool.get_connection() as connection:
                assert cast("object", connection) is c1
                raise asyncio.CancelledError()

        assert c1.closed is True

        with pool.get_connection() as next_connection:
            assert cast("object", next_connection) is c2
            assert c2.closed is False
    finally:
        pool.close()


def test_sybase_pool_clean_exit_preserves_thread_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    """A clean exit retains the connection for the thread."""
    monkeypatch.setenv("TDSVER", "5.0")
    connection = FakeConnection()
    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=lambda **_: connection))
    pool = SybaseConnectionPool({"server": "ase.example.test"}, recycle_seconds=0, health_check_interval=0)
    try:
        with pool.get_connection() as conn:
            assert cast("object", conn) is connection
        assert connection.closed is False
        with pool.get_connection() as next_conn:
            assert next_conn is conn
    finally:
        pool.close()


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
    cfg_alias = build_connection_config({
        "server_alias": "SYBASE_ASE",
        "port": 5000,
        "user": "sa",
        "database": "master",
    })
    pool_alias = SybaseConnectionPool(cfg_alias)
    conn_alias = pool_alias.acquire()
    pool_alias.release(conn_alias)
    assert cast("object", conn_alias) is fake_conn


@pytest.mark.parametrize("use_alias", [False, True], ids=["host", "freetds-alias"])
def test_sybase_pool_transmits_native_tds_50_login_packet_over_loopback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, use_alias: bool
) -> None:
    """SybaseConnectionPool.new_connection transmits a native 619-byte TDS 5.0 login packet via FreeTDS."""
    monkeypatch.setenv("TDSVER", "5.0")
    monkeypatch.delenv("TDSPORT", raising=False)
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
        server_sock.settimeout(2.0)
        port = server_sock.getsockname()[1]
        server_sock.listen(1)

        alias_config: dict[str, Any] = {"server": "127.0.0.1"}
        if use_alias:
            freetds_config = tmp_path / "freetds.conf"
            freetds_config.write_text(
                f"[SQLSPEC_ASE_TEST]\nhost = 127.0.0.1\nport = {port}\ntds version = 5.0\n", encoding="utf-8"
            )
            monkeypatch.setenv("FREETDSCONF", str(freetds_config))
            monkeypatch.delenv("TDSVER", raising=False)
            alias_config = {"server_alias": "SQLSPEC_ASE_TEST"}

        listener_thread = threading.Thread(target=_listener, args=(server_sock,), daemon=True)
        listener_thread.start()
        ready.wait(timeout=2.0)

        cfg = build_connection_config({
            **alias_config,
            "port": port,
            "user": "sa",
            "password": "secret",
            "database": "master",
            "login_timeout": 1,
            "timeout": 1,
        })
        pool = SybaseConnectionPool(cfg)
        with pytest.raises(pymssql.OperationalError):
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

    def _capture(_self: Any, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        captured.append((sql, tuple(params)))
        return []

    config = MagicMock()
    config.extension_config = {"adk": {}}
    cursor = config.provide_connection.return_value.__enter__.return_value.cursor.return_value
    cursor.execute.side_effect = lambda sql, params=(): _capture(None, sql, params)
    cursor.fetchall.return_value = []
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


def _sybase_migration_config(tmp_path: Path, **migration_options: str) -> SybaseConfig:
    return SybaseConfig(
        connection_config={"user": "sa", "password": "secret", "database": "demo"},
        migration_config={"script_location": str(tmp_path), **migration_options},
    )


@pytest.mark.parametrize(
    ("option", "message"),
    [
        ("default_schema", "SybaseConfig does not support migration default schemas"),
        ("version_table_schema", "SybaseConfig does not support migration version table schemas"),
    ],
)
def test_sybase_rejects_migration_schema_options(tmp_path: Path, option: str, message: str) -> None:
    """Schema options raise when commands are created, before any connection opens."""
    config = _sybase_migration_config(tmp_path, **{option: "audit"})

    with pytest.raises(MigrationError, match=message):
        SyncMigrationCommands(config)


def test_sybase_rejects_migration_schema_directive(tmp_path: Path) -> None:
    """A schema directive raises before any driver call."""
    migration_file = tmp_path / "0001_directive.sql"
    migration_file.write_text("-- schema: audit\n-- name: migrate-0001-up\nSELECT 1;\n")
    runner = SyncMigrationRunner(tmp_path, {}, MigrationContext(config=_sybase_migration_config(tmp_path)), {})
    migration = runner.load_migration(migration_file, "0001")

    with pytest.raises(MigrationError, match="does not support migration schema directives"):
        runner.execute_upgrade(Mock(), migration, use_transaction=False)


def _recording_reconcile_driver(
    *, tables: list[dict[str, Any]] | None = None, indexes: list[dict[str, Any]] | None = None
) -> tuple[SimpleNamespace, list[str], list[str]]:
    executed: list[str] = []
    scripts: list[str] = []
    table_rows = tables or []
    index_rows = indexes or []
    data_dictionary = SimpleNamespace(
        get_tables=Mock(side_effect=lambda _drv, schema=None: list(table_rows)),
        get_columns=Mock(side_effect=lambda _drv, table=None, schema=None: []),
        get_indexes=Mock(side_effect=lambda _drv, table=None, schema=None: list(index_rows)),
    )
    driver = SimpleNamespace(
        data_dictionary=data_dictionary,
        execute=Mock(side_effect=lambda sql, *_args, **_kwargs: executed.append(str(sql))),
        execute_script=Mock(side_effect=lambda sql, *_args, **_kwargs: scripts.append(str(sql))),
        commit=Mock(),
    )
    return driver, executed, scripts


def test_sybase_reconcile_guard_accepts_adapter() -> None:
    """validate_builtin_reconcile_adapter accepts SybaseConfig unconditionally."""
    config = SybaseConfig(connection_config={"user": "sa", "password": "secret", "database": "demo"})
    assert validate_builtin_reconcile_adapter(config) == "sybase"


def test_sybase_reconcile_litestar_target_and_table_ddl_unchanged() -> None:
    """SybaseStore parses unwrapped DDL for SchemaTarget while preserving _table_ddl byte-for-byte."""
    config = SybaseConfig(extension_config={"litestar": {"session_table": "litestar_session"}})
    store = SybaseStore(config)

    expected_ddl = (
        "IF OBJECT_ID('[dbo].[litestar_session]') IS NULL EXECUTE('"
        "CREATE TABLE [dbo].[litestar_session] ("
        "session_id VARCHAR(255) NOT NULL PRIMARY KEY, "
        "data IMAGE NOT NULL, "
        "expires_at BIGDATETIME NULL, "
        "created_at BIGDATETIME DEFAULT GETUTCDATE() NOT NULL, "
        "updated_at BIGDATETIME DEFAULT GETUTCDATE() NOT NULL"
        ")');\n"
        "IF NOT EXISTS (SELECT 1 FROM dbo.sysindexes WHERE name = 'IX_litestar_session_expires_at' "
        "AND id = OBJECT_ID('[dbo].[litestar_session]')) EXECUTE('"
        "CREATE INDEX [IX_litestar_session_expires_at] ON [dbo].[litestar_session](expires_at)');"
    )
    assert store._table_ddl() == expected_ddl

    targets = store.get_schema_targets()
    assert len(targets) == 1
    target = targets[0]
    assert target.table_name == "litestar_session"
    assert target.schema == "dbo"
    columns = {col.name: col.dtype for col in target.create_table.columns}
    assert list(columns) == ["session_id", "data", "expires_at", "created_at", "updated_at"]
    assert columns["data"] == "IMAGE"
    assert columns["expires_at"] == "BIGDATETIME"
    assert columns["created_at"] == "BIGDATETIME"
    assert columns["updated_at"] == "BIGDATETIME"

    companion_indexes = store.get_companion_indexes()
    assert len(companion_indexes) == 1
    companion = companion_indexes[0]
    assert companion.name == "IX_litestar_session_expires_at"
    assert companion.table == "litestar_session"
    assert companion.schema == "dbo"
    assert companion.ddl == (
        "IF NOT EXISTS (SELECT 1 FROM dbo.sysindexes WHERE name = 'IX_litestar_session_expires_at' "
        "AND id = OBJECT_ID('[dbo].[litestar_session]')) EXECUTE('"
        "CREATE INDEX [IX_litestar_session_expires_at] ON [dbo].[litestar_session](expires_at)');"
    )

    missing_driver, executed, scripts = _recording_reconcile_driver(indexes=[])
    store.reconcile_companion_objects_sync(missing_driver)
    assert executed == []
    assert scripts == [companion.ddl]
    missing_driver.commit.assert_not_called()

    present_driver, present_executed, present_scripts = _recording_reconcile_driver(
        indexes=[
            {"schema_name": "dbo", "table_name": "litestar_session", "index_name": "IX_litestar_session_expires_at"}
        ]
    )
    store.reconcile_companion_objects_sync(present_driver)
    assert present_executed == []
    assert present_scripts == []


def test_sybase_reconcile_event_queue_target_parses_columns() -> None:
    """SybaseEventQueueStore parses unwrapped queue DDL without ValueError."""
    config = SybaseConfig(extension_config={"events": {"queue_table": "event_queue"}})
    store = SybaseEventQueueStore(config)

    targets = store.get_schema_targets()
    assert len(targets) == 1
    target = targets[0]
    assert target.table_name == "event_queue"
    columns = {col.name: col.dtype for col in target.create_table.columns}
    assert list(columns) == [
        "event_id",
        "channel",
        "payload_json",
        "metadata_json",
        "status",
        "available_at",
        "lease_expires_at",
        "attempts",
        "created_at",
        "acknowledged_at",
    ]
    assert columns["payload_json"] == "TEXT"
    assert columns["available_at"] == "BIGDATETIME"
    assert isinstance(target.create_statement, str)
    assert target.create_statement == store.create_statements()[0]
    assert target.create_statement.startswith("IF OBJECT_ID('[event_queue]') IS NULL EXECUTE(")


def test_sybase_reconcile_adk_targets_ddl_unchanged_and_guarded_index_repair() -> None:
    """SybaseADKStore and SybaseADKMemoryStore keep wrapped DDL unchanged and probe catalog before index creation."""
    config = SybaseConfig(extension_config={"adk": {"enable_memory": True}})
    adk_store = SybaseADKStore(config)
    mem_store = SybaseADKMemoryStore(config)
    disabled_mem_store = SybaseADKMemoryStore(SybaseConfig(extension_config={"adk": {"enable_memory": False}}))

    expected_sessions_ddl = (
        "IF OBJECT_ID('[dbo].[adk_session]') IS NULL EXECUTE('"
        "CREATE TABLE [dbo].[adk_session] ("
        "id VARCHAR(128) NOT NULL, "
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        "state TEXT NOT NULL, "
        "create_time BIGDATETIME DEFAULT GETUTCDATE() NOT NULL, "
        "update_time BIGDATETIME DEFAULT GETUTCDATE() NOT NULL, "
        "CONSTRAINT [pk_adk_session_id] PRIMARY KEY (id)"
        ")');"
    )
    expected_metadata_ddl = (
        "IF OBJECT_ID('[dbo].[adk_internal_metadata]') IS NULL EXECUTE('"
        "CREATE TABLE [dbo].[adk_internal_metadata] ("
        "[key] VARCHAR(128) NOT NULL, "
        "value VARCHAR(512) NOT NULL, "
        "CONSTRAINT [pk_adk_internal_metadata_key] PRIMARY KEY ([key])"
        ")');"
    )
    expected_memory_ddl = (
        "IF OBJECT_ID('[dbo].[adk_memory]') IS NULL EXECUTE('"
        "CREATE TABLE [dbo].[adk_memory] ("
        "id VARCHAR(128) NOT NULL, "
        "session_id VARCHAR(128) NOT NULL, "
        "app_name VARCHAR(128) NOT NULL, "
        "user_id VARCHAR(128) NOT NULL, "
        "scope VARCHAR(16) DEFAULT ''user'' NOT NULL, "
        "event_id VARCHAR(128) NOT NULL, "
        "author VARCHAR(256) NULL, "
        "timestamp BIGDATETIME NOT NULL, "
        "content_json TEXT NOT NULL, "
        "content_text TEXT NOT NULL, "
        "metadata_json TEXT NULL, "
        "inserted_at BIGDATETIME DEFAULT GETUTCDATE() NOT NULL, "
        "CONSTRAINT [pk_adk_memory_id] PRIMARY KEY (id), "
        "CONSTRAINT [uq_adk_memory_event_id] UNIQUE (app_name, user_id, event_id)"
        ")');"
    )
    assert adk_store._sessions_table_ddl() == expected_sessions_ddl
    assert adk_store._metadata_table_ddl() == expected_metadata_ddl
    assert mem_store._memory_table_ddl() == expected_memory_ddl

    driver, executed, scripts = _recording_reconcile_driver(indexes=[])
    adk_targets = adk_store.get_schema_targets_sync(driver)
    assert [t.table_name for t in adk_targets] == [
        "adk_session",
        "adk_event",
        "adk_app_state",
        "adk_user_state",
        "adk_internal_metadata",
    ]
    assert all(t.schema == "dbo" for t in adk_targets)
    assert [col.name for col in adk_targets[-1].create_table.columns] == ["key", "value"]

    assert disabled_mem_store.get_schema_targets_sync(driver) == []
    mem_targets = mem_store.get_schema_targets_sync(driver)
    assert [t.table_name for t in mem_targets] == ["adk_memory"]

    adk_store.reconcile_companion_objects_sync(driver)
    assert scripts == []
    assert len(executed) == 7
    assert executed[0] == "CREATE INDEX [idx_adk_session_app_user] ON [dbo].[adk_session] (app_name, user_id)"
    driver.commit.assert_not_called()

    mem_driver, mem_executed, _ = _recording_reconcile_driver(
        indexes=[{"schema_name": "dbo", "table_name": "adk_memory", "index_name": "idx_adk_memory_session"}]
    )
    mem_store.reconcile_companion_objects_sync(mem_driver)
    assert len(mem_executed) == 3
    assert all("idx_adk_memory_session" not in stmt for stmt in mem_executed)
    mem_driver.commit.assert_not_called()


def test_sybase_reconcile_orchestrators_converge_litestar_events_and_adk() -> None:
    """Built-in Litestar, events, and ADK reconcilers converge Sybase tables and indexes without committing."""
    config = SybaseConfig(
        connection_config={"user": "sa", "password": "secret", "database": "demo"},
        extension_config={
            "litestar": {"session_table": "litestar_session", "migration_mode": "reconcile"},
            "events": {"queue_table": "event_queue", "migration_mode": "reconcile"},
            "adk": {"enable_memory": True, "migration_mode": "reconcile"},
        },
    )
    driver, executed, scripts = _recording_reconcile_driver(tables=[], indexes=[])
    context = MigrationContext(config=config, driver=cast("Any", driver))

    assert litestar_reconcile(context) is None
    assert events_reconcile(context) is None
    assert adk_reconcile(context) is None

    assert any("CREATE TABLE [dbo].[litestar_session]" in stmt for stmt in scripts)
    assert any("CREATE TABLE event_queue" in stmt for stmt in scripts)
    assert any("CREATE TABLE [dbo].[adk_session]" in stmt for stmt in scripts)
    assert any("CREATE TABLE [dbo].[adk_memory]" in stmt for stmt in scripts)
    assert len(executed) == 11
    driver.commit.assert_not_called()


def test_replica_derivation_switches_freetds_alias_and_tcp_without_stale_options() -> None:
    primary = SybaseConfig(connection_config={"freetds_server": "ASE_WRITER", "database": "app"})
    reader = primary.derive_pool(host="reader", port=5001, db="reader_app", username="reader")
    assert reader.connection_config["server"] == "reader"
    assert reader.connection_config["database"] == "reader_app"
    assert reader.connection_config["user"] == "reader"
    assert reader.connection_config.get("_sybase_server_alias") is None
    alias = reader.derive_pool(server="ASE_READER")
    assert alias.connection_config["server"] == "ASE_READER"
    assert alias.connection_config.get("port") is None
    assert alias.connection_config["_sybase_server_alias"] is True


def test_replica_sybase_url_shorthand_parses_endpoint_and_database() -> None:
    primary = SybaseConfig(connection_config={"server": "writer"}, replicas="sybase://reader:pw@reader:5001/app")
    reader = primary.replica_groups["default"].configs[0]
    assert reader.connection_config["server"] == "reader"
    assert reader.connection_config["port"] == 5001
    assert reader.connection_config["database"] == "app"


@pytest.mark.parametrize(
    "error", (Exception(3906, b"database is read-only"), Exception("[25006] read-only transaction"))
)
def test_replica_write_errors_preserve_native_cause(error: Exception) -> None:
    from sqlspec.exceptions import ReadOnlyTransactionError

    mapped = create_mapped_exception(error)
    assert isinstance(mapped, ReadOnlyTransactionError)
    assert mapped.__cause__ is error


def test_replica_url_override_replaces_freetds_alias() -> None:
    primary = SybaseConfig(connection_config={"freetds_server": "ASE_WRITER"})
    replica = primary.derive_pool(url="sybase://reader:pw@reader:5001/app")
    assert replica.connection_config["server"] == "reader"
    assert replica.connection_config["port"] == 5001
    assert replica.connection_config.get("_sybase_server_alias") is None


@pytest.mark.parametrize("endpoint", ("", "postgresql://reader/app", "reader:invalid"))
def test_invalid_replica_endpoints_fail_before_pool_creation(endpoint: str) -> None:
    with pytest.raises(ImproperConfigurationError):
        SybaseConfig(connection_config={"server": "writer"}, replicas=[endpoint])


@pytest.mark.parametrize("autocommit", [False, True], ids=["autocommit-off", "autocommit-on"])
def test_sybase_pool_checkin_rolls_back_only_when_autocommit_off(
    monkeypatch: pytest.MonkeyPatch, autocommit: bool
) -> None:
    """SybaseConnectionPool rolls back on outermost checkin only when autocommit_state is False."""
    monkeypatch.setenv("TDSVER", "5.0")
    connection = FakeConnection()
    connection.autocommit(autocommit)
    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=lambda **_kwargs: connection))
    pool = SybaseConnectionPool({"server": "ase.example.test"}, recycle_seconds=0, health_check_interval=0)
    try:
        with pool.get_connection() as outer:
            with pool.get_connection() as inner:
                assert inner is outer
            assert connection.rollbacks == 0
        assert connection.rollbacks == (0 if autocommit else 1)
    finally:
        pool.close()


def test_sybase_pool_closes_on_failed_hook_and_failed_checkin_rollback(monkeypatch: pytest.MonkeyPatch) -> None:
    """SybaseConnectionPool closes sockets on failed _on_connection_create and retires on failed rollback."""
    monkeypatch.setenv("TDSVER", "5.0")
    hook_conn = FakeConnection()
    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=lambda **_kwargs: hook_conn))
    hook_pool = SybaseConnectionPool(
        {"server": "ase.example.test"},
        on_connection_create=lambda _c: (_ for _ in ()).throw(RuntimeError("ase hook failed")),
    )
    with pytest.raises(RuntimeError, match="ase hook failed"):
        hook_pool.new_connection()
    assert hook_conn.closed is True

    first = FakeConnection()
    first.autocommit(False)
    second = FakeConnection()
    conns = [first, second]
    monkeypatch.setattr(first, "rollback", MagicMock(side_effect=OSError("ase rollback broken")))
    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=lambda **_kwargs: conns.pop(0)))
    pool = SybaseConnectionPool({"server": "ase.example.test"}, recycle_seconds=0, health_check_interval=0)
    try:
        with pytest.raises(OSError, match="ase rollback broken"), pool.get_connection():
            pass
        assert first.closed is True
        assert pool.checked_out() == 0
        assert pool.size() == 0
        with pool.get_connection() as fresh:
            assert cast("object", fresh) is second
    finally:
        pool.close()


def _sybase_event_fixture(**overrides: Any) -> StoredEvent:
    payload: dict[str, Any] = {
        "id": "evt-1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "s1",
        "invocation_id": "inv-1",
        "timestamp": datetime(2026, 1, 1, tzinfo=UTC),
        "event_data": {"text": "hello"},
    }
    payload.update(overrides)
    return cast("StoredEvent", payload)


@pytest.mark.parametrize(
    ("renew_for", "expect_renew"),
    [
        pytest.param(None, False, id="none"),
        pytest.param(0, False, id="zero"),
        pytest.param(-1, False, id="negative"),
        pytest.param(timedelta(0), False, id="zero-timedelta"),
        pytest.param(timedelta(milliseconds=250), True, id="subsecond-timedelta"),
        pytest.param(30, True, id="positive-int"),
    ],
)
def test_sybase_adk_get_session_renews_only_for_positive_duration(
    monkeypatch: pytest.MonkeyPatch, renew_for: "int | timedelta | None", expect_renew: bool
) -> None:
    """SybaseADKStore.get_session executes UPDATE only when renew_for is positive."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    writes: list[tuple[str, tuple[Any, ...]]] = []

    def capture_execute(_self: SybaseADKStore, sql: str, params: "tuple[Any, ...]" = (), **_kw: Any) -> int:
        writes.append((" ".join(sql.split()), tuple(params)))
        return 1

    monkeypatch.setattr(SybaseADKStore, "_execute", capture_execute)
    monkeypatch.setattr(
        SybaseADKStore, "_execute_fetchone", lambda *_a, **_kw: ("s1", "app", "u1", '{"step":1}', now, now)
    )
    config = MagicMock()
    config.extension_config = {"adk": {}}
    session = SybaseADKStore(cast("Any", config)).get_session("app", "u1", "s1", renew_for=renew_for)

    assert session is not None
    assert session["id"] == "s1"
    if expect_renew:
        assert writes == [
            (
                (
                    "UPDATE [dbo].[adk_session] SET update_time = GETUTCDATE() "
                    "WHERE app_name = %s AND user_id = %s AND id = %s"
                ),
                ("app", "u1", "s1"),
            )
        ]
    else:
        assert writes == []


@pytest.mark.parametrize("session_exists", [True, False], ids=["owned-session", "missing-or-unowned"])
def test_sybase_adk_append_event_verifies_session_ownership(session_exists: bool) -> None:
    """SybaseADKStore.append_event probes session ownership before inserting and rolls back when unowned."""
    config = MagicMock()
    config.extension_config = {"adk": {}}
    conn = config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.fetchone.return_value = (1,) if session_exists else None
    store = SybaseADKStore(cast("Any", config))

    if session_exists:
        store.append_event(_sybase_event_fixture())
        assert cursor.execute.call_count == 2
        ownership_sql, ownership_params = cursor.execute.call_args_list[0].args
        insert_sql, _ = cursor.execute.call_args_list[1].args
        assert " ".join(ownership_sql.split()) == (
            "SELECT TOP 1 1 FROM [dbo].[adk_session] WHERE id = %s AND app_name = %s AND user_id = %s"
        )
        assert ownership_params == ("s1", "app", "u1")
        assert " ".join(insert_sql.split()).startswith("INSERT INTO [dbo].[adk_event]")
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
    else:
        with pytest.raises(ValueError, match="Session s1 not found for app='app', user='u1'"):
            store.append_event(_sybase_event_fixture())
        assert cursor.execute.call_count == 1
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"app_name": "other"}, id="app-mismatch"),
        pytest.param({"user_id": "other"}, id="user-mismatch"),
        pytest.param({"session_id": "other"}, id="session-mismatch"),
    ],
)
def test_sybase_adk_append_event_and_update_state_rejects_mismatched_scope(overrides: "dict[str, str]") -> None:
    """Mismatched event scope fails before acquiring a Sybase connection."""
    config = MagicMock()
    config.extension_config = {"adk": {}}
    store = SybaseADKStore(cast("Any", config))

    with pytest.raises(ValueError, match="does not match"):
        store.append_event_and_update_state(_sybase_event_fixture(**overrides), "app", "u1", "s1", {"step": 2})

    config.provide_connection.assert_not_called()


@pytest.mark.parametrize("session_exists", [True, False], ids=["owned-session", "missing-session"])
def test_sybase_adk_append_event_and_update_state_orders_update_before_insert(session_exists: bool) -> None:
    """SybaseADKStore.append_event_and_update_state updates and checks session row before inserting event."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    config = MagicMock()
    config.extension_config = {"adk": {}}
    conn = config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.fetchone.return_value = ("s1", "app", "u1", '{"step":2}', now, now) if session_exists else None
    store = SybaseADKStore(cast("Any", config))

    if session_exists:
        session = store.append_event_and_update_state(_sybase_event_fixture(), "app", "u1", "s1", {"step": 2})
        assert session["state"] == {"step": 2}
        executed_sql = [" ".join(c.args[0].split()) for c in cursor.execute.call_args_list]
        assert len(executed_sql) == 3
        assert executed_sql[0].startswith("UPDATE [dbo].[adk_session]")
        assert executed_sql[1].startswith("SELECT TOP 1 id, app_name")
        assert executed_sql[2].startswith("INSERT INTO [dbo].[adk_event]")
        conn.commit.assert_called_once()
    else:
        with pytest.raises(ValueError, match="Session s1 not found"):
            store.append_event_and_update_state(_sybase_event_fixture(), "app", "u1", "s1", {"step": 2})
        assert cursor.execute.call_count == 2
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()


def test_sybase_adk_delete_session_deletes_scoped_events_before_session() -> None:
    """SybaseADKStore.delete_session removes scoped events before deleting the session row."""
    config = MagicMock()
    config.extension_config = {"adk": {}}
    conn = config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    SybaseADKStore(cast("Any", config)).delete_session("app", "u1", "s1")

    executed = [(" ".join(c.args[0].split()), c.args[1]) for c in cursor.execute.call_args_list]
    assert executed == [
        ("DELETE FROM [dbo].[adk_event] WHERE app_name = %s AND user_id = %s AND session_id = %s", ("app", "u1", "s1")),
        ("DELETE FROM [dbo].[adk_session] WHERE app_name = %s AND user_id = %s AND id = %s", ("app", "u1", "s1")),
    ]
    conn.commit.assert_called_once()


@pytest.mark.parametrize(
    ("app_name", "user_id", "expected_sql", "expected_params"),
    [
        pytest.param(None, None, "DELETE FROM [dbo].[adk_memory] WHERE session_id = %s", ("s1",), id="session-only"),
        pytest.param(
            "app",
            None,
            "DELETE FROM [dbo].[adk_memory] WHERE session_id = %s AND app_name = %s",
            ("s1", "app"),
            id="session-and-app",
        ),
        pytest.param(
            None,
            "u1",
            "DELETE FROM [dbo].[adk_memory] WHERE session_id = %s AND user_id = %s",
            ("s1", "u1"),
            id="session-and-user",
        ),
        pytest.param(
            "app",
            "u1",
            "DELETE FROM [dbo].[adk_memory] WHERE session_id = %s AND app_name = %s AND user_id = %s",
            ("s1", "app", "u1"),
            id="session-app-and-user",
        ),
    ],
)
def test_sybase_adk_memory_dedup_and_scoped_delete_entries_by_session(
    monkeypatch: pytest.MonkeyPatch,
    app_name: "str | None",
    user_id: "str | None",
    expected_sql: str,
    expected_params: "tuple[str, ...]",
) -> None:
    """SybaseADKMemoryStore deduplicates on (app_name, user_id, event_id) and scopes session deletes."""
    config = MagicMock()
    config.extension_config = {"adk": {}}
    conn = config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.rowcount = 1
    now = datetime(2026, 1, 1, tzinfo=UTC)
    entry = cast(
        "Any",
        {
            "id": "mem-1",
            "session_id": "s1",
            "app_name": "app",
            "user_id": "u1",
            "event_id": "evt-1",
            "timestamp": now,
            "content_json": {"text": "hello"},
            "content_text": "hello",
            "metadata_json": None,
            "inserted_at": now,
        },
    )
    SybaseADKMemoryStore(cast("Any", config)).insert_memory_entries([entry])
    insert_sql, insert_params = cursor.execute.call_args.args
    assert (
        "WHERE NOT EXISTS ( SELECT 1 FROM [dbo].[adk_memory] HOLDLOCK "
        "WHERE app_name = %s AND user_id = %s AND event_id = %s )"
    ) in " ".join(insert_sql.split())
    assert insert_params[-3:] == ("app", "u1", "evt-1")

    delete_calls: list[tuple[str, tuple[Any, ...]]] = []

    def capture_delete(_self: SybaseADKMemoryStore, sql: str, params: "tuple[Any, ...]" = (), **_kw: Any) -> int:
        delete_calls.append((" ".join(sql.split()), tuple(params)))
        return 1

    monkeypatch.setattr(SybaseADKMemoryStore, "_execute", capture_delete)
    assert (
        SybaseADKMemoryStore(cast("Any", config)).delete_entries_by_session("s1", app_name=app_name, user_id=user_id)
        == 1
    )
    assert delete_calls == [(expected_sql, expected_params)]


def test_sybase_provide_session_releases_and_detaches_when_prepare_driver_fails(
    monkeypatch: pytest.MonkeyPatch, recording_sybase_config: tuple[SybaseConfig, FakeCursor]
) -> None:
    """SybaseConfig.provide_session releases the pool checkout and detaches the driver if setup raises."""
    config, _cursor = recording_sybase_config
    pool = config.provide_pool()
    seen: list[Any] = []

    def fail_prepare(_self: Any, driver: Any) -> Any:
        seen.append(driver)
        raise RuntimeError("prepare boom")

    monkeypatch.setattr(SybaseConfig, "_prepare_driver", fail_prepare)

    with pytest.raises(RuntimeError, match="prepare boom"), config.provide_session():
        pass

    assert pool.checked_out() == 0
    assert seen[0].connection is _DETACHED_CONNECTION


@pytest.mark.parametrize("autocommit", [False, True], ids=["autocommit-off", "autocommit-on"])
def test_sybase_pool_nested_acquire_rolls_back_only_on_outermost_release(
    monkeypatch: pytest.MonkeyPatch, autocommit: bool
) -> None:
    """Nested SybaseConnectionPool acquire/release calls roll back only on the outermost release."""
    monkeypatch.setenv("TDSVER", "5.0")
    connection = FakeConnection()
    connection.autocommit(autocommit)
    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=lambda **_kwargs: connection))
    pool = SybaseConnectionPool({"server": "ase.example.test"}, recycle_seconds=0, health_check_interval=0)
    try:
        outer = pool.acquire()
        inner = pool.acquire()
        assert inner is outer
        assert pool.checked_out() == 2

        pool.release(inner)
        assert pool.checked_out() == 1
        assert connection.rollbacks == 0

        pool.release(outer)
        assert pool.checked_out() == 0
        assert connection.rollbacks == (0 if autocommit else 1)
    finally:
        pool.close()


@pytest.mark.parametrize("method", ["provide_read_connection", "provide_read_session"])
@pytest.mark.parametrize(
    ("driver_exc", "expected_mapped"),
    [
        (pymssql.OperationalError(20009, b"Adaptive Server is unavailable"), DatabaseConnectionError),
        (pymssql.InterfaceError("Socket disconnected"), DatabaseConnectionError),
        (pymssql.OperationalError("Server lost"), OperationalError),
    ],
)
def test_sybase_read_replica_failover_maps_connection_errors(
    monkeypatch: pytest.MonkeyPatch, method: str, driver_exc: Exception, expected_mapped: type[Exception]
) -> None:
    """SybaseConfig connection, session, and replica paths map driver errors while preserving new_connection."""
    monkeypatch.setenv("TDSVER", "5.0")
    healthy_conn = FakeConnection()

    def connect(**kwargs: Any) -> Any:
        if kwargs.get("server") == "r1":
            raise driver_exc
        return healthy_conn

    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=connect))
    config = SybaseConfig(
        connection_config={"server": "writer", "user": "sa", "password": "secret", "database": "app"},
        replicas=["r1", "r2"],
    )
    try:
        with getattr(config, method)() as acquired:
            conn = acquired if method == "provide_read_connection" else acquired.connection
            assert conn is healthy_conn

        failing_only = SybaseConfig(
            connection_config={"server": "r1", "user": "sa", "password": "secret", "database": "app"}
        )
        try:
            with pytest.raises(expected_mapped) as exc_info:
                failing_only.create_connection()
            assert exc_info.value.__cause__ is driver_exc
            with pytest.raises(expected_mapped), failing_only.provide_connection():
                pass
            with pytest.raises(expected_mapped), failing_only.provide_session():
                pass
        finally:
            failing_only.close_pool()
    finally:
        config.close_pool()


def test_sybase_hook_runtime_error_not_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    """User hook RuntimeError propagates through Sybase provide_read_session without failover."""
    monkeypatch.setenv("TDSVER", "5.0")
    hook_conn = FakeConnection()
    monkeypatch.setattr(sybase_pool, "PYMSSQL_MODULE", SimpleNamespace(connect=lambda **_kwargs: hook_conn))

    def failing_hook(_conn: Any) -> None:
        raise RuntimeError("hook boom")

    config = SybaseConfig(
        connection_config={"server": "writer", "user": "sa", "password": "secret", "database": "app"},
        driver_features={"on_connection_create": failing_hook},
        replicas=["r1", "r2"],
    )
    try:
        with pytest.raises(RuntimeError, match="hook boom"), config.provide_read_session():
            pass
        assert hook_conn.closed is True
    finally:
        config.close_pool()
