"""Unit tests for MySQL schema-qualified ADK table support across all MySQL drivers."""

import importlib
import inspect
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Self
from unittest.mock import MagicMock

import pytest

from sqlspec.extensions.adk import StoredEvent, StoredMemory
from sqlspec.migrations.schema import SchemaTarget

_ADAPTERS: list[tuple[str, str, str, bool]] = [
    ("aiomysql", "AiomysqlADKStore", "AiomysqlADKMemoryStore", True),
    ("asyncmy", "AsyncmyADKStore", "AsyncmyADKMemoryStore", True),
    ("pymysql", "PyMysqlADKStore", "PyMysqlADKMemoryStore", False),
    ("mysqlconnector", "MysqlConnectorSyncADKStore", "MysqlConnectorSyncADKMemoryStore", False),
    ("mysqlconnector", "MysqlConnectorAsyncADKStore", "MysqlConnectorAsyncADKMemoryStore", True),
]


def _adapter_id(adapter_info: tuple[str, str, str, bool]) -> str:
    if adapter_info[0] == "mysqlconnector":
        return "mysqlconnector-async" if adapter_info[3] else "mysqlconnector-sync"
    return adapter_info[0]


_QUALIFIED_SESSION_SETTINGS: dict[str, str] = {
    "session_table": "app.t",
    "events_table": "app.e",
    "app_state_table": "app.a",
    "user_state_table": "app.u",
    "metadata_table": "app.d",
}
_QUALIFIED_MEMORY_SETTINGS: dict[str, str] = {"memory_table": "app.m"}

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_SAMPLE_EVENT = StoredEvent(
    id="e1", app_name="a", user_id="u", session_id="s", invocation_id="inv1", timestamp=_NOW, event_data={"k": "v"}
)
_SAMPLE_MEMORY = StoredMemory(
    id="m1",
    session_id="s",
    app_name="a",
    user_id="u",
    scope="user",
    event_id="e1",
    author="tester",
    timestamp=_NOW,
    content_json={"text": "hello"},
    content_text="hello",
    metadata_json={"tag": "greet"},
    inserted_at=_NOW,
    embedding=None,
)
_SESSION_ROW: tuple[Any, ...] = ("s", "a", "u", "{}", _NOW, _NOW)


class _RecordingLog:
    """Accumulates executed SQL statements."""

    __slots__ = ("statements",)

    def __init__(self) -> None:
        self.statements: list[str] = []

    def record(self, sql: str) -> None:
        self.statements.append(sql)


class _AsyncCursorHandle:
    """Awaitable and directly usable asynchronous recording cursor."""

    __slots__ = ("_cursor",)

    def __init__(self, cursor: "_AsyncCursor") -> None:
        self._cursor = cursor

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)

    def __await__(self) -> Any:
        cursor = self._cursor

        async def _resolve() -> "_AsyncCursor":
            return cursor

        return _resolve().__await__()

    async def __aenter__(self) -> "_AsyncCursor":
        return self._cursor

    async def __aexit__(self, *args: Any) -> None:
        return None


class _AsyncCursor:
    """Asynchronous cursor fake recording statements and yielding dummy rows."""

    __slots__ = ("_cursor_rowcount", "_log", "_session_row", "description", "rowcount")

    def __init__(
        self, log: _RecordingLog, session_row: tuple[Any, ...] | None, *, cursor_rowcount: int | None = None
    ) -> None:
        self._log = log
        self._session_row = session_row
        self._cursor_rowcount = cursor_rowcount
        self.rowcount = 1 if cursor_rowcount is None else cursor_rowcount
        self.description = None

    async def execute(self, sql: str, params: Any = None) -> None:
        self._log.record(sql)
        if self._cursor_rowcount is not None:
            self.rowcount = self._cursor_rowcount

    async def executemany(self, sql: str, params_list: Any) -> None:
        self._log.record(sql)
        if self._cursor_rowcount is not None:
            self.rowcount = self._cursor_rowcount
        else:
            self.rowcount = len(params_list) if hasattr(params_list, "__len__") else 1

    async def fetchone(self) -> tuple[Any, ...] | None:
        return self._session_row

    async def fetchall(self) -> list[Any]:
        return []

    async def close(self) -> None:
        return None

    def __await__(self) -> Any:
        cursor = self

        async def _resolve() -> "_AsyncCursor":
            return cursor

        return _resolve().__await__()


class _AsyncConnection:
    """Asynchronous connection fake providing recording cursor."""

    __slots__ = ("_cursor",)

    def __init__(self, cursor: _AsyncCursor) -> None:
        self._cursor = cursor

    def cursor(self, *args: Any, **kwargs: Any) -> _AsyncCursorHandle:
        return _AsyncCursorHandle(self._cursor)

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None


class _AsyncDriver:
    """Asynchronous driver fake for provide_session lifecycle operations."""

    __slots__ = ("_log",)

    def __init__(self, log: _RecordingLog) -> None:
        self._log = log

    async def execute(self, sql: str, *args: Any, **kwargs: Any) -> None:
        self._log.record(sql)

    async def execute_script(self, sql: str, *args: Any, **kwargs: Any) -> None:
        self._log.record(sql)

    async def commit(self) -> None:
        return None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None


class _SyncCursor:
    """Synchronous cursor fake recording statements and yielding dummy rows."""

    __slots__ = ("_cursor_rowcount", "_log", "_session_row", "description", "rowcount")

    def __init__(
        self, log: _RecordingLog, session_row: tuple[Any, ...] | None, *, cursor_rowcount: int | None = None
    ) -> None:
        self._log = log
        self._session_row = session_row
        self._cursor_rowcount = cursor_rowcount
        self.rowcount = 1 if cursor_rowcount is None else cursor_rowcount
        self.description = None

    def execute(self, sql: str, params: Any = None) -> None:
        self._log.record(sql)
        if self._cursor_rowcount is not None:
            self.rowcount = self._cursor_rowcount

    def executemany(self, sql: str, params_list: Any) -> None:
        self._log.record(sql)
        if self._cursor_rowcount is not None:
            self.rowcount = self._cursor_rowcount
        else:
            self.rowcount = len(params_list) if hasattr(params_list, "__len__") else 1

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._session_row

    def fetchall(self) -> list[Any]:
        return []

    def close(self) -> None:
        return None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        return None


class _SyncConnection:
    """Synchronous connection fake providing recording cursor."""

    __slots__ = ("_cursor",)

    def __init__(self, cursor: _SyncCursor) -> None:
        self._cursor = cursor

    def cursor(self, *args: Any, **kwargs: Any) -> _SyncCursor:
        return self._cursor

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        return None


class _SyncDriver:
    """Synchronous driver fake for provide_session lifecycle operations."""

    __slots__ = ("_log",)

    def __init__(self, log: _RecordingLog) -> None:
        self._log = log

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> None:
        self._log.record(sql)

    def execute_script(self, sql: str, *args: Any, **kwargs: Any) -> None:
        self._log.record(sql)

    def commit(self) -> None:
        return None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        return None


def _load_store_class(adapter_pkg: str, cls_name: str) -> Any:
    pytest.importorskip(f"sqlspec.adapters.{adapter_pkg}")
    module = importlib.import_module(f"sqlspec.adapters.{adapter_pkg}.adk")
    return getattr(module, cls_name)


def _store(
    adapter_info: tuple[str, str, str, bool],
    settings: dict[str, Any],
    session_row: tuple[Any, ...] | None = None,
    is_memory: bool = False,
    cursor_rowcount: int | None = None,
) -> tuple[Any, list[str]]:
    adapter_pkg, session_cls_name, memory_cls_name, is_async = adapter_info
    store_cls_name = memory_cls_name if is_memory else session_cls_name
    store_cls = _load_store_class(adapter_pkg, store_cls_name)

    log = _RecordingLog()
    adk_settings = dict(settings)
    if "use_fts" in adk_settings:
        adk_settings["memory_use_fts"] = adk_settings["use_fts"]

    config = MagicMock()
    config.extension_config = {"adk": adk_settings}

    conn: _AsyncConnection | _SyncConnection
    driver: _AsyncDriver | _SyncDriver
    if is_async:
        cursor = _AsyncCursor(log, session_row, cursor_rowcount=cursor_rowcount)
        conn = _AsyncConnection(cursor)
        driver = _AsyncDriver(log)
    else:
        sync_cursor = _SyncCursor(log, session_row, cursor_rowcount=cursor_rowcount)
        conn = _SyncConnection(sync_cursor)
        driver = _SyncDriver(log)

    config.provide_connection = lambda *args, **kwargs: conn
    config.provide_session = lambda *args, **kwargs: driver

    store_instance = store_cls(config)
    return store_instance, log.statements


async def _call(is_async: bool, target: Any, method_name: str, *args: Any, **kwargs: Any) -> Any:
    fn = getattr(target, method_name)
    result = fn(*args, **kwargs)
    if is_async and inspect.isawaitable(result):
        return await result
    return result


@dataclass(frozen=True, slots=True)
class _DMLCase:
    name: str
    is_memory: bool
    method: str
    args_fn: Callable[[], tuple[Any, ...]]
    kwargs_fn: Callable[[], dict[str, Any]]
    extra_settings: dict[str, Any] | None
    needs_session_row: bool
    required_refs: tuple[str, ...]


_DML_CASES: list[_DMLCase] = [
    _DMLCase("create_session", False, "create_session", lambda: ("s", "a", "u", {}), dict, None, True, ("`app`.`t`",)),
    _DMLCase(
        "create_session_owner",
        False,
        "create_session",
        lambda: ("s", "a", "u", {}, 1),
        dict,
        {"owner_id_column": "tenant_id INT"},
        True,
        ("`app`.`t`",),
    ),
    _DMLCase("get_session", False, "get_session", lambda: ("a", "u", "s"), dict, None, False, ("`app`.`t`",)),
    _DMLCase(
        "get_session_renew",
        False,
        "get_session",
        lambda: ("a", "u", "s"),
        lambda: {"renew_for": 60},
        None,
        True,
        ("`app`.`t`",),
    ),
    _DMLCase(
        "update_session_state",
        False,
        "update_session_state",
        lambda: ("a", "u", "s", {}),
        dict,
        None,
        False,
        ("`app`.`t`",),
    ),
    _DMLCase("list_sessions", False, "list_sessions", lambda: ("a",), dict, None, False, ("`app`.`t`",)),
    _DMLCase("delete_session", False, "delete_session", lambda: ("a", "u", "s"), dict, None, False, ("`app`.`t`",)),
    _DMLCase("append_event", False, "append_event", lambda: (_SAMPLE_EVENT,), dict, None, False, ("`app`.`e`",)),
    _DMLCase(
        "append_event_and_update_state",
        False,
        "append_event_and_update_state",
        lambda: (_SAMPLE_EVENT, "a", "u", "s", {}),
        dict,
        None,
        True,
        ("`app`.`e`", "`app`.`t`"),
    ),
    _DMLCase("get_events", False, "get_events", lambda: ("a", "u", "s"), dict, None, False, ("`app`.`e`",)),
    _DMLCase(
        "delete_expired_events", False, "delete_expired_events", lambda: (_NOW, "a"), dict, None, False, ("`app`.`e`",)
    ),
    _DMLCase("delete_idle_sessions", False, "delete_idle_sessions", lambda: (_NOW,), dict, None, False, ("`app`.`t`",)),
    _DMLCase(
        "delete_idle_user_states", False, "delete_idle_user_states", lambda: (_NOW,), dict, None, False, ("`app`.`u`",)
    ),
    _DMLCase("get_app_state", False, "get_app_state", lambda: ("a",), dict, None, False, ("`app`.`a`",)),
    _DMLCase("upsert_app_state", False, "upsert_app_state", lambda: ("a", {}), dict, None, False, ("`app`.`a`",)),
    _DMLCase("get_user_state", False, "get_user_state", lambda: ("a", "u"), dict, None, False, ("`app`.`u`",)),
    _DMLCase(
        "upsert_user_state", False, "upsert_user_state", lambda: ("a", "u", {}), dict, None, False, ("`app`.`u`",)
    ),
    _DMLCase("get_metadata", False, "get_metadata", lambda: ("k",), dict, None, False, ("`app`.`d`",)),
    _DMLCase("set_metadata", False, "set_metadata", lambda: ("k", "v"), dict, None, False, ("`app`.`d`",)),
    _DMLCase(
        "drop_tables",
        False,
        "drop_tables",
        tuple,
        dict,
        None,
        False,
        ("`app`.`e`", "`app`.`t`", "`app`.`a`", "`app`.`u`", "`app`.`d`"),
    ),
    _DMLCase(
        "insert_memory_entries",
        True,
        "insert_memory_entries",
        lambda: ([_SAMPLE_MEMORY],),
        dict,
        None,
        False,
        ("`app`.`m`",),
    ),
    _DMLCase(
        "insert_memory_entries_owner",
        True,
        "insert_memory_entries",
        lambda: ([_SAMPLE_MEMORY], 1),
        dict,
        {"owner_id_column": "tenant_id INT"},
        False,
        ("`app`.`m`",),
    ),
    _DMLCase("search_entries", True, "search_entries", lambda: ("q", "a", "u"), dict, None, False, ("`app`.`m`",)),
    _DMLCase(
        "search_entries_fts",
        True,
        "search_entries",
        lambda: ("q", "a", "u"),
        dict,
        {"use_fts": True},
        False,
        ("`app`.`m`",),
    ),
    _DMLCase(
        "delete_entries_by_session",
        True,
        "delete_entries_by_session",
        lambda: ("s",),
        dict,
        None,
        False,
        ("`app`.`m`",),
    ),
    _DMLCase(
        "delete_entries_older_than", True, "delete_entries_older_than", lambda: (7,), dict, None, False, ("`app`.`m`",)
    ),
    _DMLCase("drop_tables_memory", True, "drop_tables", tuple, dict, None, False, ("`app`.`m`",)),
]

_UNQUOTED_QUALIFIED_PATTERN = re.compile(r"(?<![`\w])app\.")


@pytest.mark.parametrize("adapter_info", _ADAPTERS, ids=_adapter_id)
@pytest.mark.parametrize("case", _DML_CASES, ids=lambda c: c.name)
async def test_mysql_adk_qualified_dml_sweep(adapter_info: tuple[str, str, str, bool], case: _DMLCase) -> None:
    """Qualified DML calls format database.table references with backticks and avoid unquoted parts."""
    base_settings = _QUALIFIED_MEMORY_SETTINGS if case.is_memory else _QUALIFIED_SESSION_SETTINGS
    settings = {**base_settings, **(case.extra_settings or {})}
    session_row = _SESSION_ROW if case.needs_session_row else None
    store, log = _store(adapter_info, settings, session_row=session_row, is_memory=case.is_memory)

    await _call(adapter_info[3], store, case.method, *case.args_fn(), **case.kwargs_fn())

    assert log, f"Expected non-empty statement log for {case.name}"
    for required in case.required_refs:
        assert any(required in stmt for stmt in log), f"Expected reference {required!r} in log for {case.name}: {log}"
    for stmt in log:
        assert not _UNQUOTED_QUALIFIED_PATTERN.search(stmt), f"Found unquoted app. in statement: {stmt}"


@pytest.mark.parametrize("adapter_info", _ADAPTERS, ids=_adapter_id)
@pytest.mark.parametrize("case", _DML_CASES, ids=lambda c: c.name)
async def test_mysql_adk_bare_dml_keeps_unquoted_names(
    adapter_info: tuple[str, str, str, bool], case: _DMLCase
) -> None:
    """Bare default table names stay unquoted across all DML operations."""
    settings = dict(case.extra_settings or {})
    session_row = _SESSION_ROW if case.needs_session_row else None
    store, log = _store(adapter_info, settings, session_row=session_row, is_memory=case.is_memory)

    await _call(adapter_info[3], store, case.method, *case.args_fn(), **case.kwargs_fn())

    assert log, f"Expected non-empty statement log for bare {case.name}"
    prohibited = ("`adk_session`", "`adk_events`", "`adk_event`", "`adk_memory`")
    for stmt in log:
        for token in prohibited:
            assert token not in stmt, f"Found prohibited backticked token {token!r} in bare statement: {stmt}"


@pytest.mark.parametrize("adapter_info", _ADAPTERS, ids=_adapter_id)
async def test_mysql_adk_qualified_session_ddl(adapter_info: tuple[str, str, str, bool]) -> None:
    """Session DDL uses backtick table references and bare-name index identifiers."""
    store, _ = _store(adapter_info, _QUALIFIED_SESSION_SETTINGS)
    ddl = await _call(adapter_info[3], store, "_sessions_table_ddl")

    assert "CREATE TABLE IF NOT EXISTS `app`.`t` (" in ddl
    assert "INDEX idx_t_app_user (app_name, user_id)" in ddl
    assert "INDEX idx_t_update_time (update_time DESC)" in ddl
    assert "idx_app" not in ddl


@pytest.mark.parametrize("adapter_info", _ADAPTERS, ids=_adapter_id)
async def test_mysql_adk_qualified_events_reference_sessions(adapter_info: tuple[str, str, str, bool]) -> None:
    """Events DDL references qualified session table and derives index from bare table name."""
    store, _ = _store(adapter_info, _QUALIFIED_SESSION_SETTINGS)
    ddl = await _call(adapter_info[3], store, "_events_table_ddl")

    assert "FOREIGN KEY (session_id) REFERENCES `app`.`t`(id) ON DELETE CASCADE" in ddl
    assert "INDEX idx_e_session" in ddl


@pytest.mark.parametrize("adapter_info", _ADAPTERS, ids=_adapter_id)
async def test_mysql_adk_qualified_memory_ddl(adapter_info: tuple[str, str, str, bool]) -> None:
    """Memory DDL formats qualified table and bare index names including full-text index."""
    settings = {"memory_table": "app.m", "use_fts": True}
    store, _ = _store(adapter_info, settings, is_memory=True)
    ddl = await _call(adapter_info[3], store, "_memory_table_ddl")

    assert "CREATE TABLE IF NOT EXISTS `app`.`m` (" in ddl
    assert "INDEX idx_m_session (session_id)" in ddl
    assert "FULLTEXT INDEX idx_m_fts (content_text)" in ddl


@pytest.mark.parametrize("adapter_info", _ADAPTERS, ids=_adapter_id)
async def test_mysql_adk_qualified_schema_target(adapter_info: tuple[str, str, str, bool]) -> None:
    """SchemaTarget parses schema and table_name components from qualified ADK DDL."""
    store, _ = _store(adapter_info, _QUALIFIED_SESSION_SETTINGS)
    ddl = await _call(adapter_info[3], store, "_sessions_table_ddl")

    target = SchemaTarget.from_ddl("app.t", ddl, dialect="mysql")
    assert target.schema == "app"
    assert target.table_name == "t"


@pytest.mark.parametrize("adapter_info", _ADAPTERS, ids=_adapter_id)
async def test_mysql_adk_bare_ddl_is_unchanged(adapter_info: tuple[str, str, str, bool]) -> None:
    """Default table names produce identical unquoted DDL and preserved index identifiers."""
    store, _ = _store(adapter_info, {})
    ddl = await _call(adapter_info[3], store, "_sessions_table_ddl")

    assert "CREATE TABLE IF NOT EXISTS adk_session (" in ddl
    assert "INDEX idx_adk_session_app_user (app_name, user_id)" in ddl


@pytest.mark.parametrize("adapter_info", _ADAPTERS, ids=_adapter_id)
async def test_mysql_adk_split_database_tables(adapter_info: tuple[str, str, str, bool]) -> None:
    """Tables located across different databases maintain independent qualification and FKs."""
    settings = {"session_table": "a.t", "events_table": "b.e"}
    store, log = _store(adapter_info, settings)

    events_ddl = await _call(adapter_info[3], store, "_events_table_ddl")
    assert "CREATE TABLE IF NOT EXISTS `b`.`e` (" in events_ddl
    assert "REFERENCES `a`.`t`(id)" in events_ddl

    await _call(adapter_info[3], store, "get_session", "a", "u", "s")
    assert any("`a`.`t`" in stmt for stmt in log)

    await _call(adapter_info[3], store, "append_event", _SAMPLE_EVENT)
    assert any("`b`.`e`" in stmt for stmt in log)


@pytest.mark.parametrize("adapter_info", _ADAPTERS, ids=_adapter_id)
async def test_mysql_adk_same_bare_table_in_two_databases(adapter_info: tuple[str, str, str, bool]) -> None:
    """Identical bare table names in distinct databases produce non-conflicting table-scoped indexes."""
    store1, _ = _store(adapter_info, {"session_table": "a.t"})
    store2, _ = _store(adapter_info, {"session_table": "b.t"})

    ddl1 = await _call(adapter_info[3], store1, "_sessions_table_ddl")
    ddl2 = await _call(adapter_info[3], store2, "_sessions_table_ddl")

    assert "INDEX idx_t_app_user" in ddl1
    assert "INDEX idx_t_app_user" in ddl2


@pytest.mark.parametrize("adapter_info", _ADAPTERS, ids=_adapter_id)
@pytest.mark.parametrize("invalid_name", ["x.app.t", "app.t-1", "`app`.`t`", '"app"."t"'])
def test_mysql_adk_rejects_invalid_qualified_names(adapter_info: tuple[str, str, str, bool], invalid_name: str) -> None:
    """Names with >2 parts, invalid identifier syntax, or explicit quoting are rejected."""
    with pytest.raises(ValueError, match="Invalid table name"):
        _store(adapter_info, {"session_table": invalid_name}, is_memory=False)

    with pytest.raises(ValueError, match="Invalid table name"):
        _store(adapter_info, {"memory_table": invalid_name}, is_memory=True)


@pytest.mark.parametrize("adapter_info", _ADAPTERS, ids=_adapter_id)
def test_mysql_adk_public_table_properties_keep_configured_value(adapter_info: tuple[str, str, str, bool]) -> None:
    """Configured qualified table names are preserved verbatim on public store properties."""
    session_store, _ = _store(adapter_info, _QUALIFIED_SESSION_SETTINGS, is_memory=False)
    memory_store, _ = _store(adapter_info, _QUALIFIED_MEMORY_SETTINGS, is_memory=True)

    assert session_store.session_table == "app.t"
    assert session_store.events_table == "app.e"
    assert session_store.app_state_table == "app.a"
    assert session_store.user_state_table == "app.u"
    assert session_store.metadata_table == "app.d"
    assert memory_store.memory_table == "app.m"


@pytest.mark.parametrize("adapter_info", _ADAPTERS, ids=_adapter_id)
async def test_mysql_adk_insert_memory_entries_returns_zero_on_duplicates(
    adapter_info: tuple[str, str, str, bool],
) -> None:
    """Duplicate INSERT IGNORE batches with cursor.rowcount == 0 return 0 across all MySQL drivers."""
    store, _ = _store(adapter_info, _QUALIFIED_MEMORY_SETTINGS, is_memory=True, cursor_rowcount=0)
    inserted = await _call(adapter_info[3], store, "insert_memory_entries", [_SAMPLE_MEMORY])
    assert inserted == 0
