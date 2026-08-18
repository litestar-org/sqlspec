# pyright: reportPrivateUsage=false
"""Unit tests for PyMySQL ADK store extension configuration."""

from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

import pytest
from typing_extensions import NotRequired, Self

from sqlspec.adapters.pymysql.adk import PyMysqlADKConfig, PyMysqlADKMemoryStore, PyMysqlADKStore
from sqlspec.config import ADKConfig


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


class _MysqlMissingTableError(Exception):
    errno = 1146


def test_pymysql_table_missing_uses_errno_attribute() -> None:
    from sqlspec.adapters.pymysql.adk.store import _is_mysql_table_missing

    assert _is_mysql_table_missing(_MysqlMissingTableError()) is True


def test_pymysql_adk_config_types_adapter_local_mysql_options() -> None:
    """PyMySQL ADK MySQL options are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", PyMysqlADKConfig).__optional_keys__

    expected_types: dict[str, object] = {
        "enable_event_generated_columns": bool,
        "enable_covering_indexes": bool,
        "session_table_options": str,
        "events_table_options": str,
        "app_state_table_options": str,
        "user_state_table_options": str,
        "memory_table_options": str,
    }
    for feature_name, expected_type in expected_types.items():
        annotation = cast("Any", PyMysqlADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)


def test_pymysql_adk_tables_use_plain_mysql_schema_by_default() -> None:
    """PyMySQL ADK profile DDL stays opt-in through extension_config["adk"]."""

    store = PyMysqlADKStore(_mock_config())
    memory_store = PyMysqlADKMemoryStore(_mock_config())

    events_sql = store._events_table_ddl()
    memory_sql = memory_store._memory_table_ddl()

    assert "author_gc" not in events_sql
    assert "node_path_gc" not in events_sql
    assert "timestamp ASC, invocation_id" not in events_sql
    assert "COMMENT='adk-events'" not in events_sql
    assert "COMMENT='adk-memory'" not in memory_sql


def test_pymysql_adk_tables_apply_adapter_local_mysql_profile() -> None:
    """PyMySQL ADK options add generated columns, covering keys, and table options."""

    store = PyMysqlADKStore(
        _mock_config({
            "enable_event_generated_columns": True,
            "enable_covering_indexes": True,
            "session_table_options": "COMMENT='adk-session'",
            "events_table_options": "COMMENT='adk-events'",
            "app_state_table_options": "COMMENT='adk-app-state'",
            "user_state_table_options": "COMMENT='adk-user-state'",
        })
    )
    memory_store = PyMysqlADKMemoryStore(_mock_config({"memory_table_options": "COMMENT='adk-memory'"}))

    session_sql = store._sessions_table_ddl()
    events_sql = store._events_table_ddl()
    app_state_sql = store._app_states_table_ddl()
    user_state_sql = store._user_states_table_ddl()
    memory_sql = memory_store._memory_table_ddl()

    assert (
        "author_gc VARCHAR(256) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.author'))) STORED"
        in events_sql
    )
    assert (
        "node_path_gc VARCHAR(512) GENERATED ALWAYS AS "
        "(JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.node_info.path'))) STORED" in events_sql
    )
    assert "INDEX idx_adk_event_author_gc (session_id, author_gc, timestamp ASC)" in events_sql
    assert "INDEX idx_adk_event_node_path_gc (session_id, node_path_gc, timestamp ASC)" in events_sql
    assert "INDEX idx_adk_event_scope (app_name, user_id, session_id, timestamp ASC, invocation_id)" in events_sql
    assert "INDEX idx_adk_event_session (session_id, timestamp ASC, invocation_id)" in events_sql
    assert "COMMENT='adk-session'" in session_sql
    assert "COMMENT='adk-events'" in events_sql
    assert "COMMENT='adk-app-state'" in app_state_sql
    assert "COMMENT='adk-user-state'" in user_state_sql
    assert "COMMENT='adk-memory'" in memory_sql


class _RecordingCursor:
    """Records the SQL and bound parameters a session listing issues."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def record(self, sql: str, params: "Any" = None) -> None:
        self.calls.append((sql, tuple(params or ())))


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


class _SyncCursor(_RecordingCursor):
    def execute(self, sql: str, params: "Any" = None) -> None:
        self.record(sql, params)

    def fetchall(self) -> "list[Any]":
        return []

    def close(self) -> None:
        return None


class _SyncConnection:
    def __init__(self, cursor: _SyncCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _SyncCursor:
        return self._cursor

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, *_: Any) -> None:
        return None


def _sync_session_store_with_cursor() -> "tuple[PyMysqlADKStore, _SyncCursor]":
    cursor = _SyncCursor()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: _SyncConnection(cursor)
    return PyMysqlADKStore(config), cursor


def test_pymysql_list_sessions_binds_order_and_page() -> None:
    """Explicit ordering renders inline while page bounds bind as pyformat parameters."""
    store, cursor = _sync_session_store_with_cursor()

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = cursor.calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = %s AND user_id = %s "
        "ORDER BY create_time ASC, id ASC LIMIT %s OFFSET %s"
    )
    assert params == ("app", "u1", 10, 20)


def test_pymysql_list_sessions_defaults_to_recent_first_without_a_page() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, cursor = _sync_session_store_with_cursor()

    store.list_sessions("app")

    sql, params = cursor.calls[0]
    assert _normalized(sql).endswith("WHERE app_name = %s ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_pymysql_list_sessions_orders_page_after_scope_parameters() -> None:
    """Page values bind after the scope parameters that are actually present."""
    store, cursor = _sync_session_store_with_cursor()

    store.list_sessions("app", limit=5)

    sql, params = cursor.calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC LIMIT %s OFFSET %s")
    assert params == ("app", 5, 0)


def test_pymysql_list_sessions_zero_limit_never_queries() -> None:
    """A zero limit short-circuits before any database work."""
    store, cursor = _sync_session_store_with_cursor()

    assert store.list_sessions("app", limit=0) == []
    assert cursor.calls == []


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"order_by": "id"}, id="unknown-order-column"),
        pytest.param({"limit": -1}, id="negative-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"offset": 5}, id="unbounded-offset"),
    ],
)
def test_pymysql_list_sessions_rejects_invalid_options(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, cursor = _sync_session_store_with_cursor()

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert cursor.calls == []
