"""Unit tests for PyMySQL ADK store extension configuration."""

from datetime import UTC, datetime, timedelta
from typing import Any, NotRequired, Self, cast, get_args, get_origin
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.pymysql._typing import PyMysqlMySQLError
from sqlspec.adapters.pymysql.adk import PyMysqlADKConfig, PyMysqlADKMemoryStore, PyMysqlADKStore
from sqlspec.adapters.pymysql.adk.store import _is_mysql_table_missing
from sqlspec.adapters.pymysql.core import PyMysqlStreamSource
from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import StoredEvent


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


class _MysqlMissingTableError(Exception):
    errno = 1146


def test_pymysql_table_missing_uses_errno_attribute() -> None:
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


def test_pymysql_adk_memory_store_insert_and_search_json_handling() -> None:
    """PyMysqlADKMemoryStore handles missing author, metadata_json=None, and deserializes JSON strings."""
    cursor = MagicMock()
    cursor.rowcount = 1
    now = datetime.now(tz=UTC)
    cursor.description = [
        ("id",),
        ("session_id",),
        ("app_name",),
        ("user_id",),
        ("scope",),
        ("event_id",),
        ("author",),
        ("timestamp",),
        ("content_json",),
        ("content_text",),
        ("metadata_json",),
        ("inserted_at",),
    ]
    cursor.fetchall.return_value = [
        (
            "mem-1",
            "sess-1",
            "app",
            "user-1",
            "user",
            "evt-1",
            None,
            now,
            '{"text": "hello"}',
            "hello",
            '{"source": "unit"}',
            now,
        )
    ]
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None
    config = _mock_config({"owner_id_column": "owner_id INT NULL"})
    config.provide_connection = lambda *_a, **_k: conn
    store = PyMysqlADKMemoryStore(config)
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

    inserted = store._insert_memory_entries([entry], owner_id=99)
    records = store._search_entries("hello", "app", "user-1")

    assert inserted == 1
    insert_params = cursor.execute.call_args_list[0].args[1]
    assert insert_params[6] is None
    assert insert_params[11] is None
    assert len(records) == 1
    assert records[0]["content_json"] == {"text": "hello"}
    assert records[0]["metadata_json"] == {"source": "unit"}


def test_pymysql_stream_source_closes_cursor_when_execute_raises() -> None:
    """PyMysqlStreamSource.start() closes the cursor if execute() fails."""
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("execute boom")
    connection = MagicMock()
    connection.cursor.return_value = cursor
    driver = MagicMock(connection=connection)
    driver._run_with_exception_handler = lambda _handler, fn: fn()
    source = PyMysqlStreamSource(driver, "SELECT 1", (), 100, set())

    with pytest.raises(RuntimeError, match="execute boom"):
        source.start()

    cursor.close.assert_called_once()
    assert source._cursor is None


def test_pymysql_adk_memory_store_missing_table_returns_empty_or_zero() -> None:
    """_search_entries and deletes return empty/0 when table is missing (errno 1146)."""
    missing_err = PyMysqlMySQLError(1146, "Table 'test.mem' doesn't exist")

    cursor = MagicMock()
    cursor.execute.side_effect = missing_err
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None
    conn.cursor.return_value = cursor
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn

    store = PyMysqlADKMemoryStore(config)
    assert store.search_entries("test", "app", "u1") == []
    assert store.delete_entries_by_session("sess-1") == 0
    assert store.delete_entries_older_than(30) == 0


def _make_pymysql_companion_driver(
    rows_by_lookup: dict[tuple[str | None, str | None], list[dict[str, Any]]], *, execute_side_effect: Any = None
) -> MagicMock:
    driver = MagicMock()
    driver.data_dictionary = MagicMock()

    def _get_indexes(_drv: Any, *, table: str | None = None, schema: str | None = None) -> list[dict[str, Any]]:
        return list(rows_by_lookup.get((schema, table), []))

    driver.data_dictionary.get_indexes = MagicMock(side_effect=_get_indexes)
    driver.execute = MagicMock(side_effect=execute_side_effect)
    driver.commit = MagicMock()
    return driver


def test_pymysql_companions_create_only_missing_session_index() -> None:
    store = PyMysqlADKStore(
        _mock_config({
            "session_table": "app.t",
            "events_table": "app.e",
            "enable_event_generated_columns": True,
            "enable_covering_indexes": True,
        })
    )
    driver = _make_pymysql_companion_driver({
        ("app", "t"): [
            {"index_name": "idx_t_app_user", "table_name": "t"},
            {"index_name": "idx_t_update_time", "table_name": "unrelated_table"},
        ],
        ("app", "e"): [
            {"index_name": "idx_e_scope"},
            {"index_name": "idx_e_session"},
            {"index_name": "idx_e_app_timestamp"},
            {"index_name": "idx_e_author_gc"},
            {"index_name": "idx_e_node_path_gc"},
        ],
    })

    store.reconcile_companion_objects_sync(driver)

    executed = [call.args[0] for call in driver.execute.call_args_list]
    assert executed == ["CREATE INDEX idx_t_update_time ON `app`.`t` (update_time DESC)"]
    assert driver.data_dictionary.get_indexes.call_args_list[0].kwargs == {"table": "t", "schema": "app"}
    assert driver.data_dictionary.get_indexes.call_args_list[1].kwargs == {"table": "e", "schema": "app"}
    driver.commit.assert_not_called()


def test_pymysql_companions_use_connection_database_for_bare_names() -> None:
    store = PyMysqlADKStore(
        _mock_config({
            "session_table": "custom_sessions",
            "events_table": "custom_events",
            "enable_event_generated_columns": True,
            "enable_covering_indexes": True,
        })
    )
    driver = _make_pymysql_companion_driver({})

    store.reconcile_companion_objects_sync(driver)

    executed = [call.args[0] for call in driver.execute.call_args_list]
    assert "CREATE INDEX idx_custom_sessions_app_user ON custom_sessions (app_name, user_id)" in executed
    assert (
        "CREATE INDEX idx_custom_events_scope ON custom_events (app_name, user_id, session_id, timestamp ASC, invocation_id)"
        in executed
    )
    assert (
        "CREATE INDEX idx_custom_events_author_gc ON custom_events (session_id, author_gc, timestamp ASC)" in executed
    )
    assert driver.data_dictionary.get_indexes.call_args_list[0].kwargs == {"table": "custom_sessions", "schema": None}
    assert driver.data_dictionary.get_indexes.call_args_list[1].kwargs == {"table": "custom_events", "schema": None}


def test_pymysql_companions_repeat_is_noop() -> None:
    store = PyMysqlADKStore(_mock_config({"session_table": "app.t", "events_table": "app.e"}))
    driver = _make_pymysql_companion_driver({
        ("app", "t"): [{"index_name": "IDX_T_APP_USER"}, {"index_name": "idx_t_update_time"}],
        ("app", "e"): [
            {"index_name": "idx_e_scope"},
            {"index_name": "idx_e_session"},
            {"index_name": "idx_e_app_timestamp"},
        ],
    })

    store.reconcile_companion_objects_sync(driver)

    assert driver.data_dictionary.get_indexes.call_count == 2
    driver.execute.assert_not_called()
    driver.commit.assert_not_called()


def test_pymysql_memory_companions_create_fulltext() -> None:
    memory_store = PyMysqlADKMemoryStore(_mock_config({"memory_table": "app.m", "memory_use_fts": True}))
    driver = _make_pymysql_companion_driver({})

    memory_store.reconcile_companion_objects_sync(driver)

    executed = [call.args[0] for call in driver.execute.call_args_list]
    assert executed == [
        "CREATE INDEX idx_m_app_scope_user_time ON `app`.`m` (app_name, scope, user_id, timestamp)",
        "CREATE INDEX idx_m_scope ON `app`.`m` (app_name, scope)",
        "CREATE INDEX idx_m_session ON `app`.`m` (session_id)",
        "CREATE FULLTEXT INDEX idx_m_fts ON `app`.`m` (content_text)",
    ]
    assert driver.data_dictionary.get_indexes.call_args_list[0].kwargs == {"table": "m", "schema": "app"}
    driver.commit.assert_not_called()

    disabled_store = PyMysqlADKMemoryStore(_mock_config({"enable_memory": False}))
    disabled_driver = _make_pymysql_companion_driver({})
    disabled_store.reconcile_companion_objects_sync(disabled_driver)
    disabled_driver.data_dictionary.get_indexes.assert_not_called()
    disabled_driver.execute.assert_not_called()


def test_pymysql_companions_propagate_failure_and_retry() -> None:
    store = PyMysqlADKStore(_mock_config({"session_table": "app.t", "events_table": "app.e"}))
    failing_driver = _make_pymysql_companion_driver({}, execute_side_effect=[None, RuntimeError("boom")])

    with pytest.raises(RuntimeError, match="boom"):
        store.reconcile_companion_objects_sync(failing_driver)

    failing_driver.commit.assert_not_called()

    retry_driver = _make_pymysql_companion_driver({("app", "t"): [{"index_name": "idx_t_app_user"}], ("app", "e"): []})
    store.reconcile_companion_objects_sync(retry_driver)

    retry_executed = [call.args[0] for call in retry_driver.execute.call_args_list]
    assert retry_executed == [
        "CREATE INDEX idx_t_update_time ON `app`.`t` (update_time DESC)",
        "CREATE INDEX idx_e_scope ON `app`.`e` (app_name, user_id, session_id, timestamp ASC)",
        "CREATE INDEX idx_e_session ON `app`.`e` (session_id, timestamp ASC)",
        "CREATE INDEX idx_e_app_timestamp ON `app`.`e` (app_name, timestamp ASC)",
    ]
    retry_driver.commit.assert_not_called()


def _make_pymysql_scoping_mocks(
    *, rowcount: int = 1, session_row: tuple[Any, ...] | None = None
) -> tuple[MagicMock, MagicMock, MagicMock]:
    cursor = MagicMock()
    cursor.rowcount = rowcount
    cursor.fetchone.return_value = session_row
    cursor.fetchall.return_value = []
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    return config, conn, cursor


@pytest.mark.parametrize(
    ("renew_for", "expect_update"),
    [
        pytest.param(None, False, id="none"),
        pytest.param(0, False, id="zero"),
        pytest.param(-1, False, id="negative"),
        pytest.param(timedelta(milliseconds=500), True, id="subsecond"),
        pytest.param(30, True, id="positive"),
    ],
)
def test_pymysql_get_session_renews_only_for_positive_duration(
    renew_for: "int | timedelta | None", expect_update: bool
) -> None:
    """get_session executes an UPDATE only when renew_for is positive."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    config, conn, cursor = _make_pymysql_scoping_mocks(session_row=("s1", "app", "u1", "{}", now, now))
    store = PyMysqlADKStore(config)

    result = store.get_session("app", "u1", "s1", renew_for=renew_for)

    assert result is not None
    executed_sqls = [_normalized(call.args[0]) for call in cursor.execute.call_args_list]
    assert any(sql.startswith("UPDATE adk_session") for sql in executed_sqls) is expect_update
    assert conn.commit.call_count == (1 if expect_update else 0)


@pytest.mark.parametrize(
    ("rowcount", "expect_succeed"),
    [pytest.param(1, True, id="owner-found"), pytest.param(0, False, id="owner-missing")],
)
def test_pymysql_append_event_enforces_session_ownership(rowcount: int, expect_succeed: bool) -> None:
    """append_event selects from adk_session by (id, app_name, user_id) and raises ValueError when 0 rows match."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    config, conn, cursor = _make_pymysql_scoping_mocks(rowcount=rowcount)
    store = PyMysqlADKStore(config)
    event = StoredEvent(
        id="evt-1",
        app_name="app-a",
        user_id="user-1",
        session_id="sess-1",
        invocation_id="inv-1",
        timestamp=now,
        event_data={"msg": "hi"},
    )

    if expect_succeed:
        store.append_event(event)
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
    else:
        with pytest.raises(ValueError, match="not found"):
            store.append_event(event)
        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()

    sql, params = cursor.execute.call_args_list[0].args
    assert "FROM adk_session WHERE id = %s AND app_name = %s AND user_id = %s" in _normalized(sql)
    assert params[-3:] == ("sess-1", "app-a", "user-1")


@pytest.mark.parametrize(
    ("record_app", "record_user", "record_session"),
    [
        pytest.param("other-app", "user-1", "sess-1", id="app-mismatch"),
        pytest.param("app-a", "other-user", "sess-1", id="user-mismatch"),
        pytest.param("app-a", "user-1", "other-sess", id="session-mismatch"),
    ],
)
def test_pymysql_append_event_and_update_state_rejects_mismatched_record(
    record_app: str, record_user: str, record_session: str
) -> None:
    """Mismatched event_record identifiers fail before executing any SQL statement."""
    config, _conn, cursor = _make_pymysql_scoping_mocks()
    store = PyMysqlADKStore(config)
    event = StoredEvent(
        id="evt-1",
        app_name=record_app,
        user_id=record_user,
        session_id=record_session,
        invocation_id="inv-1",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        event_data={},
    )

    with pytest.raises(ValueError, match="event_record scope"):
        store.append_event_and_update_state(event, "app-a", "user-1", "sess-1", {})

    cursor.execute.assert_not_called()


@pytest.mark.parametrize(
    ("rowcount", "expect_succeed"),
    [pytest.param(1, True, id="session-updated"), pytest.param(0, False, id="session-missing")],
)
def test_pymysql_append_event_and_update_state_updates_session_before_insert(
    rowcount: int, expect_succeed: bool
) -> None:
    """Session UPDATE runs before event INSERT; missing session raises ValueError without inserting."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    session_row = ("sess-1", "app-a", "user-1", '{"k": 1}', now, now) if expect_succeed else None
    config, conn, cursor = _make_pymysql_scoping_mocks(rowcount=rowcount, session_row=session_row)
    store = PyMysqlADKStore(config)
    event = StoredEvent(
        id="evt-1",
        app_name="app-a",
        user_id="user-1",
        session_id="sess-1",
        invocation_id="inv-1",
        timestamp=now,
        event_data={"k": 1},
    )

    if expect_succeed:
        updated = store.append_event_and_update_state(event, "app-a", "user-1", "sess-1", {"k": 1})
        assert updated["id"] == "sess-1"
        executed = [_normalized(call.args[0]) for call in cursor.execute.call_args_list]
        assert executed[0].startswith("UPDATE adk_session")
        assert executed[1].startswith("SELECT id, app_name, user_id, state")
        assert executed[2].startswith("INSERT INTO adk_event")
        conn.commit.assert_called_once()
    else:
        with pytest.raises(ValueError, match="not found"):
            store.append_event_and_update_state(event, "app-a", "user-1", "sess-1", {"k": 1})
        executed = [_normalized(call.args[0]) for call in cursor.execute.call_args_list]
        assert executed == [
            (
                "UPDATE adk_session SET state = %s, update_time = UTC_TIMESTAMP(6) "
                "WHERE app_name = %s AND user_id = %s AND id = %s"
            )
        ]
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()


def test_pymysql_get_events_joins_session_on_three_columns() -> None:
    """get_events joins adk_event to adk_session on (session_id, app_name, user_id)."""
    cutoff = datetime(2026, 1, 1, tzinfo=UTC)
    config, _conn, cursor = _make_pymysql_scoping_mocks()
    store = PyMysqlADKStore(config)

    store.get_events("app-a", "user-1", "sess-1", after_timestamp=cutoff, limit=5)

    sql, params = cursor.execute.call_args_list[0].args
    assert _normalized(sql) == (
        "SELECT e.id, e.app_name, e.user_id, e.session_id, e.invocation_id, e.timestamp, e.event_data "
        "FROM adk_event e JOIN adk_session s "
        "ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id "
        "WHERE s.app_name = %s AND s.user_id = %s AND s.id = %s AND e.timestamp > %s "
        "ORDER BY e.timestamp ASC LIMIT %s"
    )
    assert params == ("app-a", "user-1", "sess-1", cutoff, 5)


@pytest.mark.parametrize(
    ("app_name", "user_id", "expected_where", "expected_params"),
    [
        pytest.param(None, None, "session_id = %s", ("sess-1",), id="session-only"),
        pytest.param("app-a", None, "session_id = %s AND app_name = %s", ("sess-1", "app-a"), id="app-scoped"),
        pytest.param(
            "app-a",
            "user-1",
            "session_id = %s AND app_name = %s AND user_id = %s",
            ("sess-1", "app-a", "user-1"),
            id="app-user-scoped",
        ),
    ],
)
def test_pymysql_delete_entries_by_session_scopes_predicates(
    app_name: "str | None", user_id: "str | None", expected_where: str, expected_params: tuple[str, ...]
) -> None:
    """delete_entries_by_session appends app_name and user_id predicates when provided."""
    config, _conn, cursor = _make_pymysql_scoping_mocks(rowcount=2)
    memory_store = PyMysqlADKMemoryStore(config)

    deleted = memory_store.delete_entries_by_session("sess-1", app_name=app_name, user_id=user_id)

    assert deleted == 2
    sql, params = cursor.execute.call_args_list[0].args
    assert _normalized(sql) == f"DELETE FROM adk_memory WHERE {expected_where}"
    assert params == expected_params
    memory_ddl = memory_store._memory_table_ddl()
    assert "UNIQUE KEY idx_adk_memory_app_user_event (app_name, user_id, event_id)" in memory_ddl
    assert "NOT NULL UNIQUE" not in memory_ddl
