"""Unit tests for mysql-connector ADK store extension configuration."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, NotRequired, Self, cast, get_args, get_origin
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.mysqlconnector._typing import MysqlConnectorError
from sqlspec.adapters.mysqlconnector.adk import (
    MysqlConnectorADKConfig,
    MysqlConnectorAsyncADKMemoryStore,
    MysqlConnectorAsyncADKStore,
    MysqlConnectorSyncADKMemoryStore,
    MysqlConnectorSyncADKStore,
)
from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import StoredEvent


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def test_mysqlconnector_adk_config_types_adapter_local_mysql_options() -> None:
    """mysql-connector ADK MySQL options are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", MysqlConnectorADKConfig).__optional_keys__

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
        annotation = cast("Any", MysqlConnectorADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)


def test_mysqlconnector_async_adk_tables_use_plain_mysql_schema_by_default() -> None:
    """mysql-connector async ADK profile DDL stays opt-in through extension_config["adk"]."""

    store = MysqlConnectorAsyncADKStore(_mock_config())
    memory_store = MysqlConnectorAsyncADKMemoryStore(_mock_config())

    events_sql = asyncio.run(store._events_table_ddl())
    memory_sql = asyncio.run(memory_store._memory_table_ddl())

    assert "author_gc" not in events_sql
    assert "node_path_gc" not in events_sql
    assert "timestamp ASC, invocation_id" not in events_sql
    assert "COMMENT='adk-events'" not in events_sql
    assert "COMMENT='adk-memory'" not in memory_sql


def test_mysqlconnector_async_adk_tables_apply_adapter_local_mysql_profile() -> None:
    """mysql-connector async ADK options add generated columns, covering keys, and table options."""

    store = MysqlConnectorAsyncADKStore(
        _mock_config({
            "enable_event_generated_columns": True,
            "enable_covering_indexes": True,
            "session_table_options": "COMMENT='adk-session'",
            "events_table_options": "COMMENT='adk-events'",
            "app_state_table_options": "COMMENT='adk-app-state'",
            "user_state_table_options": "COMMENT='adk-user-state'",
        })
    )
    memory_store = MysqlConnectorAsyncADKMemoryStore(_mock_config({"memory_table_options": "COMMENT='adk-memory'"}))

    session_sql = asyncio.run(store._sessions_table_ddl())
    events_sql = asyncio.run(store._events_table_ddl())
    app_state_sql = asyncio.run(store._app_states_table_ddl())
    user_state_sql = asyncio.run(store._user_states_table_ddl())
    memory_sql = asyncio.run(memory_store._memory_table_ddl())

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


def test_mysqlconnector_sync_adk_tables_apply_same_mysql_profile() -> None:
    """mysql-connector sync ADK store uses the same extension_config["adk"] MySQL profile."""

    store = MysqlConnectorSyncADKStore(
        _mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True})
    )
    memory_store = MysqlConnectorSyncADKMemoryStore(_mock_config({"memory_table_options": "COMMENT='adk-memory'"}))

    events_sql = store._events_table_ddl()
    memory_sql = memory_store._memory_table_ddl()

    assert (
        "author_gc VARCHAR(256) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(event_data, '$.author'))) STORED"
        in events_sql
    )
    assert "INDEX idx_adk_event_session (session_id, timestamp ASC, invocation_id)" in events_sql
    assert "COMMENT='adk-memory'" in memory_sql


class _RecordingCursor:
    """Records the SQL and bound parameters a session listing issues."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def record(self, sql: str, params: "Any" = None) -> None:
        self.calls.append((sql, tuple(params or ())))


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


class _AsyncCursor(_RecordingCursor):
    async def execute(self, sql: str, params: "Any" = None) -> None:
        self.record(sql, params)

    async def fetchall(self) -> "list[Any]":
        return []

    async def close(self) -> None:
        return None


class _SyncCursor(_RecordingCursor):
    def execute(self, sql: str, params: "Any" = None) -> None:
        self.record(sql, params)

    def fetchall(self) -> "list[Any]":
        return []

    def close(self) -> None:
        return None


class _AsyncConnection:
    def __init__(self, cursor: _AsyncCursor) -> None:
        self._cursor = cursor

    async def cursor(self) -> _AsyncCursor:
        return self._cursor

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(self, *_: Any) -> None:
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


def _session_store_with_cursor() -> "tuple[MysqlConnectorAsyncADKStore, _AsyncCursor]":
    cursor = _AsyncCursor()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: _AsyncConnection(cursor)
    return MysqlConnectorAsyncADKStore(config), cursor


def _sync_session_store_with_cursor() -> "tuple[MysqlConnectorSyncADKStore, _SyncCursor]":
    cursor = _SyncCursor()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: _SyncConnection(cursor)
    return MysqlConnectorSyncADKStore(config), cursor


async def test_mysqlconnector_async_list_sessions_binds_order_and_page() -> None:
    """Explicit ordering renders inline while page bounds bind as pyformat parameters."""
    store, cursor = _session_store_with_cursor()

    await store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = cursor.calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = %s AND user_id = %s "
        "ORDER BY create_time ASC, id ASC LIMIT %s OFFSET %s"
    )
    assert params == ("app", "u1", 10, 20)


async def test_mysqlconnector_async_list_sessions_defaults_to_recent_first_without_a_page() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, cursor = _session_store_with_cursor()

    await store.list_sessions("app")

    sql, params = cursor.calls[0]
    assert _normalized(sql).endswith("WHERE app_name = %s ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


async def test_mysqlconnector_async_list_sessions_orders_page_after_scope_parameters() -> None:
    """Page values bind after the scope parameters that are actually present."""
    store, cursor = _session_store_with_cursor()

    await store.list_sessions("app", limit=5)

    sql, params = cursor.calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC LIMIT %s OFFSET %s")
    assert params == ("app", 5, 0)


async def test_mysqlconnector_async_list_sessions_zero_limit_never_queries() -> None:
    """A zero limit short-circuits before any database work."""
    store, cursor = _session_store_with_cursor()

    assert await store.list_sessions("app", limit=0) == []
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
async def test_mysqlconnector_async_list_sessions_rejects_invalid_options(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, cursor = _session_store_with_cursor()

    with pytest.raises(ValueError):
        await store.list_sessions("app", **options)

    assert cursor.calls == []


def test_mysqlconnector_sync_list_sessions_binds_order_and_page() -> None:
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


def test_mysqlconnector_sync_list_sessions_defaults_to_recent_first_without_a_page() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, cursor = _sync_session_store_with_cursor()

    store.list_sessions("app")

    sql, params = cursor.calls[0]
    assert _normalized(sql).endswith("WHERE app_name = %s ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_mysqlconnector_sync_list_sessions_orders_page_after_scope_parameters() -> None:
    """Page values bind after the scope parameters that are actually present."""
    store, cursor = _sync_session_store_with_cursor()

    store.list_sessions("app", limit=5)

    sql, params = cursor.calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC LIMIT %s OFFSET %s")
    assert params == ("app", 5, 0)


def test_mysqlconnector_sync_list_sessions_zero_limit_never_queries() -> None:
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
def test_mysqlconnector_sync_list_sessions_rejects_invalid_options(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, cursor = _sync_session_store_with_cursor()

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert cursor.calls == []


def test_mysqlconnector_sync_adk_memory_store_insert_and_search_json_handling() -> None:
    """MysqlConnectorSyncADKMemoryStore handles missing author, metadata_json=None, and deserializes JSON strings."""
    now = datetime.now(tz=UTC)
    cursor = MagicMock()
    cursor.rowcount = 1
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
    store = MysqlConnectorSyncADKMemoryStore(config)
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

    inserted = store.insert_memory_entries([entry], owner_id=99)
    records = store.search_entries("hello", "app", "user-1")

    assert inserted == 1
    insert_params = cursor.execute.call_args_list[0].args[1]
    assert insert_params[6] is None
    assert insert_params[11] is None
    assert len(records) == 1
    assert records[0]["content_json"] == {"text": "hello"}
    assert records[0]["metadata_json"] == {"source": "unit"}


async def test_mysqlconnector_async_adk_memory_store_insert_and_search_json_handling() -> None:
    """MysqlConnectorAsyncADKMemoryStore handles missing author, metadata_json=None, and deserializes JSON strings."""
    now = datetime.now(tz=UTC)
    cursor = MagicMock()
    cursor.rowcount = 1
    cursor.execute = AsyncMock()
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
    cursor.fetchall = AsyncMock(
        return_value=[
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
    )
    cursor.close = AsyncMock()
    conn = MagicMock()
    conn.cursor = AsyncMock(return_value=cursor)
    conn.commit = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    config = _mock_config({"owner_id_column": "owner_id INT NULL"})
    config.provide_connection = lambda *_a, **_k: conn
    store = MysqlConnectorAsyncADKMemoryStore(config)
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

    inserted = await store.insert_memory_entries([entry], owner_id=99)
    records = await store.search_entries("hello", "app", "user-1")

    assert inserted == 1
    insert_params = cursor.execute.call_args_list[0].args[1]
    assert insert_params[6] is None
    assert insert_params[11] is None
    assert len(records) == 1
    assert records[0]["content_json"] == {"text": "hello"}
    assert records[0]["metadata_json"] == {"source": "unit"}


def test_mysqlconnector_sync_append_event_rolls_back_if_commit_fails() -> None:
    """MysqlConnectorSyncADKStore rolls back when conn.commit() fails."""
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = ("sess-1", "app", "u1", "{}", None, None)
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None
    conn.cursor.return_value = cursor
    conn.commit.side_effect = RuntimeError("commit failed")
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn

    store = MysqlConnectorSyncADKStore(config)
    event: StoredEvent = {
        "id": "e-1",
        "session_id": "sess-1",
        "app_name": "app",
        "user_id": "u1",
        "event_data": {"msg": "hi"},
        "timestamp": datetime.now(UTC),
        "invocation_id": "inv-1",
    }

    with pytest.raises(RuntimeError, match="commit failed"):
        store.append_event_and_update_state(event, "app", "u1", "sess-1", {"state": "active"})

    assert conn.rollback.called


async def test_mysqlconnector_memory_store_missing_table_handling() -> None:
    """search_entries and deletes return empty/0 when table is missing."""
    missing_err = MysqlConnectorError(errno=1146, msg="Table 'test.mem' doesn't exist")

    sync_cursor = MagicMock()
    sync_cursor.rowcount = 0
    sync_cursor.execute.side_effect = missing_err
    sync_conn = MagicMock()
    sync_conn.__enter__.return_value = sync_conn
    sync_conn.__exit__.return_value = None
    sync_conn.cursor.return_value = sync_cursor
    sync_config = _mock_config()
    sync_config.provide_connection = lambda *_a, **_k: sync_conn

    sync_store = MysqlConnectorSyncADKMemoryStore(sync_config)
    assert sync_store.search_entries("test", "app", "u1") == []
    assert sync_store.delete_entries_by_session("sess-1") == 0
    assert sync_store.delete_entries_older_than(30) == 0

    async_cursor = MagicMock()
    async_cursor.execute = AsyncMock(side_effect=missing_err)
    async_cursor.close = AsyncMock()
    async_conn = MagicMock()
    async_conn.cursor = AsyncMock(return_value=async_cursor)
    async_conn.__aenter__ = AsyncMock(return_value=async_conn)
    async_conn.__aexit__ = AsyncMock(return_value=None)
    async_config = _mock_config()
    async_config.provide_connection = lambda *_a, **_k: async_conn

    async_store = MysqlConnectorAsyncADKMemoryStore(async_config)
    assert await async_store.search_entries("test", "app", "u1") == []
    assert await async_store.delete_entries_by_session("sess-1") == 0
    assert await async_store.delete_entries_older_than(30) == 0


def _make_mysqlconnector_companion_driver(
    mode: str,
    rows_by_lookup: dict[tuple[str | None, str | None], list[dict[str, Any]]],
    *,
    execute_side_effect: Any = None,
) -> MagicMock:
    driver = MagicMock()
    driver.data_dictionary = MagicMock()

    def _lookup(_drv: Any, *, table: str | None = None, schema: str | None = None) -> list[dict[str, Any]]:
        return list(rows_by_lookup.get((schema, table), []))

    if mode == "async":

        async def _get_indexes_async(
            drv: Any, *, table: str | None = None, schema: str | None = None
        ) -> list[dict[str, Any]]:
            return _lookup(drv, table=table, schema=schema)

        driver.data_dictionary.get_indexes = AsyncMock(side_effect=_get_indexes_async)
        driver.execute = AsyncMock(side_effect=execute_side_effect)
        driver.commit = AsyncMock()
    else:
        driver.data_dictionary.get_indexes = MagicMock(side_effect=_lookup)
        driver.execute = MagicMock(side_effect=execute_side_effect)
        driver.commit = MagicMock()
    return driver


async def _run_mysqlconnector_companion(mode: str, store: Any, driver: MagicMock) -> None:
    if mode == "async":
        await store.reconcile_companion_objects_async(driver)
    else:
        store.reconcile_companion_objects_sync(driver)


def _build_mysqlconnector_session_store(mode: str, adk_config: dict[str, object]) -> Any:
    config = _mock_config(adk_config)
    if mode == "async":
        return MysqlConnectorAsyncADKStore(config)
    return MysqlConnectorSyncADKStore(config)


def _build_mysqlconnector_memory_store(mode: str, adk_config: dict[str, object]) -> Any:
    config = _mock_config(adk_config)
    if mode == "async":
        return MysqlConnectorAsyncADKMemoryStore(config)
    return MysqlConnectorSyncADKMemoryStore(config)


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysqlconnector_companions_create_only_missing_session_index(mode: str) -> None:
    store = _build_mysqlconnector_session_store(
        mode,
        {
            "session_table": "app.t",
            "events_table": "app.e",
            "enable_event_generated_columns": True,
            "enable_covering_indexes": True,
        },
    )
    driver = _make_mysqlconnector_companion_driver(
        mode,
        {
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
        },
    )

    await _run_mysqlconnector_companion(mode, store, driver)

    executed = [call.args[0] for call in driver.execute.call_args_list]
    assert executed == ["CREATE INDEX idx_t_update_time ON `app`.`t` (update_time DESC)"]
    assert driver.data_dictionary.get_indexes.call_args_list[0].kwargs == {"table": "t", "schema": "app"}
    assert driver.data_dictionary.get_indexes.call_args_list[1].kwargs == {"table": "e", "schema": "app"}
    driver.commit.assert_not_called()


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysqlconnector_companions_use_connection_database_for_bare_names(mode: str) -> None:
    store = _build_mysqlconnector_session_store(
        mode,
        {
            "session_table": "custom_sessions",
            "events_table": "custom_events",
            "enable_event_generated_columns": True,
            "enable_covering_indexes": True,
        },
    )
    driver = _make_mysqlconnector_companion_driver(mode, {})

    await _run_mysqlconnector_companion(mode, store, driver)

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


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysqlconnector_companions_repeat_is_noop(mode: str) -> None:
    store = _build_mysqlconnector_session_store(mode, {"session_table": "app.t", "events_table": "app.e"})
    driver = _make_mysqlconnector_companion_driver(
        mode,
        {
            ("app", "t"): [{"index_name": "IDX_T_APP_USER"}, {"index_name": "idx_t_update_time"}],
            ("app", "e"): [
                {"index_name": "idx_e_scope"},
                {"index_name": "idx_e_session"},
                {"index_name": "idx_e_app_timestamp"},
            ],
        },
    )

    await _run_mysqlconnector_companion(mode, store, driver)

    assert driver.data_dictionary.get_indexes.call_count == 2
    driver.execute.assert_not_called()
    driver.commit.assert_not_called()


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysqlconnector_memory_companions_create_fulltext(mode: str) -> None:
    memory_store = _build_mysqlconnector_memory_store(mode, {"memory_table": "app.m", "memory_use_fts": True})
    driver = _make_mysqlconnector_companion_driver(mode, {})

    await _run_mysqlconnector_companion(mode, memory_store, driver)

    executed = [call.args[0] for call in driver.execute.call_args_list]
    assert executed == [
        "CREATE INDEX idx_m_app_scope_user_time ON `app`.`m` (app_name, scope, user_id, timestamp)",
        "CREATE INDEX idx_m_scope ON `app`.`m` (app_name, scope)",
        "CREATE INDEX idx_m_session ON `app`.`m` (session_id)",
        "CREATE FULLTEXT INDEX idx_m_fts ON `app`.`m` (content_text)",
    ]
    assert driver.data_dictionary.get_indexes.call_args_list[0].kwargs == {"table": "m", "schema": "app"}
    driver.commit.assert_not_called()

    disabled_store = _build_mysqlconnector_memory_store(mode, {"enable_memory": False})
    disabled_driver = _make_mysqlconnector_companion_driver(mode, {})
    await _run_mysqlconnector_companion(mode, disabled_store, disabled_driver)
    disabled_driver.data_dictionary.get_indexes.assert_not_called()
    disabled_driver.execute.assert_not_called()


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysqlconnector_companions_propagate_failure_and_retry(mode: str) -> None:
    store = _build_mysqlconnector_session_store(mode, {"session_table": "app.t", "events_table": "app.e"})
    failing_driver = _make_mysqlconnector_companion_driver(mode, {}, execute_side_effect=[None, RuntimeError("boom")])

    with pytest.raises(RuntimeError, match="boom"):
        await _run_mysqlconnector_companion(mode, store, failing_driver)

    failing_driver.commit.assert_not_called()

    retry_driver = _make_mysqlconnector_companion_driver(
        mode, {("app", "t"): [{"index_name": "idx_t_app_user"}], ("app", "e"): []}
    )
    await _run_mysqlconnector_companion(mode, store, retry_driver)

    retry_executed = [call.args[0] for call in retry_driver.execute.call_args_list]
    assert retry_executed == [
        "CREATE INDEX idx_t_update_time ON `app`.`t` (update_time DESC)",
        "CREATE INDEX idx_e_scope ON `app`.`e` (app_name, user_id, session_id, timestamp ASC)",
        "CREATE INDEX idx_e_session ON `app`.`e` (session_id, timestamp ASC)",
        "CREATE INDEX idx_e_app_timestamp ON `app`.`e` (app_name, timestamp ASC)",
    ]
    retry_driver.commit.assert_not_called()


def _make_mysqlconnector_scoping_mocks(
    mode: str, *, rowcount: int = 1, session_row: tuple[Any, ...] | None = None
) -> tuple[Any, Any, MagicMock, MagicMock]:
    cursor = MagicMock()
    cursor.rowcount = rowcount
    conn = MagicMock()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    if mode == "async":
        cursor.execute = AsyncMock()
        cursor.fetchone = AsyncMock(return_value=session_row)
        cursor.fetchall = AsyncMock(return_value=[])
        cursor.close = AsyncMock()
        conn.cursor = AsyncMock(return_value=cursor)
        conn.commit = AsyncMock()
        conn.rollback = AsyncMock()
        conn.__aenter__ = AsyncMock(return_value=conn)
        conn.__aexit__ = AsyncMock(return_value=None)
        return MysqlConnectorAsyncADKStore(config), MysqlConnectorAsyncADKMemoryStore(config), conn, cursor
    cursor.fetchone.return_value = session_row
    cursor.fetchall.return_value = []
    conn.cursor.return_value = cursor
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = None
    return MysqlConnectorSyncADKStore(config), MysqlConnectorSyncADKMemoryStore(config), conn, cursor


async def _invoke(mode: str, target: Any, method_name: str, *args: Any, **kwargs: Any) -> Any:
    fn = getattr(target, method_name)
    if mode == "async":
        return await fn(*args, **kwargs)
    return fn(*args, **kwargs)


@pytest.mark.parametrize("mode", ["sync", "async"])
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
async def test_mysqlconnector_get_session_renews_only_for_positive_duration(
    mode: str, renew_for: "int | timedelta | None", expect_update: bool
) -> None:
    """get_session executes an UPDATE only when renew_for is positive across sync and async stores."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store, _mem_store, conn, cursor = _make_mysqlconnector_scoping_mocks(
        mode, session_row=("s1", "app", "u1", "{}", now, now)
    )

    result = await _invoke(mode, store, "get_session", "app", "u1", "s1", renew_for=renew_for)

    assert result is not None
    executed_sqls = [_normalized(call.args[0]) for call in cursor.execute.call_args_list]
    assert any(sql.startswith("UPDATE adk_session") for sql in executed_sqls) is expect_update
    assert conn.commit.call_count == (1 if expect_update else 0)


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    ("rowcount", "expect_succeed"),
    [pytest.param(1, True, id="owner-found"), pytest.param(0, False, id="owner-missing")],
)
async def test_mysqlconnector_append_event_enforces_session_ownership(
    mode: str, rowcount: int, expect_succeed: bool
) -> None:
    """append_event selects from adk_session by (id, app_name, user_id) and raises ValueError when 0 rows match."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    store, _mem_store, conn, cursor = _make_mysqlconnector_scoping_mocks(mode, rowcount=rowcount)
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
        await _invoke(mode, store, "append_event", event)
        assert conn.commit.call_count == 1
        assert conn.rollback.call_count == 0
    else:
        with pytest.raises(ValueError, match="not found"):
            await _invoke(mode, store, "append_event", event)
        assert conn.commit.call_count == 0
        assert conn.rollback.call_count == 1

    sql, params = cursor.execute.call_args_list[0].args
    assert "FROM adk_session WHERE id = %s AND app_name = %s AND user_id = %s" in _normalized(sql)
    assert params[-3:] == ("sess-1", "app-a", "user-1")


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    ("record_app", "record_user", "record_session"),
    [
        pytest.param("other-app", "user-1", "sess-1", id="app-mismatch"),
        pytest.param("app-a", "other-user", "sess-1", id="user-mismatch"),
        pytest.param("app-a", "user-1", "other-sess", id="session-mismatch"),
    ],
)
async def test_mysqlconnector_append_event_and_update_state_rejects_mismatched_record(
    mode: str, record_app: str, record_user: str, record_session: str
) -> None:
    """Mismatched event_record identifiers fail before executing any SQL statement."""
    store, _mem_store, _conn, cursor = _make_mysqlconnector_scoping_mocks(mode)
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
        await _invoke(mode, store, "append_event_and_update_state", event, "app-a", "user-1", "sess-1", {})

    assert cursor.execute.call_count == 0


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    ("rowcount", "expect_succeed"),
    [pytest.param(1, True, id="session-updated"), pytest.param(0, False, id="session-missing")],
)
async def test_mysqlconnector_append_event_and_update_state_updates_session_before_insert(
    mode: str, rowcount: int, expect_succeed: bool
) -> None:
    """Session UPDATE runs before event INSERT; missing session raises ValueError without inserting."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    session_row = ("sess-1", "app-a", "user-1", '{"k": 1}', now, now) if expect_succeed else None
    store, _mem_store, conn, cursor = _make_mysqlconnector_scoping_mocks(
        mode, rowcount=rowcount, session_row=session_row
    )
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
        updated = await _invoke(
            mode, store, "append_event_and_update_state", event, "app-a", "user-1", "sess-1", {"k": 1}
        )
        assert updated["id"] == "sess-1"
        executed = [_normalized(call.args[0]) for call in cursor.execute.call_args_list]
        assert executed[0].startswith("UPDATE adk_session")
        assert executed[1].startswith("SELECT id, app_name, user_id, state")
        assert executed[2].startswith("INSERT INTO adk_event")
        assert conn.commit.call_count == 1
    else:
        with pytest.raises(ValueError, match="not found"):
            await _invoke(mode, store, "append_event_and_update_state", event, "app-a", "user-1", "sess-1", {"k": 1})
        executed = [_normalized(call.args[0]) for call in cursor.execute.call_args_list]
        assert executed == [
            (
                "UPDATE adk_session SET state = %s, update_time = UTC_TIMESTAMP(6) "
                "WHERE app_name = %s AND user_id = %s AND id = %s"
            )
        ]
        assert conn.rollback.call_count == 1
        assert conn.commit.call_count == 0


@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysqlconnector_get_events_joins_session_on_three_columns(mode: str) -> None:
    """get_events joins adk_event to adk_session on (session_id, app_name, user_id)."""
    cutoff = datetime(2026, 1, 1, tzinfo=UTC)
    store, _mem_store, _conn, cursor = _make_mysqlconnector_scoping_mocks(mode)

    await _invoke(mode, store, "get_events", "app-a", "user-1", "sess-1", after_timestamp=cutoff, limit=5)

    sql, params = cursor.execute.call_args_list[0].args
    assert _normalized(sql) == (
        "SELECT e.id, e.app_name, e.user_id, e.session_id, e.invocation_id, e.timestamp, e.event_data "
        "FROM adk_event e JOIN adk_session s "
        "ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id "
        "WHERE s.app_name = %s AND s.user_id = %s AND s.id = %s AND e.timestamp > %s "
        "ORDER BY e.timestamp ASC LIMIT %s"
    )
    assert tuple(params) == ("app-a", "user-1", "sess-1", cutoff, 5)


@pytest.mark.parametrize("mode", ["sync", "async"])
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
async def test_mysqlconnector_delete_entries_by_session_scopes_predicates(
    mode: str, app_name: "str | None", user_id: "str | None", expected_where: str, expected_params: tuple[str, ...]
) -> None:
    """delete_entries_by_session appends app_name and user_id predicates when provided."""
    _store, memory_store, _conn, cursor = _make_mysqlconnector_scoping_mocks(mode, rowcount=2)

    deleted = await _invoke(
        mode, memory_store, "delete_entries_by_session", "sess-1", app_name=app_name, user_id=user_id
    )

    assert deleted == 2
    sql, params = cursor.execute.call_args_list[0].args
    assert _normalized(sql) == f"DELETE FROM adk_memory WHERE {expected_where}"
    assert params == expected_params
    memory_ddl = await _invoke(mode, memory_store, "_memory_table_ddl")
    assert "UNIQUE KEY idx_adk_memory_app_user_event (app_name, user_id, event_id)" in memory_ddl
    assert "NOT NULL UNIQUE" not in memory_ddl
