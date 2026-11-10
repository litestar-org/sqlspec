"""Unit tests for asyncmy ADK store extension configuration."""

import asyncio
from datetime import UTC
from typing import Any, NotRequired, Self, cast, get_args, get_origin
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.asyncmy.adk import AsyncmyADKConfig, AsyncmyADKMemoryStore, AsyncmyADKStore
from sqlspec.config import ADKConfig


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


class _MysqlMissingTableError(Exception):
    errno = 1146


def test_asyncmy_table_missing_uses_errno_attribute() -> None:
    from sqlspec.adapters.asyncmy.adk.store import _is_mysql_table_missing

    assert _is_mysql_table_missing(_MysqlMissingTableError()) is True


def test_asyncmy_adk_config_types_adapter_local_mysql_options() -> None:
    """asyncmy ADK MySQL options are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", AsyncmyADKConfig).__optional_keys__

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
        annotation = cast("Any", AsyncmyADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)


def test_asyncmy_adk_tables_use_plain_mysql_schema_by_default() -> None:
    """asyncmy ADK profile DDL stays opt-in through extension_config["adk"]."""

    store = AsyncmyADKStore(_mock_config())
    memory_store = AsyncmyADKMemoryStore(_mock_config())

    events_sql = asyncio.run(store._events_table_ddl())
    memory_sql = asyncio.run(memory_store._memory_table_ddl())

    assert "author_gc" not in events_sql
    assert "node_path_gc" not in events_sql
    assert "timestamp ASC, invocation_id" not in events_sql
    assert "COMMENT='adk-events'" not in events_sql
    assert "COMMENT='adk-memory'" not in memory_sql


def test_asyncmy_adk_tables_apply_adapter_local_mysql_profile() -> None:
    """asyncmy ADK MySQL options add generated columns, covering keys, and table options."""

    store = AsyncmyADKStore(
        _mock_config({
            "enable_event_generated_columns": True,
            "enable_covering_indexes": True,
            "session_table_options": "COMMENT='adk-session'",
            "events_table_options": "COMMENT='adk-events'",
            "app_state_table_options": "COMMENT='adk-app-state'",
            "user_state_table_options": "COMMENT='adk-user-state'",
        })
    )
    memory_store = AsyncmyADKMemoryStore(_mock_config({"memory_table_options": "COMMENT='adk-memory'"}))

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

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


class _AsyncConnection:
    def __init__(self, cursor: _AsyncCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _AsyncCursor:
        return self._cursor

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


def _session_store_with_cursor() -> "tuple[AsyncmyADKStore, _AsyncCursor]":
    cursor = _AsyncCursor()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: _AsyncConnection(cursor)
    return AsyncmyADKStore(config), cursor


async def test_asyncmy_list_sessions_binds_order_and_page() -> None:
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


async def test_asyncmy_list_sessions_defaults_to_recent_first_without_a_page() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, cursor = _session_store_with_cursor()

    await store.list_sessions("app")

    sql, params = cursor.calls[0]
    assert _normalized(sql).endswith("WHERE app_name = %s ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


async def test_asyncmy_list_sessions_orders_page_after_scope_parameters() -> None:
    """Page values bind after the scope parameters that are actually present."""
    store, cursor = _session_store_with_cursor()

    await store.list_sessions("app", limit=5)

    sql, params = cursor.calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC LIMIT %s OFFSET %s")
    assert params == ("app", 5, 0)


async def test_asyncmy_list_sessions_zero_limit_never_queries() -> None:
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
async def test_asyncmy_list_sessions_rejects_invalid_options(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, cursor = _session_store_with_cursor()

    with pytest.raises(ValueError):
        await store.list_sessions("app", **options)

    assert cursor.calls == []


async def test_asyncmy_adk_memory_store_insert_and_search_json_handling() -> None:
    """AsyncmyADKMemoryStore handles missing author, metadata_json=None, and deserializes JSON strings."""
    from datetime import datetime
    from unittest.mock import AsyncMock

    now = datetime.now(tz=UTC)
    cursor = MagicMock()
    cursor.rowcount = 1
    cursor.execute = AsyncMock()
    cursor.executemany = AsyncMock()
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
    cursor.__aenter__ = AsyncMock(return_value=cursor)
    cursor.__aexit__ = AsyncMock(return_value=None)
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)
    conn.commit = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    config = _mock_config({"owner_id_column": "owner_id INT NULL"})
    config.provide_connection = lambda *_a, **_k: conn
    store = AsyncmyADKMemoryStore(config)
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
    insert_params = cursor.executemany.call_args_list[0].args[1][0]
    assert insert_params[6] is None
    assert insert_params[11] is None
    assert len(records) == 1
    assert records[0]["content_json"] == {"text": "hello"}
    assert records[0]["metadata_json"] == {"source": "unit"}


async def test_asyncmy_insert_memory_entries_batches_with_executemany() -> None:
    """insert_memory_entries batches multiple entries in a single cursor.executemany call."""
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock

    now = datetime.now(tz=UTC)
    cursor = MagicMock()
    cursor.rowcount = 2
    cursor.executemany = AsyncMock()
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)
    conn.commit = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    store = AsyncmyADKMemoryStore(config)

    entries = [
        cast(
            "Any",
            {
                "id": f"mem-{i}",
                "session_id": "sess-1",
                "app_name": "app",
                "user_id": "user-1",
                "event_id": f"evt-{i}",
                "timestamp": now,
                "content_json": {"idx": i},
                "content_text": f"text {i}",
                "metadata_json": None,
                "inserted_at": now,
            },
        )
        for i in range(2)
    ]

    inserted = await store.insert_memory_entries(entries)

    assert inserted == 2
    assert cursor.executemany.call_count == 1
    batch_params = cursor.executemany.call_args_list[0].args[1]
    assert len(batch_params) == 2


async def test_asyncmy_search_entries_escapes_like_wildcards() -> None:
    """search_entries escapes %, _, and \\ wildcards and appends ESCAPE clause."""
    from unittest.mock import AsyncMock

    cursor = MagicMock()
    cursor.fetchall = AsyncMock(return_value=[])
    cursor.description = []
    cursor.execute = AsyncMock()
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    store = AsyncmyADKMemoryStore(config)

    await store.search_entries("100%_val\\id", "app", "user-1")

    assert cursor.execute.call_count == 1
    executed_sql = cursor.execute.call_args_list[0].args[0]
    executed_params = cursor.execute.call_args_list[0].args[1]
    assert "LIKE %s ESCAPE '\\\\'" in executed_sql
    assert "%100\\%\\_val\\\\id%" in executed_params


async def test_asyncmy_memory_table_ddl_uses_longtext() -> None:
    """_memory_table_ddl uses LONGTEXT for content_text."""
    config = _mock_config()
    store = AsyncmyADKMemoryStore(config)
    ddl = await store._memory_table_ddl()
    assert "content_text LONGTEXT NOT NULL" in ddl


async def test_asyncmy_stream_source_closes_cursor_when_execute_raises() -> None:
    """AsyncmyStreamSource.start() closes the cursor if execute() fails."""
    from unittest.mock import AsyncMock

    from sqlspec.adapters.asyncmy.core import AsyncmyStreamSource

    cursor = MagicMock()
    cursor.execute = AsyncMock(side_effect=RuntimeError("execute boom"))
    cursor.close = AsyncMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor
    driver = MagicMock(connection=connection)
    driver._run_with_exception_handler = lambda _handler, fn: fn()
    source = AsyncmyStreamSource(driver, "SELECT 1", (), 100, set())

    with pytest.raises(RuntimeError, match="execute boom"):
        await source.start()

    cursor.close.assert_awaited_once()
    assert source._cursor is None


def _mock_companion_driver(index_map: dict[tuple[str | None, str], list[dict[str, Any]]]) -> MagicMock:
    driver = MagicMock()
    driver.execute = AsyncMock()
    driver.commit = AsyncMock()

    async def _get_indexes(_drv: Any, *, table: str | None = None, schema: str | None = None) -> list[dict[str, Any]]:
        assert table is not None
        return list(index_map.get((schema, table), []))

    driver.data_dictionary.get_indexes = AsyncMock(side_effect=_get_indexes)
    return driver


async def test_asyncmy_companions_create_only_missing_session_index() -> None:
    """Qualified database.table targets scope catalog lookup and create only absent indexes."""
    driver = _mock_companion_driver({
        ("app", "t"): [
            {"index_name": "idx_t_app_user", "table_name": "t"},
            {"index_name": "idx_t_update_time", "table_name": "unrelated_table"},
        ],
        ("app", "e"): [
            {"index_name": "idx_e_scope", "table_name": "e"},
            {"index_name": "idx_e_session", "table_name": "e"},
            {"index_name": "idx_e_app_timestamp", "table_name": "e"},
        ],
    })
    store = AsyncmyADKStore(_mock_config({"session_table": "app.t", "events_table": "app.e"}))

    await store.reconcile_companion_objects_async(driver)

    assert [call.args[0] for call in driver.execute.call_args_list] == [
        "CREATE INDEX idx_t_update_time ON `app`.`t` (update_time DESC)"
    ]
    assert driver.data_dictionary.get_indexes.call_args_list[0].kwargs == {"table": "t", "schema": "app"}
    assert driver.data_dictionary.get_indexes.call_args_list[1].kwargs == {"table": "e", "schema": "app"}
    driver.commit.assert_not_called()


async def test_asyncmy_companions_use_connection_database_for_bare_names() -> None:
    """Bare table names pass schema=None and emit unquoted table DDL including generated/covering indexes."""
    driver = _mock_companion_driver({})
    store = AsyncmyADKStore(
        _mock_config({
            "session_table": "custom_sessions",
            "events_table": "custom_events",
            "enable_event_generated_columns": True,
            "enable_covering_indexes": True,
        })
    )

    await store.reconcile_companion_objects_async(driver)

    statements = [call.args[0] for call in driver.execute.call_args_list]
    assert "CREATE INDEX idx_custom_sessions_app_user ON custom_sessions (app_name, user_id)" in statements
    assert (
        "CREATE INDEX idx_custom_events_session ON custom_events (session_id, timestamp ASC, invocation_id)"
        in statements
    )
    assert (
        "CREATE INDEX idx_custom_events_author_gc ON custom_events (session_id, author_gc, timestamp ASC)" in statements
    )
    assert driver.data_dictionary.get_indexes.call_args_list[0].kwargs == {"table": "custom_sessions", "schema": None}
    driver.commit.assert_not_called()


async def test_asyncmy_companions_repeat_is_noop() -> None:
    """Case-folded catalog matches skip existing session and event indexes without executing DDL."""
    driver = _mock_companion_driver({
        ("app", "t"): [
            {"index_name": "IDX_T_APP_USER", "table_name": "t"},
            {"index_name": "idx_t_update_time", "table_name": "t"},
        ],
        ("app", "e"): [
            {"index_name": "IDX_E_SCOPE", "table_name": "e"},
            {"index_name": "idx_e_session", "table_name": "e"},
            {"index_name": "idx_e_app_timestamp", "table_name": "e"},
        ],
    })
    store = AsyncmyADKStore(_mock_config({"session_table": "app.t", "events_table": "app.e"}))

    await store.reconcile_companion_objects_async(driver)

    assert driver.data_dictionary.get_indexes.call_count == 2
    driver.execute.assert_not_called()
    driver.commit.assert_not_called()


async def test_asyncmy_memory_companions_create_fulltext() -> None:
    """Enabled FTS memory stores emit CREATE FULLTEXT INDEX while disabled stores skip catalog queries."""
    disabled_driver = _mock_companion_driver({})
    disabled_store = AsyncmyADKMemoryStore(_mock_config({"enable_memory": False}))
    await disabled_store.reconcile_companion_objects_async(disabled_driver)
    disabled_driver.data_dictionary.get_indexes.assert_not_called()
    disabled_driver.execute.assert_not_called()

    driver = _mock_companion_driver({})
    store = AsyncmyADKMemoryStore(
        _mock_config({"enable_memory": True, "memory_table": "app.m", "memory_use_fts": True})
    )

    await store.reconcile_companion_objects_async(driver)

    statements = [call.args[0] for call in driver.execute.call_args_list]
    assert statements == [
        "CREATE INDEX idx_m_app_scope_user_time ON `app`.`m` (app_name, scope, user_id, timestamp)",
        "CREATE INDEX idx_m_scope ON `app`.`m` (app_name, scope)",
        "CREATE INDEX idx_m_session ON `app`.`m` (session_id)",
        "CREATE FULLTEXT INDEX idx_m_fts ON `app`.`m` (content_text)",
    ]
    driver.commit.assert_not_called()


async def test_asyncmy_companions_propagate_failure_and_retry() -> None:
    """Mid-batch DDL failures propagate cleanly and subsequent retries create only remaining absent indexes."""
    catalog: dict[tuple[str | None, str], list[dict[str, Any]]] = {
        (None, "adk_session"): [],
        (None, "adk_event"): [
            {"index_name": "idx_adk_event_scope", "table_name": "adk_event"},
            {"index_name": "idx_adk_event_session", "table_name": "adk_event"},
            {"index_name": "idx_adk_event_app_timestamp", "table_name": "adk_event"},
        ],
    }
    driver = _mock_companion_driver(catalog)
    driver.execute = AsyncMock(side_effect=[None, RuntimeError("boom")])
    store = AsyncmyADKStore(_mock_config())

    with pytest.raises(RuntimeError, match="boom"):
        await store.reconcile_companion_objects_async(driver)

    catalog[(None, "adk_session")] = [{"index_name": "idx_adk_session_app_user", "table_name": "adk_session"}]
    retry_driver = _mock_companion_driver(catalog)

    await store.reconcile_companion_objects_async(retry_driver)

    assert [call.args[0] for call in retry_driver.execute.call_args_list] == [
        "CREATE INDEX idx_adk_session_update_time ON adk_session (update_time DESC)"
    ]
