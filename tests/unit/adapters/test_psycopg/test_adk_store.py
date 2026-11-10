"""Unit tests for psycopg ADK store sync wrappers."""

import inspect
from datetime import UTC, datetime
from typing import Any, Self, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from psycopg.types.json import Jsonb

from sqlspec.adapters.psycopg.adk import (
    PsycopgADKConfig,
    PsycopgAsyncADKMemoryStore,
    PsycopgAsyncADKStore,
    PsycopgSyncADKMemoryStore,
    PsycopgSyncADKStore,
)
from sqlspec.adapters.psycopg.adk.store import decode_postgres_vector
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.exceptions import ImproperConfigurationError, MigrationError, SQLSpecError
from sqlspec.extensions.adk.migrations.reconcile import reconcile
from sqlspec.migrations.context import MigrationContext


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


class _DummyCursor:
    def __init__(self, rows: "list[dict[str, Any]] | None" = None) -> None:
        self.execute_calls: list[tuple[Any, Any]] = []
        self._rows = rows or []
        self.rowcount = 1

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def execute(self, query: Any, params: Any = None) -> None:
        self.execute_calls.append((query, params))

    def fetchall(self) -> "list[dict[str, Any]]":
        return self._rows

    def fetchone(self) -> "dict[str, Any] | None":
        return self._rows[0] if self._rows else None


class _DummyConnection:
    def __init__(self, cursor: _DummyCursor) -> None:
        self._cursor = cursor
        self.commit_called = False

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def cursor(self, **kwargs: Any) -> _DummyCursor:
        return self._cursor

    def commit(self) -> None:
        self.commit_called = True


class _DummyAsyncCursor:
    def __init__(self, rows: "list[dict[str, Any]] | None" = None) -> None:
        self.execute_calls: list[tuple[Any, Any]] = []
        self._rows = rows or []
        self.rowcount = 1

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    async def execute(self, query: Any, params: Any = None) -> None:
        self.execute_calls.append((query, params))

    async def fetchall(self) -> "list[dict[str, Any]]":
        return self._rows


class _DummyAsyncConnection:
    def __init__(self, cursor: _DummyAsyncCursor) -> None:
        self._cursor = cursor
        self.commit_called = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def cursor(self, **kwargs: Any) -> _DummyAsyncCursor:
        return self._cursor

    async def commit(self) -> None:
        self.commit_called = True


class _DummyConfig:
    def __init__(self, connection: _DummyConnection | _DummyAsyncConnection) -> None:
        self._connection = connection
        self.extension_config: dict[str, Any] = {"adk": {}}

    def provide_connection(self) -> _DummyConnection | _DummyAsyncConnection:
        return self._connection

    def _ensure_pg_textsearch_available(self) -> None:
        return None


def _build_store(
    rows: "list[dict[str, Any]] | None" = None,
) -> "tuple[PsycopgSyncADKStore, _DummyCursor, _DummyConnection]":
    cursor = _DummyCursor(rows)
    connection = _DummyConnection(cursor)
    store: Any = PsycopgSyncADKStore.__new__(PsycopgSyncADKStore)
    config = _DummyConfig(connection)
    store._config = config
    store._events_table = "test_events"
    store._session_table = "test_sessions"
    store._app_state_table = "test_app_state"
    store._user_state_table = "test_user_state"
    store._metadata_table = "test_metadata"
    store._owner_id_column_ddl = None
    store._owner_id_column_name = None
    return cast("PsycopgSyncADKStore", store), cursor, connection


async def test_psycopg_typed_memory_tuning_reaches_schema() -> None:
    settings = PsycopgADKConfig(vector_index_type="ivfflat", vector_dimensions=512)
    config = PsycopgAsyncConfig(extension_config={"adk": settings})
    store = PsycopgAsyncADKMemoryStore(config)

    ddl = await store._memory_table_ddl()

    assert "embedding VECTOR(512)" in ddl
    assert "USING ivfflat (embedding vector_cosine_ops)" in ddl


async def test_psycopg_async_adk_events_table_uses_plain_schema_by_default() -> None:
    """Psycopg async ADK optimization DDL stays opt-in through extension config."""

    store = PsycopgAsyncADKStore(_mock_config())

    sql = await store._events_table_ddl()

    assert "author_gc" not in sql
    assert "node_path_gc" not in sql
    assert "INCLUDE (invocation_id)" not in sql


async def test_psycopg_async_adk_events_table_applies_adapter_local_extension_config() -> None:
    """Psycopg async ADK extension settings enable PostgreSQL-specific event DDL."""

    store = PsycopgAsyncADKStore(
        _mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True})
    )

    sql = await store._events_table_ddl()

    assert "author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED" in sql
    assert "node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED" in sql
    assert "idx_adk_event_author_gc" in sql
    assert "idx_adk_event_node_path_gc" in sql
    assert "INCLUDE (invocation_id)" in sql


def test_psycopg_sync_adk_events_table_uses_plain_schema_by_default() -> None:
    """Psycopg sync ADK optimization DDL stays opt-in through extension config."""

    store = PsycopgSyncADKStore(_mock_config())

    sql = store._events_table_ddl()

    assert "author_gc" not in sql
    assert "node_path_gc" not in sql
    assert "INCLUDE (invocation_id)" not in sql


def test_psycopg_sync_adk_events_table_applies_adapter_local_extension_config() -> None:
    """Psycopg sync ADK extension settings enable PostgreSQL-specific event DDL."""

    store = PsycopgSyncADKStore(_mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True}))

    sql = store._events_table_ddl()

    assert "author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED" in sql
    assert "node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED" in sql
    assert "idx_adk_event_author_gc" in sql
    assert "idx_adk_event_node_path_gc" in sql
    assert "INCLUDE (invocation_id)" in sql


def test_sync_append_event_inserts_without_session_update() -> None:
    """append_event must insert a single event without writing session state."""
    store, cursor, connection = _build_store()
    event_record = {
        "id": "event-1",
        "app_name": "app",
        "user_id": "user",
        "session_id": "session-1",
        "invocation_id": "",
        "timestamp": datetime.now(UTC),
        "event_data": {"id": "event-1"},
    }

    store.append_event(cast("Any", event_record))

    assert len(cursor.execute_calls) == 1
    _, params = cursor.execute_calls[0]
    assert params[0] == "event-1"
    assert params[1:3] == ("app", "user")
    assert params[3] == "session-1"
    assert isinstance(params[6], Jsonb)
    assert connection.commit_called


@pytest.mark.parametrize("owner_column", [None, "tenant_id UUID"])
@pytest.mark.parametrize("embedding", [None, [0.1, 0.2, 0.3]])
def test_psycopg_sync_memory_insert_binds_embedding_with_portable_cast(
    owner_column: str | None, embedding: list[float] | None
) -> None:
    """Psycopg memory inserts keep nullable embeddings in the expected parameter position."""
    cursor = _DummyCursor()
    connection = _DummyConnection(cursor)
    config = _DummyConfig(connection)
    config.extension_config = {"adk": {"owner_id_column": owner_column}} if owner_column else {"adk": {}}
    store = PsycopgSyncADKMemoryStore(cast("Any", config))
    entry = {
        "id": "memory-1",
        "session_id": "session-1",
        "app_name": "app",
        "user_id": "user",
        "scope": "user",
        "event_id": "event-1",
        "author": "user",
        "timestamp": datetime.now(UTC),
        "embedding": embedding,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": datetime.now(UTC),
    }

    assert store.insert_memory_entries([cast("Any", entry)], owner_id="tenant-1") == 1

    query, params = cursor.execute_calls[0]
    assert "::float8[]::vector" in query.as_string()
    embedding_index = 9 if owner_column else 8
    assert params[embedding_index] == embedding


@pytest.mark.parametrize("owner_column", [None, "tenant_id UUID"])
async def test_psycopg_async_memory_insert_binds_embedding_with_portable_cast(owner_column: str | None) -> None:
    """The async psycopg INSERT path carries the same embedding cast and ordering."""
    cursor = _DummyAsyncCursor()
    config = _DummyConfig(_DummyAsyncConnection(cursor))
    config.extension_config = {"adk": {"owner_id_column": owner_column}} if owner_column else {"adk": {}}
    store = PsycopgAsyncADKMemoryStore(cast("Any", config))
    entry = {
        "id": "memory-1",
        "session_id": "session-1",
        "app_name": "app",
        "user_id": "user",
        "scope": "user",
        "event_id": "event-1",
        "author": "user",
        "timestamp": datetime.now(UTC),
        "embedding": [0.1, 0.2],
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": datetime.now(UTC),
    }

    assert await store.insert_memory_entries([cast("Any", entry)], owner_id="tenant-1") == 1
    query, params = cursor.execute_calls[0]
    assert "::float8[]::vector" in query.as_string()
    assert params[9 if owner_column else 8] == [0.1, 0.2]


@pytest.mark.parametrize("enable_bm25, query", [(False, ""), (True, "hello")])
async def test_psycopg_async_memory_search_casts_vector_operands(enable_bm25: bool, query: str) -> None:
    """Async vector-only and hybrid searches cast list bindings portably."""
    cursor = _DummyAsyncCursor()
    config = _DummyConfig(_DummyAsyncConnection(cursor))
    config.extension_config = {"adk": {"enable_bm25": enable_bm25}}
    store = PsycopgAsyncADKMemoryStore(cast("Any", config))

    await store.search_entries(query, "app", "user", embedding=[0.1, 0.2])

    rendered = cursor.execute_calls[0][0].as_string()
    assert "embedding <=> %s::float8[]::vector" in rendered


@pytest.mark.parametrize("enable_bm25, query", [(False, ""), (True, "hello")])
def test_psycopg_sync_memory_search_casts_vector_operands(enable_bm25: bool, query: str) -> None:
    """Sync vector-only and hybrid searches cast list bindings portably."""
    cursor = _DummyCursor()
    config = _DummyConfig(_DummyConnection(cursor))
    config.extension_config = {"adk": {"enable_bm25": enable_bm25}}
    store = PsycopgSyncADKMemoryStore(cast("Any", config))

    store.search_entries(query, "app", "user", embedding=[0.1, 0.2])

    rendered = cursor.execute_calls[0][0].as_string()
    assert "embedding <=> %s::float8[]::vector" in rendered


def test_sync_get_events_passes_after_timestamp_and_limit() -> None:
    """get_events must forward after_timestamp and limit to the sync query."""
    base_time = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        {
            "session_id": "session-1",
            "invocation_id": "",
            "timestamp": base_time,
            "event_data": {"id": "event-2"},
            "id": "event-2",
            "app_name": "app",
            "user_id": "user",
        }
    ]
    store, cursor, _ = _build_store(rows)

    result = store.get_events("app", "user", "session-1", after_timestamp=base_time, limit=1)

    assert len(cursor.execute_calls) == 1
    _, params = cursor.execute_calls[0]
    assert params == ("app", "user", "session-1", base_time, 1)
    assert result[0]["event_data"]["id"] == "event-2"


def test_sync_get_events_limit_zero_returns_empty_without_query() -> None:
    """get_events(limit=0) must return no events without querying."""
    store, cursor, _ = _build_store()

    result = store.get_events("app", "user", "session-1", limit=0)

    assert result == []
    assert cursor.execute_calls == []


def test_sync_append_event_and_update_state_writes_scoped_state_in_one_unit() -> None:
    """append_event_and_update_state must use event_data and optional scoped state."""
    base_time = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        {
            "id": "session-1",
            "app_name": "app",
            "user_id": "user",
            "state": {"session": True},
            "create_time": base_time,
            "update_time": base_time,
        }
    ]
    store, cursor, connection = _build_store(rows)
    event_record = {
        "id": "event-1",
        "app_name": "app",
        "user_id": "user",
        "session_id": "session-1",
        "invocation_id": "invoke-1",
        "timestamp": base_time,
        "event_data": {"id": "event-1"},
    }

    result = store.append_event_and_update_state(
        cast("Any", event_record),
        "app",
        "user",
        "session-1",
        {"session": True},
        app_state={},
        user_state={"user:theme": "dark"},
    )

    assert result["id"] == "session-1"
    assert len(cursor.execute_calls) == 4
    _, insert_params = cursor.execute_calls[0]
    _, update_params = cursor.execute_calls[1]
    _, app_state_params = cursor.execute_calls[2]
    _, user_state_params = cursor.execute_calls[3]
    assert insert_params[0] == "event-1"
    assert insert_params[1:3] == ("app", "user")
    assert isinstance(insert_params[6], Jsonb)
    assert getattr(update_params[0], "obj", None) == {"session": True}
    assert update_params[1:4] == ("app", "user", "session-1")
    assert app_state_params[0] == "app"
    assert getattr(app_state_params[1], "obj", None) == {}
    assert user_state_params[:2] == ("app", "user")
    assert getattr(user_state_params[2], "obj", None) == {"user:theme": "dark"}
    assert connection.commit_called


class _AsyncDummyCursor:
    def __init__(self, rows: "list[dict[str, Any]] | None" = None) -> None:
        self.execute_calls: list[tuple[Any, Any]] = []
        self._rows = rows or []
        self.rowcount = 1

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    async def execute(self, query: Any, params: Any = None) -> None:
        self.execute_calls.append((query, params))

    async def fetchall(self) -> "list[dict[str, Any]]":
        return self._rows

    async def fetchone(self) -> "dict[str, Any] | None":
        return self._rows[0] if self._rows else None


class _AsyncDummyConnection:
    def __init__(self, cursor: _AsyncDummyCursor) -> None:
        self._cursor = cursor
        self.commit_called = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def cursor(self, **kwargs: Any) -> _AsyncDummyCursor:
        return self._cursor

    async def commit(self) -> None:
        self.commit_called = True


def _rendered(query: Any) -> str:
    return " ".join(query.as_string(None).split())


def _async_session_store() -> "tuple[PsycopgAsyncADKStore, _AsyncDummyCursor]":
    cursor = _AsyncDummyCursor()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: _AsyncDummyConnection(cursor)
    return PsycopgAsyncADKStore(config), cursor


def _sync_session_store() -> "tuple[PsycopgSyncADKStore, _DummyCursor]":
    cursor = _DummyCursor()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: _DummyConnection(cursor)
    return PsycopgSyncADKStore(config), cursor


async def test_psycopg_async_list_sessions_binds_order_and_page() -> None:
    """Explicit ordering renders inline while page bounds bind as pyformat parameters."""
    store, cursor = _async_session_store()

    await store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    query, params = cursor.execute_calls[0]
    assert _rendered(query) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        'FROM "adk_session" WHERE app_name = %s AND user_id = %s '
        "ORDER BY create_time ASC, id ASC LIMIT %s OFFSET %s"
    )
    assert params == ("app", "u1", 10, 20)


def test_psycopg_sync_list_sessions_binds_order_and_page() -> None:
    """The sync path produces the same bounded query as the async path."""
    store, cursor = _sync_session_store()

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    query, params = cursor.execute_calls[0]
    assert _rendered(query) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        'FROM "adk_session" WHERE app_name = %s AND user_id = %s '
        "ORDER BY create_time ASC, id ASC LIMIT %s OFFSET %s"
    )
    assert params == ("app", "u1", 10, 20)


async def test_psycopg_async_list_sessions_defaults_to_recent_first() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, cursor = _async_session_store()

    await store.list_sessions("app")

    query, params = cursor.execute_calls[0]
    assert _rendered(query).endswith("WHERE app_name = %s ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_psycopg_sync_list_sessions_orders_page_after_scope_parameters() -> None:
    """Page placeholders follow the scope parameters that are actually present."""
    store, cursor = _sync_session_store()

    store.list_sessions("app", limit=5)

    query, params = cursor.execute_calls[0]
    assert _rendered(query).endswith("ORDER BY update_time DESC, id DESC LIMIT %s OFFSET %s")
    assert params == ("app", 5, 0)


def test_psycopg_sync_list_sessions_zero_limit_never_queries() -> None:
    """A zero limit short-circuits before any database work."""
    store, cursor = _sync_session_store()

    assert store.list_sessions("app", limit=0) == []
    assert cursor.execute_calls == []


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"order_by": "id"}, id="unknown-order-column"),
        pytest.param({"limit": -1}, id="negative-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"offset": 5}, id="unbounded-offset"),
    ],
)
def test_psycopg_sync_list_sessions_rejects_invalid_options(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, cursor = _sync_session_store()

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert cursor.execute_calls == []


async def test_psycopg_async_adk_store_commits_mutating_operations() -> None:
    """Async ADK session and memory mutating operations must commit their transactions."""
    cursor = _AsyncDummyCursor()
    connection = _AsyncDummyConnection(cursor)
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: connection
    store = PsycopgAsyncADKStore(config)

    await store.update_session_state("app", "u1", "s1", {"k": "v"})
    assert connection.commit_called

    connection.commit_called = False
    await store.delete_session("app", "u1", "s1")
    assert connection.commit_called

    connection.commit_called = False
    await store.upsert_app_state("app", {"k": "v"})
    assert connection.commit_called

    connection.commit_called = False
    await store.upsert_user_state("app", "u1", {"k": "v"})
    assert connection.commit_called

    connection.commit_called = False
    await store.set_metadata("k", "v")
    assert connection.commit_called


def test_psycopg_sync_adk_store_commits_mutating_operations() -> None:
    """Sync ADK session and memory mutating operations must commit their transactions."""
    store, _, connection = _build_store()

    store.update_session_state("app", "u1", "s1", {"k": "v"})
    assert connection.commit_called

    connection.commit_called = False
    store.delete_session("app", "u1", "s1")
    assert connection.commit_called


class _FakePsycopgVector:
    def __init__(self, values: list[float]) -> None:
        self._values = values

    def to_list(self) -> list[float]:
        return list(self._values)


@pytest.mark.parametrize(
    ("raw_embedding", "expected"),
    [
        (None, None),
        ("[0.1, 0.2, 0.3]", [0.1, 0.2, 0.3]),
        (b"[0.1,0.2,0.3]", [0.1, 0.2, 0.3]),
        (_FakePsycopgVector([0.1, 0.2, 0.3]), [0.1, 0.2, 0.3]),
        ((0.1, 0.2, 0.3), [0.1, 0.2, 0.3]),
    ],
)
def test_psycopg_memory_store_decodes_vector_text_and_bytes(raw_embedding: Any, expected: list[float] | None) -> None:
    """Psycopg sync and async memory stores normalize text, bytes, PgVector, and sequence embeddings."""
    now = datetime.now(UTC)
    row = {
        "id": "mem-1",
        "session_id": "sess-1",
        "app_name": "app",
        "user_id": "user-1",
        "scope": "user",
        "event_id": "evt-1",
        "author": "user",
        "timestamp": now,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": now,
        "embedding": raw_embedding,
    }
    assert PsycopgSyncADKMemoryStore._decode_row(row)["embedding"] == expected
    assert PsycopgAsyncADKMemoryStore._decode_row(row)["embedding"] == expected
    assert decode_postgres_vector(raw_embedding) == expected


def test_psycopg_sync_memory_fts_and_simple_select_13_columns() -> None:
    """Psycopg sync FTS and simple search project and decode the 13th embedding column."""
    now = datetime.now(UTC)
    cursor = _DummyCursor([
        {
            "id": "mem-1",
            "session_id": "sess-1",
            "app_name": "app",
            "user_id": "user-1",
            "scope": "user",
            "event_id": "evt-1",
            "author": "user",
            "timestamp": now,
            "content_json": {"text": "hello"},
            "content_text": "hello",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": "[0.5, 0.25]",
        }
    ])
    config = _DummyConfig(_DummyConnection(cursor))
    config.extension_config = {"adk": {"use_fts": True}}
    store = PsycopgSyncADKMemoryStore(cast("Any", config))

    fts_rows = store._search_entries_fts("hello", "app", "user-1", 5)
    simple_rows = store._search_entries_simple("hello", "app", "user-1", 5)

    assert "embedding" in cursor.execute_calls[0][0].as_string()
    assert "embedding" in cursor.execute_calls[1][0].as_string()
    assert fts_rows[0]["embedding"] == [0.5, 0.25]
    assert simple_rows[0]["embedding"] == [0.5, 0.25]


async def _await_if_needed(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _normalized_sql(sql: str) -> str:
    return " ".join(sql.split())


def _mock_migration_driver(*, is_async: bool) -> MagicMock:
    driver = MagicMock()
    driver.connection = MagicMock()
    driver.data_dictionary = MagicMock()
    if is_async:
        driver.execute_script = AsyncMock()
        driver.commit = AsyncMock()
        driver.data_dictionary.get_tables = AsyncMock(return_value=[])
        driver.data_dictionary.get_columns = AsyncMock(return_value=[])
    else:
        driver.execute_script = MagicMock()
        driver.commit = MagicMock()
        driver.data_dictionary.get_tables = MagicMock(return_value=[])
        driver.data_dictionary.get_columns = MagicMock(return_value=[])
    return driver


def _recorded_scripts(driver: MagicMock, *, is_async: bool) -> list[str]:
    calls = driver.execute_script.await_args_list if is_async else driver.execute_script.call_args_list
    return [_normalized_sql(call.args[0]) for call in calls]


_SESSION_STORE_MODES = [
    pytest.param(PsycopgAsyncADKStore, True, "reconcile_companion_objects_async", id="async"),
    pytest.param(PsycopgSyncADKStore, False, "reconcile_companion_objects_sync", id="sync"),
]

_MEMORY_STORE_MODES = [
    pytest.param(
        PsycopgAsyncADKMemoryStore,
        True,
        "prepare_schema_async",
        "prepare_reconcile_async",
        "reconcile_companion_objects_async",
        "finalize_reconcile_async",
        id="async",
    ),
    pytest.param(
        PsycopgSyncADKMemoryStore,
        False,
        "prepare_schema_sync",
        "prepare_reconcile_sync",
        "reconcile_companion_objects_sync",
        "finalize_reconcile_sync",
        id="sync",
    ),
]


@pytest.mark.parametrize(("store_cls", "is_async", "companion_method"), _SESSION_STORE_MODES)
async def test_psycopg_session_companions_create_guarded_indexes_only(
    store_cls: Any, is_async: bool, companion_method: str
) -> None:
    store = store_cls(_mock_config())
    driver = _mock_migration_driver(is_async=is_async)

    await _await_if_needed(getattr(store, companion_method)(driver))

    statements = _recorded_scripts(driver, is_async=is_async)
    assert statements == [
        "CREATE INDEX IF NOT EXISTS idx_adk_session_app_user ON adk_session(app_name, user_id);",
        "CREATE INDEX IF NOT EXISTS idx_adk_session_update_time ON adk_session(update_time DESC);",
        "CREATE INDEX IF NOT EXISTS idx_adk_session_state ON adk_session USING GIN (state) WHERE state != '{}'::jsonb;",
        "CREATE INDEX IF NOT EXISTS idx_adk_event_session ON adk_event(session_id, timestamp ASC);",
        "CREATE INDEX IF NOT EXISTS idx_adk_event_app_timestamp ON adk_event(app_name, timestamp ASC);",
    ]
    assert all("CREATE TABLE" not in sql for sql in statements)
    driver.commit.assert_not_called()


@pytest.mark.parametrize(("store_cls", "is_async", "companion_method"), _SESSION_STORE_MODES)
async def test_psycopg_event_companions_follow_generated_and_covering_settings(
    store_cls: Any, is_async: bool, companion_method: str
) -> None:
    store = store_cls(_mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True}))
    driver = _mock_migration_driver(is_async=is_async)

    await _await_if_needed(getattr(store, companion_method)(driver))

    statements = _recorded_scripts(driver, is_async=is_async)
    assert (
        "CREATE INDEX IF NOT EXISTS idx_adk_event_session ON adk_event(session_id, timestamp ASC) INCLUDE (invocation_id);"
        in statements
    )
    assert (
        "CREATE INDEX IF NOT EXISTS idx_adk_event_author_gc ON adk_event(session_id, author_gc, timestamp ASC);"
        in statements
    )
    assert (
        "CREATE INDEX IF NOT EXISTS idx_adk_event_node_path_gc ON adk_event(session_id, node_path_gc, timestamp ASC);"
        in statements
    )


@pytest.mark.parametrize(
    ("store_cls", "is_async", "_prep_schema", "_prep_rec", "companion_method", "_finalize"), _MEMORY_STORE_MODES
)
async def test_psycopg_memory_companions_follow_search_settings(
    store_cls: Any, is_async: bool, _prep_schema: str, _prep_rec: str, companion_method: str, _finalize: str
) -> None:
    store = store_cls(_mock_config({"enable_bm25": True, "memory_use_fts": True, "vector_index_type": "ivfflat"}))
    driver = _mock_migration_driver(is_async=is_async)

    await _await_if_needed(getattr(store, companion_method)(driver))

    statements = _recorded_scripts(driver, is_async=is_async)
    assert any("USING GIN (to_tsvector('english', content_text));" in sql for sql in statements)
    assert any("USING bm25 (content_text) WITH (text_config='english');" in sql for sql in statements)
    assert any("idx_adk_memory_ivfflat" in sql for sql in statements)
    assert all("idx_adk_memory_hnsw" not in sql for sql in statements)
    assert all("CREATE TABLE" not in sql for sql in statements)
    driver.commit.assert_not_called()


async def test_psycopg_sync_and_async_companions_emit_identical_statements() -> None:
    async_store = PsycopgAsyncADKMemoryStore(_mock_config({"enable_bm25": True}))
    sync_store = PsycopgSyncADKMemoryStore(_mock_config({"enable_bm25": True}))
    async_driver = _mock_migration_driver(is_async=True)
    sync_driver = _mock_migration_driver(is_async=False)

    await async_store.prepare_reconcile_async(async_driver)
    await async_store.reconcile_companion_objects_async(async_driver)
    sync_store.prepare_reconcile_sync(sync_driver)
    sync_store.reconcile_companion_objects_sync(sync_driver)

    async_statements = _recorded_scripts(async_driver, is_async=True)
    assert len(async_statements) >= 7
    assert async_statements == _recorded_scripts(sync_driver, is_async=False)


@pytest.mark.parametrize(
    ("store_cls", "is_async", "prep_schema_method", "prep_reconcile_method", "_companion", "_finalize"),
    _MEMORY_STORE_MODES,
)
async def test_psycopg_memory_prepare_reconcile_installs_extensions_and_prepare_schema_is_noop(
    store_cls: Any, is_async: bool, prep_schema_method: str, prep_reconcile_method: str, _companion: str, _finalize: str
) -> None:
    driver = _mock_migration_driver(is_async=is_async)
    default_store = store_cls(_mock_config())

    await _await_if_needed(getattr(default_store, prep_schema_method)(driver))
    driver.execute_script.assert_not_called()

    await _await_if_needed(getattr(default_store, prep_reconcile_method)(driver))
    assert _recorded_scripts(driver, is_async=is_async) == ["CREATE EXTENSION IF NOT EXISTS vector"]

    driver.execute_script.reset_mock()
    bm25_store = store_cls(_mock_config({"enable_bm25": True}))
    await _await_if_needed(getattr(bm25_store, prep_schema_method)(driver))
    driver.execute_script.assert_not_called()

    await _await_if_needed(getattr(bm25_store, prep_reconcile_method)(driver))
    assert _recorded_scripts(driver, is_async=is_async) == [
        "CREATE EXTENSION IF NOT EXISTS vector",
        "CREATE EXTENSION IF NOT EXISTS pg_textsearch",
    ]


@pytest.mark.parametrize(
    ("config_cls", "store_cls", "is_async", "prep_method", "finalize_method", "register_attr"),
    [
        pytest.param(
            PsycopgAsyncConfig,
            PsycopgAsyncADKMemoryStore,
            True,
            "prepare_reconcile_async",
            "finalize_reconcile_async",
            "register_pgvector_async",
            id="async",
        ),
        pytest.param(
            PsycopgSyncConfig,
            PsycopgSyncADKMemoryStore,
            False,
            "prepare_reconcile_sync",
            "finalize_reconcile_sync",
            "register_pgvector_sync",
            id="sync",
        ),
    ],
)
async def test_psycopg_memory_finalize_reconcile_refreshes_config_extension_state(
    monkeypatch: pytest.MonkeyPatch,
    config_cls: Any,
    store_cls: Any,
    is_async: bool,
    prep_method: str,
    finalize_method: str,
    register_attr: str,
) -> None:
    register_mock = AsyncMock() if is_async else MagicMock()
    monkeypatch.setattr(f"sqlspec.adapters.psycopg.adk.store.{register_attr}", register_mock)

    config = config_cls(
        driver_features={"enable_pgvector": True, "enable_pg_textsearch": True},
        extension_config={"adk": {"enable_bm25": True}},
    )
    stale_error = RuntimeError("stale")
    config._pgvector_available = False
    config._pg_textsearch_available = False
    config._pg_textsearch_probe_error = stale_error

    store = store_cls(config)
    driver = _mock_migration_driver(is_async=is_async)

    await _await_if_needed(getattr(store, prep_method)(driver))
    assert config._pgvector_available is False
    assert config._pg_textsearch_available is False
    assert config._pg_textsearch_probe_error is stale_error
    register_mock.assert_not_called()

    await _await_if_needed(getattr(store, finalize_method)(driver))
    assert config._pgvector_available is True
    assert config._pg_textsearch_available is True
    assert config._pg_textsearch_probe_error is None
    assert config._ensure_pg_textsearch_available() is None
    register_mock.assert_called_once_with(driver.connection)


@pytest.mark.parametrize(
    ("store_cls", "is_async", "_prep_schema", "prep_method", "companion_method", "finalize_method"), _MEMORY_STORE_MODES
)
async def test_psycopg_disabled_memory_prepares_nothing(
    store_cls: Any, is_async: bool, _prep_schema: str, prep_method: str, companion_method: str, finalize_method: str
) -> None:
    store = store_cls(_mock_config({"enable_memory": False}))
    driver = _mock_migration_driver(is_async=is_async)

    await _await_if_needed(getattr(store, prep_method)(driver))
    await _await_if_needed(getattr(store, companion_method)(driver))
    await _await_if_needed(getattr(store, finalize_method)(driver))

    driver.execute_script.assert_not_called()


@pytest.mark.parametrize(
    ("store_cls", "is_async", "_prep_schema", "prep_method", "_companion", "_finalize"), _MEMORY_STORE_MODES
)
@pytest.mark.parametrize(
    ("adk_settings", "expected_match"),
    [
        pytest.param({"vector_index_type": "scann", "scann_quantizer": "BAD"}, "scann_quantizer", id="bad-quantizer"),
        pytest.param({"vector_index_type": "scann", "scann_num_leaves": -1}, "scann_num_leaves", id="bad-leaves"),
        pytest.param({"vector_index_type": "unknown"}, "vector_index_type", id="bad-vector-type"),
    ],
)
async def test_psycopg_memory_prepare_reconcile_validates_settings_first(
    store_cls: Any,
    is_async: bool,
    _prep_schema: str,
    prep_method: str,
    _companion: str,
    _finalize: str,
    adk_settings: dict[str, object],
    expected_match: str,
) -> None:
    store = store_cls(_mock_config(adk_settings))
    driver = _mock_migration_driver(is_async=is_async)

    with pytest.raises(ImproperConfigurationError, match=expected_match):
        await _await_if_needed(getattr(store, prep_method)(driver))

    driver.execute_script.assert_not_called()


@pytest.mark.parametrize(
    ("store_cls", "is_async", "_prep_schema", "prep_method", "_companion", "_finalize"), _MEMORY_STORE_MODES
)
async def test_psycopg_memory_prepare_reconcile_reports_missing_extension(
    store_cls: Any, is_async: bool, _prep_schema: str, prep_method: str, _companion: str, _finalize: str
) -> None:
    store = store_cls(_mock_config())
    driver = _mock_migration_driver(is_async=is_async)
    err = SQLSpecError("not available")
    driver.execute_script.side_effect = err

    with pytest.raises(MigrationError, match="vector") as exc_info:
        await _await_if_needed(getattr(store, prep_method)(driver))

    assert exc_info.value.__cause__ is err


@pytest.mark.parametrize(
    ("config_cls", "is_async", "register_attr"),
    [
        pytest.param(PsycopgAsyncConfig, True, "register_pgvector_async", id="async"),
        pytest.param(PsycopgSyncConfig, False, "register_pgvector_sync", id="sync"),
    ],
)
async def test_psycopg_reconcile_prepares_before_targets_and_companions(
    monkeypatch: pytest.MonkeyPatch, config_cls: Any, is_async: bool, register_attr: str
) -> None:
    monkeypatch.setattr(f"sqlspec.adapters.psycopg.adk.store.{register_attr}", AsyncMock() if is_async else MagicMock())
    config = config_cls(extension_config={"adk": {"enable_sessions": False}})
    driver = _mock_migration_driver(is_async=is_async)

    await _await_if_needed(reconcile(MigrationContext(config=config, dialect="postgres", driver=driver)))

    statements = _recorded_scripts(driver, is_async=is_async)
    assert statements[0] == "CREATE EXTENSION IF NOT EXISTS vector"
    assert statements[1].startswith("CREATE TABLE IF NOT EXISTS adk_memory")
    assert all(sql.startswith("CREATE INDEX IF NOT EXISTS") for sql in statements[2:])
