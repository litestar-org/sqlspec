"""Unit tests for CockroachDB psycopg ADK store extension configuration."""

import inspect
from datetime import UTC, datetime, timedelta
from typing import Any, NotRequired, Self, cast, get_args, get_origin
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig
from sqlspec.adapters.cockroach_psycopg.adk import (
    CockroachPsycopgADKConfig,
    CockroachPsycopgAsyncADKMemoryStore,
    CockroachPsycopgAsyncADKStore,
    CockroachPsycopgSyncADKMemoryStore,
    CockroachPsycopgSyncADKStore,
)
from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def test_cockroach_psycopg_adk_config_types_adapter_local_optimizations() -> None:
    """Cockroach ADK optimizations are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", CockroachPsycopgADKConfig).__optional_keys__

    expected_types: dict[str, object] = {
        "table_locality": str,
        "session_table_locality": str,
        "events_table_locality": str,
        "app_state_table_locality": str,
        "user_state_table_locality": str,
        "metadata_table_locality": str,
        "memory_table_locality": str,
        "enable_hash_sharded_indexes": bool,
        "hash_shard_bucket_count": int,
        "enable_storing_indexes": bool,
        "enable_memory_trigram_index": bool,
    }
    for feature_name, expected_type in expected_types.items():
        annotation = cast("Any", CockroachPsycopgADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)


@pytest.mark.anyio
async def test_cockroach_psycopg_async_adk_tables_use_plain_schema_by_default() -> None:
    """Async Cockroach ADK optimization DDL stays opt-in through adapter-local extension config."""

    store = CockroachPsycopgAsyncADKStore(_mock_config())
    memory_store = CockroachPsycopgAsyncADKMemoryStore(_mock_config())

    session_sql = await store._sessions_table_ddl()
    events_sql = await store._events_table_ddl()
    memory_sql = await memory_store._memory_table_ddl()

    assert "LOCALITY" not in session_sql
    assert "LOCALITY" not in events_sql
    assert "LOCALITY" not in memory_sql
    assert "USING HASH" not in session_sql
    assert "USING HASH" not in events_sql
    assert "USING HASH" not in memory_sql
    assert "STORING (" not in session_sql
    assert "STORING (" not in events_sql
    assert "gin_trgm_ops" not in memory_sql


def test_cockroach_psycopg_sync_adk_tables_use_plain_schema_by_default() -> None:
    """Sync Cockroach ADK optimization DDL stays opt-in through adapter-local extension config."""

    store = CockroachPsycopgSyncADKStore(_mock_config())
    memory_store = CockroachPsycopgSyncADKMemoryStore(_mock_config())

    session_sql = store._sessions_table_ddl()
    events_sql = store._events_table_ddl()
    memory_sql = memory_store._memory_table_ddl()

    assert "LOCALITY" not in session_sql
    assert "LOCALITY" not in events_sql
    assert "LOCALITY" not in memory_sql
    assert "USING HASH" not in session_sql
    assert "USING HASH" not in events_sql
    assert "USING HASH" not in memory_sql
    assert "STORING (" not in session_sql
    assert "STORING (" not in events_sql
    assert "gin_trgm_ops" not in memory_sql


@pytest.mark.anyio
async def test_cockroach_psycopg_async_adk_tables_apply_locality_hash_and_storing_indexes() -> None:
    """Async Cockroach ADK settings enable CockroachDB-specific table and index DDL."""

    config = _mock_config({
        "table_locality": 'LOCALITY REGIONAL BY TABLE IN "us-east1"',
        "events_table_locality": "LOCALITY REGIONAL BY ROW",
        "enable_hash_sharded_indexes": True,
        "hash_shard_bucket_count": 8,
        "enable_storing_indexes": True,
    })
    store = CockroachPsycopgAsyncADKStore(config)

    session_sql = await store._sessions_table_ddl()
    events_sql = await store._events_table_ddl()

    assert 'LOCALITY REGIONAL BY TABLE IN "us-east1"' in session_sql
    assert "LOCALITY REGIONAL BY ROW" in events_sql
    assert "ON adk_session(update_time DESC) USING HASH WITH (bucket_count = 8)" in session_sql
    assert "ON adk_event(session_id, timestamp ASC) USING HASH WITH (bucket_count = 8)" in events_sql
    assert "STORING (state, create_time, update_time)" in session_sql
    assert "STORING (invocation_id, event_data)" in events_sql


def test_cockroach_psycopg_sync_adk_tables_apply_locality_hash_and_storing_indexes() -> None:
    """Sync Cockroach ADK settings enable CockroachDB-specific table and index DDL."""

    config = _mock_config({
        "table_locality": 'LOCALITY REGIONAL BY TABLE IN "us-east1"',
        "events_table_locality": "LOCALITY REGIONAL BY ROW",
        "enable_hash_sharded_indexes": True,
        "hash_shard_bucket_count": 8,
        "enable_storing_indexes": True,
    })
    store = CockroachPsycopgSyncADKStore(config)

    session_sql = store._sessions_table_ddl()
    events_sql = store._events_table_ddl()

    assert 'LOCALITY REGIONAL BY TABLE IN "us-east1"' in session_sql
    assert "LOCALITY REGIONAL BY ROW" in events_sql
    assert "ON adk_session(update_time DESC) USING HASH WITH (bucket_count = 8)" in session_sql
    assert "ON adk_event(session_id, timestamp ASC) USING HASH WITH (bucket_count = 8)" in events_sql
    assert "STORING (state, create_time, update_time)" in session_sql
    assert "STORING (invocation_id, event_data)" in events_sql


@pytest.mark.anyio
async def test_cockroach_psycopg_async_memory_table_applies_trigram_index_and_locality() -> None:
    """Async memory search DDL is enabled from extension_config["adk"], not driver features."""

    config = _mock_config({"memory_table_locality": "LOCALITY GLOBAL", "enable_memory_trigram_index": True})
    memory_store = CockroachPsycopgAsyncADKMemoryStore(config)

    sql = await memory_store._memory_table_ddl()

    assert "LOCALITY GLOBAL" in sql
    assert "idx_adk_memory_content_trgm" in sql
    assert "ON adk_memory USING GIN (content_text gin_trgm_ops)" in sql


def test_cockroach_psycopg_sync_memory_table_applies_trigram_index_and_locality() -> None:
    """Sync memory search DDL is enabled from extension_config["adk"], not driver features."""

    config = _mock_config({"memory_table_locality": "LOCALITY GLOBAL", "enable_memory_trigram_index": True})
    memory_store = CockroachPsycopgSyncADKMemoryStore(config)

    sql = memory_store._memory_table_ddl()

    assert "LOCALITY GLOBAL" in sql
    assert "idx_adk_memory_content_trgm" in sql
    assert "ON adk_memory USING GIN (content_text gin_trgm_ops)" in sql


class _RecordingCursor:
    """Captures the SQL and bound parameters a session store issues."""

    def __init__(self, *, rows: "list[dict[str, Any]] | None" = None, rowcount: int = 1) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self._rows = rows or []
        self.rowcount = rowcount

    def execute(self, sql: Any, params: Any = None) -> None:
        rendered = (
            sql.decode() if isinstance(sql, bytes) else sql.as_string(None) if hasattr(sql, "as_string") else str(sql)
        )
        self.calls.append((rendered, tuple(params or ())))

    def fetchall(self) -> "list[Any]":
        return self._rows

    def fetchone(self) -> "dict[str, Any] | None":
        return self._rows[0] if self._rows else None

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, *_: Any) -> None:
        return None


class _RecordingAsyncCursor:
    def __init__(self, *, rows: "list[dict[str, Any]] | None" = None, rowcount: int = 1) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self._rows = rows or []
        self.rowcount = rowcount

    async def execute(self, sql: Any, params: Any = None) -> None:
        rendered = (
            sql.decode() if isinstance(sql, bytes) else sql.as_string(None) if hasattr(sql, "as_string") else str(sql)
        )
        self.calls.append((rendered, tuple(params or ())))

    async def fetchall(self) -> "list[Any]":
        return self._rows

    async def fetchone(self) -> "dict[str, Any] | None":
        return self._rows[0] if self._rows else None

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


class _RecordingConnection:
    def __init__(self, cursor: _RecordingCursor) -> None:
        self._cursor = cursor
        self.commit_called = False
        self.rollback_called = False

    def cursor(self, **_: Any) -> Any:
        return self._cursor

    def commit(self) -> None:
        self.commit_called = True

    def rollback(self) -> None:
        self.rollback_called = True

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, *_: Any) -> None:
        return None


class _RecordingAsyncConnection:
    def __init__(self, cursor: _RecordingAsyncCursor) -> None:
        self._cursor = cursor
        self.commit_called = False
        self.rollback_called = False

    def cursor(self, **_: Any) -> Any:
        return self._cursor

    async def commit(self) -> None:
        self.commit_called = True

    async def rollback(self) -> None:
        self.rollback_called = True

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _async_session_store() -> "tuple[CockroachPsycopgAsyncADKStore, _RecordingAsyncCursor]":
    cursor = _RecordingAsyncCursor()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: _RecordingAsyncConnection(cursor)
    return CockroachPsycopgAsyncADKStore(config), cursor


def _sync_session_store() -> "tuple[CockroachPsycopgSyncADKStore, _RecordingCursor]":
    cursor = _RecordingCursor()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: _RecordingConnection(cursor)
    return CockroachPsycopgSyncADKStore(config), cursor


async def test_cockroach_psycopg_async_list_sessions_binds_order_and_page() -> None:
    """Explicit ordering renders inline while page bounds bind as pyformat parameters."""
    store, cursor = _async_session_store()

    await store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = cursor.calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = %s AND user_id = %s "
        "ORDER BY create_time ASC, id ASC LIMIT %s OFFSET %s"
    )
    assert params == ("app", "u1", 10, 20)


def test_cockroach_psycopg_sync_list_sessions_binds_order_and_page() -> None:
    """The sync path produces the same bounded query as the async path."""
    store, cursor = _sync_session_store()

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = cursor.calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = %s AND user_id = %s "
        "ORDER BY create_time ASC, id ASC LIMIT %s OFFSET %s"
    )
    assert params == ("app", "u1", 10, 20)


async def test_cockroach_psycopg_async_list_sessions_defaults_to_recent_first() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, cursor = _async_session_store()

    await store.list_sessions("app")

    sql, params = cursor.calls[0]
    assert _normalized(sql).endswith("WHERE app_name = %s ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_cockroach_psycopg_sync_list_sessions_orders_page_after_scope_parameters() -> None:
    """Page placeholders follow the scope parameters that are actually present."""
    store, cursor = _sync_session_store()

    store.list_sessions("app", limit=5)

    sql, params = cursor.calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC LIMIT %s OFFSET %s")
    assert params == ("app", 5, 0)


async def test_cockroach_psycopg_async_list_sessions_zero_limit_never_queries() -> None:
    """A zero limit short-circuits before any database work."""
    store, cursor = _async_session_store()

    assert await store.list_sessions("app", limit=0) == []
    assert cursor.calls == []


def test_cockroach_psycopg_sync_list_sessions_zero_limit_never_queries() -> None:
    """A zero limit short-circuits before any database work on the sync path."""
    store, cursor = _sync_session_store()

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
def test_cockroach_psycopg_sync_list_sessions_rejects_invalid_options(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, cursor = _sync_session_store()

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert cursor.calls == []


def _memory_store_with_cursor(async_mode: bool, adk_config: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> Any:
    """Provide real stores with SDK cursor and connection boundaries substituted."""
    config_type = CockroachPsycopgAsyncConfig if async_mode else CockroachPsycopgSyncConfig
    cursor = MagicMock()
    cursor.rowcount = 1
    cursor.description = []
    cursor.fetchall.return_value = []
    conn = MagicMock()
    context = MagicMock()
    context.__enter__.return_value = conn
    context.__aenter__.return_value = conn
    cursor.__enter__.return_value = cursor
    cursor.__aenter__.return_value = cursor
    conn.cursor.return_value = cursor
    if async_mode:
        cursor.execute = AsyncMock()
        cursor.fetchall = AsyncMock(return_value=[])
        conn.commit = AsyncMock()
    monkeypatch.setattr(config_type, "provide_connection", lambda *_a, **_k: context)
    if async_mode:
        return CockroachPsycopgAsyncADKMemoryStore(
            CockroachPsycopgAsyncConfig(extension_config={"adk": adk_config})
        ), cursor
    return CockroachPsycopgSyncADKMemoryStore(CockroachPsycopgSyncConfig(extension_config={"adk": adk_config})), cursor


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize(("owner_type", "owner_id"), [("INTEGER", 42), ("UUID", UUID(int=1))])
async def test_cockroach_psycopg_memory_batch_preserves_owner_type(
    async_mode: bool, owner_type: str, owner_id: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sync and async batches bind one native owner scalar outside UNNEST."""
    store, cursor = _memory_store_with_cursor(async_mode, {"owner_id_column": f"owner_id {owner_type}"}, monkeypatch)
    now = datetime.now(UTC)
    entry = {
        "id": "mem-1",
        "session_id": "sess-1",
        "app_name": "app",
        "user_id": "u1",
        "scope": "user",
        "event_id": "evt-1",
        "author": "user",
        "timestamp": now,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": now,
        "embedding": None,
    }
    inserted = store.insert_memory_entries([entry], owner_id)
    if async_mode:
        inserted = await inserted
    assert inserted == 1

    query, params = cursor.execute.call_args.args
    assert "author, %s, timestamp" in _normalized(query.as_string())
    assert params[0] == owner_id
    assert params[1] == ["mem-1"]


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize("use_fts", [False, True])
async def test_cockroach_psycopg_memory_embedding_keeps_text_search(
    async_mode: bool, use_fts: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Embedding arguments keep searching text columns present in the memory schema."""
    store, cursor = _memory_store_with_cursor(async_mode, {"memory_use_fts": use_fts}, monkeypatch)
    result = store.search_entries("hello", "app", "u1", embedding=[0.1])
    if async_mode:
        result = await result
    assert result == []
    query, params = cursor.execute.call_args.args
    sql = query.decode()
    assert "plainto_tsquery" in sql if use_fts else "content_text ILIKE" in sql
    assert params == ("app", "u1", "hello" if use_fts else "%hello%", 20)

    empty = store.search_entries("", "app", "u1", embedding=[0.1])
    if async_mode:
        empty = await empty
    assert empty == []
    assert cursor.execute.call_count == 1


@pytest.mark.parametrize("async_mode", [False, True])
async def test_cockroach_psycopg_session_companions_use_qualified_identity(async_mode: bool) -> None:
    """Sync and async session companion DDL uses schema-qualified index names without table DDL or commits."""
    config = _mock_config({"session_table": "app.sessions", "events_table": "app.events"})
    driver = MagicMock()
    if async_mode:
        driver.execute_script = AsyncMock()
        driver.commit = AsyncMock()
        store = CockroachPsycopgAsyncADKStore(config)
        await store.reconcile_companion_objects_async(driver)
    else:
        sync_store = CockroachPsycopgSyncADKStore(config)
        sync_store.reconcile_companion_objects_sync(driver)

    statements = [_normalized(call.args[0]) for call in driver.execute_script.call_args_list]
    assert statements == [
        "CREATE INDEX IF NOT EXISTS idx_app_sessions_app_user ON app.sessions(app_name, user_id);",
        "CREATE INDEX IF NOT EXISTS idx_app_sessions_update_time ON app.sessions(update_time DESC);",
        "CREATE INDEX IF NOT EXISTS idx_app_sessions_state ON app.sessions USING GIN (state) WHERE state != '{}'::jsonb;",
        "CREATE INDEX IF NOT EXISTS idx_app_events_session ON app.events(session_id, timestamp ASC);",
        "CREATE INDEX IF NOT EXISTS idx_app_events_event_data ON app.events USING GIN (event_data);",
        "CREATE INDEX IF NOT EXISTS idx_app_events_app_timestamp ON app.events(app_name, timestamp ASC);",
    ]
    assert all("CREATE TABLE" not in sql for sql in statements)
    driver.commit.assert_not_called()


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize(
    ("adk_config", "expected_names"),
    [
        pytest.param({"enable_memory": False}, [], id="disabled"),
        pytest.param({"enable_memory": True}, ["idx_adk_memory_app_user_time", "idx_adk_memory_session"], id="default"),
        pytest.param(
            {"enable_memory": True, "memory_use_fts": True, "enable_memory_trigram_index": True},
            [
                "idx_adk_memory_app_user_time",
                "idx_adk_memory_session",
                "idx_adk_memory_fts",
                "idx_adk_memory_content_trgm",
            ],
            id="fts-and-trigram",
        ),
    ],
)
async def test_cockroach_psycopg_memory_companions_follow_settings(
    async_mode: bool, adk_config: dict[str, object], expected_names: list[str]
) -> None:
    """Sync and async memory companion indexes follow enable_memory, FTS, and trigram settings."""
    driver = MagicMock()
    if async_mode:
        driver.execute_script = AsyncMock()
        driver.commit = AsyncMock()
        store = CockroachPsycopgAsyncADKMemoryStore(_mock_config(adk_config))
        await store.reconcile_companion_objects_async(driver)
    else:
        sync_store = CockroachPsycopgSyncADKMemoryStore(_mock_config(adk_config))
        sync_store.reconcile_companion_objects_sync(driver)

    statements = [_normalized(call.args[0]) for call in driver.execute_script.call_args_list]
    assert len(statements) == len(expected_names)
    for statement, index_name in zip(statements, expected_names, strict=True):
        assert f"CREATE INDEX IF NOT EXISTS {index_name} ON adk_memory" in statement
    driver.commit.assert_not_called()


@pytest.mark.parametrize("async_mode", [False, True])
async def test_cockroach_psycopg_companions_carry_tuning_clauses(async_mode: bool) -> None:
    """Sync and async companion statements carry USING HASH and STORING clauses."""
    config = _mock_config({"enable_hash_sharded_indexes": True, "enable_storing_indexes": True})
    driver = MagicMock()
    if async_mode:
        driver.execute_script = AsyncMock()
        store = CockroachPsycopgAsyncADKStore(config)
        await store.reconcile_companion_objects_async(driver)
    else:
        sync_store = CockroachPsycopgSyncADKStore(config)
        sync_store.reconcile_companion_objects_sync(driver)

    statements = [_normalized(call.args[0]) for call in driver.execute_script.call_args_list]
    assert (
        "CREATE INDEX IF NOT EXISTS idx_adk_session_app_user "
        "ON adk_session(app_name, user_id) STORING (state, create_time, update_time);"
    ) in statements
    assert (
        "CREATE INDEX IF NOT EXISTS idx_adk_session_update_time ON adk_session(update_time DESC) USING HASH;"
    ) in statements
    assert (
        "CREATE INDEX IF NOT EXISTS idx_adk_event_session "
        "ON adk_event(session_id, timestamp ASC) USING HASH STORING (invocation_id, event_data);"
    ) in statements


@pytest.mark.parametrize("async_mode", [False, True])
async def test_cockroach_psycopg_prepare_installs_nothing(async_mode: bool) -> None:
    """Sync and async CockroachDB memory stores install zero PostgreSQL extensions during prep."""
    driver = MagicMock()
    if async_mode:
        driver.execute_script = AsyncMock()
        store = CockroachPsycopgAsyncADKMemoryStore(_mock_config({"enable_memory": True}))
        await store.prepare_schema_async(driver)
        await store.prepare_reconcile_async(driver)
    else:
        sync_store = CockroachPsycopgSyncADKMemoryStore(_mock_config({"enable_memory": True}))
        sync_store.prepare_schema_sync(driver)
        sync_store.prepare_reconcile_sync(driver)

    driver.execute_script.assert_not_called()


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize(
    ("adk_config", "expected_key"),
    [
        pytest.param({"enable_bm25": True}, "enable_bm25", id="bm25"),
        pytest.param({"vector_index_type": "hnsw"}, "vector_index_type", id="vector-index"),
    ],
)
def test_cockroach_psycopg_rejects_unsupported_search_settings(
    async_mode: bool, adk_config: dict[str, object], expected_key: str
) -> None:
    """Unsupported PostgreSQL vector/BM25 settings raise ImproperConfigurationError before DDL."""
    config_cls = CockroachPsycopgAsyncConfig if async_mode else CockroachPsycopgSyncConfig
    with pytest.raises(ImproperConfigurationError, match=expected_key):
        config_cls(extension_config={"adk": cast("Any", adk_config)})


async def _await_if_needed(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


_COCKROACH_PSYCOPG_SESSION_MODES = [
    pytest.param(CockroachPsycopgAsyncADKStore, True, id="async"),
    pytest.param(CockroachPsycopgSyncADKStore, False, id="sync"),
]

_COCKROACH_PSYCOPG_MEMORY_MODES = [
    pytest.param(CockroachPsycopgAsyncADKMemoryStore, True, id="async"),
    pytest.param(CockroachPsycopgSyncADKMemoryStore, False, id="sync"),
]


def _build_cockroach_psycopg_store(
    store_cls: Any,
    *,
    is_async: bool,
    adk_config: "dict[str, object] | None" = None,
    rows: "list[dict[str, Any]] | None" = None,
    rowcount: int = 1,
) -> "tuple[Any, Any, Any]":
    cursor: Any = (
        _RecordingAsyncCursor(rows=rows, rowcount=rowcount)
        if is_async
        else _RecordingCursor(rows=rows, rowcount=rowcount)
    )
    connection: Any = _RecordingAsyncConnection(cursor) if is_async else _RecordingConnection(cursor)
    config = _mock_config(adk_config)
    config.provide_connection = lambda *_a, **_k: connection
    return store_cls(config), cursor, connection


@pytest.mark.parametrize(("store_cls", "is_async"), _COCKROACH_PSYCOPG_SESSION_MODES)
@pytest.mark.parametrize(
    ("renew_for", "expect_update"),
    [
        pytest.param(None, False, id="none"),
        pytest.param(0, False, id="zero"),
        pytest.param(-1, False, id="negative"),
        pytest.param(timedelta(milliseconds=500), True, id="subsecond"),
        pytest.param(30, True, id="positive-int"),
    ],
)
async def test_cockroach_psycopg_get_session_renews_only_for_positive_durations(
    store_cls: Any, is_async: bool, renew_for: "int | timedelta | None", expect_update: bool
) -> None:
    now = datetime.now(UTC)
    store, cursor, connection = _build_cockroach_psycopg_store(
        store_cls,
        is_async=is_async,
        rows=[{"id": "s1", "app_name": "app", "user_id": "u1", "state": {}, "create_time": now, "update_time": now}],
    )

    result = await _await_if_needed(store.get_session("app", "u1", "s1", renew_for=renew_for))

    assert result is not None
    sql, params = cursor.calls[0]
    if expect_update:
        assert _normalized(sql).startswith("UPDATE adk_session SET update_time = CURRENT_TIMESTAMP")
        assert connection.commit_called is True
    else:
        assert _normalized(sql).startswith("SELECT id, app_name, user_id, state, create_time, update_time")
        assert connection.commit_called is False
    assert params == ("app", "u1", "s1")


@pytest.mark.parametrize(("store_cls", "is_async"), _COCKROACH_PSYCOPG_SESSION_MODES)
@pytest.mark.parametrize(("rowcount", "should_raise"), [(1, False), (0, True)])
async def test_cockroach_psycopg_append_event_checks_session_ownership(
    store_cls: Any, is_async: bool, rowcount: int, should_raise: bool
) -> None:
    store, cursor, connection = _build_cockroach_psycopg_store(store_cls, is_async=is_async, rowcount=rowcount)
    now = datetime.now(UTC)
    event_record = {
        "id": "evt-1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "s1",
        "invocation_id": "inv-1",
        "timestamp": now,
        "event_data": {"author": "user"},
    }

    if should_raise:
        with pytest.raises(ValueError, match="Session s1 not found"):
            await _await_if_needed(store.append_event(cast("Any", event_record)))
        assert connection.commit_called is False
    else:
        await _await_if_needed(store.append_event(cast("Any", event_record)))
        assert connection.commit_called is True

    sql, params = cursor.calls[0]
    assert "WHERE EXISTS ( SELECT 1 FROM adk_session WHERE id = %s AND app_name = %s AND user_id = %s )" in _normalized(
        sql
    )
    assert params[-3:] == ("s1", "app", "u1")


@pytest.mark.parametrize(("store_cls", "is_async"), _COCKROACH_PSYCOPG_SESSION_MODES)
@pytest.mark.parametrize(
    ("record_overrides", "expect_mismatch"),
    [
        pytest.param({}, False, id="matching"),
        pytest.param({"app_name": "other-app"}, True, id="mismatched-app"),
        pytest.param({"user_id": "other-user"}, True, id="mismatched-user"),
        pytest.param({"session_id": "other-session"}, True, id="mismatched-session"),
    ],
)
async def test_cockroach_psycopg_append_event_and_update_state_validates_scope_and_updates_first(
    store_cls: Any, is_async: bool, record_overrides: dict[str, str], expect_mismatch: bool
) -> None:
    now = datetime.now(UTC)
    store, cursor, _ = _build_cockroach_psycopg_store(
        store_cls,
        is_async=is_async,
        rows=[
            {
                "id": "s1",
                "app_name": "app",
                "user_id": "u1",
                "state": {"k": "v"},
                "create_time": now,
                "update_time": now,
            }
        ],
    )
    event_record = {
        "id": "evt-1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "s1",
        "invocation_id": "inv-1",
        "timestamp": now,
        "event_data": {"author": "user"},
        **record_overrides,
    }

    if expect_mismatch:
        with pytest.raises(ValueError, match="event_record scope"):
            await _await_if_needed(
                store.append_event_and_update_state(cast("Any", event_record), "app", "u1", "s1", {"k": "v"})
            )
        assert cursor.calls == []
        return

    updated = await _await_if_needed(
        store.append_event_and_update_state(cast("Any", event_record), "app", "u1", "s1", {"k": "v"})
    )
    assert updated["id"] == "s1"
    assert _normalized(cursor.calls[0][0]).startswith("UPDATE adk_session")
    assert _normalized(cursor.calls[1][0]).startswith("INSERT INTO adk_event")


@pytest.mark.parametrize(("store_cls", "is_async"), _COCKROACH_PSYCOPG_SESSION_MODES)
async def test_cockroach_psycopg_get_events_joins_on_session_and_owner_columns(store_cls: Any, is_async: bool) -> None:
    store, cursor, _ = _build_cockroach_psycopg_store(store_cls, is_async=is_async)

    await _await_if_needed(store.get_events("app", "u1", "s1"))

    sql, params = cursor.calls[0]
    normalized = _normalized(sql)
    assert (
        "JOIN adk_session s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id" in normalized
    )
    assert "WHERE s.app_name = %s AND s.user_id = %s AND s.id = %s" in normalized
    assert params == ("app", "u1", "s1")


@pytest.mark.parametrize(("store_cls", "is_async"), _COCKROACH_PSYCOPG_MEMORY_MODES)
@pytest.mark.parametrize(
    ("app_name", "user_id", "expected_where", "expected_params"),
    [
        pytest.param(None, None, "WHERE session_id = %s", ("s1",), id="session-only"),
        pytest.param("app", None, "WHERE session_id = %s AND app_name = %s", ("s1", "app"), id="session-and-app"),
        pytest.param(None, "u1", "WHERE session_id = %s AND user_id = %s", ("s1", "u1"), id="session-and-user"),
        pytest.param(
            "app",
            "u1",
            "WHERE session_id = %s AND app_name = %s AND user_id = %s",
            ("s1", "app", "u1"),
            id="session-app-user",
        ),
    ],
)
async def test_cockroach_psycopg_delete_entries_by_session_scopes_predicates(
    store_cls: Any,
    is_async: bool,
    app_name: "str | None",
    user_id: "str | None",
    expected_where: str,
    expected_params: tuple[str, ...],
) -> None:
    store, cursor, connection = _build_cockroach_psycopg_store(
        store_cls, is_async=is_async, adk_config={"enable_memory": True}, rowcount=2
    )

    deleted = await _await_if_needed(store.delete_entries_by_session("s1", app_name=app_name, user_id=user_id))

    assert deleted == 2
    assert connection.commit_called is True
    sql, params = cursor.calls[0]
    assert _normalized(sql) == f"DELETE FROM adk_memory {expected_where}"
    assert params == expected_params


@pytest.mark.parametrize(("store_cls", "is_async"), _COCKROACH_PSYCOPG_MEMORY_MODES)
@pytest.mark.parametrize("owner_column", [None, "tenant_id UUID"])
async def test_cockroach_psycopg_memory_ddl_and_insert_use_composite_dedup_and_targetless_conflict(
    store_cls: Any, is_async: bool, owner_column: "str | None"
) -> None:
    adk_cfg: dict[str, object] = {"enable_memory": True}
    if owner_column:
        adk_cfg["owner_id_column"] = owner_column
    store, cursor, _ = _build_cockroach_psycopg_store(store_cls, is_async=is_async, adk_config=adk_cfg)
    ddl = await _await_if_needed(store._memory_table_ddl())
    assert "event_id VARCHAR(128) NOT NULL," in ddl
    assert "event_id VARCHAR(128) NOT NULL UNIQUE" not in ddl
    assert "UNIQUE (app_name, user_id, event_id)" in ddl

    now = datetime.now(UTC)
    entry = {
        "id": "mem-1",
        "session_id": "s1",
        "app_name": "app",
        "user_id": "u1",
        "scope": "user",
        "event_id": "evt-1",
        "author": "user",
        "timestamp": now,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": now,
        "embedding": None,
    }
    await _await_if_needed(store.insert_memory_entries([cast("Any", entry)], owner_id="tenant-1"))
    sql, _ = cursor.calls[0]
    assert "ON CONFLICT DO NOTHING" in sql
    assert "ON CONFLICT (event_id)" not in sql
