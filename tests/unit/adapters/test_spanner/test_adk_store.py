# pyright: reportPrivateUsage=false
"""Unit tests for Spanner ADK store behavior."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast, get_args, get_origin
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.api_core.exceptions import NotFound
from google.cloud.spanner_v1 import param_types
from typing_extensions import NotRequired

from sqlspec.adapters.spanner.adk import (
    SpannerADKConfig,
    SpannerADKRetentionConfig,
    SpannerAsyncADKMemoryStore,
    SpannerAsyncADKStore,
    SpannerSyncADKMemoryStore,
    SpannerSyncADKStore,
)
from sqlspec.adapters.spanner.adk.store import _spanner_drop_statement_table
from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import StoredEvent, StoredMemory


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def _spanner_not_found(message: str) -> NotFound:
    return NotFound(message)  # type: ignore[no-untyped-call]


def test_spanner_adk_config_types_adapter_local_optimizations() -> None:
    """Spanner ADK optimization settings are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", SpannerADKConfig).__optional_keys__

    expected_types: dict[str, object] = {
        "shard_count": int,
        "session_table_options": str,
        "events_table_options": str,
        "memory_table_options": str,
        "expires_index_options": str,
        "retention": SpannerADKRetentionConfig,
        "vector_dimensions": int | None,
        "vector_distance_type": str,
        "vector_index_enabled": bool,
        "scann_tree_depth": int,
        "scann_num_leaves": int,
        "enable_hybrid_search": bool,
        "enable_memory_graph": bool,
        "memory_graph_name": str,
    }
    for feature_name, expected_type in expected_types.items():
        annotation = cast("Any", SpannerADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)

    for feature_name in ("session_ttl_seconds", "event_ttl_seconds", "memory_ttl_seconds", "artifact_ttl_seconds"):
        annotation = cast("Any", SpannerADKRetentionConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (int,)


def test_insert_event_preserves_event_record_timestamp() -> None:
    """Spanner stores the ADK event timestamp, not the commit timestamp."""
    store = SpannerSyncADKStore(_mock_config())
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    event: StoredEvent = {
        "id": "event-1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "session-1",
        "invocation_id": "inv-1",
        "timestamp": timestamp,
        "event_data": {"content": "hello"},
    }

    with patch.object(type(store), "_run_write") as run_write:
        store._insert_event(event)  # pyright: ignore[reportPrivateUsage]

    statements = run_write.call_args.args[0]
    sql, params, _types = statements[0]
    assert "@timestamp" in sql
    assert "PENDING_COMMIT_TIMESTAMP()" not in sql
    assert params["id"] == "event-1"
    assert params["timestamp"] is timestamp


def test_append_event_and_update_state_preserves_event_record_timestamp() -> None:
    """Atomic append uses the ADK event timestamp while session update uses commit time."""
    store = SpannerSyncADKStore(_mock_config())
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    event: StoredEvent = {
        "id": "event-1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "session-1",
        "invocation_id": "inv-1",
        "timestamp": timestamp,
        "event_data": {"content": "hello"},
    }
    fake_record = {
        "id": "session-1",
        "app_name": "app",
        "user_id": "u1",
        "state": {"turn": 1},
        "create_time": timestamp,
        "update_time": timestamp,
    }

    with (
        patch.object(type(store), "_run_write") as run_write,
        patch.object(type(store), "_get_session", return_value=fake_record),
    ):
        returned = store.append_event_and_update_state(event, "app", "u1", "session-1", {"turn": 1})

    event_sql, event_params, _event_types = run_write.call_args.args[0][0]
    update_sql, _state_params, _state_types = run_write.call_args.args[0][1]
    assert "@timestamp" in event_sql
    assert "PENDING_COMMIT_TIMESTAMP()" not in event_sql
    assert event_params["id"] == "event-1"
    assert event_params["timestamp"] is timestamp
    assert "PENDING_COMMIT_TIMESTAMP()" in update_sql
    assert returned == fake_record


def test_spanner_session_table_generates_row_deletion_policy_from_retention() -> None:
    store = SpannerSyncADKStore(_mock_config({"retention": {"session_ttl_seconds": 86_400}}))

    sql = store._sessions_table_ddl()

    assert "ROW DELETION POLICY (OLDER_THAN(create_time, INTERVAL 1 DAY))" in sql


def test_spanner_events_table_rounds_retention_up_to_days() -> None:
    store = SpannerSyncADKStore(_mock_config({"retention": {"event_ttl_seconds": 86_401}}))

    sql = store._events_table_ddl()

    assert "ROW DELETION POLICY (OLDER_THAN(timestamp, INTERVAL 2 DAY))" in sql


def test_spanner_memory_table_generates_ttl_and_table_options() -> None:
    store = SpannerSyncADKMemoryStore(
        _mock_config({"memory_table_options": "locality_group = 'hot'", "retention": {"memory_ttl_seconds": 604_800}})
    )

    statements = store._memory_table_ddl()
    table_sql = statements[0]

    assert "OPTIONS (locality_group = 'hot')" in table_sql
    assert "ROW DELETION POLICY (OLDER_THAN(inserted_at, INTERVAL 7 DAY))" in table_sql


def test_spanner_session_store_emits_expiration_indexes_with_configured_options() -> None:
    config = _mock_config({"expires_index_options": "locality_group = 'cold'"})
    database = config.get_database.return_value
    database.list_tables.return_value = []
    store = SpannerSyncADKStore(config)

    store.create_tables()

    ddl_statements = database.update_ddl.call_args.args[0]
    assert (
        "CREATE INDEX IF NOT EXISTS idx_adk_session_update_time "
        "ON adk_session(update_time) OPTIONS (locality_group = 'cold')"
    ) in ddl_statements
    assert (
        "CREATE INDEX IF NOT EXISTS idx_adk_event_timestamp ON adk_event(timestamp) OPTIONS (locality_group = 'cold')"
    ) in ddl_statements


def test_spanner_session_store_drops_expiration_indexes_before_tables() -> None:
    store = SpannerSyncADKStore(_mock_config())

    statements = store._drop_tables_sql()

    assert statements[:2] == ["DROP INDEX idx_adk_event_timestamp", "DROP INDEX idx_adk_session_update_time"]
    assert statements[-2:] == ["DROP TABLE adk_event", "DROP TABLE adk_session"]


def test_spanner_memory_insert_entries_writes_clean_break_record() -> None:
    config = _mock_config()
    database = config.get_database.return_value
    executed_queries: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    executed_writes: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def _run_in_txn(job: Any) -> Any:
        txn = MagicMock()

        def _exec_sql(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> list[tuple[str]]:
            executed_queries.append((sql, params, param_types))
            return [("event-existing",)]

        def _exec_update(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> int:
            executed_writes.append((sql, params, param_types))
            return 1

        def _batch_update(stmts: list[tuple[str, dict[str, Any], dict[str, Any]]]) -> tuple[Any, list[int]]:
            executed_writes.extend(stmts)
            return SimpleNamespace(code=0, message=""), [1] * len(stmts)

        txn.execute_sql = _exec_sql
        txn.execute_update = _exec_update
        txn.batch_update = _batch_update
        return job(txn)

    database.run_in_transaction.side_effect = _run_in_txn
    store = SpannerSyncADKMemoryStore(config)
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    entry_existing: StoredMemory = {
        "id": "memory-0",
        "session_id": "session-1",
        "app_name": "app",
        "user_id": "user",
        "scope": "user",
        "event_id": "event-existing",
        "author": "assistant",
        "timestamp": timestamp,
        "content_json": {"text": "old"},
        "content_text": "old",
        "metadata_json": None,
        "inserted_at": timestamp,
        "embedding": None,
    }
    entry_new: StoredMemory = {
        "id": "memory-1",
        "session_id": "session-1",
        "app_name": "app",
        "user_id": "user",
        "scope": "user",
        "event_id": "event-1",
        "author": "assistant",
        "timestamp": timestamp,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": {"source": "unit"},
        "inserted_at": timestamp,
        "embedding": [0.25, 0.75],
    }
    entry_dup: StoredMemory = {**entry_new, "id": "memory-2"}

    inserted = store.insert_memory_entries([entry_existing, entry_new, entry_dup])

    assert inserted == 1
    assert len(executed_queries) == 1
    select_sql, select_params, select_types = executed_queries[0]
    assert "WHERE event_id IN UNNEST(@event_ids)" in select_sql
    assert select_params["event_ids"] == ["event-existing", "event-1"]
    assert select_types["event_ids"] == cast("Any", param_types).Array(param_types.STRING)

    assert len(executed_writes) == 1
    sql, params, types = executed_writes[0]
    assert "INSERT INTO adk_memory" in sql
    assert "embedding" in sql
    assert params["content_json"] == '{"text":"hello"}'
    assert params["metadata_json"] == '{"source":"unit"}'
    assert params["inserted_at"] is timestamp
    assert params["embedding"] == [0.25, 0.75]
    assert types["embedding"] == cast("Any", param_types).Array(param_types.FLOAT32)


def test_spanner_memory_rows_to_records_decodes_json_and_embedding_fields() -> None:
    store = SpannerSyncADKMemoryStore(_mock_config())
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)

    records = store._rows_to_records([
        (
            "memory-1",
            "session-1",
            "app",
            "user",
            "user",
            "event-1",
            "assistant",
            timestamp,
            '{"text":"hello"}',
            "hello",
            '{"source":"unit"}',
            timestamp,
            [0.5, -0.5],
        )
    ])

    assert records[0]["content_json"] == {"text": "hello"}
    assert records[0]["metadata_json"] == {"source": "unit"}
    assert records[0]["content_text"] == "hello"
    assert records[0]["scope"] == "user"
    assert records[0]["embedding"] == [0.5, -0.5]


def test_spanner_reset_drop_tables_filters_absent_tables() -> None:
    config = _mock_config()
    config.get_database.return_value.list_tables.return_value = [SimpleNamespace(table_id="adk_events")]
    store = SpannerSyncADKStore(config)

    statements = store._reset_drop_tables_sql()

    assert statements == ["DROP INDEX idx_adk_events_timestamp", "DROP TABLE adk_events"]


def test_spanner_memory_reset_drop_tables_filters_absent_tables_and_indexes() -> None:
    config = _mock_config()
    config.get_database.return_value.list_tables.return_value = [SimpleNamespace(table_id="adk_memory_entries")]
    store = SpannerSyncADKMemoryStore(config)

    statements = store._reset_drop_memory_table_sql()

    assert statements == [
        "DROP INDEX idx_adk_memory_entries_event_id",
        "DROP INDEX idx_adk_memory_entries_session",
        "DROP INDEX idx_adk_memory_entries_app_scope_user_time",
        "DROP INDEX idx_adk_memory_entries_scope",
        "DROP TABLE adk_memory_entries",
    ]


async def test_async_spanner_reset_drop_tables_filters_absent_tables() -> None:
    """The async session store drops only the ADK tables that exist in the database."""
    config = _mock_config()
    database = MagicMock()

    async def _list_tables() -> Any:
        yield SimpleNamespace(table_id="adk_events")

    database.list_tables.side_effect = _list_tables
    config.get_database = AsyncMock(return_value=database)
    store = SpannerAsyncADKStore(config)

    statements = await store._reset_drop_tables_sql()

    assert statements == ["DROP INDEX idx_adk_events_timestamp", "DROP TABLE adk_events"]


async def test_async_spanner_memory_reset_drop_tables_filters_absent_tables_and_indexes() -> None:
    """The async memory store drops only the memory tables and indexes that exist in the database."""
    config = _mock_config()
    database = MagicMock()

    async def _list_tables() -> Any:
        yield SimpleNamespace(table_id="adk_memory_entries")

    database.list_tables.side_effect = _list_tables
    config.get_database = AsyncMock(return_value=database)
    store = SpannerAsyncADKMemoryStore(config)

    statements = await store._reset_drop_memory_table_sql()

    assert statements == [
        "DROP INDEX idx_adk_memory_entries_event_id",
        "DROP INDEX idx_adk_memory_entries_session",
        "DROP INDEX idx_adk_memory_entries_app_scope_user_time",
        "DROP INDEX idx_adk_memory_entries_scope",
        "DROP TABLE adk_memory_entries",
    ]


def test_spanner_drop_statement_table_handles_if_exists_and_quoted_identifiers() -> None:
    existing = {"adk_events", "adk_memory_entries"}

    assert _spanner_drop_statement_table("DROP TABLE IF EXISTS `adk_events`", existing) == "adk_events"
    assert _spanner_drop_statement_table("DROP TABLE IF EXISTS `adk_session`", existing) is None
    assert _spanner_drop_statement_table("DROP INDEX IF EXISTS `idx_adk_events_timestamp`", existing) == "adk_events"
    assert (
        _spanner_drop_statement_table("DROP SEARCH INDEX IF EXISTS `idx_adk_memory_entries_fts`", existing)
        == "adk_memory_entries"
    )
    assert (
        _spanner_drop_statement_table("DROP VECTOR INDEX IF EXISTS `idx_adk_memory_entries_embedding`", existing)
        == "adk_memory_entries"
    )
    assert (
        _spanner_drop_statement_table("DROP PROPERTY GRAPH `adk_memory_entries_graph`", existing)
        == "adk_memory_entries"
    )
    assert _spanner_drop_statement_table("DROP SEARCH INDEX `idx_adk_session_fts`", existing) is None


def test_spanner_memory_ddl_supports_vector_index_and_property_graph() -> None:
    """Verify memory DDL emits event_id index, vector_length, vector index, and property graph overlay."""
    default_store = SpannerSyncADKMemoryStore(_mock_config())
    default_ddl = default_store._memory_table_ddl()
    assert "embedding ARRAY<FLOAT32>" in default_ddl[0]
    assert "vector_length" not in default_ddl[0]
    assert "CREATE INDEX idx_adk_memory_event_id ON adk_memory(event_id)" in default_ddl

    configured_store = SpannerSyncADKMemoryStore(
        _mock_config({
            "vector_dimensions": 768,
            "vector_index_enabled": True,
            "vector_distance_type": "COSINE",
            "scann_tree_depth": 2,
            "scann_num_leaves": 1000,
            "enable_memory_graph": True,
        })
    )
    ddl = configured_store._memory_table_ddl()
    assert "embedding ARRAY<FLOAT32>(vector_length=>768)" in ddl[0]
    assert (
        "CREATE VECTOR INDEX idx_adk_memory_embedding ON adk_memory(embedding) "
        "STORING (app_name, scope, user_id, timestamp) "
        "WHERE embedding IS NOT NULL "
        "OPTIONS (distance_type = 'COSINE', tree_depth = 2, num_leaves = 1000)"
    ) in ddl
    assert (
        "CREATE OR REPLACE PROPERTY GRAPH adk_memory_graph "
        "NODE TABLES ("
        "adk_memory AS MemoryNode KEY (id) LABEL Memory "
        "PROPERTIES (id, session_id, app_name, user_id, scope, event_id, author, timestamp, content_text)"
        ")"
    ) in ddl

    drop_sql = configured_store._drop_memory_table_sql()
    assert drop_sql[0] == "DROP PROPERTY GRAPH adk_memory_graph"
    assert "DROP VECTOR INDEX idx_adk_memory_embedding" in drop_sql


def test_spanner_memory_search_entries_four_modes() -> None:
    """Verify search_entries supports FTS SCORE(), Vector-Only, Hybrid RRF, and Simple LIKE modes."""
    fts_store = SpannerSyncADKMemoryStore(_mock_config({"memory_use_fts": True}))

    with patch.object(type(fts_store), "_run_read", return_value=[]) as run_read:
        fts_store.search_entries("spanner graph", "app", "user", limit=5)

    fts_sql, fts_params, fts_types = run_read.call_args.args
    assert "SEARCH(content_tokens, @query)" in fts_sql
    assert "ORDER BY SCORE(content_tokens, @query) DESC, timestamp DESC" in fts_sql
    assert fts_params["query"] == "spanner graph"
    assert fts_types["query"] is param_types.STRING

    with patch.object(type(fts_store), "_run_read", return_value=[]) as run_read:
        fts_store.search_entries("spanner graph", "app", "user", limit=5, embedding=[0.1, 0.2])

    rrf_sql, rrf_params, rrf_types = run_read.call_args.args
    assert "WITH vector_matches AS" in rrf_sql
    assert "text_matches AS" in rrf_sql
    assert "COSINE_DISTANCE(embedding, @embedding)" in rrf_sql
    assert "SCORE(content_tokens, @query)" in rrf_sql
    assert "(COALESCE(1.0 / (60 + v.rank_vec), 0.0) + COALESCE(1.0 / (60 + t.rank_txt), 0.0)) AS rrf_score" in rrf_sql
    assert "ORDER BY rrf_score DESC, m.timestamp DESC" in rrf_sql
    assert rrf_params["embedding"] == [0.1, 0.2]
    assert rrf_params["candidate_limit"] == 50
    assert rrf_types["embedding"] == cast("Any", param_types).Array(param_types.FLOAT32)

    vec_store = SpannerSyncADKMemoryStore(_mock_config({"memory_use_fts": False}))
    with patch.object(type(vec_store), "_run_read", return_value=[]) as run_read:
        vec_store.search_entries("", "app", "user", limit=5, embedding=[0.3, 0.4])

    vec_sql, vec_params, vec_types = run_read.call_args.args
    assert "WHERE app_name = @app_name" in vec_sql
    assert "AND embedding IS NOT NULL" in vec_sql
    assert "ORDER BY COSINE_DISTANCE(embedding, @embedding) ASC, timestamp DESC" in vec_sql
    assert vec_params["embedding"] == [0.3, 0.4]
    assert vec_types["embedding"] == cast("Any", param_types).Array(param_types.FLOAT32)


def test_get_session_returns_none_when_spanner_session_table_missing() -> None:
    store = SpannerSyncADKStore(_mock_config())

    with patch.object(type(store), "_run_read", side_effect=_spanner_not_found("adk_session not found")):
        result = store.get_session("app", "user", "session")

    assert result is None


def test_list_sessions_returns_empty_when_spanner_session_table_missing() -> None:
    store = SpannerSyncADKStore(_mock_config())

    with patch.object(type(store), "_run_read", side_effect=_spanner_not_found("adk_session not found")):
        result = store.list_sessions("app", "user")

    assert result == []


def test_get_events_returns_empty_when_spanner_events_table_missing() -> None:
    store = SpannerSyncADKStore(_mock_config())

    with patch.object(type(store), "_run_read", side_effect=_spanner_not_found("adk_event not found")):
        result = store.get_events("app", "user", "session")

    assert result == []


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


@contextmanager
def _capture_session_list() -> "Iterator[list[tuple[str, dict[str, Any], dict[str, Any]]]]":
    calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def capture(sql: str, params: "dict[str, Any] | None" = None, types: "dict[str, Any] | None" = None) -> "list[Any]":
        calls.append((sql, dict(params or {}), dict(types or {})))
        return []

    with patch.object(SpannerSyncADKStore, "_run_read", side_effect=capture):
        yield calls


def test_spanner_list_sessions_binds_order_and_page() -> None:
    """Explicit ordering renders inline while page bounds bind as typed named parameters."""
    store = SpannerSyncADKStore(_mock_config())
    with _capture_session_list() as calls:
        store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params, types = calls[0]
    assert _normalized(sql).endswith("ORDER BY create_time ASC, id ASC LIMIT @limit OFFSET @offset")
    assert params["limit"] == 10
    assert params["offset"] == 20
    assert types["limit"] is param_types.INT64
    assert types["offset"] is param_types.INT64


def test_spanner_list_sessions_defaults_to_recent_first_without_a_page() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store = SpannerSyncADKStore(_mock_config())
    with _capture_session_list() as calls:
        store.list_sessions("app")

    sql, params, _ = calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC")
    assert set(params) == {"app_name"}


def test_spanner_list_sessions_composes_user_filter_with_a_page() -> None:
    """User filtering composes with the bound page values."""
    store = SpannerSyncADKStore(_mock_config())
    with _capture_session_list() as calls:
        store.list_sessions("app", "u1", limit=5)

    sql, params, _ = calls[0]
    assert "AND user_id = @user_id" in sql
    assert params == {"app_name": "app", "user_id": "u1", "limit": 5, "offset": 0}


def test_spanner_list_sessions_zero_limit_never_reads() -> None:
    """A zero limit short-circuits before any read is issued."""
    store = SpannerSyncADKStore(_mock_config())
    with _capture_session_list() as calls:
        assert store.list_sessions("app", limit=0) == []
    assert calls == []


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"order_by": "id"}, id="unknown-order-column"),
        pytest.param({"limit": -1}, id="negative-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"offset": 5}, id="unbounded-offset"),
    ],
)
def test_spanner_list_sessions_rejects_invalid_options(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a read is issued."""
    store = SpannerSyncADKStore(_mock_config())
    with _capture_session_list() as calls, pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert calls == []


async def test_async_adk_store_create_tables_with_async_generator_list_tables() -> None:
    """Verify SpannerAsyncADKStore.create_tables consumes async generator list_tables and awaits update_ddl().result()."""
    config = _mock_config({"expires_index_options": "locality_group = 'cold'"})
    database = MagicMock()

    async def _list_tables() -> Any:
        yield SimpleNamespace(table_id="adk_session")

    database.list_tables.side_effect = _list_tables
    op_mock = MagicMock()
    op_mock.result = AsyncMock(return_value=None)
    database.update_ddl = AsyncMock(return_value=op_mock)
    config.get_database = AsyncMock(return_value=database)

    store = SpannerAsyncADKStore(config)
    await store.create_tables()

    database.update_ddl.assert_awaited_once()
    ddl_statements = database.update_ddl.call_args.args[0]
    assert not any("CREATE TABLE adk_session" in stmt for stmt in ddl_statements)
    assert any("CREATE TABLE adk_event" in stmt for stmt in ddl_statements)
    op_mock.result.assert_awaited_once_with(timeout=300)


async def test_async_adk_store_create_get_list_delete_session() -> None:
    """Verify SpannerAsyncADKStore CRUD operations over async snapshots and transactions."""
    config = _mock_config()
    database = MagicMock()
    executed_writes: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    async def _run_in_txn(job: Any) -> Any:
        txn = MagicMock()

        async def _exec_update(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> int:
            executed_writes.append((sql, params, param_types))
            return 1

        async def _batch_update(stmts: list[tuple[str, dict[str, Any], dict[str, Any]]]) -> tuple[Any, list[int]]:
            executed_writes.extend(stmts)
            return SimpleNamespace(code=0, message=""), [1] * len(stmts)

        txn.execute_update = _exec_update
        txn.batch_update = _batch_update
        return await job(txn)

    database.run_in_transaction = _run_in_txn
    now = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)

    snapshot = MagicMock()

    async def _execute_sql(
        sql: str, params: dict[str, Any] | None = None, param_types: dict[str, Any] | None = None
    ) -> Any:
        del sql, params, param_types

        async def _rows() -> Any:
            yield ("s1", "app", "u1", '{"k":"v"}', now, now)

        return _rows()

    snapshot.execute_sql = _execute_sql
    snap_ctx = MagicMock()
    snap_ctx.__aenter__ = AsyncMock(return_value=snapshot)
    snap_ctx.__aexit__ = AsyncMock(return_value=None)
    database.snapshot.return_value = snap_ctx
    config.get_database = AsyncMock(return_value=database)

    store = SpannerAsyncADKStore(config)
    created = await store.create_session("s1", "app", "u1", {"k": "v"})
    assert created["id"] == "s1"
    assert len(executed_writes) == 1

    fetched = await store.get_session("app", "u1", "s1")
    assert fetched == {
        "id": "s1",
        "app_name": "app",
        "user_id": "u1",
        "state": {"k": "v"},
        "create_time": now,
        "update_time": now,
    }

    sessions = await store.list_sessions("app", "u1", limit=5)
    assert len(sessions) == 1
    assert sessions[0]["id"] == "s1"

    executed_writes.clear()
    await store.delete_session("app", "u1", "s1")
    assert len(executed_writes) == 2


async def test_async_adk_store_append_event_and_update_state_and_get_events() -> None:
    """Verify SpannerAsyncADKStore atomic append_event_and_update_state and get_events."""
    store = SpannerAsyncADKStore(_mock_config())
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    event: StoredEvent = {
        "id": "event-1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "session-1",
        "invocation_id": "inv-1",
        "timestamp": timestamp,
        "event_data": {"content": "hello"},
    }
    fake_record = {
        "id": "session-1",
        "app_name": "app",
        "user_id": "u1",
        "state": {"turn": 1},
        "create_time": timestamp,
        "update_time": timestamp,
    }

    with (
        patch.object(type(store), "_run_write", new_callable=AsyncMock) as run_write,
        patch.object(type(store), "_get_session", new_callable=AsyncMock, return_value=fake_record),
    ):
        returned = await store.append_event_and_update_state(event, "app", "u1", "session-1", {"turn": 1})

    assert returned == fake_record
    run_write.assert_awaited_once()
    event_sql, event_params, _ = run_write.call_args.args[0][0]
    assert "@timestamp" in event_sql
    assert event_params["timestamp"] is timestamp

    with patch.object(
        type(store),
        "_run_read",
        new_callable=AsyncMock,
        return_value=[("event-1", "session-1", "inv-1", timestamp, '{"content":"hello"}', "app", "u1")],
    ):
        events = await store.get_events("app", "u1", "session-1")
    assert len(events) == 1
    assert events[0]["event_data"] == {"content": "hello"}


async def test_async_adk_memory_store_create_drop_insert_search_and_delete() -> None:
    """Verify SpannerAsyncADKMemoryStore DDL, insert, search, and delete operations."""
    config = _mock_config()
    database = MagicMock()

    async def _list_tables() -> Any:
        if False:
            yield None

    database.list_tables.side_effect = _list_tables
    op_mock = MagicMock()
    op_mock.result = AsyncMock(return_value=None)
    database.update_ddl = AsyncMock(return_value=op_mock)

    executed_writes: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    async def _run_in_txn(job: Any) -> Any:
        txn = MagicMock()

        async def _exec_sql(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> Any:
            del sql, params, param_types

            async def _rows() -> Any:
                if False:
                    yield ("none",)

            return _rows()

        async def _exec_update(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> int:
            executed_writes.append((sql, params, param_types))
            return 1

        async def _batch_update(stmts: list[tuple[str, dict[str, Any], dict[str, Any]]]) -> tuple[Any, list[int]]:
            executed_writes.extend(stmts)
            return SimpleNamespace(code=0, message=""), [1] * len(stmts)

        txn.execute_sql = _exec_sql
        txn.execute_update = _exec_update
        txn.batch_update = _batch_update
        return await job(txn)

    database.run_in_transaction = _run_in_txn
    config.get_database = AsyncMock(return_value=database)

    store = SpannerAsyncADKMemoryStore(config)
    await store.create_tables()
    database.update_ddl.assert_awaited_once()

    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    entry: StoredMemory = {
        "id": "memory-1",
        "session_id": "session-1",
        "app_name": "app",
        "user_id": "user",
        "scope": "user",
        "event_id": "event-1",
        "author": "assistant",
        "timestamp": timestamp,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": {"source": "unit"},
        "inserted_at": timestamp,
        "embedding": [0.1, 0.2],
    }

    inserted = await store.insert_memory_entries([entry])
    assert inserted == 1
    assert len(executed_writes) == 1

    with patch.object(
        type(store),
        "_run_read",
        new_callable=AsyncMock,
        return_value=[
            (
                "memory-1",
                "session-1",
                "app",
                "user",
                "user",
                "event-1",
                "assistant",
                timestamp,
                '{"text":"hello"}',
                "hello",
                '{"source":"unit"}',
                timestamp,
                [0.1, 0.2],
            )
        ],
    ):
        results = await store.search_entries("hello", "app", "user", embedding=[0.1, 0.2])
    assert len(results) == 1
    assert results[0]["content_json"] == {"text": "hello"}
    assert results[0]["embedding"] == [0.1, 0.2]

    with patch.object(type(store), "_execute_update", new_callable=AsyncMock, return_value=2) as exec_update:
        deleted = await store.delete_entries_by_session("session-1")
        assert deleted == 2
        deleted_old = await store.delete_entries_older_than(7, app_name="app")
        assert deleted_old == 2
        assert exec_update.await_count == 2
