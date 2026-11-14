"""Unit tests for Spanner ADK store behavior."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from inspect import isawaitable
from types import SimpleNamespace
from typing import Any, NotRequired, cast, get_args, get_origin
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.api_core.exceptions import NotFound
from google.cloud.spanner_v1 import param_types

from sqlspec.adapters.spanner.adk import (
    SpannerADKConfig,
    SpannerADKRetentionConfig,
    SpannerAsyncADKMemoryStore,
    SpannerAsyncADKStore,
    SpannerSyncADKMemoryStore,
    SpannerSyncADKStore,
)
from sqlspec.adapters.spanner.adk.store import _spanner_drop_statement_table
from sqlspec.adapters.spanner.config import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.config import ADKConfig, ExtensionConfigs
from sqlspec.exceptions import MigrationError
from sqlspec.extensions.adk import StoredEvent, StoredMemory
from sqlspec.extensions.adk.migrations.reconcile import reconcile as adk_reconcile
from sqlspec.migrations.context import MigrationContext
from sqlspec.migrations.schema import ensure_schema_sync


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def _spanner_not_found(message: str) -> NotFound:
    return cast("NotFound", cast("Any", NotFound)(message))


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
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
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
        store._insert_event(event)

    statements = run_write.call_args.args[0]
    sql, params, _types = statements[0]
    assert "@timestamp" in sql
    assert "PENDING_COMMIT_TIMESTAMP()" not in sql
    assert params["id"] == "event-1"
    assert params["timestamp"] is timestamp


def test_append_event_and_update_state_preserves_event_record_timestamp() -> None:
    """Atomic append updates session state first with commit time and inserts event with ADK timestamp."""
    config = _mock_config()
    database = config.get_database.return_value
    executed_writes: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def _run_in_txn(job: Any) -> Any:
        txn = MagicMock()

        def _exec_update(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> int:
            executed_writes.append((sql, params, param_types))
            return 1

        def _batch_update(stmts: list[tuple[str, dict[str, Any], dict[str, Any]]]) -> tuple[Any, list[int]]:
            executed_writes.extend(stmts)
            return SimpleNamespace(code=0, message=""), [1] * len(stmts)

        txn.execute_update = _exec_update
        txn.batch_update = _batch_update
        return job(txn)

    database.run_in_transaction.side_effect = _run_in_txn
    store = SpannerSyncADKStore(config)
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
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

    with patch.object(type(store), "_get_session", return_value=fake_record):
        returned = store.append_event_and_update_state(event, "app", "u1", "session-1", {"turn": 1})

    assert len(executed_writes) == 2
    update_sql, _state_params, _state_types = executed_writes[0]
    event_sql, event_params, _event_types = executed_writes[1]
    assert "PENDING_COMMIT_TIMESTAMP()" in update_sql
    assert "@timestamp" in event_sql
    assert "PENDING_COMMIT_TIMESTAMP()" not in event_sql
    assert event_params["id"] == "event-1"
    assert event_params["timestamp"] is timestamp
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

        def _exec_sql(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> list[tuple[str, str, str]]:
            executed_queries.append((sql, params, param_types))
            return [("app", "user", "event-existing")]

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
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
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
    assert "SELECT app_name, user_id, event_id FROM adk_memory WHERE event_id IN UNNEST(@event_ids)" in select_sql
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
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)

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
def _capture_session_list(config: Any) -> "Iterator[list[tuple[str, dict[str, Any], dict[str, Any]]]]":
    calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def capture(
        sql: str, params: "dict[str, Any] | None" = None, param_types: "dict[str, Any] | None" = None
    ) -> "list[Any]":
        calls.append((sql, dict(params or {}), dict(param_types or {})))
        return []

    snapshot = MagicMock()
    snapshot.execute_sql.side_effect = capture
    config.provide_connection.return_value.__enter__.return_value = snapshot
    yield calls


def test_spanner_list_sessions_binds_order_and_page() -> None:
    """Explicit ordering renders inline while page bounds bind as typed named parameters."""
    store = SpannerSyncADKStore(_mock_config())
    with _capture_session_list(store._config) as calls:
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
    with _capture_session_list(store._config) as calls:
        store.list_sessions("app")

    sql, params, _ = calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC")
    assert set(params) == {"app_name"}


def test_spanner_list_sessions_composes_user_filter_with_a_page() -> None:
    """User filtering composes with the bound page values."""
    store = SpannerSyncADKStore(_mock_config())
    with _capture_session_list(store._config) as calls:
        store.list_sessions("app", "u1", limit=5)

    sql, params, _ = calls[0]
    assert "AND user_id = @user_id" in sql
    assert params == {"app_name": "app", "user_id": "u1", "limit": 5, "offset": 0}


def test_spanner_list_sessions_zero_limit_never_reads() -> None:
    """A zero limit short-circuits before any read is issued."""
    store = SpannerSyncADKStore(_mock_config())
    with _capture_session_list(store._config) as calls:
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
    with _capture_session_list(store._config) as calls, pytest.raises(ValueError):
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
    now = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)

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
    config.provide_connection.return_value = snap_ctx
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
    config.get_database = AsyncMock(return_value=database)
    store = SpannerAsyncADKStore(config)
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
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

    with patch.object(type(store), "_get_session", new_callable=AsyncMock, return_value=fake_record):
        returned = await store.append_event_and_update_state(event, "app", "u1", "session-1", {"turn": 1})

    assert returned == fake_record
    assert len(executed_writes) == 2
    update_sql, _, _ = executed_writes[0]
    event_sql, event_params, _ = executed_writes[1]
    assert "UPDATE adk_session" in update_sql
    assert "@timestamp" in event_sql
    assert event_params["timestamp"] is timestamp

    with patch.object(
        type(store),
        "_run_read",
        new_callable=AsyncMock,
        return_value=[("event-1", "session-1", "inv-1", timestamp, '{"content":"hello"}', "app", "u1")],
    ) as run_read:
        events = await store.get_events("app", "u1", "session-1")
    assert len(events) == 1
    assert events[0]["event_data"] == {"content": "hello"}
    get_events_sql = _normalized(run_read.call_args.args[0])
    assert (
        "JOIN adk_session s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id"
        in get_events_sql
    )
    assert "WHERE s.app_name = @app_name AND s.user_id = @user_id AND s.id = @session_id" in get_events_sql


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

    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
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


def _spanner_sync_config(adk_settings: dict[str, Any]) -> SpannerSyncConfig:
    ext: ExtensionConfigs = {"adk": adk_settings}
    return SpannerSyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"}, extension_config=ext
    )


def _spanner_async_config(adk_settings: dict[str, Any]) -> SpannerAsyncConfig:
    ext: ExtensionConfigs = {"adk": adk_settings}
    return SpannerAsyncConfig(
        connection_config={"project": "p", "instance_id": "i", "database_id": "d"}, extension_config=ext
    )


def test_spanner_adk_reconcile_memory_target_parses_vector_length_and_fts() -> None:
    """Memory SchemaTarget parses vector_length=>768 and FTS DDL while preserving verbatim create_statement."""
    config = _spanner_sync_config({
        "migration_mode": "reconcile",
        "memory_use_fts": True,
        "vector_dimensions": 768,
        "shard_count": 4,
    })
    store = SpannerSyncADKMemoryStore(config)
    targets = store.get_schema_targets_sync(MagicMock())

    assert len(targets) == 1
    target = targets[0]
    column_names = [col.name for col in target.create_table.columns]
    assert "embedding" in column_names
    assert "content_tokens" in column_names
    assert "shard_id" in column_names
    assert isinstance(target.create_statement, str)
    assert "ARRAY<FLOAT32>(vector_length=>768)" in target.create_statement
    assert "TOKENIZE_FULLTEXT(content_text)" in target.create_statement
    assert "CREATE INDEX" not in target.create_statement
    assert "shard_id" in target.constraint_columns
    assert "content_tokens" in target.unsafe_add_columns
    assert "shard_id" in target.unsafe_add_columns

    disabled_store = SpannerSyncADKMemoryStore(_spanner_sync_config({"enable_memory": False}))
    disabled_driver = MagicMock()
    assert disabled_store.get_schema_targets_sync(disabled_driver) == []
    disabled_store.reconcile_companion_objects_sync(disabled_driver)
    assert disabled_driver.mock_calls == []


def test_spanner_adk_reconcile_refuses_missing_generated_columns() -> None:
    """Missing generated content_tokens or shard_id columns fail strict additive reconciliation with remedy."""
    config = _spanner_sync_config({
        "migration_mode": "reconcile",
        "memory_table": "m",
        "memory_use_fts": True,
        "vector_dimensions": 768,
        "shard_count": 4,
    })
    store = SpannerSyncADKMemoryStore(config)
    targets = store.get_schema_targets_sync(MagicMock())

    base_columns = [
        {"column_name": "id", "data_type": "STRING(128)", "is_nullable": "NO"},
        {"column_name": "session_id", "data_type": "STRING(128)", "is_nullable": "NO"},
        {"column_name": "app_name", "data_type": "STRING(128)", "is_nullable": "NO"},
        {"column_name": "user_id", "data_type": "STRING(128)", "is_nullable": "NO"},
        {"column_name": "scope", "data_type": "STRING(16)", "is_nullable": "NO"},
        {"column_name": "event_id", "data_type": "STRING(128)", "is_nullable": "NO"},
        {"column_name": "author", "data_type": "STRING(256)", "is_nullable": "YES"},
        {"column_name": "timestamp", "data_type": "TIMESTAMP", "is_nullable": "NO"},
        {"column_name": "content_json", "data_type": "JSON", "is_nullable": "NO"},
        {"column_name": "content_text", "data_type": "STRING(MAX)", "is_nullable": "NO"},
        {"column_name": "embedding", "data_type": "ARRAY<FLOAT32>(vector_length=>768)", "is_nullable": "YES"},
        {"column_name": "metadata_json", "data_type": "JSON", "is_nullable": "YES"},
        {"column_name": "inserted_at", "data_type": "TIMESTAMP", "is_nullable": "NO"},
        {"column_name": "shard_id", "data_type": "INT64", "is_nullable": "YES"},
    ]
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "m"}]
    driver.data_dictionary.get_columns.return_value = base_columns

    with pytest.raises(MigrationError, match="content_tokens") as exc_info:
        ensure_schema_sync(driver, targets, manage_schema=True, create_schema=True, commit=False, strict=True)
    assert 'migration_mode="versioned"' in str(exc_info.value)

    missing_shard_columns = [
        *(row for row in base_columns if row["column_name"] != "shard_id"),
        {"column_name": "content_tokens", "data_type": "TOKENLIST", "is_nullable": "YES"},
    ]
    driver.data_dictionary.get_columns.return_value = missing_shard_columns
    with pytest.raises(MigrationError, match="shard_id") as shard_exc:
        ensure_schema_sync(driver, targets, manage_schema=True, create_schema=True, commit=False, strict=True)
    assert 'migration_mode="versioned"' in str(shard_exc.value)


def test_spanner_adk_reconcile_memory_companions_search_vector_and_graph() -> None:
    """Memory companion reconcile repairs missing search/vector indexes and runs property graph DDL."""
    store = SpannerSyncADKMemoryStore(
        _spanner_sync_config({
            "migration_mode": "reconcile",
            "memory_table": "m",
            "memory_use_fts": True,
            "vector_dimensions": 768,
            "vector_index_enabled": True,
            "enable_memory_graph": True,
        })
    )
    driver = MagicMock()
    driver.data_dictionary.get_indexes.return_value = [
        {"index_name": "idx_m_app_scope_user_time", "table_name": "m"},
        {"index_name": "idx_m_scope", "table_name": "m"},
        {"index_name": "idx_m_session", "table_name": "m"},
        {"index_name": "idx_m_event_id", "table_name": "m"},
        {"index_name": "idx_m_embedding", "table_name": "m"},
    ]

    store.reconcile_companion_objects_sync(driver)

    executed = [call.args[0] for call in driver.execute_script.call_args_list]
    assert len(executed) == 2
    assert executed[0] == "CREATE SEARCH INDEX idx_m_fts ON m(content_tokens)"
    assert executed[1].startswith("CREATE OR REPLACE PROPERTY GRAPH m_graph ")
    driver.commit.assert_not_called()

    no_graph_store = SpannerSyncADKMemoryStore(
        _spanner_sync_config({
            "migration_mode": "reconcile",
            "memory_table": "m",
            "memory_use_fts": True,
            "vector_dimensions": 768,
            "vector_index_enabled": True,
            "enable_memory_graph": False,
        })
    )
    upper_driver = MagicMock()
    upper_driver.data_dictionary.get_indexes.return_value = [
        {"index_name": "IDX_M_APP_SCOPE_USER_TIME", "table_name": "M"},
        {"index_name": "IDX_M_SCOPE", "table_name": "M"},
        {"index_name": "IDX_M_SESSION", "table_name": "M"},
        {"index_name": "IDX_M_EVENT_ID", "table_name": "M"},
        {"index_name": "IDX_M_FTS", "table_name": "M"},
        {"index_name": "IDX_M_EMBEDDING", "table_name": "M"},
    ]

    no_graph_store.reconcile_companion_objects_sync(upper_driver)

    upper_driver.execute_script.assert_not_called()


@pytest.mark.anyio
async def test_spanner_adk_reconcile_sync_and_async_parity() -> None:
    """Sync and async Spanner ADK stores emit identical targets and DDL order without committing."""
    settings: dict[str, Any] = {
        "migration_mode": "reconcile",
        "enable_memory": True,
        "memory_use_fts": True,
        "vector_dimensions": 768,
        "vector_index_enabled": True,
        "enable_memory_graph": True,
        "shard_count": 4,
    }
    sync_cfg = _spanner_sync_config(settings)
    async_cfg = _spanner_async_config(settings)

    sync_session_store = SpannerSyncADKStore(sync_cfg)
    async_session_store = SpannerAsyncADKStore(async_cfg)
    sync_memory_store = SpannerSyncADKMemoryStore(sync_cfg)
    async_memory_store = SpannerAsyncADKMemoryStore(async_cfg)

    sync_targets = [
        *sync_session_store.get_schema_targets_sync(MagicMock()),
        *sync_memory_store.get_schema_targets_sync(MagicMock()),
    ]
    async_targets = [
        *(await async_session_store.get_schema_targets_async(MagicMock())),
        *(await async_memory_store.get_schema_targets_async(MagicMock())),
    ]
    assert [t.table_name for t in sync_targets] == [
        "adk_session",
        "adk_event",
        "adk_app_state",
        "adk_user_state",
        "adk_internal_metadata",
        "adk_memory",
    ]
    assert [(t.table_name, t.create_statement, t.constraint_columns, t.unsafe_add_columns) for t in sync_targets] == [
        (t.table_name, t.create_statement, t.constraint_columns, t.unsafe_add_columns) for t in async_targets
    ]

    sync_driver = MagicMock()
    sync_driver.data_dictionary.get_tables.return_value = []
    sync_driver.data_dictionary.get_indexes.return_value = []

    async_driver = MagicMock()
    async_driver.data_dictionary.get_tables = AsyncMock(return_value=[])
    async_driver.data_dictionary.get_indexes = AsyncMock(return_value=[])
    async_driver.execute_script = AsyncMock()
    async_driver.commit = AsyncMock()

    assert adk_reconcile(MigrationContext(config=sync_cfg, dialect="spanner", driver=sync_driver)) is None
    coro = adk_reconcile(MigrationContext(config=async_cfg, dialect="spanner", driver=async_driver))
    assert coro is not None
    await coro

    sync_scripts = [call.args[0] for call in sync_driver.execute_script.call_args_list]
    async_scripts = [call.args[0] for call in async_driver.execute_script.call_args_list]
    assert sync_scripts == async_scripts
    assert len(sync_scripts) == 6 + 2 + 7
    assert all("CREATE TABLE" in sql for sql in sync_scripts[:6])
    assert all("CREATE " in sql and "CREATE TABLE" not in sql for sql in sync_scripts[6:])
    sync_driver.commit.assert_not_called()
    async_driver.commit.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize(
    ("renew_for", "expects_renew"),
    [
        pytest.param(None, False, id="none"),
        pytest.param(0, False, id="zero-int"),
        pytest.param(-1, False, id="negative-int"),
        pytest.param(timedelta(0), False, id="zero-timedelta"),
        pytest.param(timedelta(milliseconds=250), True, id="subsecond-timedelta"),
        pytest.param(30, True, id="positive-int"),
    ],
)
async def test_spanner_get_session_renews_only_for_positive_duration(
    is_async: bool, renew_for: int | timedelta | None, expects_renew: bool
) -> None:
    """get_session writes a renewal touch only when renew_for is strictly positive."""
    now = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    row = ("s1", "app", "u1", '{"k":1}', now, now)
    if is_async:
        store: Any = SpannerAsyncADKStore(_mock_config())
        with (
            patch.object(type(store), "_run_write", new_callable=AsyncMock) as run_write,
            patch.object(type(store), "_run_read", new_callable=AsyncMock, return_value=[row]),
        ):
            result = await store.get_session("app", "u1", "s1", renew_for=renew_for)
        assert result is not None
        assert (run_write.await_count == 1) is expects_renew
    else:
        store = SpannerSyncADKStore(_mock_config())
        with (
            patch.object(type(store), "_run_write") as run_write,
            patch.object(type(store), "_run_read", return_value=[row]),
        ):
            result = store.get_session("app", "u1", "s1", renew_for=renew_for)
        assert result is not None
        assert (run_write.call_count == 1) is expects_renew


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize("owner_exists", [True, False])
async def test_spanner_append_event_checks_session_ownership_in_transaction(is_async: bool, owner_exists: bool) -> None:
    """append_event verifies session ownership and inserts inside one run_in_transaction job."""
    config = _mock_config({"shard_count": 4})
    executed_queries: list[tuple[str, dict[str, Any]]] = []
    executed_updates: list[tuple[str, dict[str, Any]]] = []
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    event: StoredEvent = {
        "id": "ev-1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "s1",
        "invocation_id": "inv-1",
        "timestamp": timestamp,
        "event_data": {"x": 1},
    }

    if is_async:
        database = MagicMock()

        async def _run_async_txn(job: Any) -> Any:
            txn = MagicMock()

            async def _exec_sql(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> Any:
                del param_types
                executed_queries.append((_normalized(sql), params))

                async def _rows() -> Any:
                    if owner_exists:
                        yield (1,)

                return _rows()

            async def _exec_update(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> int:
                del param_types
                executed_updates.append((_normalized(sql), params))
                return 1

            txn.execute_sql = _exec_sql
            txn.execute_update = _exec_update
            return await job(txn)

        database.run_in_transaction = _run_async_txn
        config.get_database = AsyncMock(return_value=database)
        store: Any = SpannerAsyncADKStore(config)
    else:
        database = config.get_database.return_value

        def _run_sync_txn(job: Any) -> Any:
            txn = MagicMock()

            def _exec_sql(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> list[tuple[int]]:
                del param_types
                executed_queries.append((_normalized(sql), params))
                return [(1,)] if owner_exists else []

            def _exec_update(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> int:
                del param_types
                executed_updates.append((_normalized(sql), params))
                return 1

            txn.execute_sql = _exec_sql
            txn.execute_update = _exec_update
            return job(txn)

        database.run_in_transaction.side_effect = _run_sync_txn
        store = SpannerSyncADKStore(config)

    if owner_exists:
        maybe = store.append_event(event)
        if isawaitable(maybe):
            await maybe
        assert len(executed_queries) == 1
        assert (
            "SELECT 1 FROM adk_session WHERE id = @id AND app_name = @app_name AND user_id = @user_id "
            "AND shard_id = MOD(FARM_FINGERPRINT(@id), 4) LIMIT 1"
        ) == executed_queries[0][0]
        assert len(executed_updates) == 1
        assert executed_updates[0][0].startswith("INSERT INTO adk_event")
    else:
        with pytest.raises(ValueError, match="Session s1 not found for app='app', user='u1'"):
            maybe = store.append_event(event)
            if isawaitable(maybe):
                await maybe
        assert len(executed_queries) == 1
        assert executed_updates == []


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_spanner_append_event_and_update_state_rejects_mismatch_and_missing_session(is_async: bool) -> None:
    """append_event_and_update_state rejects mismatched records without DB work and aborts before insert when missing."""
    config = _mock_config()
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    bad_event: StoredEvent = {
        "id": "ev-1",
        "app_name": "other-app",
        "user_id": "u1",
        "session_id": "s1",
        "invocation_id": "inv-1",
        "timestamp": timestamp,
        "event_data": {"x": 1},
    }
    good_event: StoredEvent = {**bad_event, "app_name": "app"}
    executed_updates: list[str] = []

    if is_async:
        database = MagicMock()

        async def _run_async_txn(job: Any) -> Any:
            txn = MagicMock()

            async def _exec_update(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> int:
                del params, param_types
                executed_updates.append(_normalized(sql))
                return 0

            txn.execute_update = _exec_update
            return await job(txn)

        database.run_in_transaction = _run_async_txn
        config.get_database = AsyncMock(return_value=database)
        store: Any = SpannerAsyncADKStore(config)
    else:
        database = config.get_database.return_value

        def _run_sync_txn(job: Any) -> Any:
            txn = MagicMock()

            def _exec_update(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> int:
                del params, param_types
                executed_updates.append(_normalized(sql))
                return 0

            txn.execute_update = _exec_update
            return job(txn)

        database.run_in_transaction.side_effect = _run_sync_txn
        store = SpannerSyncADKStore(config)

    with pytest.raises(ValueError, match="does not match"):
        maybe = store.append_event_and_update_state(bad_event, "app", "u1", "s1", {"turn": 1})
        if isawaitable(maybe):
            await maybe
    assert executed_updates == []

    with pytest.raises(ValueError, match="Session s1 not found during append_event_and_update_state"):
        maybe = store.append_event_and_update_state(good_event, "app", "u1", "s1", {"turn": 1})
        if isawaitable(maybe):
            await maybe
    assert len(executed_updates) == 1
    assert executed_updates[0].startswith("UPDATE adk_session")


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
async def test_spanner_memory_deduplicates_by_composite_owner_tuple(is_async: bool) -> None:
    """Memory batch deduplication and lookup compare (app_name, user_id, event_id) tuples."""
    config = _mock_config()
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    entry_a: StoredMemory = {
        "id": "m1",
        "session_id": "s1",
        "app_name": "app-a",
        "user_id": "u1",
        "scope": "user",
        "event_id": "shared-event",
        "author": "user",
        "timestamp": timestamp,
        "content_json": {"text": "a"},
        "content_text": "a",
        "metadata_json": None,
        "inserted_at": timestamp,
        "embedding": None,
    }
    entry_a_dup: StoredMemory = {**entry_a, "id": "m2"}
    entry_b: StoredMemory = {**entry_a, "id": "m3", "app_name": "app-b", "user_id": "u2"}
    executed_writes: list[dict[str, Any]] = []

    if is_async:
        database = MagicMock()

        async def _run_async_txn(job: Any) -> Any:
            txn = MagicMock()

            async def _exec_sql(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> Any:
                del sql, params, param_types

                async def _rows() -> Any:
                    yield ("app-a", "u1", "shared-event")

                return _rows()

            async def _exec_update(sql: str, params: dict[str, Any], param_types: dict[str, Any]) -> int:
                del sql, param_types
                executed_writes.append(params)
                return 1

            txn.execute_sql = _exec_sql
            txn.execute_update = _exec_update
            return await job(txn)

        database.run_in_transaction = _run_async_txn
        config.get_database = AsyncMock(return_value=database)
        store: Any = SpannerAsyncADKMemoryStore(config)
    else:
        database = config.get_database.return_value

        def _run_sync_txn(job: Any) -> Any:
            txn = MagicMock()
            txn.execute_sql = lambda *_a, **_k: [("app-a", "u1", "shared-event")]
            txn.execute_update = lambda _sql, params=None, param_types=None: executed_writes.append(params or {}) or 1
            return job(txn)

        database.run_in_transaction.side_effect = _run_sync_txn
        store = SpannerSyncADKMemoryStore(config)

    deduped = store._deduplicate_entries([entry_a, entry_a_dup, entry_b])
    assert [item["id"] for item in deduped] == ["m1", "m3"]

    inserted = store.insert_memory_entries([entry_a, entry_a_dup, entry_b])
    if isawaitable(inserted):
        inserted = await inserted
    assert inserted == 1
    assert len(executed_writes) == 1
    assert (executed_writes[0]["app_name"], executed_writes[0]["user_id"]) == ("app-b", "u2")


@pytest.mark.anyio
@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize(
    ("app_name", "user_id", "expected_where", "expected_params"),
    [
        pytest.param(None, None, "session_id = @session_id", {"session_id": "s1"}, id="session-only"),
        pytest.param(
            "app",
            None,
            "session_id = @session_id AND app_name = @app_name",
            {"session_id": "s1", "app_name": "app"},
            id="with-app",
        ),
        pytest.param(
            None,
            "u1",
            "session_id = @session_id AND user_id = @user_id",
            {"session_id": "s1", "user_id": "u1"},
            id="with-user",
        ),
        pytest.param(
            "app",
            "u1",
            "session_id = @session_id AND app_name = @app_name AND user_id = @user_id",
            {"session_id": "s1", "app_name": "app", "user_id": "u1"},
            id="with-app-and-user",
        ),
    ],
)
async def test_spanner_memory_delete_entries_by_session_scopes_owner_predicates(
    is_async: bool, app_name: str | None, user_id: str | None, expected_where: str, expected_params: dict[str, Any]
) -> None:
    """delete_entries_by_session appends @app_name and @user_id predicates when provided."""
    if is_async:
        store: Any = SpannerAsyncADKMemoryStore(_mock_config())
        with patch.object(type(store), "_execute_update", new_callable=AsyncMock, return_value=1) as exec_update:
            deleted = await store.delete_entries_by_session("s1", app_name=app_name, user_id=user_id)
        sql, params, _ = exec_update.call_args.args
    else:
        store = SpannerSyncADKMemoryStore(_mock_config())
        with patch.object(type(store), "_execute_update", return_value=1) as exec_update:
            deleted = store.delete_entries_by_session("s1", app_name=app_name, user_id=user_id)
        sql, params, _ = exec_update.call_args.args

    assert deleted == 1
    assert sql == f"DELETE FROM adk_memory WHERE {expected_where}"
    assert params == expected_params
