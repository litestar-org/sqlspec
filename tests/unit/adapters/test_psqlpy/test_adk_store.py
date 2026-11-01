"""Unit tests for psqlpy ADK store extension configuration."""

from datetime import UTC, datetime
from typing import Any, NotRequired, Self, cast, get_args, get_origin
from unittest.mock import MagicMock

import pytest
from psqlpy.exceptions import ConnectionExecuteError, DatabaseError

from sqlspec.adapters.psqlpy.adk import PsqlpyADKConfig, PsqlpyADKMemoryStore, PsqlpyADKStore
from sqlspec.adapters.psqlpy.adk.store import _is_psqlpy_database_error, _is_table_missing_error, decode_postgres_vector
from sqlspec.config import ADKConfig


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def test_psqlpy_adk_config_types_adapter_local_optimizations() -> None:
    """Psqlpy ADK optimization switches live on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", PsqlpyADKConfig).__optional_keys__
    assert cast("Any", PsqlpyADKConfig).__optional_keys__ - cast("Any", ADKConfig).__optional_keys__ == {
        "autovacuum_analyze_scale_factor",
        "autovacuum_vacuum_scale_factor",
        "enable_bm25",
        "enable_event_generated_columns",
        "enable_covering_indexes",
        "fillfactor",
        "scann_num_leaves",
        "scann_quantizer",
        "vector_dimensions",
        "vector_index_type",
    }

    for feature_name in ("enable_event_generated_columns", "enable_covering_indexes"):
        annotation = cast("Any", PsqlpyADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (bool,)


async def test_psqlpy_adk_events_table_uses_plain_schema_by_default() -> None:
    """Psqlpy ADK optimization DDL stays opt-in through extension config."""

    store = PsqlpyADKStore(_mock_config())

    sql = await store._events_table_ddl()

    assert "author_gc" not in sql
    assert "node_path_gc" not in sql
    assert "INCLUDE (invocation_id)" not in sql


async def test_psqlpy_adk_events_table_applies_adapter_local_extension_config() -> None:
    """Psqlpy ADK extension settings enable PostgreSQL-specific event DDL."""

    store = PsqlpyADKStore(_mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True}))

    sql = await store._events_table_ddl()

    assert "author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED" in sql
    assert "node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED" in sql
    assert "idx_adk_event_author_gc" in sql
    assert "idx_adk_event_node_path_gc" in sql
    assert "INCLUDE (invocation_id)" in sql


class _RecordingConnection:
    """Captures the SQL and bound parameters a session listing issues."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, sql: str, params: "list[Any]") -> None:
        self.calls.append((sql, tuple(params)))
        return

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _session_store_with_connection() -> "tuple[PsqlpyADKStore, _RecordingConnection]":
    conn = _RecordingConnection()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    return PsqlpyADKStore(config), conn


async def test_psqlpy_list_sessions_binds_order_and_page() -> None:
    """Explicit ordering renders inline while page bounds bind as numbered parameters."""
    store, conn = _session_store_with_connection()

    await store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = conn.calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = $1 AND user_id = $2 "
        "ORDER BY create_time ASC, id ASC LIMIT $3 OFFSET $4"
    )
    assert params == ("app", "u1", 10, 20)


async def test_psqlpy_list_sessions_defaults_to_recent_first_without_a_page() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, conn = _session_store_with_connection()

    await store.list_sessions("app")

    sql, params = conn.calls[0]
    assert _normalized(sql).endswith("WHERE app_name = $1 ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


async def test_psqlpy_list_sessions_numbers_page_placeholders_without_a_user_filter() -> None:
    """Page placeholders follow the scope parameters that are actually present."""
    store, conn = _session_store_with_connection()

    await store.list_sessions("app", limit=5)

    sql, params = conn.calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC LIMIT $2 OFFSET $3")
    assert params == ("app", 5, 0)


async def test_psqlpy_list_sessions_zero_limit_never_opens_a_connection() -> None:
    """A zero limit short-circuits before any database work."""
    store, conn = _session_store_with_connection()

    assert await store.list_sessions("app", limit=0) == []
    assert conn.calls == []


@pytest.mark.parametrize(
    "options",
    [
        pytest.param({"order_by": "id"}, id="unknown-order-column"),
        pytest.param({"limit": -1}, id="negative-limit"),
        pytest.param({"limit": True}, id="boolean-limit"),
        pytest.param({"offset": 5}, id="unbounded-offset"),
    ],
)
async def test_psqlpy_list_sessions_rejects_invalid_options_before_connecting(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, conn = _session_store_with_connection()

    with pytest.raises(ValueError):
        await store.list_sessions("app", **options)

    assert conn.calls == []


@pytest.mark.parametrize(
    "message, missing",
    [("relation sessions does not exist", True), ('column "foo" does not exist', False), ("syntax error", False)],
)
def test_psqlpy_store_classifies_native_database_errors(message: str, missing: bool) -> None:
    database_error = DatabaseError(message)
    assert _is_psqlpy_database_error(database_error) is True
    assert _is_table_missing_error(database_error) is missing
    assert _is_table_missing_error(ConnectionExecuteError(message)) is missing
    assert _is_psqlpy_database_error(ValueError(message)) is False
    assert _is_table_missing_error(ValueError(message)) is False


async def test_psqlpy_memory_store_disabled_guards_raise_runtime_error() -> None:
    """Disabled PsqlpyADKMemoryStore should raise RuntimeError from delete_entries_by_session and delete_entries_older_than."""
    store = PsqlpyADKMemoryStore(_mock_config({"enable_memory": False}))

    with pytest.raises(RuntimeError, match="Memory store is disabled"):
        await store.delete_entries_by_session("s1")

    with pytest.raises(RuntimeError, match="Memory store is disabled"):
        await store.delete_entries_older_than(7)


async def test_psqlpy_memory_store_vector_and_bm25_ddl_parity() -> None:
    """PsqlpyADKMemoryStore should emit vector embedding column and BM25/vector index DDL when configured."""
    store = PsqlpyADKMemoryStore(
        _mock_config({"vector_dimensions": 768, "vector_index_type": "hnsw", "enable_bm25": True})
    )

    table_ddl = await store._memory_table_ddl()

    assert "embedding VECTOR(768)" in table_ddl
    assert "USING hnsw (embedding vector_cosine_ops)" in table_ddl
    assert "USING bm25 (content_text)" in table_ddl


class _FakePgVector:
    def __init__(self, values: list[float]) -> None:
        self._values = values

    def to_list(self) -> list[float]:
        return list(self._values)


class _FakeMemoryFetchResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def result(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeMemoryConnection:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = rows or []
        self.fetch_calls: list[tuple[str, list[Any]]] = []
        self.execute_calls: list[tuple[str, list[Any]]] = []

    async def fetch(self, sql: str, params: list[Any]) -> _FakeMemoryFetchResult:
        self.fetch_calls.append((sql, list(params)))
        return _FakeMemoryFetchResult(self._rows)

    async def execute(self, sql: str, params: list[Any]) -> str:
        self.execute_calls.append((sql, list(params)))
        return "INSERT 0 1"

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


def _sample_memory_row(embedding: Any) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
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
        "embedding": embedding,
    }


@pytest.mark.parametrize(
    ("raw_embedding", "expected"),
    [
        (None, None),
        ("[0.25, 0.5, 0.75]", [0.25, 0.5, 0.75]),
        (b"[0.25,0.5,0.75]", [0.25, 0.5, 0.75]),
        (_FakePgVector([0.25, 0.5, 0.75]), [0.25, 0.5, 0.75]),
        ((0.25, 0.5, 0.75), [0.25, 0.5, 0.75]),
    ],
)
def test_psqlpy_memory_store_decodes_vector_text_and_pgvector(raw_embedding: Any, expected: list[float] | None) -> None:
    """PsqlpyADKMemoryStore._decode_row normalizes text, bytes, PgVector, and sequence embeddings."""
    decoded = PsqlpyADKMemoryStore._decode_row(_sample_memory_row(raw_embedding))
    assert decoded["embedding"] == expected
    assert decode_postgres_vector(raw_embedding) == expected


async def test_psqlpy_memory_store_fts_and_simple_select_13_columns() -> None:
    """FTS and simple search paths project and decode the 13th embedding column."""
    conn = _FakeMemoryConnection([_sample_memory_row("[0.1, 0.2, 0.3]")])
    config = _mock_config({"use_fts": True})
    config.provide_connection = lambda *_a, **_k: conn
    store = PsqlpyADKMemoryStore(config)

    fts_results = await store._search_entries_fts("hello", "app", "user-1", 5)
    simple_results = await store._search_entries_simple("hello", "app", "user-1", 5)

    assert "embedding" in conn.fetch_calls[0][0]
    assert "embedding" in conn.fetch_calls[1][0]
    assert fts_results[0]["embedding"] == [0.1, 0.2, 0.3]
    assert simple_results[0]["embedding"] == [0.1, 0.2, 0.3]


async def test_psqlpy_memory_store_normalizes_tuple_embeddings_on_insert_and_search() -> None:
    """Tuple embeddings are normalized to list[float] for psqlpy parameter binding."""
    conn = _FakeMemoryConnection([_sample_memory_row(_FakePgVector([0.1, 0.2]))])
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    store = PsqlpyADKMemoryStore(config)
    entry = _sample_memory_row((0.1, 0.2))

    inserted = await store.insert_memory_entries([cast("Any", entry)])
    results = await store.search_entries("", "app", "user-1", embedding=(0.1, 0.2))

    assert inserted == 1
    assert "unnest" in conn.fetch_calls[0][0].lower()
    assert conn.fetch_calls[1][1][0] == [0.1, 0.2]
    assert results[0]["embedding"] == [0.1, 0.2]
