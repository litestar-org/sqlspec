# pyright: reportPrivateUsage=false
"""Unit tests for asyncpg ADK store extension configuration."""

from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

import pytest
from typing_extensions import NotRequired, Self

from sqlspec.adapters.asyncpg.adk import AsyncpgADKConfig, AsyncpgADKMemoryStore, AsyncpgADKStore
from sqlspec.config import ADKConfig


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def test_asyncpg_adk_config_types_adapter_local_optimizations() -> None:
    """Asyncpg ADK optimizations are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", AsyncpgADKConfig).__optional_keys__
    assert cast("Any", AsyncpgADKConfig).__optional_keys__ - cast("Any", ADKConfig).__optional_keys__ == {
        "autovacuum_analyze_scale_factor",
        "autovacuum_vacuum_scale_factor",
        "enable_event_generated_columns",
        "enable_covering_indexes",
        "fillfactor",
    }

    for feature_name in ("enable_event_generated_columns", "enable_covering_indexes"):
        annotation = cast("Any", AsyncpgADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (bool,)


async def test_asyncpg_adk_events_table_uses_plain_schema_by_default() -> None:
    """Asyncpg ADK optimization DDL stays opt-in through adapter-local extension config."""

    store = AsyncpgADKStore(_mock_config())

    sql = await store._events_table_ddl()

    assert "author_gc" not in sql
    assert "node_path_gc" not in sql
    assert "INCLUDE (invocation_id)" not in sql


async def test_asyncpg_adk_events_table_applies_adapter_local_extension_config() -> None:
    """Asyncpg ADK extension settings enable PostgreSQL-specific event DDL."""

    store = AsyncpgADKStore(_mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True}))

    sql = await store._events_table_ddl()

    assert "author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED" in sql
    assert "node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED" in sql
    assert "idx_adk_event_author_gc" in sql
    assert "idx_adk_event_node_path_gc" in sql
    assert "INCLUDE (invocation_id)" in sql


async def test_asyncpg_adk_event_table_applies_postgres_tuning_options() -> None:
    """Append-heavy ADK event tables honor PostgreSQL tuning settings."""
    store = AsyncpgADKStore(
        _mock_config({"fillfactor": 75, "autovacuum_vacuum_scale_factor": 0.1, "autovacuum_analyze_scale_factor": 0.2})
    )

    sql = await store._events_table_ddl()

    assert "fillfactor = 75" in sql
    assert "autovacuum_vacuum_scale_factor = 0.1" in sql
    assert "autovacuum_analyze_scale_factor = 0.2" in sql


async def _memory_ddl(adk_config: "dict[str, object] | None" = None) -> str:
    store = AsyncpgADKMemoryStore(_mock_config(adk_config))
    return await store._memory_table_ddl()


async def test_asyncpg_memory_ddl_defaults_to_hnsw_without_bm25() -> None:
    """The default vector index is hnsw, and BM25 stays off until enabled."""

    ddl = await _memory_ddl()

    assert "USING hnsw (embedding" in ddl
    assert "bm25" not in ddl
    assert "scann" not in ddl
    assert "ivfflat" not in ddl


async def test_asyncpg_memory_ddl_emits_bm25_index_when_enabled() -> None:
    """enable_bm25 adds a BM25 index over content_text."""

    ddl = await _memory_ddl({"enable_bm25": True})

    assert "USING bm25 (content_text)" in ddl
    assert "idx_adk_memory_bm25" in ddl


async def test_asyncpg_memory_ddl_emits_scann_index_with_configured_tuning() -> None:
    """A ScaNN index carries the configured leaf count and quantizer."""

    ddl = await _memory_ddl({
        "vector_index_type": "scann",
        "vector_dimensions": 768,
        "scann_num_leaves": 250,
        "scann_quantizer": "sq8",
    })

    assert "USING scann (embedding)" in ddl
    assert "num_leaves = 250" in ddl
    assert "quantizer = 'sq8'" in ddl


async def test_asyncpg_memory_ddl_supports_alternate_vector_index_types() -> None:
    """ivfflat and hnsw are emitted when selected, and are mutually exclusive with scann."""

    ivfflat = await _memory_ddl({"vector_index_type": "ivfflat", "vector_dimensions": 768})
    assert "USING ivfflat (embedding" in ivfflat
    assert "scann" not in ivfflat

    scann = await _memory_ddl({"vector_index_type": "scann", "vector_dimensions": 768})
    assert "USING scann (embedding)" in scann
    assert "hnsw" not in scann


class _RecordingConnection:
    """Captures the SQL and parameters a memory search issues."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, sql: str, *params: Any) -> list[Any]:
        self.calls.append((sql, params))
        return []

    async def __aenter__(self) -> "Self":
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None


def _memory_store_with_connection(adk_config: "dict[str, object]") -> tuple[Any, _RecordingConnection]:
    conn = _RecordingConnection()
    config = _mock_config(adk_config)
    config.provide_connection = lambda *_a, **_k: conn
    return AsyncpgADKMemoryStore(config), conn


async def test_asyncpg_memory_search_fuses_vector_and_text_ranks() -> None:
    """Supplying an embedding with BM25 enabled produces the RRF hybrid query."""

    store, conn = _memory_store_with_connection({"enable_bm25": True})

    await store.search_entries(query="hello", app_name="app", user_id="user", embedding=[0.1, 0.2, 0.3])

    sql, params = conn.calls[0]
    assert "RANK() OVER (ORDER BY embedding <=>" in sql
    assert "RANK() OVER (ORDER BY content_text <@>" in sql
    assert "rrf_score" in sql
    assert "ORDER BY rrf_score DESC" in sql
    assert [0.1, 0.2, 0.3] in params
    assert "hello" in params


async def test_asyncpg_memory_search_without_embedding_stays_text_only() -> None:
    """Omitting an embedding never emits vector-distance or RRF terms."""

    store, conn = _memory_store_with_connection({"enable_bm25": True})

    await store.search_entries(query="hello", app_name="app", user_id="user")

    sql, _ = conn.calls[0]
    assert "rrf_score" not in sql
    assert "embedding <=>" not in sql


async def test_asyncpg_memory_search_embedding_only_orders_by_distance() -> None:
    """An embedding without BM25 uses a plain vector-distance ordering."""

    store, conn = _memory_store_with_connection({})

    await store.search_entries(query="", app_name="app", user_id="user", embedding=[0.5, 0.6])

    sql, params = conn.calls[0]
    assert "ORDER BY embedding <=>" in sql
    assert "rrf_score" not in sql
    assert [0.5, 0.6] in params


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _session_store_with_connection() -> "tuple[AsyncpgADKStore, _RecordingConnection]":
    conn = _RecordingConnection()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    return AsyncpgADKStore(config), conn


async def test_asyncpg_list_sessions_binds_order_and_page() -> None:
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


async def test_asyncpg_list_sessions_defaults_to_recent_first_without_a_page() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, conn = _session_store_with_connection()

    await store.list_sessions("app")

    sql, params = conn.calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = $1 ORDER BY update_time DESC, id DESC"
    )
    assert params == ("app",)


async def test_asyncpg_list_sessions_numbers_page_placeholders_without_a_user_filter() -> None:
    """Page placeholders follow the scope parameters that are actually present."""
    store, conn = _session_store_with_connection()

    await store.list_sessions("app", limit=5)

    sql, params = conn.calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC LIMIT $2 OFFSET $3")
    assert params == ("app", 5, 0)


async def test_asyncpg_list_sessions_zero_limit_never_opens_a_connection() -> None:
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
async def test_asyncpg_list_sessions_rejects_invalid_options_before_connecting(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, conn = _session_store_with_connection()

    with pytest.raises(ValueError):
        await store.list_sessions("app", **options)

    assert conn.calls == []
