"""Unit tests for asyncpg ADK store extension configuration."""

import inspect
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from typing import Any, Self, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.asyncpg.adk import AsyncpgADKConfig, AsyncpgADKMemoryStore, AsyncpgADKStore
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.exceptions import ImproperConfigurationError, MigrationError, SQLSpecError
from sqlspec.extensions.adk.migrations.reconcile import reconcile
from sqlspec.migrations.context import MigrationContext


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


async def test_asyncpg_typed_memory_tuning_reaches_schema() -> None:
    settings = AsyncpgADKConfig(
        vector_index_type="scann", vector_dimensions=256, enable_bm25=True, scann_num_leaves=32, scann_quantizer="FP32"
    )
    config = AsyncpgConfig(extension_config={"adk": settings})
    store = AsyncpgADKMemoryStore(config)

    ddl = await store._memory_table_ddl()

    assert "embedding VECTOR(256)" in ddl
    assert "USING bm25 (content_text)" in ddl
    assert "USING scann (embedding) WITH (num_leaves = 32, quantizer = 'FP32')" in ddl


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
    assert "WITH (text_config='english')" in ddl
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
    """Captures the SQL and parameters issued against an asyncpg connection."""

    def __init__(self, *, status: str = "INSERT 0 1", row: "dict[str, Any] | None" = None) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.status = status
        self.row = row

    async def fetch(self, sql: str, *params: Any) -> list[Any]:
        self.calls.append((sql, params))
        return [self.row] if self.row is not None else []

    async def fetchrow(self, sql: str, *params: Any) -> "dict[str, Any] | None":
        self.calls.append((sql, params))
        return self.row

    async def execute(self, sql: str, *params: Any) -> str:
        self.calls.append((sql, params))
        return self.status

    def transaction(self) -> Any:
        return nullcontext()

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
    assert "embedding <=> $3::float8[]::vector" in sql
    assert [0.1, 0.2, 0.3] in params
    assert "hello" in params


async def test_asyncpg_memory_search_without_embedding_stays_text_only() -> None:
    """Omitting an embedding never emits vector-distance or RRF terms."""

    store, conn = _memory_store_with_connection({"enable_bm25": True})

    await store.search_entries(query="hello", app_name="app", user_id="user")

    sql, _ = conn.calls[0]
    assert "rrf_score" not in sql
    assert "embedding <=>" not in sql


async def test_asyncpg_missing_pg_textsearch_prevents_hybrid_sql_execution() -> None:
    """A missing BM25 capability fails before the prepared hybrid query is sent."""
    store, conn = _memory_store_with_connection({"enable_bm25": True})
    store._config._ensure_pg_textsearch_available.side_effect = ImproperConfigurationError("missing")

    with pytest.raises(ImproperConfigurationError, match="missing"):
        await store.search_entries(query="hello", app_name="app", user_id="user", embedding=[0.1, 0.2])

    assert conn.calls == []


async def test_asyncpg_memory_search_embedding_only_orders_by_distance() -> None:
    """An embedding without BM25 uses a plain vector-distance ordering."""

    store, conn = _memory_store_with_connection({})

    await store.search_entries(query="", app_name="app", user_id="user", embedding=[0.5, 0.6])

    sql, params = conn.calls[0]
    assert "ORDER BY embedding <=>" in sql
    assert "embedding <=> $3::float8[]::vector" in sql
    assert "rrf_score" not in sql
    assert [0.5, 0.6] in params


@pytest.mark.parametrize("owner_column", [None, "tenant_id UUID"])
@pytest.mark.parametrize("embedding", [None, [0.1, 0.2, 0.3]])
async def test_asyncpg_memory_insert_binds_embedding_with_portable_cast(
    owner_column: str | None, embedding: list[float] | None
) -> None:
    """Memory inserts persist nullable embeddings without requiring an asyncpg vector codec."""
    store, conn = _memory_store_with_connection({"owner_id_column": owner_column} if owner_column else {})
    entry = {
        "id": "memory-1",
        "session_id": "session-1",
        "app_name": "app",
        "user_id": "user",
        "scope": "user",
        "event_id": "event-1",
        "author": "user",
        "timestamp": "2026-08-22T00:00:00Z",
        "embedding": embedding,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": "2026-08-22T00:00:00Z",
    }

    assert await store.insert_memory_entries([cast("Any", entry)], owner_id="tenant-1") == 1

    sql, params = conn.calls[0]
    assert "::float8[]::vector" in sql
    embedding_index = 9 if owner_column else 8
    assert params[embedding_index] == embedding


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


async def test_asyncpg_adk_scann_quantizer_validation() -> None:
    """Invalid SCaNN quantizer must raise ImproperConfigurationError."""
    settings = AsyncpgADKConfig(vector_index_type="scann", scann_quantizer="INVALID")
    config = AsyncpgConfig(extension_config={"adk": settings})
    store = AsyncpgADKMemoryStore(config)
    with pytest.raises(ImproperConfigurationError, match="Invalid SCaNN quantizer"):
        await store._memory_table_ddl()


def _mock_async_driver() -> MagicMock:
    driver = MagicMock()
    driver.execute_script = AsyncMock()
    driver.execute = AsyncMock()
    driver.commit = AsyncMock()
    driver.connection = MagicMock()
    driver.data_dictionary = MagicMock()
    driver.data_dictionary.get_tables = AsyncMock(return_value=[])
    driver.data_dictionary.get_columns = AsyncMock(return_value=[])
    return driver


def _recorded_scripts(driver: MagicMock) -> list[str]:
    return [_normalized(call.args[0]) for call in driver.execute_script.await_args_list]


async def test_asyncpg_session_companions_create_guarded_indexes_only() -> None:
    store = AsyncpgADKStore(_mock_config())
    driver = _mock_async_driver()

    await store.reconcile_companion_objects_async(driver)

    statements = _recorded_scripts(driver)
    assert statements == [
        "CREATE INDEX IF NOT EXISTS idx_adk_session_app_user ON adk_session(app_name, user_id);",
        "CREATE INDEX IF NOT EXISTS idx_adk_session_update_time ON adk_session(update_time DESC);",
        "CREATE INDEX IF NOT EXISTS idx_adk_session_state ON adk_session USING GIN (state) WHERE state != '{}'::jsonb;",
        "CREATE INDEX IF NOT EXISTS idx_adk_event_session ON adk_event(session_id, timestamp ASC);",
        "CREATE INDEX IF NOT EXISTS idx_adk_event_app_timestamp ON adk_event(app_name, timestamp ASC);",
    ]
    assert all("CREATE TABLE" not in sql for sql in statements)
    driver.commit.assert_not_called()


async def test_asyncpg_event_companions_follow_generated_and_covering_settings() -> None:
    store = AsyncpgADKStore(_mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True}))
    driver = _mock_async_driver()

    await store.reconcile_companion_objects_async(driver)

    statements = _recorded_scripts(driver)
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


async def test_asyncpg_memory_companions_follow_search_settings() -> None:
    store = AsyncpgADKMemoryStore(
        _mock_config({"enable_bm25": True, "memory_use_fts": True, "vector_index_type": "ivfflat"})
    )
    driver = _mock_async_driver()

    await store.reconcile_companion_objects_async(driver)

    statements = _recorded_scripts(driver)
    assert any("USING GIN (to_tsvector('english', content_text));" in sql for sql in statements)
    assert any("USING bm25 (content_text) WITH (text_config='english');" in sql for sql in statements)
    assert any("idx_adk_memory_ivfflat" in sql for sql in statements)
    assert all("idx_adk_memory_hnsw" not in sql for sql in statements)
    assert all("CREATE TABLE" not in sql for sql in statements)
    driver.commit.assert_not_called()


async def test_asyncpg_memory_prepare_reconcile_installs_extensions_and_prepare_schema_is_noop() -> None:
    driver = _mock_async_driver()
    default_store = AsyncpgADKMemoryStore(_mock_config())

    await default_store.prepare_schema_async(driver)
    driver.execute_script.assert_not_called()

    await default_store.prepare_reconcile_async(driver)
    assert _recorded_scripts(driver) == ["CREATE EXTENSION IF NOT EXISTS vector"]

    driver.execute_script.reset_mock()
    bm25_store = AsyncpgADKMemoryStore(_mock_config({"enable_bm25": True}))
    await bm25_store.prepare_schema_async(driver)
    driver.execute_script.assert_not_called()

    await bm25_store.prepare_reconcile_async(driver)
    assert _recorded_scripts(driver) == [
        "CREATE EXTENSION IF NOT EXISTS vector",
        "CREATE EXTENSION IF NOT EXISTS pg_textsearch",
    ]


async def test_asyncpg_memory_finalize_reconcile_refreshes_config_extension_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register_mock = AsyncMock()
    monkeypatch.setattr("sqlspec.adapters.asyncpg.adk.store.register_pgvector_support", register_mock)

    config = AsyncpgConfig(
        driver_features={"enable_pgvector": True, "enable_pg_textsearch": True},
        extension_config={"adk": {"enable_bm25": True}},
    )
    stale_error = RuntimeError("stale")
    config._pgvector_available = False
    config._pg_textsearch_available = False
    config._pg_textsearch_probe_error = stale_error

    store = AsyncpgADKMemoryStore(config)
    driver = _mock_async_driver()

    await store.prepare_reconcile_async(driver)
    assert config._pgvector_available is False
    assert config._pg_textsearch_available is False
    assert config._pg_textsearch_probe_error is stale_error
    register_mock.assert_not_awaited()

    await store.finalize_reconcile_async(driver)
    assert config._pgvector_available is True
    assert config.pg_textsearch_available is True
    assert config._pg_textsearch_probe_error is None
    assert config._ensure_pg_textsearch_available() is None
    register_mock.assert_awaited_once_with(driver.connection)


async def test_asyncpg_disabled_memory_prepares_nothing() -> None:
    store = AsyncpgADKMemoryStore(_mock_config({"enable_memory": False}))
    driver = _mock_async_driver()

    await store.prepare_reconcile_async(driver)
    await store.reconcile_companion_objects_async(driver)
    await store.finalize_reconcile_async(driver)

    driver.execute_script.assert_not_called()


async def test_asyncpg_memory_prepare_reconcile_validates_settings_first() -> None:
    store = AsyncpgADKMemoryStore(_mock_config({"vector_index_type": "scann", "scann_quantizer": "BAD"}))
    driver = _mock_async_driver()

    with pytest.raises(ImproperConfigurationError, match="BAD"):
        await store.prepare_reconcile_async(driver)

    driver.execute_script.assert_not_called()


async def test_asyncpg_memory_prepare_reconcile_reports_missing_extension() -> None:
    store = AsyncpgADKMemoryStore(_mock_config())
    driver = _mock_async_driver()
    err = SQLSpecError("not available")
    driver.execute_script.side_effect = err

    with pytest.raises(MigrationError, match="vector") as exc_info:
        await store.prepare_reconcile_async(driver)

    assert exc_info.value.__cause__ is err


async def test_asyncpg_reconcile_prepares_before_targets_and_companions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sqlspec.adapters.asyncpg.adk.store.register_pgvector_support", AsyncMock())
    config = AsyncpgConfig(extension_config={"adk": {"enable_sessions": False}})
    driver = _mock_async_driver()

    result = reconcile(MigrationContext(config=config, dialect="postgres", driver=driver))
    if inspect.isawaitable(result):
        await result

    statements = _recorded_scripts(driver)
    assert statements[0] == "CREATE EXTENSION IF NOT EXISTS vector"
    assert statements[1].startswith("CREATE TABLE IF NOT EXISTS adk_memory")
    assert all(sql.startswith("CREATE INDEX IF NOT EXISTS") for sql in statements[2:])


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
async def test_asyncpg_get_session_renews_only_for_positive_durations(
    renew_for: "int | timedelta | None", expect_update: bool
) -> None:
    now = datetime.now(UTC)
    conn = _RecordingConnection(
        row={"id": "s1", "app_name": "app", "user_id": "u1", "state": {}, "create_time": now, "update_time": now}
    )
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    store = AsyncpgADKStore(config)

    result = await store.get_session("app", "u1", "s1", renew_for=renew_for)

    assert result is not None
    sql, params = conn.calls[0]
    if expect_update:
        assert _normalized(sql).startswith("UPDATE adk_session SET update_time = CURRENT_TIMESTAMP")
    else:
        assert _normalized(sql).startswith("SELECT id, app_name, user_id, state, create_time, update_time")
    assert params == ("app", "u1", "s1")


@pytest.mark.parametrize(("status", "should_raise"), [("INSERT 0 1", False), ("INSERT 0 0", True)])
async def test_asyncpg_append_event_checks_session_ownership(status: str, should_raise: bool) -> None:
    conn = _RecordingConnection(status=status)
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    store = AsyncpgADKStore(config)
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
            await store.append_event(cast("Any", event_record))
    else:
        await store.append_event(cast("Any", event_record))

    sql, params = conn.calls[0]
    assert "WHERE EXISTS ( SELECT 1 FROM adk_session WHERE id = $4 AND app_name = $2 AND user_id = $3 )" in _normalized(
        sql
    )
    assert params[1:4] == ("app", "u1", "s1")


@pytest.mark.parametrize(
    ("record_overrides", "expect_mismatch"),
    [
        pytest.param({}, False, id="matching"),
        pytest.param({"app_name": "other-app"}, True, id="mismatched-app"),
        pytest.param({"user_id": "other-user"}, True, id="mismatched-user"),
        pytest.param({"session_id": "other-session"}, True, id="mismatched-session"),
    ],
)
async def test_asyncpg_append_event_and_update_state_validates_scope_and_updates_first(
    record_overrides: dict[str, str], expect_mismatch: bool
) -> None:
    now = datetime.now(UTC)
    conn = _RecordingConnection(
        row={
            "id": "s1",
            "app_name": "app",
            "user_id": "u1",
            "state": {"k": "v"},
            "create_time": now,
            "update_time": now,
        }
    )
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    store = AsyncpgADKStore(config)
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
            await store.append_event_and_update_state(cast("Any", event_record), "app", "u1", "s1", {"k": "v"})
        assert conn.calls == []
        return

    updated = await store.append_event_and_update_state(cast("Any", event_record), "app", "u1", "s1", {"k": "v"})
    assert updated["id"] == "s1"
    assert _normalized(conn.calls[0][0]).startswith("UPDATE adk_session")
    assert _normalized(conn.calls[1][0]).startswith("INSERT INTO adk_event")


async def test_asyncpg_get_events_joins_on_session_and_owner_columns() -> None:
    store, conn = _session_store_with_connection()

    await store.get_events("app", "u1", "s1")

    sql, params = conn.calls[0]
    normalized = _normalized(sql)
    assert (
        "JOIN adk_session s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id" in normalized
    )
    assert "WHERE s.app_name = $1 AND s.user_id = $2 AND s.id = $3" in normalized
    assert params == ("app", "u1", "s1")


@pytest.mark.parametrize(
    ("app_name", "user_id", "expected_where", "expected_params"),
    [
        pytest.param(None, None, "WHERE session_id = $1", ("s1",), id="session-only"),
        pytest.param("app", None, "WHERE session_id = $1 AND app_name = $2", ("s1", "app"), id="session-and-app"),
        pytest.param(None, "u1", "WHERE session_id = $1 AND user_id = $2", ("s1", "u1"), id="session-and-user"),
        pytest.param(
            "app",
            "u1",
            "WHERE session_id = $1 AND app_name = $2 AND user_id = $3",
            ("s1", "app", "u1"),
            id="session-app-user",
        ),
    ],
)
async def test_asyncpg_delete_entries_by_session_scopes_predicates(
    app_name: "str | None", user_id: "str | None", expected_where: str, expected_params: tuple[str, ...]
) -> None:
    conn = _RecordingConnection(status="DELETE 2")
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    store = AsyncpgADKMemoryStore(config)

    deleted = await store.delete_entries_by_session("s1", app_name=app_name, user_id=user_id)

    assert deleted == 2
    sql, params = conn.calls[0]
    assert _normalized(sql) == f"DELETE FROM adk_memory {expected_where}"
    assert params == expected_params


@pytest.mark.parametrize("owner_column", [None, "tenant_id UUID"])
async def test_asyncpg_memory_ddl_and_insert_use_composite_dedup_and_targetless_conflict(
    owner_column: "str | None",
) -> None:
    adk_cfg: dict[str, object] = {"owner_id_column": owner_column} if owner_column else {}
    store, conn = _memory_store_with_connection(adk_cfg)
    ddl = await store._memory_table_ddl()
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
        "embedding": None,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": now,
    }
    await store.insert_memory_entries([cast("Any", entry)], owner_id="tenant-1")
    sql, _ = conn.calls[0]
    assert "ON CONFLICT DO NOTHING" in sql
    assert "ON CONFLICT (event_id)" not in sql
