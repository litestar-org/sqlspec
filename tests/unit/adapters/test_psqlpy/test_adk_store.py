"""Unit tests for psqlpy ADK store extension configuration."""

import inspect
from datetime import UTC, datetime
from typing import Any, NotRequired, Self, cast, get_args, get_origin
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from psqlpy.exceptions import ConnectionExecuteError, DatabaseError

from sqlspec.adapters.psqlpy import PsqlpyConfig
from sqlspec.adapters.psqlpy.adk import PsqlpyADKConfig, PsqlpyADKMemoryStore, PsqlpyADKStore
from sqlspec.adapters.psqlpy.adk.store import _is_psqlpy_database_error, _is_table_missing_error, decode_postgres_vector
from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError, MigrationError, SQLSpecError
from sqlspec.extensions.adk.migrations.reconcile import reconcile
from sqlspec.migrations.context import MigrationContext


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
    assert conn.fetch_calls[0][1][-1] == ["[0.1,0.2]"]
    assert conn.fetch_calls[1][1][0] == [0.1, 0.2]
    assert results[0]["embedding"] == [0.1, 0.2]


@pytest.mark.parametrize(("owner_type", "owner_id"), [("INTEGER", 42), ("UUID", UUID(int=1))])
async def test_psqlpy_memory_batch_preserves_owner_type(
    owner_type: str, owner_id: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A scalar owner bind lets the destination column infer its native type."""
    conn = _FakeMemoryConnection([_sample_memory_row(None)])
    monkeypatch.setattr(PsqlpyConfig, "provide_connection", lambda *_a, **_k: conn)
    config = PsqlpyConfig(extension_config={"adk": {"owner_id_column": f"owner_id {owner_type}"}})
    store = PsqlpyADKMemoryStore(config)

    assert await store.insert_memory_entries([cast("Any", _sample_memory_row(None))], owner_id) == 1

    sql, params = conn.fetch_calls[0]
    assert "author, $14, timestamp" in _normalized(sql)
    assert params[-1] == owner_id
    assert len(params[0]) == 1


def _mock_async_driver() -> MagicMock:
    driver = MagicMock()
    driver.connection = MagicMock()
    driver.execute_script = AsyncMock()
    driver.commit = AsyncMock()
    driver.data_dictionary = MagicMock()
    driver.data_dictionary.get_tables = AsyncMock(return_value=[])
    driver.data_dictionary.get_columns = AsyncMock(return_value=[])
    return driver


def _recorded_scripts(driver: MagicMock) -> list[str]:
    return [_normalized(call.args[0]) for call in driver.execute_script.await_args_list]


async def test_psqlpy_session_companions_create_guarded_indexes_only() -> None:
    store = PsqlpyADKStore(_mock_config())
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


async def test_psqlpy_event_companions_follow_generated_and_covering_settings() -> None:
    store = PsqlpyADKStore(_mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True}))
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


async def test_psqlpy_memory_companions_follow_search_settings() -> None:
    store = PsqlpyADKMemoryStore(
        _mock_config({"enable_bm25": True, "memory_use_fts": True, "vector_index_type": "none"})
    )
    driver = _mock_async_driver()

    await store.reconcile_companion_objects_async(driver)

    statements = _recorded_scripts(driver)
    assert any("USING GIN (to_tsvector('english', content_text));" in sql for sql in statements)
    assert any("USING bm25 (content_text) WITH (text_config='english');" in sql for sql in statements)
    assert all("USING hnsw" not in sql for sql in statements)
    assert all("USING ivfflat" not in sql for sql in statements)
    assert all("USING scann" not in sql for sql in statements)
    assert all("CREATE TABLE" not in sql for sql in statements)
    driver.commit.assert_not_called()


async def test_psqlpy_memory_prepare_reconcile_installs_extensions_and_prepare_schema_is_noop() -> None:
    driver = _mock_async_driver()
    none_store = PsqlpyADKMemoryStore(_mock_config({"vector_index_type": "none"}))

    await none_store.prepare_schema_async(driver)
    driver.execute_script.assert_not_called()

    await none_store.prepare_reconcile_async(driver)
    assert _recorded_scripts(driver) == ["CREATE EXTENSION IF NOT EXISTS vector"]

    driver.execute_script.reset_mock()
    bm25_store = PsqlpyADKMemoryStore(_mock_config({"enable_bm25": True}))
    await bm25_store.prepare_schema_async(driver)
    driver.execute_script.assert_not_called()

    await bm25_store.prepare_reconcile_async(driver)
    assert _recorded_scripts(driver) == [
        "CREATE EXTENSION IF NOT EXISTS vector",
        "CREATE EXTENSION IF NOT EXISTS pg_textsearch",
    ]


async def test_psqlpy_memory_finalize_reconcile_refreshes_config_extension_state() -> None:
    config = PsqlpyConfig(
        driver_features={"enable_pgvector": True, "enable_pg_textsearch": True},
        extension_config={"adk": {"enable_bm25": True, "vector_index_type": "none"}},
    )
    stale_error = RuntimeError("stale")
    config._pgvector_available = False
    config._pg_textsearch_available = False
    config._pg_textsearch_probe_error = stale_error

    store = PsqlpyADKMemoryStore(config)
    driver = _mock_async_driver()

    await store.prepare_reconcile_async(driver)
    assert config._pgvector_available is False
    assert config._pg_textsearch_available is False
    assert config._pg_textsearch_probe_error is stale_error

    await store.finalize_reconcile_async(driver)
    assert config._pgvector_available is True
    assert config.pg_textsearch_available is True
    assert config._pg_textsearch_probe_error is None
    assert config._ensure_pg_textsearch_available() is None


async def test_psqlpy_disabled_memory_prepares_nothing() -> None:
    store = PsqlpyADKMemoryStore(_mock_config({"enable_memory": False}))
    driver = _mock_async_driver()

    await store.prepare_reconcile_async(driver)
    await store.reconcile_companion_objects_async(driver)
    await store.finalize_reconcile_async(driver)

    driver.execute_script.assert_not_called()


@pytest.mark.parametrize(
    ("adk_settings", "expected_match"),
    [
        pytest.param({"vector_index_type": "scann", "scann_quantizer": "BAD"}, "scann_quantizer", id="bad-quantizer"),
        pytest.param({"vector_index_type": "scann", "scann_num_leaves": -1}, "scann_num_leaves", id="bad-leaves"),
        pytest.param({"vector_index_type": "unknown"}, "vector_index_type", id="bad-vector-type"),
    ],
)
async def test_psqlpy_memory_prepare_reconcile_validates_settings_first(
    adk_settings: dict[str, object], expected_match: str
) -> None:
    store = PsqlpyADKMemoryStore(_mock_config(adk_settings))
    driver = _mock_async_driver()

    with pytest.raises(ImproperConfigurationError, match=expected_match):
        await store.prepare_reconcile_async(driver)

    driver.execute_script.assert_not_called()


async def test_psqlpy_memory_prepare_reconcile_reports_missing_extension() -> None:
    store = PsqlpyADKMemoryStore(_mock_config())
    driver = _mock_async_driver()
    err = SQLSpecError("not available")
    driver.execute_script.side_effect = err

    with pytest.raises(MigrationError, match="vector") as exc_info:
        await store.prepare_reconcile_async(driver)

    assert exc_info.value.__cause__ is err


async def test_psqlpy_reconcile_prepares_before_targets_and_companions() -> None:
    config = PsqlpyConfig(extension_config={"adk": {"enable_sessions": False}})
    driver = _mock_async_driver()

    result = reconcile(MigrationContext(config=config, dialect="postgres", driver=driver))
    if inspect.isawaitable(result):
        await result

    statements = _recorded_scripts(driver)
    assert statements[0] == "CREATE EXTENSION IF NOT EXISTS vector"
    assert statements[1].startswith("CREATE TABLE IF NOT EXISTS adk_memory")
    assert all(sql.startswith("CREATE INDEX IF NOT EXISTS") for sql in statements[2:])
