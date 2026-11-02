"""SQLite ADK adapter-local configuration tests."""

from typing import Any, Self, get_type_hints
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.sqlite.adk import SqliteADKConfig, SqliteADKMemoryStore, SqliteADKStore
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk import StoredMemory


class RecordingConnection:
    """Record SQL executed by ADK PRAGMA setup."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def test_sqlite_adk_config_extends_shared_config_and_exports_sqlite_knobs() -> None:
    """SqliteADKConfig should inherit shared ADK settings and add only SQLite-specific knobs."""
    annotations = get_type_hints(SqliteADKConfig, include_extras=True)

    assert set(ADKConfig.__annotations__) <= set(annotations)
    assert {"pragma_overrides", "fts_tokenize", "fts_detail"} <= set(annotations)


def test_sqlite_adk_defaults_preserve_existing_pragma_profile_and_ignore_driver_features() -> None:
    """Default ADK PRAGMAs should stay fixed and not read global driver_features."""
    config = SqliteConfig(driver_features={"pragma_overrides": {"cache_size": -32000}})
    store = SqliteADKStore(config)
    connection = RecordingConnection()

    store._apply_pragmas(connection)

    assert connection.statements == [
        "PRAGMA foreign_keys = ON",
        "PRAGMA cache_size = -64000",
        "PRAGMA mmap_size = 30000000",
        "PRAGMA journal_size_limit = 67108864",
    ]


def test_sqlite_adk_pragma_overrides_apply_after_defaults() -> None:
    """Adapter-local ADK PRAGMA overrides should render through SQLite validation and apply last."""
    config = SqliteConfig(
        extension_config={
            "adk": {"pragma_overrides": {"cache_size": -32000, "foreign_keys": False, "journal_mode": "WAL"}}
        }
    )
    store = SqliteADKStore(config)
    connection = RecordingConnection()

    store._apply_pragmas(connection)

    assert connection.statements[-3:] == [
        "PRAGMA cache_size = -32000",
        "PRAGMA foreign_keys = 0",
        "PRAGMA journal_mode = WAL",
    ]


def test_sqlite_adk_pragma_overrides_reuse_sqlite_validation() -> None:
    """ADK PRAGMA overrides should reject unsafe names before SQL rendering."""
    with pytest.raises(ImproperConfigurationError, match="PRAGMA name"):
        SqliteADKStore(
            SqliteConfig(extension_config={"adk": {"pragma_overrides": {"cache_size; DROP TABLE x": -32000}}})
        )


def test_sqlite_adk_fts5_options_render_only_when_fts_enabled() -> None:
    """FTS5 tokenizer/detail options should be emitted only for opt-in FTS DDL."""
    default_store = SqliteADKMemoryStore(SqliteConfig())
    configured_store = SqliteADKMemoryStore(
        SqliteConfig(
            extension_config={
                "adk": {"memory_use_fts": True, "fts_tokenize": "porter unicode61", "fts_detail": "column"}
            }
        )
    )

    assert "tokenize" not in default_store._memory_table_ddl()
    sql = configured_store._memory_table_ddl()
    assert "tokenize = 'porter unicode61'" in sql
    assert "detail = column" in sql


class _SessionListCursor:
    def __init__(self) -> None:
        self.rows: list[Any] = []

    def fetchall(self) -> "list[Any]":
        return self.rows


class _SessionListConnection:
    """Record the session-list query and its bound parameters."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: "tuple[Any, ...] | None" = None) -> _SessionListCursor:
        if params is not None:
            self.calls.append((sql, params))
        return _SessionListCursor()

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, *_: Any) -> None:
        return None


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _session_store_with_connection() -> "tuple[SqliteADKStore, _SessionListConnection]":
    conn = _SessionListConnection()
    config = MagicMock()
    config.extension_config = {"adk": {}}
    config.provide_connection = lambda *_a, **_k: conn
    return SqliteADKStore(config), conn


def test_sqlite_list_sessions_binds_order_and_page() -> None:
    """Explicit ordering renders inline while page bounds bind as qmark parameters."""
    store, conn = _session_store_with_connection()

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = conn.calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM adk_session WHERE app_name = ? AND user_id = ? "
        "ORDER BY create_time ASC, id ASC LIMIT ? OFFSET ?"
    )
    assert params == ("app", "u1", 10, 20)


def test_sqlite_list_sessions_defaults_to_recent_first_without_a_page() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, conn = _session_store_with_connection()

    store.list_sessions("app")

    sql, params = conn.calls[0]
    assert _normalized(sql).endswith("WHERE app_name = ? ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_sqlite_list_sessions_orders_page_after_scope_parameters() -> None:
    """Page values bind after the scope parameters that are actually present."""
    store, conn = _session_store_with_connection()

    store.list_sessions("app", limit=5)

    sql, params = conn.calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC LIMIT ? OFFSET ?")
    assert params == ("app", 5, 0)


def test_sqlite_list_sessions_zero_limit_never_opens_a_connection() -> None:
    """A zero limit short-circuits before any database work."""
    store, conn = _session_store_with_connection()

    assert store.list_sessions("app", limit=0) == []
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
def test_sqlite_list_sessions_rejects_invalid_options_before_connecting(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, conn = _session_store_with_connection()

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert conn.calls == []


def test_sqlite_adk_get_events_with_limit_binds_parameter() -> None:
    """Verify get_events parameterizes LIMIT instead of string interpolation."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = SqliteADKStore(config)
    calls: list[tuple[str, tuple[Any, ...]]] = []

    class _MockEventsCursor:
        def fetchall(self) -> list[Any]:
            return []

    class _MockEventsConnection:
        def execute(self, sql: str, params: Any = None) -> _MockEventsCursor:
            calls.append((sql, params))
            return _MockEventsCursor()

    class _SyncContextManager:
        def __enter__(self) -> Any:
            return _MockEventsConnection()

        def __exit__(self, *_: Any) -> None:
            return None

    config.provide_connection = MagicMock(return_value=_SyncContextManager())  # type: ignore[method-assign]

    events = store.get_events("app", "user", "session", limit=10)
    assert events == []
    assert len(calls) >= 1
    sql, params = calls[-1]
    assert "LIMIT ?" in sql
    assert 10 in params


def test_sqlite_adk_memory_store_persists_and_retrieves_float32_embeddings() -> None:
    """Verify memory entries persist and retrieve float32 embeddings."""
    from datetime import UTC, datetime
    from uuid import uuid4

    config = SqliteConfig(connection_config={"database": ":memory:"})
    store = SqliteADKMemoryStore(config)
    try:
        store.create_tables()

        now = datetime.now(UTC)
        embedding = [0.1, 0.2, 0.3, 0.4]
        record = StoredMemory(
            id=str(uuid4()),
            session_id="s1",
            app_name="app",
            user_id="u1",
            scope="user",
            event_id="evt-1",
            author="user",
            timestamp=now,
            content_json={"text": "vector search test"},
            content_text="vector search test",
            metadata_json=None,
            inserted_at=now,
            embedding=embedding,
        )
        inserted = store.insert_memory_entries([record])
        assert inserted == 1

        results = store.search_entries(query="vector", app_name="app", user_id="u1", embedding=embedding)
        assert len(results) == 1
        assert results[0]["event_id"] == "evt-1"
        assert results[0]["embedding"] is not None
        assert len(results[0]["embedding"]) == 4
        for orig, rec in zip(embedding, results[0]["embedding"], strict=True):
            assert abs(orig - rec) < 1e-6
    finally:
        config.close_pool()
