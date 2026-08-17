# pyright: reportPrivateUsage=false
"""Unit tests for DuckDB ADK store extension configuration."""

from textwrap import dedent
from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

import pytest
from typing_extensions import NotRequired, Self

import sqlspec.adapters.duckdb.adk as duckdb_adk
from sqlspec.adapters.duckdb.adk import DuckdbADKMemoryStore, DuckdbADKStore
from sqlspec.config import ADKConfig


def _mock_config(adk_config: "dict[str, object] | None" = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def _normalize_sql(sql: str) -> str:
    return dedent(sql).strip()


def test_duckdb_adk_config_types_adapter_local_optimizations() -> None:
    """DuckDB ADK optimization settings are typed on the adapter-local extension config."""

    assert hasattr(duckdb_adk, "DuckdbADKConfig")
    assert hasattr(duckdb_adk, "DuckdbADKFTSOptions")
    duckdb_adk_config = cast("Any", duckdb_adk.DuckdbADKConfig)
    duckdb_adk_fts_options = cast("Any", duckdb_adk.DuckdbADKFTSOptions)

    assert cast("Any", ADKConfig).__optional_keys__ <= duckdb_adk_config.__optional_keys__

    expected_types: dict[str, object] = {
        "enable_event_generated_columns": bool,
        "enable_event_generated_column_indexes": bool,
        "memory_fts_options": duckdb_adk_fts_options,
    }
    for feature_name, expected_type in expected_types.items():
        annotation = cast("Any", duckdb_adk_config.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)

    for feature_name, expected_type in {
        "stemmer": str,
        "stopwords": str,
        "ignore": str,
        "strip_accents": bool | int,
        "lower": bool | int,
        "overwrite": bool | int,
    }.items():
        annotation = cast("Any", duckdb_adk_fts_options.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)


def test_duckdb_adk_events_table_uses_plain_schema_by_default() -> None:
    """DuckDB ADK event DDL stays unchanged unless DuckDB-specific config opts in."""

    store = DuckdbADKStore(_mock_config())

    sql = store._events_table_ddl()

    assert _normalize_sql(sql) == _normalize_sql(
        """
        CREATE TABLE IF NOT EXISTS adk_event (
            id VARCHAR PRIMARY KEY,
            app_name VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            session_id VARCHAR NOT NULL,
            invocation_id VARCHAR,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_data JSON NOT NULL,
            FOREIGN KEY (session_id) REFERENCES adk_session(id)
        );
        CREATE INDEX IF NOT EXISTS idx_adk_event_session ON adk_event(session_id, timestamp ASC);
        CREATE INDEX IF NOT EXISTS idx_adk_event_app_timestamp ON adk_event(app_name, timestamp ASC);
        """
    )
    assert "author_gc" not in sql
    assert "node_path_gc" not in sql


def test_duckdb_adk_events_table_applies_adapter_local_generated_columns() -> None:
    """DuckDB ADK generated event projection columns and indexes are opt-in."""

    store = DuckdbADKStore(
        _mock_config({"enable_event_generated_columns": True, "enable_event_generated_column_indexes": True})
    )

    sql = store._events_table_ddl()

    assert (
        "author_gc VARCHAR GENERATED ALWAYS AS "
        "((event_data::STRUCT(author VARCHAR, node_info STRUCT(path VARCHAR))).author) VIRTUAL"
    ) in sql
    assert (
        "node_path_gc VARCHAR GENERATED ALWAYS AS "
        "((event_data::STRUCT(author VARCHAR, node_info STRUCT(path VARCHAR))).node_info.path) VIRTUAL"
    ) in sql
    assert "CREATE INDEX IF NOT EXISTS idx_adk_event_author_gc" in sql
    assert "ON adk_event(session_id, author_gc, timestamp ASC)" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_adk_event_node_path_gc" in sql
    assert "ON adk_event(session_id, node_path_gc, timestamp ASC)" in sql


def test_duckdb_adk_memory_fts_pragmas_use_default_options() -> None:
    """DuckDB ADK FTS PRAGMA rendering preserves the current default options."""

    store = DuckdbADKMemoryStore(_mock_config({"memory_use_fts": True}))

    assert (
        store._fts_index_ddl(overwrite=False) == "PRAGMA create_fts_index('adk_memory', 'id', 'content_text', "
        "stemmer='porter', stopwords='english', strip_accents=1, lower=1)"
    )
    assert (
        store._fts_index_ddl(overwrite=True) == "PRAGMA create_fts_index('adk_memory', 'id', 'content_text', "
        "overwrite=1, stemmer='porter', stopwords='english', strip_accents=1, lower=1)"
    )


def test_duckdb_adk_memory_fts_pragmas_apply_adapter_local_options() -> None:
    """DuckDB ADK FTS options come from extension_config["adk"], not driver_features."""

    store = DuckdbADKMemoryStore(
        _mock_config({
            "memory_use_fts": True,
            "memory_fts_options": {
                "stemmer": "none",
                "stopwords": "none",
                "ignore": "[^a-z]+",
                "strip_accents": False,
                "lower": False,
            },
        })
    )

    create_sql = store._fts_index_ddl(overwrite=False)
    refresh_sql = store._fts_index_ddl(overwrite=True)

    assert create_sql == (
        "PRAGMA create_fts_index('adk_memory', 'id', 'content_text', "
        "stemmer='none', stopwords='none', ignore='[^a-z]+', strip_accents=0, lower=0)"
    )
    assert refresh_sql == (
        "PRAGMA create_fts_index('adk_memory', 'id', 'content_text', "
        "overwrite=1, stemmer='none', stopwords='none', ignore='[^a-z]+', strip_accents=0, lower=0)"
    )


class _SessionListCursor:
    def fetchall(self) -> "list[Any]":
        return []


class _SessionListConnection:
    """Record the session-list query and its bound parameters."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def execute(self, sql: str, params: "tuple[Any, ...] | None" = None) -> _SessionListCursor:
        self.calls.append((sql, tuple(params or ())))
        return _SessionListCursor()

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, *_: Any) -> None:
        return None


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _session_store_with_connection() -> "tuple[DuckdbADKStore, _SessionListConnection]":
    conn = _SessionListConnection()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    return DuckdbADKStore(config), conn


def test_duckdb_list_sessions_binds_order_and_page() -> None:
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


def test_duckdb_list_sessions_defaults_to_recent_first_without_a_page() -> None:
    """The default listing keeps recent-first ordering and binds no page values."""
    store, conn = _session_store_with_connection()

    store.list_sessions("app")

    sql, params = conn.calls[0]
    assert _normalized(sql).endswith("WHERE app_name = ? ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_duckdb_list_sessions_orders_page_after_scope_parameters() -> None:
    """Page values bind after the scope parameters that are actually present."""
    store, conn = _session_store_with_connection()

    store.list_sessions("app", limit=5)

    sql, params = conn.calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC LIMIT ? OFFSET ?")
    assert params == ("app", 5, 0)


def test_duckdb_list_sessions_zero_limit_never_opens_a_connection() -> None:
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
def test_duckdb_list_sessions_rejects_invalid_options_before_connecting(options: "dict[str, Any]") -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, conn = _session_store_with_connection()

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert conn.calls == []
