# pyright: reportPrivateUsage=false
"""Unit tests for DuckDB ADK store extension configuration."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from textwrap import dedent
from typing import Any, NotRequired, Self, cast, get_args, get_origin
from unittest.mock import MagicMock

import duckdb
import pytest

import sqlspec.adapters.duckdb.adk as duckdb_adk
from sqlspec.adapters.duckdb.adk import DuckDBADKMemoryStore, DuckDBADKStore
from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.adapters.duckdb.litestar.store import DuckDBStore
from sqlspec.config import ADKConfig


def _mock_config(adk_config: "dict[str, object] | None" = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def _normalize_sql(sql: str) -> str:
    return dedent(sql).strip()


def test_duckdb_adk_config_types_adapter_local_optimizations() -> None:
    """DuckDB ADK optimization settings are typed on the adapter-local extension config."""

    assert hasattr(duckdb_adk, "DuckDBADKConfig")
    assert hasattr(duckdb_adk, "DuckDBADKFTSOptions")
    duckdb_adk_config = cast("Any", duckdb_adk.DuckDBADKConfig)
    duckdb_adk_fts_options = cast("Any", duckdb_adk.DuckDBADKFTSOptions)

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

    store = DuckDBADKStore(_mock_config())

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

    store = DuckDBADKStore(
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

    store = DuckDBADKMemoryStore(_mock_config({"memory_use_fts": True}))

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

    store = DuckDBADKMemoryStore(
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


def _session_store_with_connection() -> "tuple[DuckDBADKStore, _SessionListConnection]":
    conn = _SessionListConnection()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    return DuckDBADKStore(config), conn


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


def test_duckdb_adk_delete_helpers_use_returning_row_count() -> None:
    """_delete_expired_events, _delete_idle_sessions, and _delete_idle_user_states use DELETE ... RETURNING 1."""

    class _DeleteConn:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute(self, sql: str, params: Any = None) -> "_DeleteConn":
            self.queries.append(" ".join(sql.split()))
            return self

        def fetchall(self) -> list[tuple[int]]:
            return [(1,), (1,)]

        def commit(self) -> None:
            pass

        def __enter__(self) -> "Self":
            return self

        def __exit__(self, *_: Any) -> None:
            return None

    conn = _DeleteConn()
    config = _mock_config()
    config.provide_connection = lambda *_a, **_k: conn
    store = DuckDBADKStore(config)
    cutoff = datetime.now(UTC)

    assert store._delete_expired_events(cutoff) == 2
    assert store._delete_idle_sessions(cutoff) == 2
    assert store._delete_idle_user_states(cutoff) == 2
    assert all("RETURNING 1" in q for q in conn.queries if q.startswith("DELETE FROM adk_session"))


def test_duckdb_adk_ensure_fts_extension_loads_before_installing() -> None:
    """_ensure_fts_extension should try LOAD fts first and skip INSTALL fts when already installed."""
    store = DuckDBADKMemoryStore(_mock_config({"memory_use_fts": True}))
    executed: list[str] = []

    class _Conn:
        def execute(self, sql: str) -> None:
            executed.append(sql)

    assert store._ensure_fts_extension(_Conn()) is True
    assert executed == ["LOAD fts"]


def test_duckdb_litestar_store_uses_timestamptz_and_normalizes_utc() -> None:
    """DuckDBStore DDL and queries use TIMESTAMPTZ/CURRENT_TIMESTAMP and normalize naive timestamps to UTC."""
    store = DuckDBStore(_mock_config())
    ddl = store._table_ddl()
    assert "TIMESTAMPTZ" in ddl
    naive = datetime(2026, 1, 1, 12, 0, 0)
    converted = store._timestamp_to_datetime(naive)
    assert converted is not None
    assert converted.tzinfo == UTC


def test_typing_import() -> None:
    """DuckDB ADK store imports TypedDict from standard typing."""
    import sqlspec.adapters.duckdb.adk.store as store_mod

    assert store_mod.TypedDict.__module__ == "typing"


def test_search_entries_embedding_warning() -> None:
    """DuckDBADKMemoryStore.search_entries raises NotImplementedError if embedding is passed."""
    store = DuckDBADKMemoryStore(_mock_config())
    with pytest.raises(NotImplementedError, match="does not support embedding-based search"):
        store.search_entries("query", "app", "user", embedding=[0.1, 0.2, 0.3])


def test_schema_qualified_adk_tables_create_in_the_configured_schema() -> None:
    """Qualified session and memory DDL creates tables and indexes in DuckDB."""
    session_store = DuckDBADKStore(_mock_config({"session_table": "custom.sessions"}))
    memory_store = DuckDBADKMemoryStore(_mock_config({"memory_table": "custom.memories"}))

    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA custom")
        connection.execute(session_store._sessions_table_ddl())
        connection.execute(memory_store._memory_table_ddl())

        assert connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name", ["custom"]
        ).fetchall() == [("memories",), ("sessions",)]


def test_duckdb_adk_rejects_catalog_qualified_table_names() -> None:
    """The ADK schema helpers support schema.table, without a catalog component."""
    with pytest.raises(ValueError, match="Invalid table name"):
        DuckDBADKMemoryStore(_mock_config({"memory_table": "catalog.custom.memories"}))


@pytest.mark.parametrize(
    ("memory_table", "fts_schema", "index_prefix"),
    [
        ("adk_memory", "fts_main_adk_memory", "idx_adk_memory"),
        ("custom.my_memories", "fts_custom_my_memories", "idx_custom_my_memories"),
    ],
)
def test_schema_qualified_fts_table(
    monkeypatch: pytest.MonkeyPatch, memory_table: str, fts_schema: str, index_prefix: str
) -> None:
    """Memory tables format indexes without dots and share fts_{schema}_{table} identity."""
    store = DuckDBADKMemoryStore(_mock_config({"memory_table": memory_table}))
    ddl = store._memory_table_ddl()
    assert store._fts_schema_name() == fts_schema
    assert f"CREATE INDEX IF NOT EXISTS {index_prefix}_app_scope_user_time" in ddl
    assert f"CREATE INDEX IF NOT EXISTS {index_prefix}_scope" in ddl
    assert f"CREATE INDEX IF NOT EXISTS {index_prefix}_session" in ddl

    class _CaptureConn:
        def __init__(self) -> None:
            self.sql: str = ""

        def execute(self, sql: str, params: Any = None) -> "_CaptureConn":
            self.sql = sql
            return self

        def fetchall(self) -> list[Any]:
            return []

        @property
        def description(self) -> list[Any]:
            return []

        def __enter__(self) -> "Self":
            return self

        def __exit__(self, *_: Any) -> None:
            return None

    conn = _CaptureConn()
    config = _mock_config({"memory_table": memory_table, "memory_use_fts": True})
    config.provide_connection = lambda *_a, **_k: conn
    store_fts = DuckDBADKMemoryStore(config)
    monkeypatch.setattr(DuckDBADKMemoryStore, "_ensure_fts_extension", lambda _s, _c: True)
    store_fts.search_entries("test", "app", "user")
    assert f"{fts_schema}.match_bm25" in conn.sql


def test_duckdb_companions_restore_dropped_session_index(tmp_path: Path) -> None:
    """Companion reconciliation restores dropped DuckDB session indexes without disturbing rows."""
    db_path = tmp_path / "adk_session_companions.duckdb"
    config = DuckDBConfig(connection_config={"database": str(db_path)})
    try:
        store = DuckDBADKStore(config)
        store.create_tables()
        store.create_session("s1", "app", "user", {})

        with config.provide_session() as driver:
            driver.execute("DROP INDEX idx_adk_session_update_time")
            driver.commit()
            store.reconcile_companion_objects_sync(driver)
            store.reconcile_companion_objects_sync(driver)
            driver.commit()
            rows = driver.select("SELECT index_name FROM duckdb_indexes() WHERE table_name = ?", ("adk_session",))

        assert "idx_adk_session_update_time" in {str(row["index_name"]) for row in rows}
        session = store.get_session("app", "user", "s1")
        assert session is not None and session["id"] == "s1"
    finally:
        config.close_pool()


def test_duckdb_companions_do_not_commit(tmp_path: Path) -> None:
    """Companion reconciliation participates in the caller transaction without committing."""
    db_path = tmp_path / "adk_no_commit.duckdb"
    config = DuckDBConfig(connection_config={"database": str(db_path)})
    try:
        store = DuckDBADKStore(config)
        store.create_tables()

        with config.provide_session() as driver:
            driver.execute("DROP INDEX idx_adk_session_update_time")
            driver.commit()
            driver.begin()
            store.reconcile_companion_objects_sync(driver)
            driver.rollback()
            rows = driver.select("SELECT index_name FROM duckdb_indexes() WHERE table_name = ?", ("adk_session",))

        assert "idx_adk_session_update_time" not in {str(row["index_name"]) for row in rows}
    finally:
        config.close_pool()


@pytest.mark.parametrize(
    ("adk_config", "expected_present"),
    [
        ({"enable_event_generated_columns": True, "enable_event_generated_column_indexes": True}, True),
        ({"enable_event_generated_columns": True, "enable_event_generated_column_indexes": False}, False),
        ({"enable_event_generated_columns": False, "enable_event_generated_column_indexes": True}, False),
        ({}, False),
    ],
)
def test_duckdb_companion_statements_include_generated_indexes(
    adk_config: dict[str, object], expected_present: bool
) -> None:
    """Generated-column event index statements appear only when both flags are enabled."""
    store = DuckDBADKStore(_mock_config(adk_config))
    statements = store._companion_index_statements()

    assert any("CREATE INDEX IF NOT EXISTS idx_adk_event_author_gc" in stmt for stmt in statements) is expected_present
    assert (
        any("CREATE INDEX IF NOT EXISTS idx_adk_event_node_path_gc" in stmt for stmt in statements) is expected_present
    )


@pytest.mark.parametrize(
    ("adk_config", "expected_session_index"),
    [
        ({}, "idx_adk_session_app_user"),
        ({"session_table": "main.adk_session", "events_table": "main.adk_event"}, "idx_adk_session_app_user"),
        ({"session_table": "custom.s", "events_table": "custom.e"}, "idx_custom_s_app_user"),
    ],
)
def test_duckdb_companion_statements_match_creation_ddl(
    adk_config: dict[str, object], expected_session_index: str
) -> None:
    """Extracted session and event companion index statements match creation DDL."""
    store = DuckDBADKStore(_mock_config(adk_config))
    statements = store._companion_index_statements()
    combined_ddl = _normalized(f"{store._sessions_table_ddl()} {store._events_table_ddl()}")

    assert any(expected_session_index in stmt for stmt in statements)
    for stmt in statements:
        assert not stmt.rstrip().endswith(";")
        assert _normalized(stmt) in combined_ddl


@pytest.mark.parametrize(
    ("memory_table", "expected_scope_index"),
    [
        ("adk_memory", "idx_adk_memory_scope"),
        ("main.adk_memory", "idx_adk_memory_scope"),
        ("custom.my_memories", "idx_custom_my_memories_scope"),
    ],
)
def test_duckdb_memory_companion_statements_match_creation_ddl(memory_table: str, expected_scope_index: str) -> None:
    """Extracted memory companion index statements match creation DDL."""
    store = DuckDBADKMemoryStore(_mock_config({"memory_table": memory_table}))
    statements = store._memory_index_statements()
    ddl = _normalized(store._memory_table_ddl())

    assert any(expected_scope_index in stmt for stmt in statements)
    for stmt in statements:
        assert not stmt.rstrip().endswith(";")
        assert _normalized(stmt) in ddl


def test_duckdb_memory_companions_skip_disabled_store() -> None:
    """Disabled DuckDB memory stores skip companion execution."""
    store = DuckDBADKMemoryStore(_mock_config({"enable_memory": False}))
    driver = MagicMock()

    store.reconcile_companion_objects_sync(driver)

    assert driver.execute.call_count == 0


def test_duckdb_companions_propagate_execute_error() -> None:
    """DuckDB companion index execution propagates driver errors without committing."""
    store = DuckDBADKStore(_mock_config())
    driver = MagicMock()
    driver.execute.side_effect = RuntimeError("ddl failure")

    with pytest.raises(RuntimeError, match="ddl failure"):
        store.reconcile_companion_objects_sync(driver)

    assert driver.commit.call_count == 0


def _make_fts_driver(
    *,
    catalog_rows: list[dict[str, object]],
    extension_rows: list[dict[str, object]] | None = None,
    source_rows: list[dict[str, object]] | None = None,
    fail_on_execute: str | None = None,
) -> tuple[MagicMock, list[tuple[str, Any]], list[str]]:
    """Create a SQL-routing mock driver recording select and execute statements."""
    select_calls: list[tuple[str, Any]] = []
    executed_statements: list[str] = []
    resolved_extensions = extension_rows if extension_rows is not None else []
    resolved_source = source_rows if source_rows is not None else []

    def _select(sql: str, *params: Any) -> list[dict[str, object]]:
        bound = params[0] if params else ()
        select_calls.append((sql, bound))
        if "information_schema.schemata" in sql:
            return catalog_rows
        if "duckdb_extensions()" in sql:
            return resolved_extensions
        return resolved_source

    def _execute(sql: str, *params: Any) -> MagicMock:
        del params
        if fail_on_execute is not None and sql == fail_on_execute:
            msg = "extension unavailable"
            raise RuntimeError(msg)
        executed_statements.append(sql)
        return MagicMock()

    driver = MagicMock()
    driver.select.side_effect = _select
    driver.execute.side_effect = _execute
    return driver, select_calls, executed_statements


@pytest.mark.parametrize(
    ("memory_table", "fts_schema"),
    [("adk_memory", "fts_main_adk_memory"), ("custom.my_memories", "fts_custom_my_memories")],
)
def test_duckdb_fts_companion_keeps_present_index(memory_table: str, fts_schema: str) -> None:
    """Existing DuckDB FTS schemas remain untouched without INSTALL, LOAD, or PRAGMA calls."""
    store = DuckDBADKMemoryStore(_mock_config({"memory_table": memory_table, "memory_use_fts": True}))
    driver, select_calls, executed = _make_fts_driver(catalog_rows=[{"present": 1}], source_rows=[{"present": 1}])

    store.reconcile_companion_objects_sync(driver)

    assert len(select_calls) == 1
    assert "information_schema.schemata" in select_calls[0][0]
    assert select_calls[0][1] == (fts_schema,)
    assert len(executed) == 3
    assert not any(stmt.startswith(("INSTALL", "LOAD", "PRAGMA")) for stmt in executed)
    assert driver.commit.call_count == 0


def test_duckdb_fts_companion_loads_installed_extension() -> None:
    """Installed but unloaded DuckDB FTS extension runs LOAD fts without INSTALL fts."""
    store = DuckDBADKMemoryStore(_mock_config({"memory_table": "custom.my_memories", "memory_use_fts": True}))
    driver, _, executed = _make_fts_driver(catalog_rows=[], extension_rows=[{"loaded": False, "installed": True}])

    store.reconcile_companion_objects_sync(driver)

    assert "INSTALL fts" not in executed
    assert executed[-2:] == [
        "LOAD fts",
        "PRAGMA create_fts_index('custom.my_memories', 'id', 'content_text', stemmer='porter', stopwords='english', strip_accents=1, lower=1)",
    ]
    assert driver.commit.call_count == 0


def test_duckdb_fts_companion_installs_missing_extension() -> None:
    """Uninstalled DuckDB FTS extension runs INSTALL fts, LOAD fts, and PRAGMA in order."""
    store = DuckDBADKMemoryStore(_mock_config({"memory_table": "custom.my_memories", "memory_use_fts": True}))
    driver, _, executed = _make_fts_driver(catalog_rows=[], extension_rows=[{"loaded": False, "installed": False}])

    store.reconcile_companion_objects_sync(driver)

    assert executed[-3:] == [
        "INSTALL fts",
        "LOAD fts",
        "PRAGMA create_fts_index('custom.my_memories', 'id', 'content_text', stemmer='porter', stopwords='english', strip_accents=1, lower=1)",
    ]
    assert driver.commit.call_count == 0


def test_duckdb_fts_companion_skips_load_when_loaded() -> None:
    """Already-loaded DuckDB FTS extension skips both INSTALL fts and LOAD fts."""
    store = DuckDBADKMemoryStore(_mock_config({"memory_use_fts": True}))
    driver, _, executed = _make_fts_driver(catalog_rows=[], extension_rows=[{"loaded": True, "installed": True}])

    store.reconcile_companion_objects_sync(driver)

    assert not any(stmt.startswith(("INSTALL", "LOAD")) for stmt in executed)
    pragma_calls = [stmt for stmt in executed if stmt.startswith("PRAGMA")]
    assert len(pragma_calls) == 1
    assert driver.commit.call_count == 0


@pytest.mark.parametrize(
    ("memory_table", "fts_schema"),
    [("adk_memory", "fts_main_adk_memory"), ("custom.my_memories", "fts_custom_my_memories")],
)
def test_duckdb_fts_companion_builds_for_populated_source(memory_table: str, fts_schema: str) -> None:
    """Missing DuckDB FTS schema builds once for populated tables without overwrite."""
    store = DuckDBADKMemoryStore(_mock_config({"memory_table": memory_table, "memory_use_fts": True}))
    driver, select_calls, executed = _make_fts_driver(
        catalog_rows=[], extension_rows=[{"loaded": True, "installed": True}], source_rows=[{"present": 1}]
    )

    store.reconcile_companion_objects_sync(driver)

    assert select_calls[0][1] == (fts_schema,)
    pragma_calls = [stmt for stmt in executed if stmt.startswith("PRAGMA create_fts_index")]
    assert len(pragma_calls) == 1
    assert "overwrite" not in pragma_calls[0]


def test_duckdb_fts_companion_preserves_configured_options() -> None:
    """DuckDB FTS companion build preserves configured memory_fts_options without overwrite."""
    store = DuckDBADKMemoryStore(_mock_config({"memory_use_fts": True, "memory_fts_options": {"stemmer": "german"}}))
    driver, _, executed = _make_fts_driver(catalog_rows=[], extension_rows=[{"loaded": True, "installed": True}])

    store.reconcile_companion_objects_sync(driver)

    pragma_calls = [stmt for stmt in executed if stmt.startswith("PRAGMA create_fts_index")]
    assert len(pragma_calls) == 1
    assert "stemmer='german'" in pragma_calls[0]
    assert "overwrite" not in pragma_calls[0]


def test_duckdb_fts_companion_propagates_load_failure() -> None:
    """Failed LOAD fts propagates immediately without executing PRAGMA or commit."""
    store = DuckDBADKMemoryStore(_mock_config({"memory_use_fts": True}))
    driver, _, executed = _make_fts_driver(
        catalog_rows=[], extension_rows=[{"loaded": False, "installed": True}], fail_on_execute="LOAD fts"
    )

    with pytest.raises(RuntimeError, match="extension unavailable"):
        store.reconcile_companion_objects_sync(driver)

    assert not any(stmt.startswith("PRAGMA") for stmt in executed)
    assert driver.commit.call_count == 0


def test_duckdb_fts_companion_skips_disabled_fts() -> None:
    """Stores with memory_use_fts=False run ordinary indexes without querying schemata."""
    store = DuckDBADKMemoryStore(_mock_config({"memory_use_fts": False}))
    driver, select_calls, executed = _make_fts_driver(catalog_rows=[])

    store.reconcile_companion_objects_sync(driver)

    assert not any("information_schema.schemata" in sql for sql, _ in select_calls)
    assert len(executed) == 3


@pytest.mark.parametrize(
    ("renew_for", "expect_touch"),
    [
        pytest.param(None, False, id="none"),
        pytest.param(0, False, id="zero"),
        pytest.param(-1, False, id="negative"),
        pytest.param(timedelta(0), False, id="zero-timedelta"),
        pytest.param(30, True, id="positive-int"),
        pytest.param(timedelta(milliseconds=250), True, id="subsecond-positive"),
    ],
)
def test_duckdb_get_session_renews_only_for_positive_duration(
    tmp_path: Path, renew_for: int | timedelta | None, expect_touch: bool
) -> None:
    """DuckDB get_session updates update_time only when renew_for is positive."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "renew.duckdb")})
    try:
        store = DuckDBADKStore(config)
        store.create_tables()
        created = store.create_session("s1", "app", "u1", {"k": 1})
        baseline_time = datetime(2020, 1, 1, tzinfo=UTC)
        with config.provide_connection() as conn:
            conn.execute("UPDATE adk_session SET update_time = ? WHERE id = ?", (baseline_time, "s1"))
            conn.commit()

        fetched = store.get_session("app", "u1", "s1", renew_for=renew_for)
        assert fetched is not None
        assert fetched["id"] == created["id"]
        if expect_touch:
            assert fetched["update_time"] > baseline_time
        else:
            assert fetched["update_time"] == baseline_time
    finally:
        config.close_pool()


@pytest.mark.parametrize(
    ("event_app", "event_user", "should_succeed"),
    [
        pytest.param("app_a", "user_1", True, id="owning-pair"),
        pytest.param("app_b", "user_1", False, id="wrong-app"),
        pytest.param("app_a", "user_2", False, id="wrong-user"),
    ],
)
def test_duckdb_append_event_enforces_session_ownership(
    tmp_path: Path, event_app: str, event_user: str, should_succeed: bool
) -> None:
    """append_event writes an event only when (app_name, user_id) owns session_id."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "append_scope.duckdb")})
    try:
        store = DuckDBADKStore(config)
        store.create_tables()
        store.create_session("s1", "app_a", "user_1", {})
        event = {
            "id": "e1",
            "app_name": event_app,
            "user_id": event_user,
            "session_id": "s1",
            "invocation_id": "inv1",
            "timestamp": datetime.now(UTC),
            "event_data": {"msg": "hello"},
        }
        if should_succeed:
            store.append_event(cast("Any", event))
            events = store.get_events("app_a", "user_1", "s1")
            assert [item["id"] for item in events] == ["e1"]
        else:
            with pytest.raises(ValueError, match="Session s1 not found"):
                store.append_event(cast("Any", event))
            with config.provide_connection() as conn:
                count = conn.execute("SELECT COUNT(*) FROM adk_event").fetchone()
            assert count == (0,)
    finally:
        config.close_pool()


def test_duckdb_append_event_and_update_state_validates_scope_and_orders_update_first(tmp_path: Path) -> None:
    """append_event_and_update_state rejects mismatched scope before connecting and leaves events empty on missing session."""
    store_mock, conn_mock = _session_store_with_connection()
    mismatched_event = {
        "id": "e1",
        "app_name": "other_app",
        "user_id": "u1",
        "session_id": "s1",
        "invocation_id": "inv1",
        "timestamp": datetime.now(UTC),
        "event_data": {},
    }
    with pytest.raises(ValueError, match="does not match target"):
        store_mock.append_event_and_update_state(cast("Any", mismatched_event), "app", "u1", "s1", {"x": 1})
    assert conn_mock.calls == []

    config = DuckDBConfig(connection_config={"database": str(tmp_path / "atomic_state.duckdb")})
    try:
        store = DuckDBADKStore(config)
        store.create_tables()
        valid_event = {
            "id": "e1",
            "app_name": "app",
            "user_id": "u1",
            "session_id": "missing_session",
            "invocation_id": "inv1",
            "timestamp": datetime.now(UTC),
            "event_data": {"ok": True},
        }
        with pytest.raises(ValueError, match="Session missing_session not found"):
            store.append_event_and_update_state(cast("Any", valid_event), "app", "u1", "missing_session", {"x": 1})
        with config.provide_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM adk_event").fetchone()
        assert count == (0,)
    finally:
        config.close_pool()


def test_duckdb_delete_session_is_scoped_and_rolls_back_on_failure(tmp_path: Path) -> None:
    """delete_session leaves another user's events untouched and rolls back event deletion if session delete fails."""
    config = DuckDBConfig(connection_config={"database": str(tmp_path / "delete_tx.duckdb")})
    try:
        store = DuckDBADKStore(config)
        store.create_tables()
        store.create_session("s1", "app", "user_b", {"owner": "b"})
        now = datetime.now(UTC)
        store.append_event({
            "id": "e_b",
            "app_name": "app",
            "user_id": "user_b",
            "session_id": "s1",
            "invocation_id": "inv_b",
            "timestamp": now,
            "event_data": {"text": "keep"},
        })
        with config.provide_connection() as conn:
            conn.execute(
                "INSERT INTO adk_event (id, app_name, user_id, session_id, invocation_id, timestamp, event_data) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("e_other_owner", "app", "user_a", "s1", "inv_a", now, '{"text":"mismatched"}'),
            )
            conn.commit()

        assert [item["id"] for item in store.get_events("app", "user_b", "s1")] == ["e_b"]

        store.delete_session("app", "user_a", "s1")
        assert store.get_session("app", "user_b", "s1") is not None
        assert [item["id"] for item in store.get_events("app", "user_b", "s1")] == ["e_b"]

        with config.provide_connection() as conn:
            conn.execute("CREATE TABLE session_pin (sid VARCHAR REFERENCES adk_session(id))")
            conn.execute("INSERT INTO session_pin VALUES ('s1')")
            conn.commit()

        with pytest.raises(Exception):
            store.delete_session("app", "user_b", "s1")

        assert [item["id"] for item in store.get_events("app", "user_b", "s1")] == ["e_b"]
    finally:
        config.close_pool()


@pytest.mark.parametrize(
    ("second_app", "second_user", "expected_second_inserted", "expected_total"),
    [
        pytest.param("app_a", "user_1", 0, 1, id="same-owner-duplicate"),
        pytest.param("app_b", "user_1", 1, 2, id="cross-app-same-event-id"),
        pytest.param("app_a", "user_2", 1, 2, id="cross-user-same-event-id"),
    ],
)
def test_duckdb_memory_deduplicates_by_app_user_and_event_id(
    tmp_path: Path, second_app: str, second_user: str, expected_second_inserted: int, expected_total: int
) -> None:
    """Memory inserts deduplicate on (app_name, user_id, event_id) and allow cross-owner reuse."""
    config = DuckDBConfig(
        connection_config={"database": str(tmp_path / "mem_dedup.duckdb")},
        extension_config={"adk": {"enable_memory": True}},
    )
    try:
        mem_store = DuckDBADKMemoryStore(config)
        mem_store.create_tables()
        now = datetime.now(UTC)
        first = {
            "id": "m1",
            "session_id": "s1",
            "app_name": "app_a",
            "user_id": "user_1",
            "scope": "user",
            "event_id": "shared_evt",
            "author": "agent",
            "timestamp": now,
            "content_json": {"text": "alpha"},
            "content_text": "alpha",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": None,
        }
        second = {**first, "id": "m2", "app_name": second_app, "user_id": second_user}
        assert mem_store.insert_memory_entries([cast("Any", first)]) == 1
        assert mem_store.insert_memory_entries([cast("Any", second)]) == expected_second_inserted
        with config.provide_connection() as conn:
            total = conn.execute("SELECT COUNT(*) FROM adk_memory").fetchone()
        assert total == (expected_total,)
    finally:
        config.close_pool()


@pytest.mark.parametrize(
    ("filter_app", "filter_user", "expected_deleted", "expected_remaining_ids"),
    [
        pytest.param(None, None, 3, [], id="unscoped-session"),
        pytest.param("app_a", None, 2, ["m3"], id="app-scoped"),
        pytest.param("app_a", "user_1", 1, ["m2", "m3"], id="app-and-user-scoped"),
    ],
)
def test_duckdb_delete_entries_by_session_scoping(
    tmp_path: Path,
    filter_app: str | None,
    filter_user: str | None,
    expected_deleted: int,
    expected_remaining_ids: list[str],
) -> None:
    """delete_entries_by_session supports optional app_name and user_id scoping."""
    config = DuckDBConfig(
        connection_config={"database": str(tmp_path / "mem_del.duckdb")},
        extension_config={"adk": {"enable_memory": True}},
    )
    try:
        mem_store = DuckDBADKMemoryStore(config)
        mem_store.create_tables()
        now = datetime.now(UTC)
        base = {
            "session_id": "s1",
            "scope": "user",
            "author": "agent",
            "timestamp": now,
            "content_json": {"text": "note"},
            "content_text": "note",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": None,
        }
        entries = [
            {**base, "id": "m1", "app_name": "app_a", "user_id": "user_1", "event_id": "e1"},
            {**base, "id": "m2", "app_name": "app_a", "user_id": "user_2", "event_id": "e2"},
            {**base, "id": "m3", "app_name": "app_b", "user_id": "user_1", "event_id": "e3"},
        ]
        assert mem_store.insert_memory_entries(cast("Any", entries)) == 3
        deleted = mem_store.delete_entries_by_session("s1", app_name=filter_app, user_id=filter_user)
        assert deleted == expected_deleted
        with config.provide_connection() as conn:
            remaining = [row[0] for row in conn.execute("SELECT id FROM adk_memory ORDER BY id").fetchall()]
        assert remaining == expected_remaining_ids
    finally:
        config.close_pool()


def test_duckdb_fts_search_isolates_applications(tmp_path: Path) -> None:
    """DuckDB FTS subquery scopes match_bm25 to the target app_name."""
    captured_calls: list[tuple[str, tuple[Any, ...]]] = []

    class _RecordingFtsConn:
        description = [("id",)]

        def execute(self, sql: str, params: Any = ()) -> "_RecordingFtsConn":
            captured_calls.append((_normalized(sql), tuple(params)))
            return self

        def fetchall(self) -> list[Any]:
            return []

        def __enter__(self) -> "Self":
            return self

        def __exit__(self, *_: Any) -> None:
            return None

    mock_cfg = _mock_config({"enable_memory": True, "memory_use_fts": True})
    mock_cfg.provide_connection = lambda *_a, **_k: _RecordingFtsConn()
    fts_store = DuckDBADKMemoryStore(mock_cfg)
    fts_store.search_entries("quarterly", "app_a", "user_1", limit=5)

    fts_queries = [(sql, params) for sql, params in captured_calls if "match_bm25" in sql]
    assert len(fts_queries) == 1
    fts_sql, fts_params = fts_queries[0]
    assert "FROM adk_memory WHERE app_name = ? ) fts ON m.id = fts.id" in fts_sql
    assert fts_params == ("quarterly", "app_a", "app_a", "user_1", 5)

    config = DuckDBConfig(
        connection_config={"database": str(tmp_path / "fts_isolation.duckdb")},
        extension_config={"adk": {"enable_memory": True, "memory_use_fts": True}},
    )
    try:
        live_store = DuckDBADKMemoryStore(config)
        live_store.create_tables()
        now = datetime.now(UTC)
        entries = [
            {
                "id": "m_a",
                "session_id": "s1",
                "app_name": "app_a",
                "user_id": "user_1",
                "scope": "user",
                "event_id": "e1",
                "author": "agent",
                "timestamp": now,
                "content_json": {"text": "zephyr forecast"},
                "content_text": "zephyr forecast",
                "metadata_json": None,
                "inserted_at": now,
                "embedding": None,
            },
            {
                "id": "m_b",
                "session_id": "s1",
                "app_name": "app_b",
                "user_id": "user_1",
                "scope": "user",
                "event_id": "e1",
                "author": "agent",
                "timestamp": now,
                "content_json": {"text": "zephyr confidential"},
                "content_text": "zephyr confidential",
                "metadata_json": None,
                "inserted_at": now,
                "embedding": None,
            },
        ]
        assert live_store.insert_memory_entries(cast("Any", entries)) == 2
        results = live_store.search_entries("zephyr", "app_a", "user_1", limit=10)
        assert [row["id"] for row in results] == ["m_a"]
    finally:
        config.close_pool()
