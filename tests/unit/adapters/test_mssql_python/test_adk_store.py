# pyright: reportPrivateUsage=false
"""Unit tests for mssql-python ADK store wiring and T-SQL generation."""

from datetime import UTC, datetime, timedelta
from typing import Any, NotRequired, cast, get_args, get_origin
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.mssql_python import MssqlPythonConfig
from sqlspec.adapters.mssql_python.adk import MssqlPythonADKConfig, MssqlPythonADKMemoryStore, MssqlPythonADKStore
from sqlspec.adapters.mssql_python.data_dictionary import MssqlVersionInfo
from sqlspec.config import ADKConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.adk import BaseSyncADKStore, StoredEvent
from sqlspec.extensions.adk.memory.store import BaseSyncADKMemoryStore


def test_mssql_python_adk_exports_sync_store_type() -> None:
    """The adapter exposes a sync store that implements the current ADK base."""

    sync_store = MssqlPythonADKStore(_mock_config())

    assert isinstance(sync_store, BaseSyncADKStore)


def test_mssql_python_adk_config_extends_base_config_without_redeclaring_base_fields() -> None:
    """Adapter-local ADK config should only add MSSQL-specific fields."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", MssqlPythonADKConfig).__optional_keys__

    base_fields = set(ADKConfig.__annotations__)
    local_fields = set(MssqlPythonADKConfig.__annotations__) - base_fields
    assert local_fields == {"native_json"}
    annotation = cast("Any", MssqlPythonADKConfig.__annotations__["native_json"])
    assert get_origin(annotation) is NotRequired
    assert get_args(annotation) == (bool,)


def test_sync_store_reads_adk_table_names_from_extension_config() -> None:
    """Store table names come from extension_config['adk'], not driver features."""

    store = MssqlPythonADKStore(
        _mock_config({
            "session_table": "custom_session",
            "events_table": "custom_event",
            "app_state_table": "custom_app_state",
            "user_state_table": "custom_user_state",
            "metadata_table": "custom_metadata",
        })
    )

    assert store.session_table == "custom_session"
    assert store.events_table == "custom_event"
    assert store.app_state_table == "custom_app_state"
    assert store.user_state_table == "custom_user_state"
    assert store.metadata_table == "custom_metadata"


def test_sync_store_generates_tsql_idempotent_schema_with_conservative_json() -> None:
    """The default MSSQL DDL uses idempotent sys.tables probes and NVARCHAR JSON storage."""

    store = MssqlPythonADKStore(_mock_config())

    sessions_sql = store._sessions_table_ddl()
    events_sql = store._events_table_ddl()
    app_state_sql = store._app_states_table_ddl()

    assert "IF NOT EXISTS (SELECT 1 FROM sys.tables" in sessions_sql
    assert "schema_id = SCHEMA_ID(N'dbo')" in sessions_sql
    assert "row_id UNIQUEIDENTIFIER NOT NULL" in sessions_sql
    assert "DEFAULT NEWSEQUENTIALID()" in sessions_sql
    assert "state NVARCHAR(MAX) NOT NULL" in sessions_sql
    assert "DATETIME2(6)" in sessions_sql
    assert "sys.indexes" not in sessions_sql
    assert "ON DELETE CASCADE" in events_sql
    assert "event_data NVARCHAR(MAX) NOT NULL" in events_sql
    assert "state NVARCHAR(MAX) NOT NULL" in app_state_sql


def test_sync_store_can_force_native_json_from_extension_config() -> None:
    """MSSQL-native JSON requires an explicit override."""

    store = MssqlPythonADKStore(_mock_config({"native_json": True}))

    assert "state JSON NOT NULL" in store._sessions_table_ddl()
    assert "event_data JSON NOT NULL" in store._events_table_ddl()


def test_sync_store_uses_tsql_upsert_and_top_limit() -> None:
    """Write/read SQL uses MERGE for scoped upserts and TOP for limited reads."""

    store = MssqlPythonADKStore(_mock_config())

    app_upsert = store._upsert_app_state_sql()
    events_sql, params = store._events_query("app", "user", "session", limit=5)

    assert "MERGE INTO [dbo].[adk_app_state]" in app_upsert
    assert "WITH (HOLDLOCK)" in app_upsert
    assert "SELECT TOP (?)" in events_sql
    assert (
        "JOIN [dbo].[adk_session] s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id"
        in _normalized(events_sql)
    )
    assert params == (5, "app", "user", "session")
    assert "LIMIT" not in events_sql


def test_mssql_python_list_sessions_binds_order_and_page(monkeypatch: "pytest.MonkeyPatch") -> None:
    """SQL Server row limiting binds the offset before the row count."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM [dbo].[adk_session] WHERE app_name = ? AND user_id = ? "
        "ORDER BY create_time ASC, id ASC "
        "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
    )
    assert params == ("app", "u1", 20, 10)


def test_mssql_python_list_sessions_defaults_to_recent_first_without_a_page(monkeypatch: "pytest.MonkeyPatch") -> None:
    """The default listing keeps recent-first ordering and emits no row limiting."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app")

    sql, params = calls[0]
    assert _normalized(sql).endswith("WHERE app_name = ? ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_mssql_python_list_sessions_pages_an_unfiltered_listing(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Row limiting composes with an app-only listing."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app", limit=5)

    sql, params = calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY")
    assert params == ("app", 0, 5)


def test_mssql_python_list_sessions_zero_limit_never_queries(monkeypatch: "pytest.MonkeyPatch") -> None:
    """A zero limit short-circuits before any database work."""
    store, calls = _session_store_with_capture(monkeypatch)

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
def test_mssql_python_list_sessions_rejects_invalid_options(
    monkeypatch: "pytest.MonkeyPatch", options: "dict[str, Any]"
) -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, calls = _session_store_with_capture(monkeypatch)

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert calls == []


def test_mssql_python_config_accepts_default_adk_extension_config() -> None:
    """The default ADK extension config resolves both session and memory stores."""
    config = MssqlPythonConfig(
        connection_config={"server": "localhost", "database": "app", "user": "u", "password": "p"},
        extension_config={"adk": {}},
    )

    assert config.extension_config["adk"] == {}


def test_mssql_python_adk_memory_store_instance() -> None:
    """MssqlPythonADKMemoryStore satisfies BaseSyncADKMemoryStore with default table name."""
    store = MssqlPythonADKMemoryStore(_mock_config())
    assert isinstance(store, BaseSyncADKMemoryStore)
    assert store.memory_table == "adk_memory"


def test_mssql_python_adk_memory_store_ddl() -> None:
    """Memory table DDL contains expected T-SQL column and constraint definitions."""
    store = MssqlPythonADKMemoryStore(_mock_config())
    ddl = store._memory_table_ddl()
    assert "IF NOT EXISTS (SELECT 1 FROM sys.tables" in ddl
    assert "content_text NVARCHAR(MAX) NOT NULL" in ddl
    assert "inserted_at DATETIME2(6) NOT NULL" in ddl
    assert "UNIQUE (app_name, user_id, event_id)" in ddl


def test_mssql_python_adk_memory_store_search_entries(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Search entries generates TOP (?) and LIKE ? queries with expected parameter bindings."""
    calls: list[tuple[str, tuple[Any, ...]]] = []

    def capture(_store: MssqlPythonADKMemoryStore, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        calls.append((sql, tuple(params)))
        return []

    config = _mock_config()
    _capture_cursor(config, capture)
    store = MssqlPythonADKMemoryStore(config)
    results = store.search_entries("latte", "app", "u1", limit=5, scope_filter="user")

    assert results == []
    assert len(calls) == 1
    sql, params = calls[0]
    normalized_sql = _normalized(sql)
    assert normalized_sql.startswith("SELECT TOP (?)")
    assert "content_text LIKE ?" in normalized_sql
    assert params == (5, "app", "u1", "%latte%")


def test_mssql_python_adk_memory_store_drop_table_sql() -> None:
    """Drop memory table statement uses T-SQL IF EXISTS syntax."""
    store = MssqlPythonADKMemoryStore(_mock_config())
    assert store._drop_memory_table_sql() == ["DROP TABLE IF EXISTS [dbo].[adk_memory]"]


@pytest.mark.parametrize(("major", "expected_type"), [(16, "NVARCHAR(MAX)"), (17, "JSON")])
def test_sync_store_defaults_to_driver_supported_json_storage(major: int, expected_type: str) -> None:
    """When native_json is unset, DDL lazily queries server version for JSON column support."""
    config = _mock_config()
    driver = config.provide_session.return_value.__enter__.return_value
    driver.data_dictionary.get_version.return_value = MssqlVersionInfo(major=major)
    store = MssqlPythonADKStore(config)

    assert f"state {expected_type} NOT NULL" in store._sessions_table_ddl()
    config.provide_session.assert_called_once()


def test_sync_store_can_force_fallback_json_from_extension_config() -> None:
    """Explicit native_json=False uses NVARCHAR(MAX) without opening a session."""
    config = _mock_config({"native_json": False})
    store = MssqlPythonADKStore(config)

    assert "state NVARCHAR(MAX) NOT NULL" in store._sessions_table_ddl()
    config.provide_session.assert_not_called()


def test_mssql_python_adk_memory_store_insert_handles_none_metadata_and_missing_author() -> None:
    """insert_memory_entries binds None for metadata_json=None and missing author."""
    config = _mock_config()
    conn = config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.rowcount = 1
    store = MssqlPythonADKMemoryStore(config)
    now = datetime.now(tz=UTC)
    entry = cast(
        "Any",
        {
            "id": "mem-1",
            "session_id": "sess-1",
            "app_name": "app",
            "user_id": "user-1",
            "event_id": "evt-1",
            "timestamp": now,
            "content_json": {"text": "hello"},
            "content_text": "hello",
            "metadata_json": None,
            "inserted_at": now,
        },
    )

    inserted = store.insert_memory_entries([entry])

    assert inserted == 1
    params = cursor.executemany.call_args.args[1][0]
    assert params[6] is None
    assert params[10] is None


def test_disabled_memory_store_rejects_operations_without_connecting() -> None:
    config = _mock_config({"enable_memory": False})
    store = MssqlPythonADKMemoryStore(config)
    store.create_tables()
    with pytest.raises(RuntimeError, match="disabled"):
        store.insert_memory_entries([])
    with pytest.raises(RuntimeError, match="disabled"):
        store.search_entries("query", "app", "user")
    config.provide_connection.assert_not_called()
    config.provide_session.assert_not_called()


def test_mssql_python_adk_search_entries_escapes_tsql_like_wildcards(monkeypatch: "pytest.MonkeyPatch") -> None:
    """search_entries must escape \\, %, _, and [ in LIKE patterns and include ESCAPE N'\\\\'."""
    calls: list[tuple[str, tuple[Any, ...]]] = []

    def capture(_store: MssqlPythonADKMemoryStore, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        calls.append((sql, tuple(params)))
        return []

    config = _mock_config()
    _capture_cursor(config, capture)
    store = MssqlPythonADKMemoryStore(config)
    store.search_entries(r"50%_off[1]\test", "app", "u1", limit=5)

    sql, params = calls[0]
    assert "LIKE ? ESCAPE N'\\'" in _normalized(sql)
    assert params[-1] == r"%50\%\_off\[1]\\test%"


def test_mssql_python_adk_store_handles_dict_rows_and_schema_qualified_tables(
    monkeypatch: "pytest.MonkeyPatch",
) -> None:
    """ADK store should support dict rows and split schema.table for DDL and constraints."""
    store = MssqlPythonADKStore(_mock_config({"native_json": False}))
    store._session_table = "custom.adk_sessions"
    store._events_table = "custom.adk_events"
    sessions_ddl = store._sessions_table_ddl()
    events_ddl = store._events_table_ddl()
    assert "SCHEMA_ID(N'custom')" in sessions_ddl
    assert "CREATE TABLE [custom].[adk_sessions]" in sessions_ddl
    assert "CONSTRAINT [pk_adk_sessions_row_id] PRIMARY KEY (row_id)" in sessions_ddl
    assert "REFERENCES [custom].[adk_sessions](id)" in events_ddl

    now = datetime.now(tz=UTC)
    dict_row = {
        "id": "s1",
        "app_name": "app",
        "user_id": "u1",
        "state": '{"k": "v"}',
        "create_time": now,
        "update_time": now,
    }
    monkeypatch.setattr(MssqlPythonADKStore, "_execute_fetchone", lambda *_a, **_kw: dict_row)
    session = store.get_session("app", "u1", "s1")
    assert session is not None
    assert session["id"] == "s1"
    assert session["state"] == {"k": "v"}


def test_mssql_python_adk_create_tables_reconciles_schema_and_memory_insert_rolls_back_on_error(
    monkeypatch: "pytest.MonkeyPatch",
) -> None:
    """create_tables calls reconcile_schema(assume_existing=True) and insert_memory_entries rolls back on error."""
    config = _mock_config({"native_json": False})
    store = MssqlPythonADKStore(config)
    reconciled: list[bool] = []
    monkeypatch.setattr(
        MssqlPythonADKStore,
        "reconcile_schema",
        lambda _self, *, assume_existing=False: reconciled.append(assume_existing),
    )
    store.create_tables()
    assert reconciled == [True]

    mem_config = _mock_config({"native_json": False})
    conn = mem_config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.execute.side_effect = RuntimeError("db failure")
    cursor.executemany.side_effect = RuntimeError("db failure")
    mem_store = MssqlPythonADKMemoryStore(mem_config)
    now = datetime.now(tz=UTC)
    entry = cast(
        "Any",
        {
            "id": "mem-1",
            "session_id": "sess-1",
            "app_name": "app",
            "user_id": "user-1",
            "event_id": "evt-1",
            "timestamp": now,
            "content_json": {"text": "hello"},
            "content_text": "hello",
            "metadata_json": None,
            "inserted_at": now,
        },
    )
    with pytest.raises(RuntimeError, match="db failure"):
        mem_store.insert_memory_entries([entry])
    conn.rollback.assert_called_once()


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _session_store_with_capture(
    monkeypatch: "pytest.MonkeyPatch",
) -> "tuple[MssqlPythonADKStore, list[tuple[str, tuple[Any, ...]]]]":
    calls: list[tuple[str, tuple[Any, ...]]] = []

    def capture(_store: MssqlPythonADKStore, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        calls.append((sql, tuple(params)))
        return []

    config = _mock_config()
    _capture_cursor(config, capture)
    return MssqlPythonADKStore(config), calls


def test_insert_memory_entries_batches_with_executemany() -> None:
    """insert_memory_entries batches rows into cursor.executemany."""
    mem_config = _mock_config({"native_json": False})
    conn = mem_config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.rowcount = 2
    mem_store = MssqlPythonADKMemoryStore(mem_config)
    now = datetime.now(tz=UTC)
    entries = [
        cast(
            "Any",
            {
                "id": f"mem-{i}",
                "session_id": "sess-1",
                "app_name": "app",
                "user_id": "user-1",
                "event_id": f"evt-{i}",
                "timestamp": now,
                "content_json": {"text": "hello"},
                "content_text": "hello",
                "metadata_json": None,
                "inserted_at": now,
            },
        )
        for i in range(2)
    ]
    inserted = mem_store.insert_memory_entries(entries)
    assert inserted == 2
    cursor.executemany.assert_called_once()
    assert len(cursor.executemany.call_args[0][1]) == 2


def test_search_entries_raises_improper_configuration_on_embedding() -> None:
    """search_entries raises ImproperConfigurationError when embedding is passed."""
    mem_config = _mock_config()
    mem_store = MssqlPythonADKMemoryStore(mem_config)
    with pytest.raises(ImproperConfigurationError, match="Vector embeddings are not configured"):
        mem_store.search_entries("test", "app", "user", embedding=[0.1, 0.2])


def _capture_cursor(config: MagicMock, capture: Any) -> None:
    cursor = config.provide_connection.return_value.__enter__.return_value.cursor.return_value
    cursor.execute.side_effect = lambda sql, params=(): capture(None, sql, params)
    cursor.fetchall.return_value = []


def _event_fixture(**overrides: Any) -> StoredEvent:
    payload: dict[str, Any] = {
        "id": "evt-1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "s1",
        "invocation_id": "inv-1",
        "timestamp": datetime(2026, 1, 1, tzinfo=UTC),
        "event_data": {"text": "hello"},
    }
    payload.update(overrides)
    return cast("StoredEvent", payload)


@pytest.mark.parametrize(
    ("renew_for", "expect_renew"),
    [
        pytest.param(None, False, id="none"),
        pytest.param(0, False, id="zero"),
        pytest.param(-1, False, id="negative"),
        pytest.param(timedelta(0), False, id="zero-timedelta"),
        pytest.param(timedelta(milliseconds=250), True, id="subsecond-timedelta"),
        pytest.param(30, True, id="positive-int"),
    ],
)
def test_mssql_python_get_session_renews_only_for_positive_duration(
    monkeypatch: "pytest.MonkeyPatch", renew_for: "int | timedelta | None", expect_renew: bool
) -> None:
    """get_session executes an UPDATE only when renew_for is strictly positive."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    writes: list[tuple[str, tuple[Any, ...]]] = []

    def capture_execute(_self: MssqlPythonADKStore, sql: str, params: "tuple[Any, ...]" = (), **_kw: Any) -> int:
        writes.append((_normalized(sql), tuple(params)))
        return 1

    monkeypatch.setattr(MssqlPythonADKStore, "_execute", capture_execute)
    monkeypatch.setattr(
        MssqlPythonADKStore, "_execute_fetchone", lambda *_a, **_kw: ("s1", "app", "u1", '{"step":1}', now, now)
    )
    store = MssqlPythonADKStore(_mock_config())

    session = store.get_session("app", "u1", "s1", renew_for=renew_for)

    assert session is not None
    assert session["id"] == "s1"
    if expect_renew:
        assert writes == [
            (
                "UPDATE [dbo].[adk_session] SET update_time = SYSUTCDATETIME() WHERE app_name = ? AND user_id = ? AND id = ?",
                ("app", "u1", "s1"),
            )
        ]
    else:
        assert writes == []


@pytest.mark.parametrize("session_exists", [True, False], ids=["owned-session", "missing-or-unowned"])
def test_mssql_python_append_event_verifies_session_ownership(session_exists: bool) -> None:
    """append_event probes session ownership before inserting and rolls back when unowned."""
    config = _mock_config()
    conn = config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.fetchone.return_value = (1,) if session_exists else None
    store = MssqlPythonADKStore(config)
    event = _event_fixture()

    if session_exists:
        store.append_event(event)
        assert cursor.execute.call_count == 2
        ownership_sql, ownership_params = cursor.execute.call_args_list[0].args
        insert_sql, _ = cursor.execute.call_args_list[1].args
        assert _normalized(ownership_sql) == (
            "SELECT TOP (1) 1 FROM [dbo].[adk_session] WHERE id = ? AND app_name = ? AND user_id = ?"
        )
        assert ownership_params == ("s1", "app", "u1")
        assert _normalized(insert_sql).startswith("INSERT INTO [dbo].[adk_event]")
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
    else:
        with pytest.raises(ValueError, match="Session s1 not found for app='app', user='u1'"):
            store.append_event(event)
        assert cursor.execute.call_count == 1
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"app_name": "other"}, id="app-mismatch"),
        pytest.param({"user_id": "other"}, id="user-mismatch"),
        pytest.param({"session_id": "other"}, id="session-mismatch"),
    ],
)
def test_mssql_python_append_event_and_update_state_rejects_mismatched_scope(overrides: "dict[str, str]") -> None:
    """Mismatched event scope fails before acquiring a connection."""
    config = _mock_config()
    store = MssqlPythonADKStore(config)

    with pytest.raises(ValueError, match="does not match"):
        store.append_event_and_update_state(_event_fixture(**overrides), "app", "u1", "s1", {"step": 2})

    config.provide_connection.assert_not_called()


@pytest.mark.parametrize("session_exists", [True, False], ids=["owned-session", "missing-session"])
def test_mssql_python_append_event_and_update_state_updates_session_before_insert(session_exists: bool) -> None:
    """append_event_and_update_state runs the scoped session UPDATE before the event INSERT."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    config = _mock_config()
    conn = config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.fetchone.return_value = ("s1", "app", "u1", '{"step":2}', now, now) if session_exists else None
    store = MssqlPythonADKStore(config)

    if session_exists:
        session = store.append_event_and_update_state(_event_fixture(), "app", "u1", "s1", {"step": 2})
        assert session["state"] == {"step": 2}
        assert cursor.execute.call_count == 2
        assert _normalized(cursor.execute.call_args_list[0].args[0]).startswith("UPDATE [dbo].[adk_session]")
        assert _normalized(cursor.execute.call_args_list[1].args[0]).startswith("INSERT INTO [dbo].[adk_event]")
        conn.commit.assert_called_once()
    else:
        with pytest.raises(ValueError, match="Session s1 not found"):
            store.append_event_and_update_state(_event_fixture(), "app", "u1", "s1", {"step": 2})
        assert cursor.execute.call_count == 1
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()


def test_mssql_python_delete_session_deletes_scoped_events_before_session() -> None:
    """delete_session removes owner-scoped events before deleting the session row."""
    config = _mock_config()
    conn = config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    store = MssqlPythonADKStore(config)

    store.delete_session("app", "u1", "s1")

    executed = [(_normalized(call.args[0]), call.args[1]) for call in cursor.execute.call_args_list]
    assert executed == [
        ("DELETE FROM [dbo].[adk_event] WHERE app_name = ? AND user_id = ? AND session_id = ?", ("app", "u1", "s1")),
        ("DELETE FROM [dbo].[adk_session] WHERE app_name = ? AND user_id = ? AND id = ?", ("app", "u1", "s1")),
    ]
    conn.commit.assert_called_once()


@pytest.mark.parametrize(
    ("app_name", "user_id", "expected_sql", "expected_params"),
    [
        pytest.param(None, None, "DELETE FROM [dbo].[adk_memory] WHERE session_id = ?", ("s1",), id="session-only"),
        pytest.param(
            "app",
            None,
            "DELETE FROM [dbo].[adk_memory] WHERE session_id = ? AND app_name = ?",
            ("s1", "app"),
            id="session-and-app",
        ),
        pytest.param(
            None,
            "u1",
            "DELETE FROM [dbo].[adk_memory] WHERE session_id = ? AND user_id = ?",
            ("s1", "u1"),
            id="session-and-user",
        ),
        pytest.param(
            "app",
            "u1",
            "DELETE FROM [dbo].[adk_memory] WHERE session_id = ? AND app_name = ? AND user_id = ?",
            ("s1", "app", "u1"),
            id="session-app-and-user",
        ),
    ],
)
def test_mssql_python_memory_dedup_and_scoped_delete_entries_by_session(
    monkeypatch: "pytest.MonkeyPatch",
    app_name: "str | None",
    user_id: "str | None",
    expected_sql: str,
    expected_params: "tuple[str, ...]",
) -> None:
    """Memory inserts deduplicate on (app_name, user_id, event_id) and deletes honor owner filters."""
    insert_config = _mock_config({"native_json": False})
    conn = insert_config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.rowcount = 1
    now = datetime(2026, 1, 1, tzinfo=UTC)
    entry = cast(
        "Any",
        {
            "id": "mem-1",
            "session_id": "s1",
            "app_name": "app",
            "user_id": "u1",
            "event_id": "evt-1",
            "timestamp": now,
            "content_json": {"text": "hello"},
            "content_text": "hello",
            "metadata_json": None,
            "inserted_at": now,
        },
    )
    MssqlPythonADKMemoryStore(insert_config).insert_memory_entries([entry])
    insert_sql, batch_params = cursor.executemany.call_args.args
    assert (
        "WHERE NOT EXISTS ( SELECT 1 FROM [dbo].[adk_memory] WITH (UPDLOCK, HOLDLOCK) "
        "WHERE app_name = ? AND user_id = ? AND event_id = ? );"
    ) in _normalized(insert_sql)
    assert batch_params[0][-3:] == ("app", "u1", "evt-1")

    delete_calls: list[tuple[str, tuple[Any, ...]]] = []

    def capture_delete(_self: MssqlPythonADKMemoryStore, sql: str, params: "tuple[Any, ...]" = (), **_kw: Any) -> int:
        delete_calls.append((_normalized(sql), tuple(params)))
        return 2

    monkeypatch.setattr(MssqlPythonADKMemoryStore, "_execute", capture_delete)
    deleted = MssqlPythonADKMemoryStore(_mock_config()).delete_entries_by_session(
        "s1", app_name=app_name, user_id=user_id
    )
    assert deleted == 2
    assert delete_calls == [(expected_sql, expected_params)]
