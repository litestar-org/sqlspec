# pyright: reportPrivateUsage=false
"""Unit tests for pymssql ADK store session listing."""

from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.pymssql.adk import PymssqlADKMemoryStore, PymssqlADKStore
from sqlspec.extensions.adk import StoredEvent


def test_pymssql_list_sessions_binds_order_and_page(monkeypatch: "pytest.MonkeyPatch") -> None:
    """SQL Server row limiting binds the offset before the row count."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app", "u1", order_by="create_time", descending=False, limit=10, offset=20)

    sql, params = calls[0]
    assert _normalized(sql) == (
        "SELECT id, app_name, user_id, state, create_time, update_time "
        "FROM [dbo].[adk_session] WHERE app_name = %s AND user_id = %s "
        "ORDER BY create_time ASC, id ASC "
        "OFFSET %s ROWS FETCH NEXT %s ROWS ONLY"
    )
    assert params == ("app", "u1", 20, 10)


def test_pymssql_list_sessions_defaults_to_recent_first_without_a_page(monkeypatch: "pytest.MonkeyPatch") -> None:
    """The default listing keeps recent-first ordering and emits no row limiting."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app")

    sql, params = calls[0]
    assert _normalized(sql).endswith("WHERE app_name = %s ORDER BY update_time DESC, id DESC")
    assert params == ("app",)


def test_pymssql_list_sessions_pages_an_unfiltered_listing(monkeypatch: "pytest.MonkeyPatch") -> None:
    """Row limiting composes with an app-only listing."""
    store, calls = _session_store_with_capture(monkeypatch)

    store.list_sessions("app", limit=5)

    sql, params = calls[0]
    assert _normalized(sql).endswith("ORDER BY update_time DESC, id DESC OFFSET %s ROWS FETCH NEXT %s ROWS ONLY")
    assert params == ("app", 0, 5)


def test_pymssql_list_sessions_zero_limit_never_queries(monkeypatch: "pytest.MonkeyPatch") -> None:
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
def test_pymssql_list_sessions_rejects_invalid_options(
    monkeypatch: "pytest.MonkeyPatch", options: "dict[str, Any]"
) -> None:
    """Invalid ordering or paging fails before a connection is acquired."""
    store, calls = _session_store_with_capture(monkeypatch)

    with pytest.raises(ValueError):
        store.list_sessions("app", **options)

    assert calls == []


def test_pymssql_adk_memory_store_insert_handles_none_metadata_and_missing_author() -> None:
    """PymssqlADKMemoryStore binds None for metadata_json=None and missing author."""
    config = _mock_config()
    conn = config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.rowcount = 1
    store = PymssqlADKMemoryStore(config)
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
    params = cursor.execute.call_args.args[1]
    assert params[6] is None
    assert params[10] is None


def test_pymssql_adk_store_requests_tuple_cursor_and_supports_dict_rows() -> None:
    """PymssqlADKStore must request as_dict=False from PymssqlCursor and handle dict rows if returned."""
    config = _mock_config({"native_json": False})
    cursor_kwargs: list[dict[str, Any]] = []
    now = datetime.now(tz=UTC)
    dict_row = {
        "id": "s1",
        "app_name": "app",
        "user_id": "u1",
        "state": '{"ok": true}',
        "create_time": now,
        "update_time": now,
    }

    class _FakeRawCursor:
        def execute(self, _sql: str, _params: Any = None) -> None:
            return None

        def fetchone(self) -> Any:
            return dict_row

        def close(self) -> None:
            return None

    class _FakeConn:
        def cursor(self, **kwargs: Any) -> _FakeRawCursor:
            cursor_kwargs.append(kwargs)
            return _FakeRawCursor()

    config.provide_connection.return_value.__enter__.return_value = _FakeConn()
    store = PymssqlADKStore(cast("Any", config))

    session = store.get_session("app", "u1", "s1")

    assert cursor_kwargs == [{"as_dict": False}]
    assert session is not None
    assert session["id"] == "s1"
    assert session["state"] == {"ok": True}


def test_pymssql_adk_search_entries_escapes_tsql_like_wildcards_and_rolls_back_on_insert_failure(
    monkeypatch: "pytest.MonkeyPatch",
) -> None:
    """PymssqlADKMemoryStore escapes T-SQL LIKE wildcards and rolls back failed inserts."""
    calls: list[tuple[str, tuple[Any, ...]]] = []

    def capture(_store: PymssqlADKMemoryStore, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        calls.append((sql, tuple(params)))
        return []

    monkeypatch.setattr(PymssqlADKMemoryStore, "_execute_fetchall", capture)
    store = PymssqlADKMemoryStore(_mock_config({"native_json": False}))
    store.search_entries(r"50%_off[1]\test", "app", "u1", limit=5)

    sql, params = calls[0]
    assert "LIKE %s ESCAPE N'\\'" in _normalized(sql)
    assert params[-1] == r"%50\%\_off\[1]\\test%"

    mem_config = _mock_config({"native_json": False})
    conn = mem_config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.execute.side_effect = RuntimeError("db failure")
    mem_store = PymssqlADKMemoryStore(mem_config)
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


def _mock_config(adk_config: "dict[str, object] | None" = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _session_store_with_capture(
    monkeypatch: "pytest.MonkeyPatch",
) -> "tuple[PymssqlADKStore, list[tuple[str, tuple[Any, ...]]]]":
    calls: list[tuple[str, tuple[Any, ...]]] = []

    def capture(_store: PymssqlADKStore, sql: str, params: "tuple[Any, ...]" = ()) -> "list[Any]":
        calls.append((sql, tuple(params)))
        return []

    monkeypatch.setattr(PymssqlADKStore, "_execute_fetchall", capture)
    return PymssqlADKStore(_mock_config()), calls


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
def test_pymssql_get_session_renews_only_for_positive_duration(
    monkeypatch: "pytest.MonkeyPatch", renew_for: "int | timedelta | None", expect_renew: bool
) -> None:
    """get_session executes an UPDATE only when renew_for is strictly positive."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    writes: list[tuple[str, tuple[Any, ...]]] = []

    def capture_execute(_self: PymssqlADKStore, sql: str, params: "tuple[Any, ...]" = (), **_kw: Any) -> int:
        writes.append((_normalized(sql), tuple(params)))
        return 1

    monkeypatch.setattr(PymssqlADKStore, "_execute", capture_execute)
    monkeypatch.setattr(
        PymssqlADKStore, "_execute_fetchone", lambda *_a, **_kw: ("s1", "app", "u1", '{"step":1}', now, now)
    )
    store = PymssqlADKStore(_mock_config())

    session = store.get_session("app", "u1", "s1", renew_for=renew_for)

    assert session is not None
    assert session["id"] == "s1"
    if expect_renew:
        assert writes == [
            (
                (
                    "UPDATE [dbo].[adk_session] SET update_time = SYSUTCDATETIME() "
                    "WHERE app_name = %s AND user_id = %s AND id = %s"
                ),
                ("app", "u1", "s1"),
            )
        ]
    else:
        assert writes == []


@pytest.mark.parametrize("session_exists", [True, False], ids=["owned-session", "missing-or-unowned"])
def test_pymssql_append_event_verifies_session_ownership(session_exists: bool) -> None:
    """append_event probes session ownership before inserting and rolls back when unowned."""
    config = _mock_config()
    conn = config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.fetchone.return_value = (1,) if session_exists else None
    store = PymssqlADKStore(config)

    if session_exists:
        store.append_event(_event_fixture())
        assert cursor.execute.call_count == 2
        ownership_sql, ownership_params = cursor.execute.call_args_list[0].args
        insert_sql, _ = cursor.execute.call_args_list[1].args
        assert _normalized(ownership_sql) == (
            "SELECT TOP (1) 1 FROM [dbo].[adk_session] WHERE id = %s AND app_name = %s AND user_id = %s"
        )
        assert ownership_params == ("s1", "app", "u1")
        assert _normalized(insert_sql).startswith("INSERT INTO [dbo].[adk_event]")
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
    else:
        with pytest.raises(ValueError, match="Session s1 not found for app='app', user='u1'"):
            store.append_event(_event_fixture())
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
def test_pymssql_append_event_and_update_state_rejects_mismatched_scope(overrides: "dict[str, str]") -> None:
    """Mismatched event scope fails before acquiring a connection."""
    config = _mock_config()
    store = PymssqlADKStore(config)

    with pytest.raises(ValueError, match="does not match"):
        store.append_event_and_update_state(_event_fixture(**overrides), "app", "u1", "s1", {"step": 2})

    config.provide_connection.assert_not_called()


@pytest.mark.parametrize("session_exists", [True, False], ids=["owned-session", "missing-session"])
def test_pymssql_append_event_and_update_state_updates_session_before_insert(session_exists: bool) -> None:
    """append_event_and_update_state runs the scoped session UPDATE before the event INSERT."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    config = _mock_config()
    conn = config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.fetchone.return_value = ("s1", "app", "u1", '{"step":2}', now, now) if session_exists else None
    store = PymssqlADKStore(config)

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


def test_pymssql_events_join_and_delete_session_scoping(monkeypatch: "pytest.MonkeyPatch") -> None:
    """get_events joins on (session_id, app_name, user_id) and delete_session scopes event deletes."""
    store, calls = _session_store_with_capture(monkeypatch)
    after = datetime(2026, 1, 1, tzinfo=UTC)
    store.get_events("app", "u1", "s1", after_timestamp=after, limit=5)

    events_sql, events_params = calls[0]
    assert (
        "FROM [dbo].[adk_event] e JOIN [dbo].[adk_session] s "
        "ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id "
        "WHERE s.app_name = %s AND s.user_id = %s AND s.id = %s AND e.timestamp > %s "
        "ORDER BY e.timestamp ASC"
    ) in _normalized(events_sql)
    assert events_params == (5, "app", "u1", "s1", after)

    del_config = _mock_config()
    conn = del_config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    PymssqlADKStore(del_config).delete_session("app", "u1", "s1")
    executed = [(_normalized(call.args[0]), call.args[1]) for call in cursor.execute.call_args_list]
    assert executed == [
        ("DELETE FROM [dbo].[adk_event] WHERE app_name = %s AND user_id = %s AND session_id = %s", ("app", "u1", "s1")),
        ("DELETE FROM [dbo].[adk_session] WHERE app_name = %s AND user_id = %s AND id = %s", ("app", "u1", "s1")),
    ]
    conn.commit.assert_called_once()


@pytest.mark.parametrize(
    ("app_name", "user_id", "expected_sql", "expected_params"),
    [
        pytest.param(None, None, "DELETE FROM [dbo].[adk_memory] WHERE session_id = %s", ("s1",), id="session-only"),
        pytest.param(
            "app",
            None,
            "DELETE FROM [dbo].[adk_memory] WHERE session_id = %s AND app_name = %s",
            ("s1", "app"),
            id="session-and-app",
        ),
        pytest.param(
            None,
            "u1",
            "DELETE FROM [dbo].[adk_memory] WHERE session_id = %s AND user_id = %s",
            ("s1", "u1"),
            id="session-and-user",
        ),
        pytest.param(
            "app",
            "u1",
            "DELETE FROM [dbo].[adk_memory] WHERE session_id = %s AND app_name = %s AND user_id = %s",
            ("s1", "app", "u1"),
            id="session-app-and-user",
        ),
    ],
)
def test_pymssql_memory_ddl_dedup_and_scoped_delete_entries_by_session(
    monkeypatch: "pytest.MonkeyPatch",
    app_name: "str | None",
    user_id: "str | None",
    expected_sql: str,
    expected_params: "tuple[str, ...]",
) -> None:
    """Memory DDL, duplicate predicate, and session deletes scope by (app_name, user_id)."""
    insert_config = _mock_config({"native_json": False})
    conn = insert_config.provide_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.rowcount = 1
    mem_store = PymssqlADKMemoryStore(insert_config)
    assert "UNIQUE (app_name, user_id, event_id)" in mem_store._memory_table_ddl()

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
    mem_store.insert_memory_entries([entry])
    insert_sql, insert_params = cursor.execute.call_args.args
    assert (
        "WHERE NOT EXISTS ( SELECT 1 FROM [dbo].[adk_memory] WITH (UPDLOCK, HOLDLOCK) "
        "WHERE app_name = %s AND user_id = %s AND event_id = %s );"
    ) in _normalized(insert_sql)
    assert insert_params[-3:] == ("app", "u1", "evt-1")

    delete_calls: list[tuple[str, tuple[Any, ...]]] = []

    def capture_delete(_self: PymssqlADKMemoryStore, sql: str, params: "tuple[Any, ...]" = (), **_kw: Any) -> int:
        delete_calls.append((_normalized(sql), tuple(params)))
        return 1

    monkeypatch.setattr(PymssqlADKMemoryStore, "_execute", capture_delete)
    assert (
        PymssqlADKMemoryStore(_mock_config()).delete_entries_by_session("s1", app_name=app_name, user_id=user_id) == 1
    )
    assert delete_calls == [(expected_sql, expected_params)]
