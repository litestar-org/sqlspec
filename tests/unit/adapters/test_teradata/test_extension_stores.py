"""Teradata extension schema contracts."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from sqlspec.adapters.teradata import TeradataConfig
from sqlspec.adapters.teradata.adk import TeradataADKMemoryStore, TeradataADKStore
from sqlspec.adapters.teradata.events import TeradataEventQueueStore
from sqlspec.adapters.teradata.litestar import TeradataStore
from sqlspec.extensions.adk import StoredEvent, StoredMemory


def test_teradata_extension_schema_targets() -> None:
    config = TeradataConfig(connection_config={"host": "localhost"}, extension_config={"adk": {"enable_memory": True}})
    session_store = TeradataADKStore(config)
    memory_store = TeradataADKMemoryStore(config)
    queue_store = TeradataEventQueueStore(config)
    litestar_store = TeradataStore(config)
    targets = [
        *session_store.get_schema_targets_sync(None),
        *memory_store.get_schema_targets_sync(None),
        *queue_store.get_schema_targets(),
        *litestar_store.get_schema_targets(),
    ]
    assert len(targets) == 8
    assert all(str(target.create_statement).startswith("CREATE MULTISET TABLE") for target in targets)
    assert (
        queue_store.create_statements()[1]
        == "CREATE INDEX idx_sqlspec_event_queue_channel_status (channel, status, available_at) ON sqlspec_event_queue"
    )


def test_teradata_adk_public_pagination_and_atomic_delete(monkeypatch) -> None:
    from contextlib import contextmanager
    from types import SimpleNamespace
    from typing import Any

    statements: list[tuple[str, tuple[Any, ...]]] = []
    states: list[bool] = []
    conn = SimpleNamespace(autocommit=True)

    def _execute(sql: str, params: tuple[Any, ...] = ()) -> None:
        statements.append((" ".join(sql.split()), params))
        states.append(conn.autocommit)

    cursor = SimpleNamespace(execute=_execute, fetchall=list, close=lambda: None)
    conn.cursor = lambda: cursor
    conn.commit = lambda: None
    conn.rollback = lambda: None

    @contextmanager
    def connection(_config):
        yield conn

    monkeypatch.setattr(TeradataConfig, "provide_connection", connection)
    store = TeradataADKStore(TeradataConfig(connection_config={"host": "localhost"}))
    assert store.list_sessions("app", "user", limit=5, offset=10) == []
    assert "QUALIFY ROW_NUMBER() OVER (ORDER BY update_time DESC, id DESC) BETWEEN ? AND ?" in statements[0][0]
    assert statements[0][1] == ("app", "user", 11, 15)
    store.delete_session("app", "user", "session")
    assert statements[-2] == (
        'DELETE FROM "ADK_EVENT" WHERE app_name = ? AND user_id = ? AND session_id = ?',
        ("app", "user", "session"),
    )
    assert states[-2:] == [False, False]
    assert conn.autocommit is True


@pytest.fixture
def teradata_connection(monkeypatch):
    from contextlib import nullcontext
    from unittest.mock import MagicMock

    cursor = MagicMock()
    cursor.rowcount = 1
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    conn = MagicMock()
    conn.autocommit = True
    conn.cursor.return_value = cursor
    monkeypatch.setattr(TeradataConfig, "provide_connection", lambda _: nullcontext(conn))
    return conn, cursor


@pytest.mark.anyio
async def test_teradata_litestar_crud(teradata_connection) -> None:
    from datetime import UTC, datetime, timedelta

    conn, cursor = teradata_connection
    store = TeradataStore(TeradataConfig(connection_config={"host": "localhost"}))
    await store.set("session", b"data", expires_in=60)
    executed_sql, executed_params = cursor.execute.call_args.args
    assert executed_sql.startswith('MERGE INTO "LITESTAR_SESSION" AS tgt USING')
    assert executed_params[:2] == ("session", b"data")
    cursor.fetchone.return_value = (b"data", datetime.now(UTC) + timedelta(minutes=2))
    assert await store.get("session", renew_for=60) == b"data"
    assert await store.exists("session") is True
    cursor.fetchone.return_value = (datetime.now(UTC) + timedelta(minutes=2),)
    expiry = await store.expires_in("session")
    assert isinstance(expiry, int) and 110 <= expiry <= 120
    await store.delete("session")
    assert "WHERE session_id = ?" in cursor.execute.call_args.args[0]
    await store.delete_all()
    assert cursor.execute.call_args.args[0] == 'DELETE FROM "LITESTAR_SESSION" ALL'
    assert await store.delete_expired() == 1
    cursor.fetchone.return_value = None
    assert await store.get("missing") is None
    assert await store.exists("missing") is False
    assert await store.expires_in("missing") is None
    assert conn.commit.call_count == 0

    conn.autocommit = False
    await store.set("session", b"data", expires_in=60)
    await store.delete("session")
    assert conn.commit.call_count == 2


def test_teradata_adk_crud_and_cleanup(teradata_connection) -> None:
    from datetime import UTC, datetime

    conn, cursor = teradata_connection
    config = TeradataConfig(connection_config={"host": "localhost"})
    store = TeradataADKStore(config)
    now = datetime.now(UTC)
    cursor.fetchone.return_value = ("session", "app", "user", '{"count":1}', now, now)
    assert store.create_session("session", "app", "user", {"count": 1})["state"] == {"count": 1}
    session = store.get_session("app", "user", "session")
    assert session and session["id"] == "session"
    store.update_session_state("app", "user", "session", {"count": 2})
    assert cursor.execute.call_args.args[1][0] == '{"count":2}'
    cursor.fetchone.return_value = ('{"active":true}',)
    assert store.get_app_state("app") == {"active": True}
    assert store.get_user_state("app", "user") == {"active": True}
    store.upsert_app_state("app", {"active": True})
    assert "MERGE INTO" in cursor.execute.call_args.args[0]
    store.upsert_user_state("app", "user", {"active": True})
    store.set_metadata("version", "1")
    cursor.fetchone.return_value = ("1",)
    assert store.get_metadata("version") == "1"
    event: StoredEvent = {
        "id": "event",
        "session_id": "session",
        "app_name": "app",
        "user_id": "user",
        "invocation_id": "invoke",
        "timestamp": now,
        "event_data": {"text": "hello"},
    }
    store.append_event(event)
    cursor.fetchall.return_value = [("event", "app", "user", "session", "invoke", now, '{"text":"hello"}')]
    assert store.get_events("app", "user", "session", limit=2)[0]["event_data"] == {"text": "hello"}
    assert "TOP 2" in cursor.execute.call_args.args[0]
    assert 'JOIN "ADK_SESSION" s ON e.session_id = s.id AND e.app_name = s.app_name AND e.user_id = s.user_id' in (
        " ".join(cursor.execute.call_args.args[0].split())
    )
    assert store.get_events("app", "user", "session", limit=0) == []
    assert store.delete_expired_events(now, "app") == 1
    assert store.delete_idle_user_states(now, "app") == 1
    assert store.delete_idle_sessions(now, "app") == 1
    assert conn.autocommit is True
    cursor.fetchone.return_value = None
    assert store.get_session("app", "user", "missing") is None
    assert store.get_app_state("missing") is None
    assert store.get_user_state("app", "missing") is None
    assert store.get_metadata("missing") is None


def test_teradata_memory_insert_search_and_delete(teradata_connection) -> None:
    from datetime import UTC, datetime

    conn, cursor = teradata_connection
    config = TeradataConfig(connection_config={"host": "localhost"}, extension_config={"adk": {"enable_memory": True}})
    store = TeradataADKMemoryStore(config)
    now = datetime.now(UTC)
    entry: StoredMemory = {
        "id": "memory",
        "session_id": "session",
        "app_name": "app",
        "user_id": "user",
        "scope": "user",
        "event_id": "event",
        "author": "agent",
        "timestamp": now,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": {},
        "inserted_at": now,
        "embedding": None,
    }
    assert store.insert_memory_entries([entry]) == 1
    assert conn.autocommit is True
    cursor.fetchall.return_value = [
        ("memory", "session", "app", "user", "user", "event", "agent", now, '{"text":"hello"}', "hello", "{}", now)
    ]
    assert store.search_entries("hello", "app", "user", limit=3)[0]["content_text"] == "hello"
    assert "TOP 3" in cursor.execute.call_args.args[0]
    assert store.search_entries("hello", "app", "user", limit=0) == []
    assert store.insert_memory_entries([]) == 0
    assert store.delete_entries_by_session("session") == 1
    assert store.delete_entries_older_than(3, "app", "user") == 1
    assert cursor.execute.call_args.args[1][1:] == ("app", "user")


@pytest.mark.parametrize(
    "operation",
    [
        "get_session",
        "get_app_state",
        "get_user_state",
        "get_metadata",
        "list_sessions",
        "get_events",
        "delete_expired_events",
        "delete_idle_user_states",
        "delete_idle_sessions",
    ],
)
def test_teradata_adk_absent_tables_are_empty(teradata_connection, operation) -> None:
    from datetime import UTC, datetime

    conn, cursor = teradata_connection
    cursor.execute.side_effect = RuntimeError("[Teradata Database] [Error 3807] Object does not exist")
    store = TeradataADKStore(TeradataConfig(connection_config={"host": "localhost"}))
    now = datetime.now(UTC)
    args = {
        "get_session": ("app", "user", "session"),
        "get_app_state": ("app",),
        "get_user_state": ("app", "user"),
        "get_metadata": ("key",),
        "list_sessions": ("app",),
        "get_events": ("app", "user", "session"),
        "delete_expired_events": (now,),
        "delete_idle_user_states": (now,),
        "delete_idle_sessions": (now,),
    }
    expected = {
        "list_sessions": [],
        "get_events": [],
        "delete_expired_events": 0,
        "delete_idle_user_states": 0,
        "delete_idle_sessions": 0,
    }
    assert getattr(store, operation)(*args[operation]) == expected.get(operation)
    assert conn.autocommit is True


def test_teradata_adk_delete_rolls_back_and_restores_connection(teradata_connection) -> None:
    conn, cursor = teradata_connection
    cursor.execute.side_effect = RuntimeError("connection failed")
    store = TeradataADKStore(TeradataConfig(connection_config={"host": "localhost"}))
    with pytest.raises(RuntimeError, match="connection failed"):
        store.delete_session("app", "user", "session")
    conn.rollback.assert_called_once()
    assert conn.autocommit is True


@pytest.mark.parametrize("index_present", [True, False])
def test_teradata_event_queue_statements_and_companion_index(index_present: bool) -> None:
    from types import SimpleNamespace

    store = TeradataEventQueueStore(TeradataConfig(connection_config={"host": "localhost"}))
    table_ddl, index_ddl = store.create_statements()
    assert table_ddl.startswith("CREATE MULTISET TABLE sqlspec_event_queue (")
    assert "payload_json JSON NOT NULL" in table_ddl
    assert index_ddl == (
        "CREATE INDEX idx_sqlspec_event_queue_channel_status (channel, status, available_at) ON sqlspec_event_queue"
    )
    assert store.drop_statements() == ["DROP TABLE sqlspec_event_queue"]
    assert store.get_schema_targets()[0].table_name == "SQLSPEC_EVENT_QUEUE"

    lookups: list[dict[str, object]] = []
    executed: list[str] = []
    rows = [{"index_name": "idx_sqlspec_event_queue_channel_status"}] if index_present else []

    def get_indexes(_driver: object, **kwargs: object) -> list[dict[str, str]]:
        lookups.append(kwargs)
        return rows

    driver = SimpleNamespace(data_dictionary=SimpleNamespace(get_indexes=get_indexes), execute_script=executed.append)
    store.reconcile_companion_objects_sync(driver)
    assert lookups == [{"table": "SQLSPEC_EVENT_QUEUE", "schema": None}]
    assert executed == ([] if index_present else [index_ddl])


def test_teradata_adk_and_memory_catalog_lifecycle(monkeypatch) -> None:
    from contextlib import nullcontext
    from types import SimpleNamespace

    statements = []
    dictionary = SimpleNamespace(get_indexes=lambda *_args, **_kwargs: [])
    driver = SimpleNamespace(
        select_one_or_none=lambda *_args: None,
        execute=lambda sql: statements.append(sql),
        commit=lambda: None,
        data_dictionary=dictionary,
    )
    monkeypatch.setattr(TeradataConfig, "provide_session", lambda _: nullcontext(driver))
    config = TeradataConfig(connection_config={"host": "localhost"}, extension_config={"adk": {"enable_memory": True}})
    store = TeradataADKStore(config)
    memory = TeradataADKMemoryStore(config)
    store.reconcile_companion_objects_sync(driver)
    memory.reconcile_companion_objects_sync(driver)
    assert len(statements) == 11
    assert statements[0] == 'CREATE INDEX "idx_ADK_SESSION_app_user" (app_name, user_id) ON "ADK_SESSION"'
    assert statements[-1] == 'CREATE INDEX "idx_ADK_MEMORY_timestamp" (timestamp) ON "ADK_MEMORY"'
    store.drop_tables()
    memory.drop_tables()
    assert len(statements) == 11


def test_teradata_native_adk_creates_and_reconciles_tables(monkeypatch) -> None:
    from contextlib import nullcontext
    from types import SimpleNamespace

    statements = []
    column_names = "id app_name user_id state create_time update_time session_id invocation_id timestamp event_data actions partial turn_complete interrupted key value scope event_id author content_json content_text metadata_json inserted_at".split()
    dictionary = SimpleNamespace(
        get_indexes=lambda *_args, **_kwargs: [],
        get_columns=lambda *_args, **_kwargs: [{"column_name": name} for name in column_names],
    )
    driver = SimpleNamespace(
        select_one_or_none=lambda *_args: None,
        execute=lambda sql: statements.append(sql),
        commit=lambda: None,
        data_dictionary=dictionary,
    )
    monkeypatch.setattr(TeradataConfig, "provide_session", lambda _: nullcontext(driver))
    config = TeradataConfig(connection_config={"host": "localhost"}, extension_config={"adk": {"enable_memory": True}})
    TeradataADKStore(config).create_tables()
    TeradataADKMemoryStore(config).create_tables()
    assert sum(sql.startswith("CREATE MULTISET TABLE") for sql in statements) == 6
    assert sum(sql.startswith("CREATE INDEX") for sql in statements) == 11


def test_teradata_adk_atomic_event_state_and_owner(teradata_connection) -> None:
    from datetime import UTC, datetime

    conn, cursor = teradata_connection
    config = TeradataConfig(
        connection_config={"host": "localhost"}, extension_config={"adk": {"owner_id_column": "tenant_id INTEGER NULL"}}
    )
    store = TeradataADKStore(config)
    now = datetime.now(UTC)
    cursor.fetchone.return_value = ("session", "app", "user", '{"count":2}', now, now)
    assert store.create_session("session", "app", "user", {"count": 2}, owner_id=5)["state"] == {"count": 2}
    event: StoredEvent = {
        "id": "event",
        "session_id": "session",
        "app_name": "app",
        "user_id": "user",
        "invocation_id": "invoke",
        "timestamp": now,
        "event_data": {"text": "hello"},
    }
    result = store.append_event_and_update_state(
        event, "app", "user", "session", {"count": 2}, app_state={"app": 1}, user_state={"user": 1}
    )
    assert result["state"] == {"count": 2}
    assert conn.autocommit is True
    cursor.fetchone.return_value = None
    missing_event: StoredEvent = {**event, "session_id": "missing"}
    with pytest.raises(ValueError, match="not found"):
        store.append_event_and_update_state(missing_event, "app", "user", "missing", {})
    conn.rollback.assert_called_once()
    assert conn.autocommit is True
    with pytest.raises(RuntimeError, match="Failed to fetch"):
        store.create_session("missing", "app", "user", {})


def test_teradata_memory_disabled_missing_and_duplicates(teradata_connection) -> None:
    from datetime import UTC, datetime

    conn, cursor = teradata_connection
    config = TeradataConfig(connection_config={"host": "localhost"}, extension_config={"adk": {"enable_memory": False}})
    disabled = TeradataADKMemoryStore(config)
    assert disabled.get_schema_targets_sync(None) == []
    with pytest.raises(RuntimeError, match="disabled"):
        disabled.insert_memory_entries([])
    with pytest.raises(RuntimeError, match="disabled"):
        disabled.search_entries("query", "app", "user")
    enabled = TeradataADKMemoryStore(
        TeradataConfig(
            connection_config={"host": "localhost"},
            extension_config={"adk": {"enable_memory": True, "owner_id_column": "tenant_id INTEGER NULL"}},
        )
    )
    assert len(enabled.get_schema_targets_sync(None)) == 1
    with pytest.raises(ValueError, match="non-negative"):
        enabled.search_entries("query", "app", "user", limit=-1)
    cursor.execute.side_effect = RuntimeError("[Error 3807] Object does not exist")
    assert enabled.search_entries("query", "app", "user", scope_filter="all") == []
    cursor.execute.side_effect = None
    now = datetime.now(UTC)
    entry: StoredMemory = {
        "id": "memory",
        "session_id": "session",
        "app_name": "app",
        "user_id": "user",
        "scope": "app",
        "event_id": "event",
        "author": None,
        "timestamp": now,
        "content_json": {},
        "content_text": "text",
        "metadata_json": None,
        "inserted_at": now,
        "embedding": None,
    }
    stored_event_ids: list[tuple[str]] = []

    def _duplicate_on_insert(sql: str, params: object = ()) -> None:
        if sql.lstrip().startswith("INSERT"):
            raise RuntimeError("[Error 2801] Duplicate unique key")

    cursor.execute.side_effect = _duplicate_on_insert
    cursor.fetchall.side_effect = lambda: list(stored_event_ids)
    with pytest.raises(RuntimeError, match="2801"):
        enabled.insert_memory_entries([entry], owner_id=5)
    assert conn.autocommit is True
    original_fetchall = cursor.fetchall.side_effect
    probes = iter([[], [("app", "user", "event")]])
    cursor.fetchall.side_effect = lambda: next(probes)
    assert enabled.insert_memory_entries([entry], owner_id=5) == 0
    assert conn.autocommit is True
    cursor.fetchall.side_effect = original_fetchall
    cursor.execute.side_effect = RuntimeError("connection failed")
    with pytest.raises(RuntimeError, match="connection failed"):
        enabled.insert_memory_entries([entry])
    assert conn.autocommit is True


@pytest.mark.anyio
async def test_teradata_litestar_catalog_lifecycle(monkeypatch) -> None:
    from contextlib import contextmanager
    from types import SimpleNamespace

    statements = []
    tables = set()
    dictionary = SimpleNamespace(
        get_indexes=lambda *_args, **_kwargs: [],
        get_columns=lambda *_args, **_kwargs: [{"column_name": name} for name in ("session_id", "data", "expires_at")],
    )

    def execute(sql):
        statements.append(sql)
        if sql.startswith("CREATE MULTISET TABLE"):
            tables.add("LITESTAR_SESSION")
        elif sql.startswith("DROP TABLE"):
            tables.clear()

    driver = SimpleNamespace(
        select_one_or_none=lambda _sql, params: {"found": 1} if params[1] in tables else None,
        execute=execute,
        execute_script=execute,
        commit=lambda: None,
        data_dictionary=dictionary,
    )

    @contextmanager
    def session(_):
        yield driver

    monkeypatch.setattr(TeradataConfig, "provide_session", session)
    store = TeradataStore(TeradataConfig(connection_config={"host": "localhost"}))
    await store.create_table()
    assert statements[0].startswith('CREATE MULTISET TABLE "LITESTAR_SESSION"')
    assert "CREATE INDEX" in statements[1]
    assert len(statements) == 2
    assert store._drop_table_sql() == ['DROP TABLE "LITESTAR_SESSION"']


@pytest.mark.anyio
async def test_teradata_litestar_missing_expiry_and_insert(teradata_connection) -> None:
    from datetime import UTC, datetime, timedelta

    _conn, cursor = teradata_connection
    store = TeradataStore(TeradataConfig(connection_config={"host": "localhost"}))
    cursor.rowcount = 0
    await store.set("new", "payload")
    assert cursor.execute.call_args.args[1] == ("new", b"payload", None)
    cursor.fetchone.return_value = (memoryview(b"payload"), None)
    assert await store.get("new", renew_for=60) == b"payload"
    cursor.fetchone.return_value = (None,)
    assert await store.expires_in("new") is None
    cursor.fetchone.return_value = (datetime.now(UTC) - timedelta(seconds=1),)
    assert await store.expires_in("expired") == 0
    assert await store.delete_expired() == 0


def test_teradata_extension_targets_preserve_database_qualifier() -> None:
    config = TeradataConfig(
        connection_config={"host": "localhost"},
        extension_config={
            "adk": {"session_table": "app.sessions", "events_table": "app.events"},
            "litestar": {"session_table": "app.http_sessions"},
            "events": {"queue_table": "app.queue"},
        },
    )
    targets = [
        TeradataADKStore(config).get_schema_targets_sync(None)[0],
        TeradataStore(config).get_schema_targets()[0],
        TeradataEventQueueStore(config).get_schema_targets()[0],
    ]
    assert [(target.schema, target.table_name) for target in targets] == [
        ("APP", "SESSIONS"),
        ("APP", "HTTP_SESSIONS"),
        ("APP", "QUEUE"),
    ]


def test_teradata_adk_transaction_rolls_back_before_restoring_autocommit(monkeypatch) -> None:
    from contextlib import nullcontext
    from types import SimpleNamespace

    rollback_autocommit_states: list[bool] = []
    cursor = SimpleNamespace(execute=lambda *_args: (_ for _ in ()).throw(RuntimeError("boom")), close=lambda: None)
    conn = SimpleNamespace(autocommit=True)
    conn.cursor = lambda: cursor
    conn.commit = lambda: None
    conn.rollback = lambda: rollback_autocommit_states.append(conn.autocommit)
    monkeypatch.setattr(TeradataConfig, "provide_connection", lambda _: nullcontext(conn))

    store = TeradataADKStore(TeradataConfig(connection_config={"host": "localhost"}))
    with pytest.raises(RuntimeError, match="boom"):
        store.delete_session("app", "user", "session")
    assert rollback_autocommit_states == [False]
    assert conn.autocommit is True


def test_teradata_memory_dedup_prequery_and_tera_concurrent_race_recovery(monkeypatch) -> None:
    from contextlib import nullcontext
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from typing import Any

    now = datetime.now(UTC)
    persisted: list[str] = ["existing_ev"]
    staged_in_txn: list[str] = []
    inject_race_once = {"armed": True}

    def _make_entry(idx: int, event_id: str) -> StoredMemory:
        return {
            "id": f"mem_{idx}",
            "session_id": "s1",
            "app_name": "app",
            "user_id": "u1",
            "scope": "user",
            "event_id": event_id,
            "author": "agent",
            "timestamp": now,
            "content_json": {"i": idx},
            "content_text": f"entry {idx}",
            "metadata_json": None,
            "inserted_at": now,
            "embedding": None,
        }

    fetch_results: list[list[tuple[str, str, str]]] = []

    def _execute(sql: str, params: tuple[Any, ...] = ()) -> None:
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT app_name, user_id, event_id FROM"):
            fetch_results.clear()
            if "WHERE event_id IN" in normalized:
                fetch_results.append([("app", "u1", ev) for ev in persisted if ev in params])
            else:
                app_p, user_p, ev_p = str(params[0]), str(params[1]), str(params[2])
                rows = [("app", "u1", ev_p)] if (app_p, user_p) == ("app", "u1") and ev_p in persisted else []
                fetch_results.append(rows)
            return
        if normalized.startswith("INSERT INTO"):
            ev_id = str(params[5])
            if ev_id == "raced_ev" and inject_race_once["armed"]:
                inject_race_once["armed"] = False
                persisted.append("raced_ev")
                staged_in_txn.clear()
                raise RuntimeError("[Teradata Database] [Error 2801] Duplicate unique prime key")
            if ev_id in persisted:
                staged_in_txn.clear()
                raise RuntimeError("[Teradata Database] [Error 2801] Duplicate unique prime key")
            staged_in_txn.append(ev_id)

    def _commit() -> None:
        persisted.extend(staged_in_txn)
        staged_in_txn.clear()

    conn = SimpleNamespace(autocommit=True)
    cursor = SimpleNamespace(
        execute=_execute,
        fetchall=lambda: list(fetch_results[0]) if fetch_results else [],
        rowcount=1,
        close=lambda: None,
    )
    conn.cursor = lambda: cursor
    conn.commit = _commit
    conn.rollback = staged_in_txn.clear
    monkeypatch.setattr(TeradataConfig, "provide_connection", lambda _: nullcontext(conn))

    store = TeradataADKMemoryStore(
        TeradataConfig(connection_config={"host": "localhost"}, extension_config={"adk": {"enable_memory": True}})
    )
    entries = [
        _make_entry(1, "existing_ev"),
        _make_entry(2, "new_ev_1"),
        _make_entry(3, "new_ev_1"),
        _make_entry(4, "raced_ev"),
        _make_entry(5, "new_ev_2"),
    ]
    inserted = store.insert_memory_entries(entries)
    assert inserted == 2
    assert persisted == ["existing_ev", "raced_ev", "new_ev_1", "new_ev_2"]
    assert conn.autocommit is True

    assert store.insert_memory_entries([_make_entry(6, "existing_ev")]) == 0


def test_teradata_adk_remaining_branches_and_row_decoders(monkeypatch) -> None:
    from contextlib import nullcontext
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from sqlspec.adapters.teradata.adk import store as adk_mod

    now = datetime.now(UTC)
    dict_row = {
        "ID": "s1",
        "app_name": "app",
        "USER_ID": "u1",
        "state": bytearray(b'{"a":1}'),
        "create_time": now,
        "update_time": now,
    }
    parsed = adk_mod._session_record_from_row(dict_row)
    assert parsed["id"] == "s1"
    assert parsed["state"] == {"a": 1}
    assert adk_mod._row_value({"other": 1}, "missing", 0) is None
    assert adk_mod._row_value(SimpleNamespace(custom="ok"), "custom", 5) == "ok"
    assert adk_mod._json_dict(None) == {}
    assert adk_mod._json_dict({"k": 2}) == {"k": 2}

    executed_session_sql: list[str] = []
    driver = SimpleNamespace(
        select_one_or_none=lambda *_a: {"found": 1},
        execute=executed_session_sql.append,
        execute_script=executed_session_sql.append,
        commit=lambda: None,
        data_dictionary=SimpleNamespace(get_indexes=lambda *_a, **_k: [], get_columns=lambda *_a, **_k: []),
    )
    monkeypatch.setattr(TeradataConfig, "provide_session", lambda _: nullcontext(driver))
    cfg = TeradataConfig(
        connection_config={"host": "localhost"},
        extension_config={"adk": {"enable_memory": True, "create_schema": False}},
    )
    s_store = TeradataADKStore(cfg)
    m_store = TeradataADKMemoryStore(cfg)
    s_store.drop_tables()
    m_store.drop_tables()
    assert any(stmt.startswith("DROP TABLE") for stmt in executed_session_sql)

    cursor = SimpleNamespace(
        execute=lambda *_a: None,
        fetchone=lambda: ("s1", "app", "u1", {"x": 1}, now, now),
        fetchall=lambda: [],
        rowcount=1,
        close=lambda: None,
    )
    conn = SimpleNamespace(autocommit=False, cursor=lambda: cursor, commit=lambda: None, rollback=lambda: None)
    monkeypatch.setattr(TeradataConfig, "provide_connection", lambda _: nullcontext(conn))

    assert s_store.get_session("app", "u1", "s1", renew_for=60) is not None
    assert s_store.list_sessions("app", limit=0) == []
    assert s_store.list_sessions("app", limit=None) == []
    assert s_store.list_sessions("app", offset=5, limit=10) == []
    assert s_store.get_events("app", "u1", "s1", after_timestamp=now) == []
    assert m_store.search_entries("hello", "app", "u1", scope_filter="user") == []
    assert m_store.search_entries("hello", "app", "u1", scope_filter="app") == []
    assert m_store.delete_entries_older_than(7) == 1


class _CatalogDriver:
    """Fake driver whose DROP TABLE fails for absent tables, like Teradata error 3807."""

    def __init__(self, tables: set[str]) -> None:
        self.tables = tables
        self.dropped: list[str] = []

    def select_one_or_none(self, _sql: str, parameters: "tuple[str | None, str]") -> "dict[str, int] | None":
        return {"found": 1} if parameters[1] in self.tables else None

    def execute_script(self, statement: str) -> None:
        name = statement.removeprefix("DROP TABLE ").strip('"').upper()
        if name not in self.tables:
            msg = f"[Error 3807] Object '{name}' does not exist."
            raise RuntimeError(msg)
        self.tables.discard(name)
        self.dropped.append(name)


def test_teradata_extension_downgrades_skip_tables_already_dropped() -> None:
    from sqlspec.extensions.adk.migrations.reconcile import _downgrade_sync as adk_downgrade
    from sqlspec.extensions.events.migrations.reconcile import _downgrade_sync as events_downgrade
    from sqlspec.extensions.litestar.migrations.reconcile import _downgrade_sync as litestar_downgrade

    config = TeradataConfig(connection_config={"host": "localhost"}, extension_config={"adk": {"enable_memory": True}})
    stores = [TeradataADKStore(config), TeradataADKMemoryStore(config)]
    driver = _CatalogDriver({"ADK_SESSION", "ADK_EVENT", "ADK_MEMORY"})
    adk_downgrade(driver, stores)
    assert driver.dropped == ["ADK_MEMORY", "ADK_EVENT", "ADK_SESSION"]
    adk_downgrade(driver, stores)
    assert driver.dropped == ["ADK_MEMORY", "ADK_EVENT", "ADK_SESSION"]

    session_store = TeradataStore(TeradataConfig(connection_config={"host": "localhost"}))
    driver = _CatalogDriver({"LITESTAR_SESSION"})
    litestar_downgrade(driver, session_store)
    litestar_downgrade(driver, session_store)
    assert driver.dropped == ["LITESTAR_SESSION"]

    queue_store = TeradataEventQueueStore(TeradataConfig(connection_config={"host": "localhost"}))
    driver = _CatalogDriver(set())
    events_downgrade(driver, queue_store, True)
    assert driver.dropped == []


@pytest.mark.parametrize(
    ("renew_for", "expects_update"),
    [
        pytest.param(None, False, id="none"),
        pytest.param(0, False, id="zero-int"),
        pytest.param(-1, False, id="negative-int"),
        pytest.param(timedelta(0), False, id="zero-timedelta"),
        pytest.param(timedelta(milliseconds=250), True, id="subsecond-timedelta"),
        pytest.param(30, True, id="positive-int"),
    ],
)
def test_teradata_adk_get_session_renews_only_for_positive_duration(
    teradata_connection: Any, renew_for: int | timedelta | None, expects_update: bool
) -> None:
    """get_session executes the update_time touch only when renew_for is strictly positive."""
    _conn, cursor = teradata_connection
    now = datetime.now(UTC)
    cursor.fetchone.return_value = ("s1", "app", "u1", '{"k":1}', now, now)
    store = TeradataADKStore(TeradataConfig(connection_config={"host": "localhost"}))

    session = store.get_session("app", "u1", "s1", renew_for=renew_for)

    assert session is not None
    executed = [" ".join(call.args[0].split()) for call in cursor.execute.call_args_list]
    has_update = any(sql.startswith('UPDATE "ADK_SESSION" SET update_time') for sql in executed)
    assert has_update is expects_update


@pytest.mark.parametrize("owner_exists", [True, False])
def test_teradata_adk_append_event_checks_session_ownership_and_rejects_mismatch(
    teradata_connection: Any, owner_exists: bool
) -> None:
    """append_event checks ownership before inserting and append_event_and_update_state rejects mismatched scope."""
    conn, cursor = teradata_connection
    cursor.fetchone.return_value = (1,) if owner_exists else None
    store = TeradataADKStore(TeradataConfig(connection_config={"host": "localhost"}))
    now = datetime.now(UTC)
    event: StoredEvent = {
        "id": "e1",
        "session_id": "s1",
        "app_name": "app",
        "user_id": "u1",
        "invocation_id": "inv",
        "timestamp": now,
        "event_data": {"k": 1},
    }

    if owner_exists:
        store.append_event(event)
        executed = [" ".join(call.args[0].split()) for call in cursor.execute.call_args_list]
        assert executed[0] == 'SELECT TOP 1 1 FROM "ADK_SESSION" WHERE id = ? AND app_name = ? AND user_id = ?'
        assert executed[1].startswith('INSERT INTO "ADK_EVENT"')
        assert conn.commit.call_count == 1
    else:
        with pytest.raises(ValueError, match="Session s1 not found for app='app', user='u1'"):
            store.append_event(event)
        assert len(cursor.execute.call_args_list) == 1
        assert conn.rollback.call_count == 1
        assert conn.commit.call_count == 0

    cursor.execute.reset_mock()
    with pytest.raises(ValueError, match="does not match"):
        store.append_event_and_update_state({**event, "user_id": "other"}, "app", "u1", "s1", {})
    assert cursor.execute.call_count == 0


@pytest.mark.parametrize(
    ("app_name", "user_id", "expected_sql", "expected_params"),
    [
        pytest.param(None, None, 'DELETE FROM "ADK_MEMORY" WHERE session_id = ?', ("s1",), id="session-only"),
        pytest.param(
            "app", None, 'DELETE FROM "ADK_MEMORY" WHERE session_id = ? AND app_name = ?', ("s1", "app"), id="with-app"
        ),
        pytest.param(
            None, "u1", 'DELETE FROM "ADK_MEMORY" WHERE session_id = ? AND user_id = ?', ("s1", "u1"), id="with-user"
        ),
        pytest.param(
            "app",
            "u1",
            'DELETE FROM "ADK_MEMORY" WHERE session_id = ? AND app_name = ? AND user_id = ?',
            ("s1", "app", "u1"),
            id="with-app-and-user",
        ),
    ],
)
def test_teradata_memory_composite_dedup_ddl_and_scoped_session_delete(
    teradata_connection: Any,
    app_name: str | None,
    user_id: str | None,
    expected_sql: str,
    expected_params: tuple[str, ...],
) -> None:
    """Memory DDL declares composite UNIQUE, batch dedup keeps cross-owner event_ids, and session delete scopes owner."""
    _conn, cursor = teradata_connection
    config = TeradataConfig(connection_config={"host": "localhost"}, extension_config={"adk": {"enable_memory": True}})
    store = TeradataADKMemoryStore(config)
    ddl = store._memory_table_ddl()
    assert 'CONSTRAINT "uq_ADK_MEMORY_event_id" UNIQUE (app_name, user_id, event_id)' in ddl

    now = datetime.now(UTC)
    entry_a: StoredMemory = {
        "id": "m1",
        "session_id": "s1",
        "app_name": "app-a",
        "user_id": "u1",
        "scope": "user",
        "event_id": "shared-ev",
        "author": "user",
        "timestamp": now,
        "content_json": {},
        "content_text": "hello",
        "metadata_json": None,
        "inserted_at": now,
        "embedding": None,
    }
    entry_b: StoredMemory = {**entry_a, "id": "m2", "app_name": "app-b", "user_id": "u2"}
    cursor.fetchall.return_value = [("app-a", "u1", "shared-ev")]
    assert store.insert_memory_entries([entry_a, entry_b]) == 1

    cursor.execute.reset_mock()
    assert store.delete_entries_by_session("s1", app_name=app_name, user_id=user_id) == 1
    assert cursor.execute.call_args.args == (expected_sql, expected_params)
