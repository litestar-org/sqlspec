"""Teradata extension schema contracts."""

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


def test_teradata_event_queue_catalog_lifecycle(monkeypatch) -> None:
    from contextlib import nullcontext
    from types import SimpleNamespace

    statements = []
    tables = set()
    indexes = set()

    def select(_sql, params):
        if "IndicesV" in _sql:
            return {"found": 1} if params[-1] in indexes else None
        return {"found": 1} if params[1] in tables else None

    def execute(sql):
        statements.append(sql)
        if sql.startswith("CREATE MULTISET TABLE"):
            tables.add("SQLSPEC_EVENT_QUEUE")
        elif sql.startswith("CREATE INDEX"):
            indexes.add("idx_sqlspec_event_queue_channel_status")
        elif sql.startswith("DROP TABLE"):
            tables.clear()
            indexes.clear()

    driver = SimpleNamespace(select_one_or_none=select, execute=execute, commit=lambda: None)
    monkeypatch.setattr(TeradataConfig, "provide_session", lambda _: nullcontext(driver))
    store = TeradataEventQueueStore(TeradataConfig(connection_config={"host": "localhost"}))
    store.create_table()
    assert len(statements) == 2
    assert store.get_schema_targets()[0].table_name == "SQLSPEC_EVENT_QUEUE"
    store.create_table()
    assert len(statements) == 2
    store.drop_table()
    store.drop_table()
    assert statements[-1] == "DROP TABLE sqlspec_event_queue"
    assert len(statements) == 3


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
    with pytest.raises(ValueError, match="not found"):
        store.append_event_and_update_state(event, "app", "user", "missing", {})
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
    probes = iter([[], [("event",)]])
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
    await store.drop_table()
    await store.drop_table()
    assert statements[-1] == 'DROP TABLE "LITESTAR_SESSION"'


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

    fetch_results: list[list[tuple[str]]] = []

    def _execute(sql: str, params: tuple[Any, ...] = ()) -> None:
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT event_id FROM"):
            fetch_results.clear()
            fetch_results.append([(ev,) for ev in persisted if ev in params])
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


def test_teradata_event_queue_type_helpers_and_dictionary_reconcile(monkeypatch) -> None:
    from contextlib import nullcontext
    from types import SimpleNamespace

    executed: list[str] = []
    dictionary = SimpleNamespace(
        get_indexes=lambda *_a, **_k: [{"index_name": "idx_sqlspec_event_queue_channel_status"}]
    )
    driver = SimpleNamespace(
        select_one_or_none=lambda *_a: {"found": 1},
        execute=executed.append,
        commit=lambda: None,
        data_dictionary=dictionary,
    )
    monkeypatch.setattr(TeradataConfig, "provide_session", lambda _: nullcontext(driver))
    store = TeradataEventQueueStore(TeradataConfig(connection_config={"host": "localhost"}))
    assert store._column_types() == ("JSON", "JSON", "TIMESTAMP(6) WITH TIME ZONE")
    assert store._string_type(128) == "VARCHAR(128)"
    assert store._string_type(40000) == "CLOB"
    assert store._timestamp_default() == "CURRENT_TIMESTAMP(6)"
    assert store._wrap_create_statement("CREATE TABLE q", "table") == "CREATE TABLE q"
    assert store._wrap_drop_statement("DROP TABLE q") == "DROP TABLE q"
    store.reconcile_companion_objects_sync(driver)
    assert executed == []


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
