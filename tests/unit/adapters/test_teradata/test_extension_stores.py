"""Teradata extension schema contracts."""

import pytest

from sqlspec.extensions.adk import StoredEvent, StoredMemory

from sqlspec.adapters.teradata import TeradataConfig
from sqlspec.adapters.teradata.adk import TeradataADKMemoryStore, TeradataADKStore
from sqlspec.adapters.teradata.events import TeradataEventQueueStore
from sqlspec.adapters.teradata.litestar import TeradataStore


def test_teradata_extension_schema_targets() -> None:
    config = TeradataConfig(
        connection_config={"host": "localhost"},
        extension_config={"adk": {"enable_memory": True}},
    )
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
    assert all(
        str(target.create_statement).startswith("CREATE MULTISET TABLE")
        for target in targets
    )
    assert (
        queue_store.create_statements()[1]
        == "CREATE INDEX idx_sqlspec_event_queue_channel_status (channel, status, available_at) ON sqlspec_event_queue"
    )


def test_teradata_adk_public_pagination_and_atomic_delete(monkeypatch) -> None:
    from contextlib import contextmanager
    from types import SimpleNamespace

    statements = []
    states = []
    conn = SimpleNamespace(autocommit=True)
    cursor = SimpleNamespace(
        execute=lambda sql, params=(): (
            statements.append((" ".join(sql.split()), params)),
            states.append(conn.autocommit),
        ),
        fetchall=list,
        close=lambda: None,
    )
    conn.cursor = lambda: cursor
    conn.commit = lambda: None
    conn.rollback = lambda: None

    @contextmanager
    def connection(_config):
        yield conn

    monkeypatch.setattr(TeradataConfig, "provide_connection", connection)
    store = TeradataADKStore(TeradataConfig(connection_config={"host": "localhost"}))
    assert store.list_sessions("app", "user", limit=5, offset=10) == []
    assert (
        "QUALIFY ROW_NUMBER() OVER (ORDER BY update_time DESC, id DESC) BETWEEN ? AND ?"
        in statements[0][0]
    )
    assert statements[0][1] == ("app", "user", 11, 15)
    store.delete_session("app", "user", "session")
    assert statements[-2] == (
        'DELETE FROM "adk_event" WHERE app_name = ? AND user_id = ? AND session_id = ?',
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
    monkeypatch.setattr(
        TeradataConfig, "provide_connection", lambda _: nullcontext(conn)
    )
    return conn, cursor


@pytest.mark.anyio
async def test_teradata_litestar_crud(teradata_connection) -> None:
    from datetime import UTC, datetime, timedelta

    conn, cursor = teradata_connection
    store = TeradataStore(TeradataConfig(connection_config={"host": "localhost"}))
    await store.set("session", b"data", expires_in=60)
    assert cursor.execute.call_args.args[1][0] == b"data"
    cursor.fetchone.return_value = (b"data", datetime.now(UTC) + timedelta(minutes=2))
    assert await store.get("session", renew_for=60) == b"data"
    assert await store.exists("session") is True
    cursor.fetchone.return_value = (datetime.now(UTC) + timedelta(minutes=2),)
    expiry = await store.expires_in("session")
    assert isinstance(expiry, int) and 110 <= expiry <= 120
    await store.delete("session")
    assert "WHERE session_id = ?" in cursor.execute.call_args.args[0]
    await store.delete_all()
    assert cursor.execute.call_args.args[0] == 'DELETE FROM "litestar_session" ALL'
    assert await store.delete_expired() == 1
    cursor.fetchone.return_value = None
    assert await store.get("missing") is None
    assert await store.exists("missing") is False
    assert await store.expires_in("missing") is None
    assert conn.commit.call_count >= 4


def test_teradata_adk_crud_and_cleanup(teradata_connection) -> None:
    from datetime import UTC, datetime

    conn, cursor = teradata_connection
    config = TeradataConfig(connection_config={"host": "localhost"})
    store = TeradataADKStore(config)
    now = datetime.now(UTC)
    cursor.fetchone.return_value = ("session", "app", "user", '{"count":1}', now, now)
    assert store.create_session("session", "app", "user", {"count": 1})["state"] == {
        "count": 1
    }
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
    cursor.fetchall.return_value = [
        ("event", "app", "user", "session", "invoke", now, '{"text":"hello"}')
    ]
    assert store.get_events("app", "user", "session", limit=2)[0]["event_data"] == {
        "text": "hello"
    }
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
    config = TeradataConfig(
        connection_config={"host": "localhost"},
        extension_config={"adk": {"enable_memory": True}},
    )
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
        (
            "memory",
            "session",
            "app",
            "user",
            "user",
            "event",
            "agent",
            now,
            '{"text":"hello"}',
            "hello",
            "{}",
            now,
        )
    ]
    assert (
        store.search_entries("hello", "app", "user", limit=3)[0]["content_text"]
        == "hello"
    )
    assert "TOP 3" in cursor.execute.call_args.args[0]
    assert store.search_entries("hello", "app", "user", limit=0) == []
    assert store.insert_memory_entries([]) == 0
    assert store.delete_entries_by_session("session") == 1
    assert store.delete_entries_older_than(3, "app", "user") == 1
    assert cursor.execute.call_args.args[1][1:] == ("app", "user")


@pytest.mark.parametrize("operation", ["get_session", "get_app_state", "get_user_state", "get_metadata", "list_sessions", "get_events", "delete_expired_events", "delete_idle_user_states", "delete_idle_sessions"])
def test_teradata_adk_absent_tables_are_empty(teradata_connection, operation) -> None:
    from datetime import UTC, datetime

    conn, cursor = teradata_connection
    cursor.execute.side_effect = RuntimeError("[Teradata Database] [Error 3807] Object does not exist")
    store = TeradataADKStore(TeradataConfig(connection_config={"host": "localhost"}))
    now = datetime.now(UTC)
    args = {"get_session": ("app", "user", "session"), "get_app_state": ("app",), "get_user_state": ("app", "user"), "get_metadata": ("key",), "list_sessions": ("app",), "get_events": ("app", "user", "session"), "delete_expired_events": (now,), "delete_idle_user_states": (now,), "delete_idle_sessions": (now,)}
    expected = {"list_sessions": [], "get_events": [], "delete_expired_events": 0, "delete_idle_user_states": 0, "delete_idle_sessions": 0}
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
