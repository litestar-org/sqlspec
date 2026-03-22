"""Unit tests for psycopg ADK store sync wrappers."""

from datetime import datetime, timezone
from typing import Any

from psycopg.types.json import Jsonb
from typing_extensions import Self

from sqlspec.adapters.psycopg.adk.store import PsycopgSyncADKStore


class _DummyCursor:
    def __init__(self, rows: "list[dict[str, Any]] | None" = None) -> None:
        self.execute_calls: list[tuple[Any, Any]] = []
        self._rows = rows or []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def execute(self, query: Any, params: Any) -> None:
        self.execute_calls.append((query, params))

    def fetchall(self) -> "list[dict[str, Any]]":
        return self._rows


class _DummyConnection:
    def __init__(self, cursor: _DummyCursor) -> None:
        self._cursor = cursor
        self.commit_called = False

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def cursor(self) -> _DummyCursor:
        return self._cursor

    def commit(self) -> None:
        self.commit_called = True


class _DummyConfig:
    def __init__(self, connection: _DummyConnection) -> None:
        self._connection = connection

    def provide_connection(self) -> _DummyConnection:
        return self._connection


def _build_store(
    rows: "list[dict[str, Any]] | None" = None,
) -> "tuple[PsycopgSyncADKStore, _DummyCursor, _DummyConnection]":
    cursor = _DummyCursor(rows)
    connection = _DummyConnection(cursor)
    store = PsycopgSyncADKStore.__new__(PsycopgSyncADKStore)  # type: ignore[call-arg]
    store._config = _DummyConfig(connection)  # type: ignore[attr-defined]
    store._events_table = "test_events"  # type: ignore[attr-defined]
    store._session_table = "test_sessions"  # type: ignore[attr-defined]
    store._owner_id_column_ddl = None  # type: ignore[attr-defined]
    store._owner_id_column_name = None  # type: ignore[attr-defined]
    return store, cursor, connection


def test_sync_append_event_inserts_without_session_update() -> None:
    """append_event must insert a single event without writing session state."""
    store, cursor, connection = _build_store()
    event_record = {
        "session_id": "session-1",
        "invocation_id": "",
        "author": "assistant",
        "timestamp": datetime.now(timezone.utc),
        "event_json": {"id": "event-1"},
    }

    store._append_event(event_record)  # type: ignore[arg-type]

    assert len(cursor.execute_calls) == 1
    _, params = cursor.execute_calls[0]
    assert params[0] == "session-1"
    assert isinstance(params[4], Jsonb)
    assert connection.commit_called


def test_sync_get_events_passes_after_timestamp_and_limit() -> None:
    """get_events must forward after_timestamp and limit to the sync query."""
    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [
        {
            "session_id": "session-1",
            "invocation_id": "",
            "author": "assistant",
            "timestamp": base_time,
            "event_json": {"id": "event-2"},
        }
    ]
    store, cursor, _ = _build_store(rows)

    result = store._get_events("session-1", after_timestamp=base_time, limit=1)

    assert len(cursor.execute_calls) == 1
    _, params = cursor.execute_calls[0]
    assert params == ("session-1", base_time, 1)
    assert result[0]["event_json"]["id"] == "event-2"
