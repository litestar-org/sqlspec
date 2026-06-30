"""Unit tests for psycopg ADK store sync wrappers."""

from datetime import datetime, timezone
from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock

from psycopg.types.json import Jsonb
from typing_extensions import NotRequired, Self

from sqlspec.adapters.psycopg.adk import PsycopgADKConfig, PsycopgAsyncADKStore, PsycopgSyncADKStore
from sqlspec.config import ADKConfig


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


class _DummyCursor:
    def __init__(self, rows: "list[dict[str, Any]] | None" = None) -> None:
        self.execute_calls: list[tuple[Any, Any]] = []
        self._rows = rows or []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def execute(self, query: Any, params: Any = None) -> None:
        self.execute_calls.append((query, params))

    def fetchall(self) -> "list[dict[str, Any]]":
        return self._rows

    def fetchone(self) -> "dict[str, Any] | None":
        return self._rows[0] if self._rows else None


class _DummyConnection:
    def __init__(self, cursor: _DummyCursor) -> None:
        self._cursor = cursor
        self.commit_called = False

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def cursor(self, **kwargs: Any) -> _DummyCursor:
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
    config = _DummyConfig(connection)
    store._config = config  # type: ignore[attr-defined]
    store._events_table = "test_events"  # type: ignore[attr-defined]
    store._session_table = "test_sessions"  # type: ignore[attr-defined]
    store._app_state_table = "test_app_state"  # type: ignore[attr-defined]
    store._user_state_table = "test_user_state"  # type: ignore[attr-defined]
    store._metadata_table = "test_metadata"  # type: ignore[attr-defined]
    store._owner_id_column_ddl = None  # type: ignore[attr-defined]
    store._owner_id_column_name = None  # type: ignore[attr-defined]
    return store, cursor, connection


def test_psycopg_adk_config_types_adapter_local_optimizations() -> None:
    """Psycopg ADK optimization switches live on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", PsycopgADKConfig).__optional_keys__
    assert cast("Any", PsycopgADKConfig).__optional_keys__ - cast("Any", ADKConfig).__optional_keys__ == {
        "enable_event_generated_columns",
        "enable_covering_indexes",
    }

    for feature_name in ("enable_event_generated_columns", "enable_covering_indexes"):
        annotation = cast("Any", PsycopgADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (bool,)


async def test_psycopg_async_adk_events_table_uses_plain_schema_by_default() -> None:
    """Psycopg async ADK optimization DDL stays opt-in through extension config."""

    store = PsycopgAsyncADKStore(_mock_config())

    sql = await store._get_create_events_table_sql()

    assert "author_gc" not in sql
    assert "node_path_gc" not in sql
    assert "INCLUDE (invocation_id)" not in sql


async def test_psycopg_async_adk_events_table_applies_adapter_local_extension_config() -> None:
    """Psycopg async ADK extension settings enable PostgreSQL-specific event DDL."""

    store = PsycopgAsyncADKStore(
        _mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True})
    )

    sql = await store._get_create_events_table_sql()

    assert "author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED" in sql
    assert "node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED" in sql
    assert "idx_adk_event_author_gc" in sql
    assert "idx_adk_event_node_path_gc" in sql
    assert "INCLUDE (invocation_id)" in sql


def test_psycopg_sync_adk_events_table_uses_plain_schema_by_default() -> None:
    """Psycopg sync ADK optimization DDL stays opt-in through extension config."""

    store = PsycopgSyncADKStore(_mock_config())

    sql = store._get_create_events_table_sql()

    assert "author_gc" not in sql
    assert "node_path_gc" not in sql
    assert "INCLUDE (invocation_id)" not in sql


def test_psycopg_sync_adk_events_table_applies_adapter_local_extension_config() -> None:
    """Psycopg sync ADK extension settings enable PostgreSQL-specific event DDL."""

    store = PsycopgSyncADKStore(_mock_config({"enable_event_generated_columns": True, "enable_covering_indexes": True}))

    sql = store._get_create_events_table_sql()

    assert "author_gc VARCHAR(256) GENERATED ALWAYS AS (event_data->>'author') STORED" in sql
    assert "node_path_gc TEXT GENERATED ALWAYS AS (event_data->'node_info'->>'path') STORED" in sql
    assert "idx_adk_event_author_gc" in sql
    assert "idx_adk_event_node_path_gc" in sql
    assert "INCLUDE (invocation_id)" in sql


def test_sync_append_event_inserts_without_session_update() -> None:
    """append_event must insert a single event without writing session state."""
    store, cursor, connection = _build_store()
    event_record = {
        "id": "event-1",
        "app_name": "app",
        "user_id": "user",
        "session_id": "session-1",
        "invocation_id": "",
        "timestamp": datetime.now(timezone.utc),
        "event_data": {"id": "event-1"},
    }

    store._append_event(event_record)  # type: ignore[arg-type]

    assert len(cursor.execute_calls) == 1
    _, params = cursor.execute_calls[0]
    assert params[0] == "event-1"
    assert params[1] == "session-1"
    assert isinstance(params[4], Jsonb)
    assert connection.commit_called


def test_sync_get_events_passes_after_timestamp_and_limit() -> None:
    """get_events must forward after_timestamp and limit to the sync query."""
    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [
        {
            "session_id": "session-1",
            "invocation_id": "",
            "timestamp": base_time,
            "event_data": {"id": "event-2"},
            "id": "event-2",
            "app_name": "app",
            "user_id": "user",
        }
    ]
    store, cursor, _ = _build_store(rows)

    result = store._get_events("app", "user", "session-1", after_timestamp=base_time, limit=1)

    assert len(cursor.execute_calls) == 1
    _, params = cursor.execute_calls[0]
    assert params == ("app", "user", "session-1", base_time, 1)
    assert result[0]["event_data"]["id"] == "event-2"


def test_sync_get_events_limit_zero_returns_empty_without_query() -> None:
    """get_events(limit=0) must return no events without querying."""
    store, cursor, _ = _build_store()

    result = store._get_events("app", "user", "session-1", limit=0)

    assert result == []
    assert cursor.execute_calls == []


def test_sync_append_event_and_update_state_writes_scoped_state_in_one_unit() -> None:
    """append_event_and_update_state must use event_data and optional scoped state."""
    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [
        {
            "id": "session-1",
            "app_name": "app",
            "user_id": "user",
            "state": {"session": True},
            "create_time": base_time,
            "update_time": base_time,
        }
    ]
    store, cursor, connection = _build_store(rows)
    event_record = {
        "id": "event-1",
        "app_name": "app",
        "user_id": "user",
        "session_id": "session-1",
        "invocation_id": "invoke-1",
        "timestamp": base_time,
        "event_data": {"id": "event-1"},
    }

    result = store._append_event_and_update_state(
        event_record,  # type: ignore[arg-type]
        "app",
        "user",
        "session-1",
        {"session": True},
        app_state={},
        user_state={"user:theme": "dark"},
    )

    assert result["id"] == "session-1"
    assert len(cursor.execute_calls) == 4
    _, insert_params = cursor.execute_calls[0]
    _, update_params = cursor.execute_calls[1]
    _, app_state_params = cursor.execute_calls[2]
    _, user_state_params = cursor.execute_calls[3]
    assert insert_params[0] == "event-1"
    assert isinstance(insert_params[4], Jsonb)
    assert getattr(update_params[0], "obj", None) == {"session": True}
    assert update_params[1:4] == ("app", "user", "session-1")
    assert app_state_params[0] == "app"
    assert getattr(app_state_params[1], "obj", None) == {}
    assert user_state_params[:2] == ("app", "user")
    assert getattr(user_state_params[2], "obj", None) == {"user:theme": "dark"}
    assert connection.commit_called
