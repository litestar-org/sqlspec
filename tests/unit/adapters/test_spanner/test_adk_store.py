# pyright: reportPrivateUsage=false
"""Unit tests for Spanner ADK store behavior."""

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast, get_args, get_origin
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import NotFound
from typing_extensions import NotRequired

from sqlspec.adapters.spanner.adk import (
    SpannerADKConfig,
    SpannerADKRetentionConfig,
    SpannerSyncADKMemoryStore,
    SpannerSyncADKStore,
)
from sqlspec.config import ADKConfig
from sqlspec.extensions.adk import EventRecord, MemoryRecord


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def _spanner_not_found(message: str) -> NotFound:
    return NotFound(message)  # type: ignore[no-untyped-call]


def test_spanner_adk_config_types_adapter_local_optimizations() -> None:
    """Spanner ADK optimization settings are typed on the adapter-local extension config."""

    assert cast("Any", ADKConfig).__optional_keys__ <= cast("Any", SpannerADKConfig).__optional_keys__

    expected_types: dict[str, object] = {
        "shard_count": int,
        "session_table_options": str,
        "events_table_options": str,
        "memory_table_options": str,
        "expires_index_options": str,
        "retention": SpannerADKRetentionConfig,
    }
    for feature_name, expected_type in expected_types.items():
        annotation = cast("Any", SpannerADKConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (expected_type,)

    for feature_name in ("session_ttl_seconds", "event_ttl_seconds", "memory_ttl_seconds"):
        annotation = cast("Any", SpannerADKRetentionConfig.__annotations__[feature_name])
        assert get_origin(annotation) is NotRequired
        assert get_args(annotation) == (int,)


def test_insert_event_preserves_event_record_timestamp() -> None:
    """Spanner stores the ADK event timestamp, not the commit timestamp."""
    store = SpannerSyncADKStore(_mock_config())
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    event: EventRecord = {
        "id": "event-1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "session-1",
        "invocation_id": "inv-1",
        "timestamp": timestamp,
        "event_data": {"content": "hello"},
    }

    with patch.object(store, "_run_write") as run_write:
        store._insert_event(event)  # pyright: ignore[reportPrivateUsage]

    statements = run_write.call_args.args[0]
    sql, params, _types = statements[0]
    assert "@timestamp" in sql
    assert "PENDING_COMMIT_TIMESTAMP()" not in sql
    assert params["id"] == "event-1"
    assert params["timestamp"] is timestamp


def test_append_event_and_update_state_preserves_event_record_timestamp() -> None:
    """Atomic append uses the ADK event timestamp while session update uses commit time."""
    store = SpannerSyncADKStore(_mock_config())
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    event: EventRecord = {
        "id": "event-1",
        "app_name": "app",
        "user_id": "u1",
        "session_id": "session-1",
        "invocation_id": "inv-1",
        "timestamp": timestamp,
        "event_data": {"content": "hello"},
    }
    # Stub the post-write SELECT — the contract requires returning the refreshed record.
    fake_record = {
        "id": "session-1",
        "app_name": "app",
        "user_id": "u1",
        "state": {"turn": 1},
        "create_time": timestamp,
        "update_time": timestamp,
    }

    with patch.object(store, "_run_write") as run_write, patch.object(store, "_get_session", return_value=fake_record):
        returned = store.append_event_and_update_state(event, "app", "u1", "session-1", {"turn": 1})

    event_sql, event_params, _event_types = run_write.call_args.args[0][0]
    update_sql, _state_params, _state_types = run_write.call_args.args[0][1]
    assert "@timestamp" in event_sql
    assert "PENDING_COMMIT_TIMESTAMP()" not in event_sql
    assert event_params["id"] == "event-1"
    assert event_params["timestamp"] is timestamp
    assert "PENDING_COMMIT_TIMESTAMP()" in update_sql
    assert returned == fake_record


def test_spanner_session_table_generates_row_deletion_policy_from_retention() -> None:
    store = SpannerSyncADKStore(_mock_config({"retention": {"session_ttl_seconds": 86_400}}))

    sql = store._get_create_sessions_table_sql()

    assert "ROW DELETION POLICY (OLDER_THAN(create_time, INTERVAL 1 DAY))" in sql


def test_spanner_events_table_rounds_retention_up_to_days() -> None:
    store = SpannerSyncADKStore(_mock_config({"retention": {"event_ttl_seconds": 86_401}}))

    sql = store._get_create_events_table_sql()

    assert "ROW DELETION POLICY (OLDER_THAN(timestamp, INTERVAL 2 DAY))" in sql


def test_spanner_memory_table_generates_ttl_and_table_options() -> None:
    store = SpannerSyncADKMemoryStore(
        _mock_config({"memory_table_options": "locality_group = 'hot'", "retention": {"memory_ttl_seconds": 604_800}})
    )

    statements = store._get_create_memory_table_sql()
    table_sql = statements[0]

    assert "OPTIONS (locality_group = 'hot')" in table_sql
    assert "ROW DELETION POLICY (OLDER_THAN(inserted_at, INTERVAL 7 DAY))" in table_sql


def test_spanner_session_store_emits_expiration_indexes_with_configured_options() -> None:
    config = _mock_config({"expires_index_options": "locality_group = 'cold'"})
    database = config.get_database.return_value
    database.list_tables.return_value = []
    store = SpannerSyncADKStore(config)

    store.create_tables()

    ddl_statements = database.update_ddl.call_args.args[0]
    assert (
        "CREATE INDEX IF NOT EXISTS idx_adk_session_update_time "
        "ON adk_session(update_time) OPTIONS (locality_group = 'cold')"
    ) in ddl_statements
    assert (
        "CREATE INDEX IF NOT EXISTS idx_adk_event_timestamp ON adk_event(timestamp) OPTIONS (locality_group = 'cold')"
    ) in ddl_statements


def test_spanner_session_store_drops_expiration_indexes_before_tables() -> None:
    store = SpannerSyncADKStore(_mock_config())

    statements = store._get_drop_tables_sql()

    assert statements[:2] == ["DROP INDEX idx_adk_event_timestamp", "DROP INDEX idx_adk_session_update_time"]
    assert statements[-2:] == ["DROP TABLE adk_event", "DROP TABLE adk_session"]


def test_spanner_memory_insert_entries_writes_clean_break_record() -> None:
    store = SpannerSyncADKMemoryStore(_mock_config())
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    entry: MemoryRecord = {
        "id": "memory-1",
        "session_id": "session-1",
        "app_name": "app",
        "user_id": "user",
        "event_id": "event-1",
        "author": "assistant",
        "timestamp": timestamp,
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": {"source": "unit"},
        "inserted_at": timestamp,
    }

    with patch.object(store, "_event_exists", return_value=False), patch.object(store, "_run_write") as run_write:
        inserted = store.insert_memory_entries([entry])

    assert inserted == 1
    statements = run_write.call_args.args[0]
    sql, params, _types = statements[0]
    assert "INSERT INTO adk_memory" in sql
    assert params["content_json"] == '{"text":"hello"}'
    assert params["metadata_json"] == '{"source":"unit"}'
    assert params["inserted_at"] is timestamp


def test_spanner_memory_rows_to_records_decodes_json_fields() -> None:
    store = SpannerSyncADKMemoryStore(_mock_config())
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)

    records = store._rows_to_records([
        (
            "memory-1",
            "session-1",
            "app",
            "user",
            "event-1",
            "assistant",
            timestamp,
            '{"text":"hello"}',
            "hello",
            '{"source":"unit"}',
            timestamp,
        )
    ])

    assert records[0]["content_json"] == {"text": "hello"}
    assert records[0]["metadata_json"] == {"source": "unit"}
    assert records[0]["content_text"] == "hello"


def test_spanner_reset_drop_tables_filters_absent_tables() -> None:
    config = _mock_config()
    config.get_database.return_value.list_tables.return_value = [SimpleNamespace(table_id="adk_events")]
    store = SpannerSyncADKStore(config)

    statements = store._get_reset_drop_tables_sql()

    assert statements == ["DROP INDEX idx_adk_events_timestamp", "DROP TABLE adk_events"]


def test_spanner_memory_reset_drop_tables_filters_absent_tables_and_indexes() -> None:
    config = _mock_config()
    config.get_database.return_value.list_tables.return_value = [SimpleNamespace(table_id="adk_memory_entries")]
    store = SpannerSyncADKMemoryStore(config)

    statements = store._get_reset_drop_memory_table_sql()

    assert statements == [
        "DROP INDEX idx_adk_memory_entries_session",
        "DROP INDEX idx_adk_memory_entries_app_user_time",
        "DROP TABLE adk_memory_entries",
    ]


def test_get_session_returns_none_when_spanner_session_table_missing() -> None:
    store = SpannerSyncADKStore(_mock_config())

    with patch.object(store, "_run_read", side_effect=_spanner_not_found("adk_session not found")):
        result = store.get_session("app", "user", "session")

    assert result is None


def test_list_sessions_returns_empty_when_spanner_session_table_missing() -> None:
    store = SpannerSyncADKStore(_mock_config())

    with patch.object(store, "_run_read", side_effect=_spanner_not_found("adk_session not found")):
        result = store.list_sessions("app", "user")

    assert result == []


def test_get_events_returns_empty_when_spanner_events_table_missing() -> None:
    store = SpannerSyncADKStore(_mock_config())

    with patch.object(store, "_run_read", side_effect=_spanner_not_found("adk_event not found")):
        result = store.get_events("app", "user", "session")

    assert result == []
