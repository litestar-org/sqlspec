# pyright: reportPrivateUsage=false
"""Unit tests for Spanner ADK store behavior."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from sqlspec.adapters.spanner.adk.store import SpannerSyncADKMemoryStore, SpannerSyncADKStore
from sqlspec.extensions.adk import EventRecord, MemoryRecord


def _mock_config(adk_config: dict[str, object] | None = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"adk": adk_config or {}}
    return config


def test_insert_event_preserves_event_record_timestamp() -> None:
    """Spanner stores the ADK event timestamp, not the commit timestamp."""
    store = SpannerSyncADKStore(_mock_config())
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    event: EventRecord = {
        "session_id": "session-1",
        "invocation_id": "inv-1",
        "author": "user",
        "timestamp": timestamp,
        "event_json": {"id": "event-1"},
    }

    with patch.object(store, "_run_write") as run_write:
        store._insert_event(event)  # pyright: ignore[reportPrivateUsage]

    statements = run_write.call_args.args[0]
    sql, params, _types = statements[0]
    assert "@timestamp" in sql
    assert "PENDING_COMMIT_TIMESTAMP()" not in sql
    assert params["timestamp"] is timestamp


async def test_append_event_and_update_state_preserves_event_record_timestamp() -> None:
    """Atomic append uses the ADK event timestamp while session update uses commit time."""
    store = SpannerSyncADKStore(_mock_config())
    timestamp = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    event: EventRecord = {
        "session_id": "session-1",
        "invocation_id": "inv-1",
        "author": "user",
        "timestamp": timestamp,
        "event_json": {"id": "event-1"},
    }

    with patch.object(store, "_run_write") as run_write:
        await store.append_event_and_update_state(event, "session-1", {"turn": 1})

    event_sql, event_params, _event_types = run_write.call_args.args[0][0]
    update_sql, _state_params, _state_types = run_write.call_args.args[0][1]
    assert "@timestamp" in event_sql
    assert "PENDING_COMMIT_TIMESTAMP()" not in event_sql
    assert event_params["timestamp"] is timestamp
    assert "PENDING_COMMIT_TIMESTAMP()" in update_sql


async def test_spanner_session_table_generates_row_deletion_policy_from_retention() -> None:
    store = SpannerSyncADKStore(_mock_config({"retention": {"session_ttl_seconds": 86_400}}))

    sql = await store._get_create_sessions_table_sql()

    assert "ROW DELETION POLICY (OLDER_THAN(create_time, INTERVAL 1 DAY))" in sql


async def test_spanner_events_table_rounds_retention_up_to_days() -> None:
    store = SpannerSyncADKStore(_mock_config({"retention": {"event_ttl_seconds": 86_401}}))

    sql = await store._get_create_events_table_sql()

    assert "ROW DELETION POLICY (OLDER_THAN(timestamp, INTERVAL 2 DAY))" in sql


async def test_spanner_memory_table_generates_ttl_and_table_options() -> None:
    store = SpannerSyncADKMemoryStore(
        _mock_config({"memory_table_options": "locality_group = 'hot'", "retention": {"memory_ttl_seconds": 604_800}})
    )

    statements = await store._get_create_memory_table_sql()
    table_sql = statements[0]

    assert "OPTIONS (locality_group = 'hot')" in table_sql
    assert "ROW DELETION POLICY (OLDER_THAN(inserted_at, INTERVAL 7 DAY))" in table_sql


async def test_spanner_memory_insert_entries_writes_clean_break_record() -> None:
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
        inserted = await store.insert_memory_entries([entry])

    assert inserted == 1
    statements = run_write.call_args.args[0]
    sql, params, _types = statements[0]
    assert "INSERT INTO adk_memory_entries" in sql
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
