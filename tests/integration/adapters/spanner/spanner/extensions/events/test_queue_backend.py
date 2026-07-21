"""Integration tests for Spanner EventChannel queue backend."""

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.spanner import SpannerSyncConfig
from sqlspec.adapters.spanner.events import SpannerSyncEventQueueStore

pytestmark = [pytest.mark.spanner, pytest.mark.integration]


def test_spanner_event_channel_queue_fallback(
    spanner_events_config: SpannerSyncConfig, spanner_event_store: SpannerSyncEventQueueStore
) -> None:
    """Queue-backed events work on Spanner via the table queue backend."""
    spec = SQLSpec()
    spec.add_config(spanner_events_config)
    channel = spec.event_channel(spanner_events_config)

    event_id = channel.publish("notifications", {"action": "spanner_event"})

    iterator = channel.iter_events("notifications", poll_interval=0.05)
    message = next(iterator)
    channel.ack(message.event_id)

    with spanner_events_config.provide_session() as driver:
        row = driver.select_one(
            "SELECT status FROM sqlspec_event_queue WHERE event_id = @event_id", {"event_id": event_id}
        )

    assert message.payload["action"] == "spanner_event"
    assert row["status"] == "acked"


def test_spanner_event_store_create_statements(spanner_events_config: SpannerSyncConfig) -> None:
    """Verify create_statements returns separate table and index statements."""
    store = SpannerSyncEventQueueStore(spanner_events_config)
    statements = store.create_statements()

    assert len(statements) == 2
    assert "CREATE TABLE" in statements[0]
    assert "PRIMARY KEY" in statements[0]
    assert "CREATE INDEX" in statements[1]


def test_spanner_event_store_drop_statements(spanner_events_config: SpannerSyncConfig) -> None:
    """Verify drop_statements returns index-first then table order."""
    store = SpannerSyncEventQueueStore(spanner_events_config)
    statements = store.drop_statements()

    assert len(statements) == 2
    assert "DROP INDEX" in statements[0]
    assert "DROP TABLE" in statements[1]


def test_spanner_event_store_no_if_exists_wrapper(spanner_events_config: SpannerSyncConfig) -> None:
    """Verify Spanner store does not wrap statements with IF NOT EXISTS."""
    store = SpannerSyncEventQueueStore(spanner_events_config)
    statements = store.create_statements()

    for stmt in statements:
        assert "IF NOT EXISTS" not in stmt
        assert "IF EXISTS" not in stmt


def test_spanner_event_store_column_types(spanner_events_config: SpannerSyncConfig) -> None:
    """Verify Spanner-specific column types are used."""
    store = SpannerSyncEventQueueStore(spanner_events_config)
    statements = store.create_statements()
    table_sql = statements[0]

    assert "STRING(64)" in table_sql
    assert "STRING(128)" in table_sql
    assert "INT64" in table_sql
    assert "JSON" in table_sql
    assert "VARCHAR" not in table_sql
    assert "INTEGER" not in table_sql


def test_spanner_event_metadata_roundtrip(
    spanner_events_config: SpannerSyncConfig, spanner_event_store: SpannerSyncEventQueueStore
) -> None:
    """Events with metadata are correctly stored and retrieved."""
    spec = SQLSpec()
    spec.add_config(spanner_events_config)
    channel = spec.event_channel(spanner_events_config)

    metadata = {"source": "test", "priority": 1}
    event_id = channel.publish("metadata_test", {"data": "value"}, metadata=metadata)

    iterator = channel.iter_events("metadata_test", poll_interval=0.05)
    message = next(iterator)
    channel.ack(message.event_id)

    assert message.event_id == event_id
    assert message.metadata == metadata
    assert message.payload == {"data": "value"}
