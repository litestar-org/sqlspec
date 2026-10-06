"""Integration tests for the Spanner EventChannel queue backend."""

from typing import Any

import pytest

from sqlspec import SQLSpec
from sqlspec.adapters.spanner import SpannerAsyncConfig, SpannerSyncConfig
from sqlspec.adapters.spanner.events import SpannerAsyncEventQueueStore, SpannerSyncEventQueueStore
from tests.integration.adapters.spanner.spanner._modes import invoke, mode_session

pytestmark = [pytest.mark.spanner, pytest.mark.integration, pytest.mark.anyio]


async def _publish_and_consume(
    config: "SpannerSyncConfig | SpannerAsyncConfig",
    channel_name: str,
    payload: "dict[str, Any]",
    **publish_kwargs: Any,
) -> "tuple[Any, Any]":
    spec = SQLSpec()
    spec.add_config(config)
    channel = spec.event_channel(config)
    event_id = await invoke(channel.publish(channel_name, payload, **publish_kwargs))
    iterator: Any = channel.iter_events(channel_name, poll_interval=0.05)
    if isinstance(config, SpannerAsyncConfig):
        message = await iterator.__anext__()
        await iterator.aclose()
    else:
        message = next(iterator)
        iterator.close()
    await invoke(channel.ack(message.event_id))
    return event_id, message


async def test_spanner_event_channel_queue_lifecycle(
    spanner_events_mode_config: "SpannerSyncConfig | SpannerAsyncConfig",
    spanner_event_store: "SpannerSyncEventQueueStore | SpannerAsyncEventQueueStore",
) -> None:
    """Queue-backed events publish, consume, and ack on both Spanner adapters."""
    event_id, message = await _publish_and_consume(
        spanner_events_mode_config, "notifications", {"action": "spanner_event"}
    )
    async with mode_session(spanner_events_mode_config) as driver:
        row = await invoke(
            driver.select_one(
                f"SELECT status FROM {spanner_event_store.table_name} WHERE event_id = @event_id",
                {"event_id": event_id},
            )
        )

    assert message.event_id == event_id
    assert message.payload == {"action": "spanner_event"}
    assert row["status"] == "acked"


async def test_spanner_event_metadata_roundtrip(
    spanner_events_mode_config: "SpannerSyncConfig | SpannerAsyncConfig",
    spanner_event_store: "SpannerSyncEventQueueStore | SpannerAsyncEventQueueStore",
) -> None:
    """Event metadata survives the queue round-trip on both Spanner adapters."""
    metadata = {"source": "test", "priority": 1}
    event_id, message = await _publish_and_consume(
        spanner_events_mode_config, "metadata_test", {"data": "value"}, metadata=metadata
    )

    assert message.event_id == event_id
    assert message.metadata == metadata
    assert message.payload == {"data": "value"}


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
