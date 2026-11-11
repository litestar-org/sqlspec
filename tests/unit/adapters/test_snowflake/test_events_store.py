"""Snowflake event queue VARIANT persistence contracts."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.snowflake.events import SnowflakeEventQueueStore, SnowflakeSyncEventQueueStore
from sqlspec.core import SQL
from sqlspec.extensions.events import EventMessage, SyncEventChannel, SyncTableEventQueue
from sqlspec.extensions.events.migrations.reconcile import downgrade, reconcile
from sqlspec.migrations.context import MigrationContext
from sqlspec.utils.adapters import resolve_extension_store_class
from tests.unit.adapters.test_snowflake._store_helpers import store_config


def test_events_store_resolution_and_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch, {"events": {"queue_table": "app_events", "backend": "poll_queue"}})
    assert SnowflakeSyncEventQueueStore is SnowflakeEventQueueStore
    assert (
        resolve_extension_store_class(
            config, extension="events", store_attr="events_store_class", store_suffix="EventQueueStore"
        )
        is SnowflakeEventQueueStore
    )
    store = SnowflakeEventQueueStore(config)
    statements = store.create_statements()
    assert len(statements) == 1
    assert "payload_json VARIANT NOT NULL" in statements[0]
    assert "metadata_json VARIANT" in statements[0]
    assert "available_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP()" in statements[0]
    assert store.companion_index_statement() is None
    assert store.drop_statements() == ["DROP TABLE IF EXISTS app_events"]
    driver.data_dictionary.get_tables.return_value = []
    context = MigrationContext(config=config, dialect="snowflake", driver=driver)
    reconcile(context)
    assert driver.execute_script.call_count == 1
    driver.commit.assert_not_called()
    driver.execute_script.reset_mock()
    downgrade(context)
    driver.execute_script.assert_called_once_with("DROP TABLE IF EXISTS app_events")


def test_events_publish_and_batch_use_select_parse_json(monkeypatch: pytest.MonkeyPatch) -> None:
    config, driver = store_config(monkeypatch)
    queue = SyncTableEventQueue(config)
    event_id = queue.publish("jobs", {"task": "sync"})
    statement = driver.execute.call_args.args[0]
    assert "SELECT :event_id, :channel, PARSE_JSON(:payload_json), PARSE_JSON(:metadata_json)" in statement.sql
    compiled_sql, parameters = statement.compile()
    assert "SELECT ?, ?, PARSE_JSON(?), PARSE_JSON(?)" in compiled_sql
    assert parameters[1:4] == ("jobs", '{"task":"sync"}', None)
    assert len(event_id) == 32
    ids = queue.publish_many([("jobs", {"n": 1}, {"trace": "abc"})])
    assert len(ids) == 1
    assert "PARSE_JSON(:payload_json)" in str(driver.execute_many.call_args.args[0])


@pytest.mark.parametrize("payload, metadata", [('{"k":1}', None), ({"k": 1}, {"trace": "abc"})])
def test_events_dequeue_hydrates_both_variant_wire_shapes(
    monkeypatch: pytest.MonkeyPatch, payload: object, metadata: object
) -> None:
    config, driver = store_config(monkeypatch)
    now = datetime.now(UTC)
    driver.select_one_or_none.return_value = {
        "event_id": "e1",
        "channel": "jobs",
        "payload_json": payload,
        "metadata_json": metadata,
        "attempts": 0,
        "available_at": now,
        "lease_expires_at": None,
        "created_at": now,
    }
    event = SyncTableEventQueue(config).dequeue("jobs")
    assert isinstance(event, EventMessage)
    assert event.payload == {"k": 1}
    assert event.metadata == metadata


@pytest.mark.parametrize("select_for_update", [False, True])
@pytest.mark.parametrize("affected_count", [0, 1])
def test_event_channel_consumes_uppercase_snowflake_rows(
    monkeypatch: pytest.MonkeyPatch, select_for_update: bool, affected_count: int
) -> None:
    config, driver = store_config(
        monkeypatch, {"events": {"backend": "poll_queue", "select_for_update": select_for_update}}
    )
    config.driver_features["enable_lowercase_column_names"] = False
    channel = SyncEventChannel(config)
    event_id = channel.publish("jobs", {"task": "sync"}, {"trace": "abc"})
    now = datetime.now(UTC)
    row: dict[str, Any] = {
        "EVENT_ID": event_id,
        "CHANNEL": "jobs",
        "PAYLOAD_JSON": '{"task":"sync"}',
        "METADATA_JSON": '{"trace":"abc"}',
        "ATTEMPTS": 0,
        "AVAILABLE_AT": now,
        "LEASE_EXPIRES_AT": None,
        "CREATED_AT": now,
    }
    driver.select_one_or_none.return_value = row

    def execute(statement: SQL) -> MagicMock:
        result = MagicMock()
        result.rows_affected = affected_count
        if statement.sql.startswith("UPDATE"):
            row["LEASE_EXPIRES_AT"] = statement.named_parameters.get("lease_expires_at")
        return result

    driver.execute.side_effect = execute
    events = channel.iter_events("jobs")
    event = next(events)
    assert event.event_id == event_id
    assert event.payload == {"task": "sync"}
    assert event.metadata == {"trace": "abc"}
    assert event.attempts == 1
    assert event.lease_expires_at == row["LEASE_EXPIRES_AT"]
    channel.ack(event_id)
    channel.shutdown()
