"""Snowflake event queue VARIANT persistence contracts."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.snowflake.config import SnowflakeConfig
from sqlspec.adapters.snowflake.events import SnowflakeEventQueueStore, SnowflakeSyncEventQueueStore
from sqlspec.core import SQL
from sqlspec.exceptions import SQLParsingError
from sqlspec.extensions.events import EventMessage, SyncEventChannel, SyncTableEventQueue
from sqlspec.extensions.events.migrations.reconcile import downgrade, reconcile
from sqlspec.migrations.context import MigrationContext
from sqlspec.utils.adapters import resolve_extension_store_class
from sqlspec.utils.serializers import from_json
from tests.unit.adapters.test_snowflake._store_helpers import store_config


def _event_sdk_boundary(
    monkeypatch: pytest.MonkeyPatch, connection: Any, *, failure_at: int | None = None
) -> tuple[list[Any], list[tuple[bool, int]]]:
    from snowflake.connector.errors import InterfaceError, ProgrammingError

    original_cursor = connection.cursor
    inserted: list[Any] = []
    transaction_states: list[tuple[bool, int]] = []
    insert_count = 0

    def cursor() -> Any:
        sdk_cursor = original_cursor()
        original_execute = sdk_cursor.execute

        def execute(sql: str, parameters: Any = None, **kwargs: Any) -> Any:
            nonlocal insert_count
            if sql.lstrip().upper().startswith("INSERT"):
                insert_count += 1
                transaction_states.append((connection.transaction_active, len(connection.committed_rows)))
                if insert_count == failure_at:
                    raise ProgrammingError(msg="second insert rejected", errno=1003, sqlstate="42000")
                inserted.append(parameters)
            return original_execute(sql, parameters, **kwargs)

        def executemany(sql: str, parameters: Any) -> Any:
            if sql.lstrip().upper().startswith("INSERT") and "SELECT" in sql.upper():
                raise InterfaceError(msg="Array binding requires INSERT VALUES")
            raise AssertionError("Event batch unexpectedly uses a different SQL shape")

        sdk_cursor.execute = execute
        sdk_cursor.executemany = executemany
        return sdk_cursor

    def connect(**options: Any) -> Any:
        connection.paramstyle = options["paramstyle"]
        return connection

    monkeypatch.setattr(connection, "cursor", cursor)
    monkeypatch.setattr(connection, "is_closed", lambda: False, raising=False)
    monkeypatch.setattr(connection, "close", lambda: None, raising=False)
    monkeypatch.setattr("snowflake.connector.connect", connect)
    return inserted, transaction_states


@pytest.mark.parametrize("paramstyle", ["qmark", "pyformat"])
def test_event_channel_batch_uses_native_driver_without_select_array_binding(
    monkeypatch: pytest.MonkeyPatch, snowflake_connection: Any, paramstyle: str
) -> None:
    inserted, transaction_states = _event_sdk_boundary(monkeypatch, snowflake_connection)
    config = SnowflakeConfig(
        connection_config={"account": "test-account", "user": "test-user", "paramstyle": paramstyle},
        driver_features={"enable_lowercase_column_names": False},
        extension_config={"events": {"backend": "poll_queue"}},
    )
    channel = SyncEventChannel(config)
    try:
        ids = channel.publish_many([("jobs", {"n": 1}, {"trace": "one"}), ("jobs", {"n": 2}, None)])
        assert len(set(ids)) == 2
        assert [row[0] for row in inserted] == ids
        assert [row[1] for row in inserted] == ["jobs", "jobs"]
        assert [from_json(row[2]) for row in inserted] == [{"n": 1}, {"n": 2}]
        assert from_json(inserted[0][3]) == {"trace": "one"}
        assert inserted[1][3] is None
        assert transaction_states == [(True, 0), (True, 0)]
        assert snowflake_connection.committed_rows == inserted
        assert snowflake_connection.pending_rows == []
        row = inserted[0]
        available_at = row[5][1] if isinstance(row[5], tuple) else row[5]
        created_at = row[8][1] if isinstance(row[8], tuple) else row[8]
        snowflake_connection.default_result = {
            "description": [
                ("EVENT_ID", 2),
                ("CHANNEL", 2),
                ("PAYLOAD_JSON", 5),
                ("METADATA_JSON", 5),
                ("ATTEMPTS", 0),
                ("AVAILABLE_AT", 8),
                ("LEASE_EXPIRES_AT", 8),
                ("CREATED_AT", 8),
            ],
            "rows": [(row[0], row[1], row[2], row[3], row[7], available_at, row[6], created_at)],
        }
        event = next(channel.iter_events("jobs"))
        assert event.event_id == ids[0]
        assert event.payload == {"n": 1}
        assert event.metadata == {"trace": "one"}
        assert event.attempts == 1
        assert isinstance(event.lease_expires_at, datetime)
        assert event.lease_expires_at > event.available_at
        assert all(cursor.closed for cursor in snowflake_connection.cursors)
    finally:
        channel.shutdown()
        config.close_pool()


def test_event_channel_native_batch_rolls_back_partial_scalar_failure(
    monkeypatch: pytest.MonkeyPatch, snowflake_connection: Any
) -> None:
    inserted, transaction_states = _event_sdk_boundary(monkeypatch, snowflake_connection, failure_at=2)
    config = SnowflakeConfig(
        connection_config={"account": "test-account", "user": "test-user"},
        extension_config={"events": {"backend": "poll_queue"}},
    )
    channel = SyncEventChannel(config)
    try:
        with pytest.raises(SQLParsingError, match="second insert rejected"):
            channel.publish_many([("jobs", {"n": 1}, None), ("jobs", {"n": 2}, None)])
        assert len(inserted) == 1
        assert transaction_states == [(True, 0), (True, 0)]
        assert snowflake_connection.committed_rows == []
        assert snowflake_connection.pending_rows == []
        assert snowflake_connection.transaction_active is False
        assert all(cursor.closed for cursor in snowflake_connection.cursors)
    finally:
        channel.shutdown()
        config.close_pool()


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
