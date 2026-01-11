# pyright: reportPrivateUsage=false
"""Unit tests for SyncTableEventQueue and AsyncTableEventQueue."""

from datetime import datetime, timezone

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.core import StatementConfig
from sqlspec.exceptions import EventChannelError
from sqlspec.extensions.events import EventMessage, SyncTableEventQueue, parse_event_timestamp


def test_table_event_queue_default_table_name(tmp_path) -> None:
    """Default table name is sqlspec_event_queue."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    assert queue._table_name == "sqlspec_event_queue"


def test_table_event_queue_custom_table_name(tmp_path) -> None:
    """Custom table names are accepted and normalized."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, queue_table="custom_events")
    assert queue._table_name == "custom_events"


def test_table_event_queue_schema_qualified_table(tmp_path) -> None:
    """Schema-qualified table names are supported."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, queue_table="app_schema.events")
    assert queue._table_name == "app_schema.events"


def test_table_event_queue_invalid_table_name_raises(tmp_path) -> None:
    """Invalid table names raise EventChannelError."""

    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    with pytest.raises(EventChannelError, match="Invalid events table name"):
        SyncTableEventQueue(config, queue_table="invalid-name")


def test_table_event_queue_lease_seconds_default(tmp_path) -> None:
    """Default lease duration is 30 seconds."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    assert queue._lease_seconds == 30


def test_table_event_queue_custom_lease_seconds(tmp_path) -> None:
    """Custom lease duration is respected."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, lease_seconds=60)
    assert queue._lease_seconds == 60


def test_table_event_queue_retention_seconds_default(tmp_path) -> None:
    """Default retention is 86400 seconds (1 day)."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    assert queue._retention_seconds == 86_400


def test_table_event_queue_custom_retention_seconds(tmp_path) -> None:
    """Custom retention duration is respected."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, retention_seconds=3600)
    assert queue._retention_seconds == 3600


def test_table_event_queue_select_for_update_disabled(tmp_path) -> None:
    """SELECT FOR UPDATE is disabled by default."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    assert "FOR UPDATE" not in queue._select_sql.upper()


def test_table_event_queue_select_for_update_enabled(tmp_path) -> None:
    """SELECT FOR UPDATE clause is added when enabled."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, select_for_update=True)
    assert "FOR UPDATE" in queue._select_sql.upper()


def test_table_event_queue_skip_locked_requires_for_update(tmp_path) -> None:
    """SKIP LOCKED is only added when FOR UPDATE is enabled."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, skip_locked=True)
    assert "SKIP LOCKED" not in queue._select_sql.upper()

    queue_with_both = SyncTableEventQueue(config, select_for_update=True, skip_locked=True)
    assert "FOR UPDATE SKIP LOCKED" in queue_with_both._select_sql.upper()


def test_table_event_queue_insert_sql_contains_table(tmp_path) -> None:
    """Insert SQL references the configured table name."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, queue_table="my_events")
    assert "my_events" in queue._upsert_sql


def test_table_event_queue_select_sql_contains_table(tmp_path) -> None:
    """Select SQL references the configured table name."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config, queue_table="my_events")
    assert "my_events" in queue._select_sql


def test_table_event_queue_oracle_dialect_uses_fetch_first(tmp_path) -> None:
    """Oracle dialect uses FETCH FIRST instead of LIMIT."""

    config = SqliteConfig(
        connection_config={"database": str(tmp_path / "test.db")}, statement_config=StatementConfig(dialect="oracle")
    )
    queue = SyncTableEventQueue(config)
    assert "FETCH FIRST 1 ROWS ONLY" in queue._select_sql.upper()
    assert "LIMIT" not in queue._select_sql.upper()


def test_table_event_queue_statement_config_property(tmp_path) -> None:
    """_statement_config property returns config's statement_config."""
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    assert queue._statement_config is config.statement_config


def test_event_message_dataclass_fields() -> None:
    """EventMessage dataclass has expected fields."""
    now = datetime.now(timezone.utc)
    event = EventMessage(
        event_id="abc123",
        channel="notifications",
        payload={"action": "test"},
        metadata={"source": "unit_test"},
        attempts=1,
        available_at=now,
        lease_expires_at=now,
        created_at=now,
    )
    assert event.event_id == "abc123"
    assert event.channel == "notifications"
    assert event.payload == {"action": "test"}
    assert event.metadata == {"source": "unit_test"}
    assert event.attempts == 1


def test_event_message_metadata_none() -> None:
    """EventMessage allows None metadata."""
    now = datetime.now(timezone.utc)
    event = EventMessage(
        event_id="abc123",
        channel="notifications",
        payload={"action": "test"},
        metadata=None,
        attempts=0,
        available_at=now,
        lease_expires_at=None,
        created_at=now,
    )
    assert event.metadata is None
    assert event.lease_expires_at is None


def test_table_event_queue_hydrate_event_dict_payload(tmp_path) -> None:
    """_hydrate_event handles dict payloads directly."""
    now = datetime.now(timezone.utc)
    row = {
        "event_id": "test123",
        "channel": "notifications",
        "payload_json": {"action": "refresh"},
        "metadata_json": None,
        "attempts": 0,
        "available_at": now,
        "lease_expires_at": None,
        "created_at": now,
    }
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    event = queue._hydrate_event(row, None)
    assert event.payload == {"action": "refresh"}
    assert event.metadata is None


def test_table_event_queue_hydrate_event_string_payload(tmp_path) -> None:
    """_hydrate_event deserializes JSON string payloads."""
    import json

    now = datetime.now(timezone.utc)
    row = {
        "event_id": "test456",
        "channel": "events",
        "payload_json": json.dumps({"action": "update"}),
        "metadata_json": json.dumps({"user": "admin"}),
        "attempts": 2,
        "available_at": now,
        "lease_expires_at": now,
        "created_at": now,
    }
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    event = queue._hydrate_event(row, now)
    assert event.payload == {"action": "update"}
    assert event.metadata == {"user": "admin"}
    assert event.lease_expires_at == now


def test_table_event_queue_hydrate_event_non_dict_payload(tmp_path) -> None:
    """Non-dict payloads are wrapped in a value key."""
    import json

    now = datetime.now(timezone.utc)
    row = {
        "event_id": "test789",
        "channel": "events",
        "payload_json": json.dumps("simple_string"),
        "metadata_json": json.dumps(42),
        "attempts": 0,
        "available_at": now,
        "lease_expires_at": None,
        "created_at": now,
    }
    config = SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
    queue = SyncTableEventQueue(config)
    event = queue._hydrate_event(row, None)
    assert event.payload == {"value": "simple_string"}
    assert event.metadata == {"value": 42}


def test_parse_event_timestamp_from_string() -> None:
    """ISO format strings are parsed to datetime."""
    result = parse_event_timestamp("2024-01-15T10:30:00Z")
    assert isinstance(result, datetime)
    assert result.tzinfo is not None


def test_parse_event_timestamp_naive_string() -> None:
    """Naive datetime strings get UTC timezone added."""
    result = parse_event_timestamp("2024-01-15T10:30:00")
    assert result.tzinfo == timezone.utc


def test_parse_event_timestamp_from_datetime() -> None:
    """Datetime objects are passed through."""
    now = datetime.now(timezone.utc)
    result = parse_event_timestamp(now)
    assert result is now


def test_parse_event_timestamp_naive_datetime() -> None:
    """Naive datetime objects get UTC timezone added."""
    naive = datetime(2024, 1, 15, 10, 30, 0)
    result = parse_event_timestamp(naive)
    assert result.tzinfo == timezone.utc


def test_parse_event_timestamp_invalid() -> None:
    """Invalid values return current UTC time."""
    result = parse_event_timestamp("not a date")
    assert isinstance(result, datetime)
    assert result.tzinfo is not None


def test_parse_event_timestamp_none() -> None:
    """None values return current UTC time."""
    result = parse_event_timestamp(None)
    assert isinstance(result, datetime)


def test_sync_table_event_queue_backend_name() -> None:
    """SyncTableEventQueue has correct backend_name."""
    assert SyncTableEventQueue.backend_name == "table_queue"


def test_sync_table_event_queue_supports_sync() -> None:
    """SyncTableEventQueue supports sync operations only."""
    assert SyncTableEventQueue.supports_sync is True
    assert SyncTableEventQueue.supports_async is False
