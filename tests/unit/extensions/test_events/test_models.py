"""Unit tests for EventMessage dataclass."""

from datetime import datetime, timezone

from sqlspec.extensions.events import EventMessage


def test_event_message_all_fields() -> None:
    """EventMessage stores all expected fields."""
    now = datetime.now(timezone.utc)
    lease_time = datetime.now(timezone.utc)

    message = EventMessage(
        event_id="evt_123",
        channel="notifications",
        payload={"action": "refresh", "data": {"id": 1}},
        metadata={"user_id": "user_456", "source": "api"},
        attempts=3,
        available_at=now,
        lease_expires_at=lease_time,
        created_at=now,
    )

    assert message.event_id == "evt_123"
    assert message.channel == "notifications"
    assert message.payload == {"action": "refresh", "data": {"id": 1}}
    assert message.metadata == {"user_id": "user_456", "source": "api"}
    assert message.attempts == 3
    assert message.available_at == now
    assert message.lease_expires_at == lease_time
    assert message.created_at == now


def test_event_message_none_metadata() -> None:
    """EventMessage allows None metadata."""
    now = datetime.now(timezone.utc)

    message = EventMessage(
        event_id="evt_789",
        channel="events",
        payload={"type": "test"},
        metadata=None,
        attempts=0,
        available_at=now,
        lease_expires_at=None,
        created_at=now,
    )

    assert message.metadata is None
    assert message.lease_expires_at is None


def test_event_message_empty_payload() -> None:
    """EventMessage accepts empty payload dict."""
    now = datetime.now(timezone.utc)

    message = EventMessage(
        event_id="evt_empty",
        channel="test",
        payload={},
        metadata=None,
        attempts=0,
        available_at=now,
        lease_expires_at=None,
        created_at=now,
    )

    assert message.payload == {}


def test_event_message_complex_payload() -> None:
    """EventMessage handles complex nested payloads."""
    now = datetime.now(timezone.utc)

    complex_payload = {
        "users": [{"id": 1, "name": "Alice", "tags": ["admin", "active"]}, {"id": 2, "name": "Bob", "tags": ["user"]}],
        "metadata": {"version": "2.0", "nested": {"deep": {"value": True}}},
        "count": 42,
        "enabled": True,
        "ratio": 0.95,
    }

    message = EventMessage(
        event_id="evt_complex",
        channel="data",
        payload=complex_payload,
        metadata={"timestamp": now.isoformat()},
        attempts=1,
        available_at=now,
        lease_expires_at=None,
        created_at=now,
    )

    assert message.payload["users"][0]["name"] == "Alice"
    assert message.payload["metadata"]["nested"]["deep"]["value"] is True


def test_event_message_attempts_zero() -> None:
    """EventMessage handles zero attempts."""
    now = datetime.now(timezone.utc)

    message = EventMessage(
        event_id="evt_new",
        channel="test",
        payload={},
        metadata=None,
        attempts=0,
        available_at=now,
        lease_expires_at=None,
        created_at=now,
    )

    assert message.attempts == 0


def test_event_message_high_attempts() -> None:
    """EventMessage handles high attempt counts."""
    now = datetime.now(timezone.utc)

    message = EventMessage(
        event_id="evt_retry",
        channel="test",
        payload={},
        metadata=None,
        attempts=100,
        available_at=now,
        lease_expires_at=None,
        created_at=now,
    )

    assert message.attempts == 100


def test_event_message_different_timestamps() -> None:
    """EventMessage can have different available_at and created_at times."""
    created = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    available = datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    lease = datetime(2024, 1, 1, 1, 0, 30, tzinfo=timezone.utc)

    message = EventMessage(
        event_id="evt_scheduled",
        channel="scheduled",
        payload={"schedule": "delayed"},
        metadata=None,
        attempts=0,
        available_at=available,
        lease_expires_at=lease,
        created_at=created,
    )

    assert message.created_at < message.available_at
    assert message.lease_expires_at is not None
    assert message.available_at < message.lease_expires_at


def test_event_message_slots_used() -> None:
    """EventMessage uses __slots__ for memory efficiency."""
    assert hasattr(EventMessage, "__slots__")
    assert "event_id" in EventMessage.__slots__
    assert "channel" in EventMessage.__slots__
    assert "payload" in EventMessage.__slots__


def test_event_message_dataclass_fields() -> None:
    """EventMessage has correct dataclass fields."""
    import dataclasses

    fields = {f.name for f in dataclasses.fields(EventMessage)}

    expected = {
        "event_id",
        "channel",
        "payload",
        "metadata",
        "attempts",
        "available_at",
        "lease_expires_at",
        "created_at",
    }

    assert fields == expected
