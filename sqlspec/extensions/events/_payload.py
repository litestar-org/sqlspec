"""Shared payload encoding/decoding utilities for event backends."""

import contextlib
from datetime import datetime, timezone
from typing import Any

from sqlspec.exceptions import EventChannelError
from sqlspec.extensions.events._models import EventMessage
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.uuids import uuid4

__all__ = ("decode_notify_payload", "encode_notify_payload", "parse_event_timestamp")

MAX_NOTIFY_BYTES = 8000


def encode_notify_payload(event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None") -> str:
    """Encode event data as JSON for NOTIFY payload.

    Raises:
        EventChannelError: If the encoded payload exceeds PostgreSQL's 8KB limit.
    """
    encoded = to_json(
        {
            "event_id": event_id,
            "payload": payload,
            "metadata": metadata,
            "published_at": datetime.now(timezone.utc).isoformat(),
        },
        as_bytes=True,
    )
    if len(encoded) > MAX_NOTIFY_BYTES:
        msg = "PostgreSQL NOTIFY payload exceeds 8 KB limit"
        raise EventChannelError(msg)
    return encoded.decode("utf-8")


def decode_notify_payload(channel: str, payload: str) -> "EventMessage":
    """Decode JSON payload from NOTIFY into an EventMessage."""
    raw = from_json(payload)
    data = raw if isinstance(raw, dict) else {"payload": raw}
    payload_val = data.get("payload")
    metadata_val = data.get("metadata")
    timestamp = parse_event_timestamp(data.get("published_at"))
    return EventMessage(
        event_id=data.get("event_id", uuid4().hex),
        channel=channel,
        payload=payload_val if isinstance(payload_val, dict) else {"value": payload_val},
        metadata=metadata_val if metadata_val is None or isinstance(metadata_val, dict) else {"value": metadata_val},
        attempts=0,
        available_at=timestamp,
        lease_expires_at=None,
        created_at=timestamp,
    )


def parse_event_timestamp(value: Any) -> "datetime":
    """Parse a timestamp value into a timezone-aware datetime.

    Handles ISO format strings, datetime objects, and falls back to
    current UTC time for invalid or missing values.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        with contextlib.suppress(ValueError):
            parsed = datetime.fromisoformat(value)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)
