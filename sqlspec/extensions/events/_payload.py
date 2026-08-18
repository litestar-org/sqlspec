"""Shared payload encoding/decoding utilities for event backends."""

import contextlib
from datetime import datetime, timezone
from typing import Any

from sqlspec.exceptions import EventChannelError
from sqlspec.extensions.events._models import EventMessage
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.uuids import uuid4

__all__ = (
    "POSTGRES_NOTIFY_MAX_PAYLOAD_BYTES",
    "coerce_dict",
    "coerce_optional_dict",
    "decode_notify_payload",
    "encode_notify_payload",
    "fits_notify_payload",
    "measure_notify_payload",
    "parse_event_timestamp",
)

POSTGRES_NOTIFY_MAX_PAYLOAD_BYTES = 7999


def coerce_dict(value: Any) -> "dict[str, Any]":
    """Coerce a value to a dict, wrapping non-dict values as {'value': ...}."""
    return value if isinstance(value, dict) else {"value": value}


def coerce_optional_dict(value: Any) -> "dict[str, Any] | None":
    """Coerce a value to a dict or None, wrapping non-dict values as {'value': ...}."""
    return value if value is None or isinstance(value, dict) else {"value": value}


def _serialize_notify_envelope(
    event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None", published_at: "datetime"
) -> bytes:
    """Serialize a native notification envelope to UTF-8 JSON bytes.

    The publication timestamp is normalized to UTC with microsecond precision so
    the encoded envelope width is independent of the clock reading.
    """
    return to_json(
        {
            "event_id": event_id,
            "payload": payload,
            "metadata": metadata,
            "published_at": published_at.astimezone(timezone.utc).isoformat(timespec="microseconds"),
        },
        as_bytes=True,
    )


def encode_notify_payload(event_id: str, payload: "dict[str, Any]", metadata: "dict[str, Any] | None") -> str:
    """Encode event data as JSON for NOTIFY payload.

    Raises:
        EventChannelError: If the encoded envelope exceeds the PostgreSQL notification budget.
    """
    encoded = _serialize_notify_envelope(event_id, payload, metadata, datetime.now(timezone.utc))
    encoded_bytes = len(encoded)
    if encoded_bytes > POSTGRES_NOTIFY_MAX_PAYLOAD_BYTES:
        msg = (
            f"PostgreSQL NOTIFY payload is {encoded_bytes} encoded bytes and exceeds the "
            f"{POSTGRES_NOTIFY_MAX_PAYLOAD_BYTES}-byte maximum. Use fits_notify_payload() or "
            "measure_notify_payload() to split the batch before publishing."
        )
        raise EventChannelError(msg)
    return encoded.decode("utf-8")


def measure_notify_payload(
    payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None, *, event_id: "str | None" = None
) -> int:
    """Return the encoded UTF-8 byte size of the native notification envelope.

    The measurement covers the complete envelope rather than only the payload
    mapping. Omitting ``event_id`` measures the canonical backend UUID-hex shape.
    """
    resolved_event_id = uuid4().hex if event_id is None else event_id
    return len(_serialize_notify_envelope(resolved_event_id, payload, metadata, datetime.now(timezone.utc)))


def fits_notify_payload(
    payload: "dict[str, Any]", metadata: "dict[str, Any] | None" = None, *, event_id: "str | None" = None
) -> bool:
    """Return whether the native notification envelope fits the PostgreSQL budget."""
    return measure_notify_payload(payload, metadata, event_id=event_id) <= POSTGRES_NOTIFY_MAX_PAYLOAD_BYTES


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
        payload=coerce_dict(payload_val),
        metadata=coerce_optional_dict(metadata_val),
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
