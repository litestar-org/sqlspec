"""Conversion functions between ADK models and database records.

Implements full-event JSON storage: the entire Event is serialized via
``Event.model_dump_json(exclude_none=True)`` into a single ``event_json``
column, with a small set of indexed scalar columns extracted alongside for
query performance.  Reconstruction uses ``Event.model_validate_json()``.

Also provides scoped-state helpers that normalise ADK state prefixes
(``app:``, ``user:``, ``temp:``) so the shared service layer can split,
filter, and merge state before handing it to backend stores.
"""

from datetime import datetime, timezone
from typing import Any

from google.adk.events.event import Event
from google.adk.sessions import Session

from sqlspec.extensions.adk._types import EventRecord, SessionRecord

__all__ = (
    "event_to_record",
    "filter_temp_state",
    "merge_scoped_state",
    "record_to_event",
    "record_to_session",
    "session_to_record",
    "split_scoped_state",
)


# ---------------------------------------------------------------------------
# Session converters
# ---------------------------------------------------------------------------


def session_to_record(session: "Session") -> SessionRecord:
    """Convert ADK Session to database record.

    Args:
        session: ADK Session object.

    Returns:
        SessionRecord for database storage.
    """
    return SessionRecord(
        id=session.id,
        app_name=session.app_name,
        user_id=session.user_id,
        state=session.state,
        create_time=datetime.now(timezone.utc),
        update_time=datetime.fromtimestamp(session.last_update_time, tz=timezone.utc),
    )


def record_to_session(record: SessionRecord, events: "list[EventRecord]") -> "Session":
    """Convert database record to ADK Session.

    Args:
        record: Session database record.
        events: List of event records for this session.

    Returns:
        ADK Session object.
    """
    event_objects = [record_to_event(event_record) for event_record in events]

    return Session(
        id=record["id"],
        app_name=record["app_name"],
        user_id=record["user_id"],
        state=record["state"],
        events=event_objects,
        last_update_time=record["update_time"].timestamp(),
    )


# ---------------------------------------------------------------------------
# Event converters  (full-event JSON storage)
# ---------------------------------------------------------------------------


def event_to_record(event: "Event", session_id: str) -> EventRecord:
    """Convert ADK Event to database record using full-event JSON storage.

    The entire Event is serialized into ``event_json`` via Pydantic's
    ``model_dump_json(exclude_none=True)``.  A small number of indexed scalar
    columns are extracted alongside for query performance.

    Args:
        event: ADK Event object.
        session_id: ID of the parent session.

    Returns:
        EventRecord for database storage.
    """
    return EventRecord(
        session_id=session_id,
        invocation_id=event.invocation_id,
        author=event.author,
        timestamp=datetime.fromtimestamp(event.timestamp, tz=timezone.utc),
        event_json=event.model_dump_json(exclude_none=True),
    )


def record_to_event(record: "EventRecord") -> "Event":
    """Convert database record to ADK Event.

    Reconstruction is lossless: the full Event is restored from
    ``event_json`` via ``Event.model_validate_json()``.

    Args:
        record: Event database record.

    Returns:
        ADK Event object.
    """
    return Event.model_validate_json(record["event_json"])


# ---------------------------------------------------------------------------
# Scoped-state helpers
# ---------------------------------------------------------------------------


def filter_temp_state(state: "dict[str, Any]") -> "dict[str, Any]":
    """Return a copy of *state* with all ``temp:`` keys removed.

    ``temp:`` keys are process-local/session-runtime state and must never be
    written to persistent storage.

    Args:
        state: ADK state dictionary (may contain ``temp:`` prefixed keys).

    Returns:
        A new dict without any ``temp:``-prefixed keys.
    """
    return {k: v for k, v in state.items() if not k.startswith("temp:")}


def split_scoped_state(
    state: "dict[str, Any]",
) -> "tuple[dict[str, Any], dict[str, Any], dict[str, Any]]":
    """Split ADK state into ``(session_local, app_scoped, user_scoped)`` dicts.

    Keys without a recognised scope prefix are session-local.  ``temp:`` keys
    are silently dropped (they must not be persisted).

    Args:
        state: ADK state dictionary.

    Returns:
        A 3-tuple of ``(session_local, app_scoped, user_scoped)`` dicts.
        Scoped dicts retain their prefix in the key (e.g. ``"app:foo"``).
    """
    session_local: dict[str, Any] = {}
    app_scoped: dict[str, Any] = {}
    user_scoped: dict[str, Any] = {}

    for k, v in state.items():
        if k.startswith("temp:"):
            continue
        elif k.startswith("app:"):
            app_scoped[k] = v
        elif k.startswith("user:"):
            user_scoped[k] = v
        else:
            session_local[k] = v

    return session_local, app_scoped, user_scoped


def merge_scoped_state(
    session_local: "dict[str, Any]",
    app_scoped: "dict[str, Any]",
    user_scoped: "dict[str, Any]",
) -> "dict[str, Any]":
    """Merge scoped state dicts back into a single ADK-compatible state dict.

    The merge order is ``session_local | app_scoped | user_scoped`` so that
    broader scopes can shadow narrower ones if keys collide (which they
    normally should not, since prefixes differ).

    Args:
        session_local: Session-local state (no prefix).
        app_scoped: App-scoped state (``app:`` prefix).
        user_scoped: User-scoped state (``user:`` prefix).

    Returns:
        A single merged state dict suitable for ``Session.state``.
    """
    merged: dict[str, Any] = {}
    merged.update(session_local)
    merged.update(app_scoped)
    merged.update(user_scoped)
    return merged
