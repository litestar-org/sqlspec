"""Conversion functions between ADK models and database records.

Implements full-event JSON storage: the entire Event is serialized via
``Event.model_dump(exclude_none=True, mode="json")`` into a single ``event_data``
column, with a small set of indexed scalar columns extracted alongside for
query performance. Reconstruction uses ``Event.model_validate()``.

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
    "compute_update_marker",
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
        state=filter_temp_state(session.state),
        # create_time is not exposed by ADK Session; a re-upsert of a restored session will reset this timestamp.
        create_time=datetime.now(timezone.utc),
        update_time=datetime.fromtimestamp(session.last_update_time, tz=timezone.utc),
    )


def compute_update_marker(update_time: "datetime") -> str:
    """Compute a stable revision marker from an update timestamp.

    Uses the same format as ADK's ``StorageSession.get_update_marker()``:
    ISO 8601 with microsecond precision, normalized to UTC.

    Args:
        update_time: The session's update timestamp.

    Returns:
        ISO 8601 string with microsecond precision.
    """
    if update_time.tzinfo is not None:
        update_time = update_time.astimezone(timezone.utc)
    else:
        update_time = update_time.replace(tzinfo=timezone.utc)
    return update_time.isoformat(timespec="microseconds")


def record_to_session(record: SessionRecord, events: "list[EventRecord]") -> "Session":
    """Convert database record to ADK Session.

    Sets ``_storage_update_marker`` so the service layer can detect
    concurrent modifications on subsequent ``append_event`` calls.

    Args:
        record: Session database record.
        events: List of event records for this session.

    Returns:
        ADK Session object with storage marker set.
    """
    event_objects = [record_to_event(event_record) for event_record in events]

    session = Session(
        id=record["id"],
        app_name=record["app_name"],
        user_id=record["user_id"],
        state=record["state"],
        events=event_objects,
        last_update_time=record["update_time"].timestamp(),
    )
    session._storage_update_marker = compute_update_marker(record["update_time"])
    return session


# ---------------------------------------------------------------------------
# Event converters  (full-event JSON storage)
# ---------------------------------------------------------------------------


def event_to_record(event: "Event", app_name: str, user_id: str, session_id: str) -> EventRecord:
    """Convert ADK Event to database record using full-event JSON storage.

    The entire Event is serialized into ``event_data`` via Pydantic's
    ``model_dump(exclude_none=True, mode="json")``. Indexed scalar columns are
    extracted alongside for scoped filtering.

    Args:
        event: ADK Event object.
        app_name: Name of the parent app.
        user_id: ID of the parent user.
        session_id: ID of the parent session.

    Returns:
        EventRecord for database storage.
    """
    event_data = _normalize_event_data(event.model_dump(exclude_none=True, mode="json"))
    return EventRecord(
        id=event.id,
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
        invocation_id=event.invocation_id,
        timestamp=datetime.fromtimestamp(event.timestamp, tz=timezone.utc),
        event_data=event_data,
    )


def record_to_event(record: "EventRecord") -> "Event":
    """Convert database record to ADK Event.

    Reconstruction is lossless for valid ADK payloads: the full Event is
    restored from ``event_data`` via ``Event.model_validate()``.

    Args:
        record: Event database record.

    Returns:
        ADK Event object.
    """
    event_data = _normalize_event_data(record["event_data"])
    event_data.setdefault("id", record["id"])
    event_data.setdefault("invocation_id", record["invocation_id"])
    event_data.setdefault("timestamp", record["timestamp"].timestamp())
    return Event.model_validate(event_data)


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


def split_scoped_state(state: "dict[str, Any]") -> "tuple[dict[str, Any], dict[str, Any], dict[str, Any]]":
    """Split state into app-scoped, user-scoped, and session-scoped buckets.

    Args:
        state: Full session state dict (temp: already stripped).

    Returns:
        Tuple of (app_state, user_state, session_state).
        app_state: keys starting with "app:"
        user_state: keys starting with "user:"
        session_state: all other keys
    """
    app_state: dict[str, Any] = {}
    user_state: dict[str, Any] = {}
    session_state: dict[str, Any] = {}
    for k, v in state.items():
        if k.startswith("app:"):
            app_state[k] = v
        elif k.startswith("user:"):
            user_state[k] = v
        else:
            session_state[k] = v
    return app_state, user_state, session_state


def merge_scoped_state(
    session_state: "dict[str, Any]",
    app_state: "dict[str, Any] | None" = None,
    user_state: "dict[str, Any] | None" = None,
) -> "dict[str, Any]":
    """Merge scoped state buckets into a single state dict.

    Priority: session_state is base, app_state and user_state overlay.
    This matches ADK's documented merge semantics on session load.

    Args:
        session_state: Per-session state.
        app_state: App-scoped state (shared across sessions for same app).
        user_state: User-scoped state (shared across sessions for same app+user).

    Returns:
        Merged state dict.
    """
    merged = dict(session_state)
    if app_state is not None:
        merged.update(app_state)
    if user_state is not None:
        merged.update(user_state)
    return merged


def _normalize_event_data(event_data: "dict[str, Any]") -> "dict[str, Any]":
    """Return event data acceptable to ADK 2.2's Event model.

    ADK 2.2 guards an assigned ``event.actions = None`` during service writes,
    but explicit ``actions: null`` does not validate as a durable Event shape.
    SQLSpec therefore omits that key before storing or restoring payloads.
    """

    normalized = dict(event_data)
    if normalized.get("actions") is None:
        normalized.pop("actions", None)
    return normalized
