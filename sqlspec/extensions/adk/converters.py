"""Conversion functions between ADK models and database records."""

import json
import pickle
from datetime import datetime, timezone
from typing import Any

from google.adk.events.event import Event
from google.adk.sessions import Session
from google.genai import types

from sqlspec.extensions.adk._types import EventRecord, SessionRecord
from sqlspec.utils.logging import get_logger

logger = get_logger("sqlspec.extensions.adk.converters")

__all__ = ("event_to_record", "record_to_event", "record_to_session", "session_to_record")


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


def event_to_record(event: "Event", session_id: str, app_name: str, user_id: str) -> EventRecord:
    """Convert ADK Event to database record.

    Args:
        event: ADK Event object.
        session_id: ID of the parent session.
        app_name: Name of the application.
        user_id: ID of the user.

    Returns:
        EventRecord for database storage.
    """
    actions_bytes = pickle.dumps(event.actions)

    long_running_tool_ids_json = None
    if event.long_running_tool_ids:
        long_running_tool_ids_json = json.dumps(list(event.long_running_tool_ids))

    content_dict = None
    if event.content:
        content_dict = event.content.model_dump(exclude_none=True, mode="json")

    grounding_metadata_dict = None
    if event.grounding_metadata:
        grounding_metadata_dict = event.grounding_metadata.model_dump(exclude_none=True, mode="json")

    custom_metadata_dict = event.custom_metadata

    return EventRecord(
        id=event.id,
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
        invocation_id=event.invocation_id,
        author=event.author,
        branch=event.branch,
        actions=actions_bytes,
        long_running_tool_ids_json=long_running_tool_ids_json,
        timestamp=datetime.fromtimestamp(event.timestamp, tz=timezone.utc),
        content=content_dict,
        grounding_metadata=grounding_metadata_dict,
        custom_metadata=custom_metadata_dict,
        partial=event.partial,
        turn_complete=event.turn_complete,
        interrupted=event.interrupted,
        error_code=event.error_code,
        error_message=event.error_message,
    )


def record_to_event(record: "EventRecord") -> "Event":
    """Convert database record to ADK Event.

    Args:
        record: Event database record.

    Returns:
        ADK Event object.
    """
    actions = pickle.loads(record["actions"])  # noqa: S301

    long_running_tool_ids = None
    if record["long_running_tool_ids_json"]:
        long_running_tool_ids = set(json.loads(record["long_running_tool_ids_json"]))

    return Event(
        id=record["id"],
        invocation_id=record["invocation_id"],
        author=record["author"],
        branch=record["branch"],
        actions=actions,
        timestamp=record["timestamp"].timestamp(),
        content=_decode_content(record["content"]),
        long_running_tool_ids=long_running_tool_ids,
        partial=record["partial"],
        turn_complete=record["turn_complete"],
        error_code=record["error_code"],
        error_message=record["error_message"],
        interrupted=record["interrupted"],
        grounding_metadata=_decode_grounding_metadata(record["grounding_metadata"]),
        custom_metadata=record["custom_metadata"],
    )


def _decode_content(content_dict: "dict[str, Any] | None") -> Any:
    """Decode content dictionary from database to ADK Content object.

    Args:
        content_dict: Content dictionary from database.

    Returns:
        ADK Content object or None.
    """
    if not content_dict:
        return None

    return types.Content.model_validate(content_dict)


def _decode_grounding_metadata(grounding_dict: "dict[str, Any] | None") -> Any:
    """Decode grounding metadata dictionary from database to ADK object.

    Args:
        grounding_dict: Grounding metadata dictionary from database.

    Returns:
        ADK GroundingMetadata object or None.
    """
    if not grounding_dict:
        return None

    return types.GroundingMetadata.model_validate(grounding_dict)
