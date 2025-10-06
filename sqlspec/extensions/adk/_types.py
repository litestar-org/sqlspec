"""Type definitions for ADK extension.

These types define the database record structures for storing sessions and events.
They are separate from the Pydantic models to keep mypyc compilation working.
"""

from datetime import datetime
from typing import Any, TypedDict

__all__ = ("EventRecord", "SessionRecord")


class SessionRecord(TypedDict):
    """Database record for a session.

    Represents the schema for sessions stored in the database.
    """

    id: str
    app_name: str
    user_id: str
    state: "dict[str, Any]"
    create_time: datetime
    update_time: datetime


class EventRecord(TypedDict):
    """Database record for an event.

    Represents the schema for events stored in the database.
    Follows the ADK Event model plus session metadata.
    """

    id: str
    app_name: str
    user_id: str
    session_id: str
    invocation_id: str
    author: str
    branch: "str | None"
    actions: bytes
    long_running_tool_ids_json: "str | None"
    timestamp: datetime
    content: "dict[str, Any] | None"
    grounding_metadata: "dict[str, Any] | None"
    custom_metadata: "dict[str, Any] | None"
    partial: "bool | None"
    turn_complete: "bool | None"
    interrupted: "bool | None"
    error_code: "str | None"
    error_message: "str | None"
