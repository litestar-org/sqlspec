"""Type definitions for ADK extension.

These types define the database record structures for storing sessions and events.
They are separate from the Pydantic models to keep mypyc compilation working.
"""

from datetime import datetime
from typing import Any, Literal, TypedDict

__all__ = ("SessionOrderBy", "StoredEvent", "StoredSession")

SessionOrderBy = Literal["create_time", "update_time"]
"""Timestamp column a session listing may be ordered by."""


class StoredSession(TypedDict):
    """Database record for a session.

    Represents the schema for sessions stored in the database.
    """

    id: str
    app_name: str
    user_id: str
    state: "dict[str, Any]"
    create_time: datetime
    update_time: datetime


class StoredEvent(TypedDict):
    """Database record for an event.

    Stores the full ADK Event as a single JSON blob (``event_data``) alongside
    indexed scalar columns used for scoped query filtering.

    This design eliminates column drift with upstream ADK: new Event fields are
    automatically captured in ``event_data`` without schema changes.
    """

    id: str
    app_name: str
    user_id: str
    session_id: str
    invocation_id: str
    timestamp: datetime
    event_data: "dict[str, Any]"
