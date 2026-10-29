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
    """Unique Google ADK session identifier."""
    app_name: str
    """Application namespace owning the session."""
    user_id: str
    """End-user identifier associated with the session."""
    state: "dict[str, Any]"
    """Serialized session-scoped state dictionary."""
    create_time: datetime
    """UTC timestamp when the session record was created."""
    update_time: datetime
    """UTC timestamp of the most recent session state or event update."""


class StoredEvent(TypedDict):
    """Database record for an event.

    Stores the full ADK Event as a single JSON blob (``event_data``) alongside
    indexed scalar columns used for scoped query filtering.

    This design eliminates column drift with upstream ADK: new Event fields are
    automatically captured in ``event_data`` without schema changes.
    """

    id: str
    """Unique ADK event identifier."""
    app_name: str
    """Application namespace owning the parent session."""
    user_id: str
    """End-user identifier owning the parent session."""
    session_id: str
    """Parent ADK session identifier."""
    invocation_id: str
    """Agent turn or invocation identifier grouping related events."""
    timestamp: datetime
    """UTC timestamp when the event was emitted."""
    event_data: "dict[str, Any]"
    """Complete serialized Google ADK ``Event`` payload."""
