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

    Stores the full ADK Event as a single JSON blob (``event_json``) alongside
    a small number of indexed scalar columns used for query filtering.

    This design eliminates column drift with upstream ADK: new Event fields are
    automatically captured in ``event_json`` without schema changes.
    """

    session_id: str
    invocation_id: str
    author: str
    timestamp: datetime
    event_json: str
