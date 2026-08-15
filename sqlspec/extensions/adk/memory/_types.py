"""Type definitions for ADK memory extension.

These types define the database record structures for storing memory entries.
They are separate from the Pydantic models to keep mypyc compilation working.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, TypedDict

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ("StoredMemory",)


class StoredMemory(TypedDict):
    """Database record for a memory entry.

    Represents the schema for memory entries stored in the database.
    Contains extracted content from ADK events for searchable long-term memory.
    """

    id: str
    session_id: str
    app_name: str
    user_id: str
    scope: str
    event_id: str
    author: "str | None"
    timestamp: datetime
    content_json: "dict[str, Any]"
    content_text: str
    metadata_json: "dict[str, Any] | None"
    inserted_at: datetime
    embedding: "Sequence[float] | None"
