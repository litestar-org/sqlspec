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
    """Unique memory entry identifier."""
    session_id: str
    """Source ADK session identifier from which the memory was extracted."""
    app_name: str
    """Application namespace owning the memory entry."""
    user_id: str
    """End-user identifier owning the memory entry."""
    scope: str
    """Visibility scope for retrieval (such as ``user`` or ``app``)."""
    event_id: str
    """Originating ADK event identifier."""
    author: "str | None"
    """Agent or participant role that authored the source event."""
    timestamp: datetime
    """UTC timestamp of the originating conversation event."""
    content_json: "dict[str, Any]"
    """Structured ADK ``Content`` payload stored as JSON."""
    content_text: str
    """Flattened plain-text projection indexed for full-text and keyword search."""
    metadata_json: "dict[str, Any] | None"
    """Optional structured metadata dictionary attached to the memory entry."""
    inserted_at: datetime
    """UTC timestamp when the memory row was persisted."""
    embedding: "Sequence[float] | None"
    """Optional dense float vector used for semantic similarity search."""
