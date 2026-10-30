"""Type definitions for ADK artifact extension.

These types define the database record structures for storing artifact metadata.
They are separate from the Pydantic models to keep mypyc compilation working.
"""

from datetime import datetime
from typing import Any, TypedDict

__all__ = ("StoredArtifact",)


class StoredArtifact(TypedDict):
    """Database record for an artifact version.

    Represents the schema for artifact metadata stored in the database.
    Content is stored separately in object storage; this record tracks
    versioning, ownership, and the canonical URI pointing to the content.

    The composite key is (app_name, user_id, session_id, filename, version),
    where session_id may be NULL for user-scoped artifacts.
    """

    app_name: str
    """Application namespace owning the artifact."""
    user_id: str
    """End-user identifier owning the artifact."""
    session_id: "str | None"
    """Session identifier for session-scoped artifacts, or ``None`` for user-scoped artifacts."""
    filename: str
    """Logical artifact filename within the user or session scope."""
    version: int
    """Zero-based or monotonic integer revision number for the artifact."""
    mime_type: "str | None"
    """IANA media type of the stored artifact payload, if known."""
    canonical_uri: str
    """Object-storage URI referencing the persisted binary content."""
    custom_metadata: "dict[str, Any] | None"
    """Optional caller-supplied metadata dictionary stored alongside the revision."""
    created_at: datetime
    """UTC timestamp when this artifact version was recorded."""
