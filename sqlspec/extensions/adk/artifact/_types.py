"""Type definitions for ADK artifact extension.

These types define the database record structures for storing artifact metadata.
They are separate from the Pydantic models to keep mypyc compilation working.
"""

from datetime import datetime
from typing import Any, TypedDict

__all__ = ("ArtifactRecord",)


class ArtifactRecord(TypedDict):
    """Database record for an artifact version.

    Represents the schema for artifact metadata stored in the database.
    Content is stored separately in object storage; this record tracks
    versioning, ownership, and the canonical URI pointing to the content.

    The composite key is (app_name, user_id, session_id, filename, version),
    where session_id may be NULL for user-scoped artifacts.
    """

    app_name: str
    user_id: str
    session_id: "str | None"
    filename: str
    version: int
    mime_type: "str | None"
    canonical_uri: str
    custom_metadata: "dict[str, Any] | None"
    created_at: datetime
