"""Conversion functions for ADK memory records.

Provides utilities for extracting searchable text from ADK Content objects
and converting between ADK models and database records.
"""

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.extensions.adk.memory._types import MemoryRecord
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from google.adk.events.event import Event
    from google.adk.memory.memory_entry import MemoryEntry
    from google.adk.sessions import Session
    from google.genai import types

logger = get_logger("sqlspec.extensions.adk.memory.converters")

__all__ = (
    "event_to_memory_record",
    "extract_content_text",
    "memory_entry_to_record",
    "record_to_memory_entry",
    "records_to_memory_entries",
    "session_to_memory_records",
)


def extract_content_text(content: "types.Content") -> str:
    """Extract plain text from ADK Content for search indexing.

    Handles multi-modal Content.parts including text, function calls,
    function responses, and other part types. Non-text parts are indexed
    by their type for discoverability.

    Args:
        content: ADK Content object with parts list.

    Returns:
        Space-separated plain text extracted from all parts.
    """
    parts_text: list[str] = []

    if not content.parts:
        return ""

    for part in content.parts:
        if part.text:
            parts_text.append(part.text)
        elif part.function_call is not None:
            parts_text.append(f"function:{part.function_call.name}")
        elif part.function_response is not None:
            parts_text.append(f"response:{part.function_response.name}")

    return " ".join(parts_text)


def event_to_memory_record(event: "Event", session_id: str, app_name: str, user_id: str) -> "MemoryRecord | None":
    """Convert an ADK Event to a memory record.

    Args:
        event: ADK Event object.
        session_id: ID of the parent session.
        app_name: Name of the application.
        user_id: ID of the user.

    Returns:
        MemoryRecord for database storage, or None if event has no content.
    """
    if event.content is None:
        return None

    content_text = extract_content_text(event.content)
    if not content_text.strip():
        return None

    content_dict = event.content.model_dump(exclude_none=True, mode="json")

    custom_metadata = event.custom_metadata or None

    now = datetime.now(timezone.utc)

    return MemoryRecord(
        id=str(uuid.uuid4()),
        session_id=session_id,
        app_name=app_name,
        user_id=user_id,
        event_id=event.id,
        author=event.author,
        timestamp=datetime.fromtimestamp(event.timestamp, tz=timezone.utc),
        content_json=content_dict,
        content_text=content_text,
        metadata_json=custom_metadata,
        inserted_at=now,
    )


def memory_entry_to_record(
    entry: "MemoryEntry", app_name: str, user_id: str, extra_metadata: "dict[str, Any] | None" = None
) -> "MemoryRecord | None":
    """Convert an ADK MemoryEntry to a database record.

    Serializes the entry's ``content`` to ``content_json``, extracts text
    from ``content.parts`` for ``content_text``, and merges entry-level
    ``custom_metadata`` with the optional ``extra_metadata`` parameter.

    Args:
        entry: ADK MemoryEntry object.
        app_name: Name of the application.
        user_id: ID of the user.
        extra_metadata: Optional call-level metadata to merge with the
            entry's own ``custom_metadata``.

    Returns:
        MemoryRecord for database storage, or None if entry has no
        indexable content.
    """
    content_text = extract_content_text(entry.content)
    if not content_text.strip():
        return None

    content_dict = entry.content.model_dump(exclude_none=True, mode="json")

    # Merge entry-level and call-level metadata
    merged_metadata: dict[str, Any] | None = None
    if entry.custom_metadata or extra_metadata:
        merged_metadata = {}
        if extra_metadata:
            merged_metadata.update(extra_metadata)
        if entry.custom_metadata:
            merged_metadata.update(entry.custom_metadata)

    now = datetime.now(timezone.utc)

    # Parse timestamp from entry if available
    timestamp = now
    if entry.timestamp:
        try:
            timestamp = datetime.fromisoformat(entry.timestamp)
        except (ValueError, TypeError):
            timestamp = now

    return MemoryRecord(
        id=entry.id or str(uuid.uuid4()),
        session_id="",
        app_name=app_name,
        user_id=user_id,
        event_id="",
        author=entry.author or "",
        timestamp=timestamp,
        content_json=content_dict,
        content_text=content_text,
        metadata_json=merged_metadata,
        inserted_at=now,
    )


def session_to_memory_records(session: "Session") -> list["MemoryRecord"]:
    """Convert a completed ADK Session to a list of memory records.

    Extracts all events with content from the session and converts
    them to memory records for storage.

    Args:
        session: ADK Session object with events.

    Returns:
        List of MemoryRecord objects for database storage.
    """
    records: list[MemoryRecord] = []

    if not session.events:
        return records

    for event in session.events:
        record = event_to_memory_record(
            event=event, session_id=session.id, app_name=session.app_name, user_id=session.user_id
        )
        if record is not None:
            records.append(record)

    return records


def record_to_memory_entry(record: "MemoryRecord") -> "MemoryEntry":
    """Convert a database record to an ADK MemoryEntry.

    Preserves ``id`` and ``custom_metadata`` fields that were previously
    dropped on readback.

    Args:
        record: Memory database record.

    Returns:
        ADK MemoryEntry object with all available fields populated.
    """
    from google.adk.memory.memory_entry import MemoryEntry
    from google.genai import types

    content = types.Content.model_validate(record["content_json"])

    timestamp_str = record["timestamp"].isoformat() if record["timestamp"] else None

    return MemoryEntry(
        id=record["id"],
        content=content,
        author=record["author"],
        timestamp=timestamp_str,
        custom_metadata=record["metadata_json"] or {},
    )


def records_to_memory_entries(records: list["MemoryRecord"]) -> list["Any"]:
    """Convert a list of database records to ADK MemoryEntry objects.

    Args:
        records: List of memory database records.

    Returns:
        List of ADK MemoryEntry objects.
    """
    return [record_to_memory_entry(record) for record in records]
