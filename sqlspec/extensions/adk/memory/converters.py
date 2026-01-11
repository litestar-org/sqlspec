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

__all__ = ("event_to_memory_record", "extract_content_text", "record_to_memory_entry", "session_to_memory_records")


def extract_content_text(content: "types.Content") -> str:
    """Extract plain text from ADK Content for search indexing.

    Handles multi-modal Content.parts including text, function calls,
    and other part types. Non-text parts are indexed by their type
    for discoverability.

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

    Args:
        record: Memory database record.

    Returns:
        ADK MemoryEntry object.
    """
    from google.adk.memory.memory_entry import MemoryEntry
    from google.genai import types

    content = types.Content.model_validate(record["content_json"])

    timestamp_str = record["timestamp"].isoformat() if record["timestamp"] else None

    return MemoryEntry(content=content, author=record["author"], timestamp=timestamp_str)


def records_to_memory_entries(records: list["MemoryRecord"]) -> list["Any"]:
    """Convert a list of database records to ADK MemoryEntry objects.

    Args:
        records: List of memory database records.

    Returns:
        List of ADK MemoryEntry objects.
    """
    return [record_to_memory_entry(record) for record in records]
