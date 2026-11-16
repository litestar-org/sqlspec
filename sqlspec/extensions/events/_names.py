"""Validated event queue and channel names."""

import re

from sqlspec.exceptions import EventChannelError

__all__ = ("normalize_event_channel_name", "normalize_queue_table_name")

_IDENTIFIER_PATTERN = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]*\Z")


def normalize_queue_table_name(name: str) -> str:
    """Validate schema-qualified identifiers and return normalized name."""
    segments = name.split(".")
    for segment in segments:
        if _IDENTIFIER_PATTERN.fullmatch(segment) is None:
            msg = f"Invalid events table name: {name}"
            raise EventChannelError(msg)
    return name


def normalize_event_channel_name(name: str) -> str:
    """Validate event channel identifiers and return normalized name."""
    if _IDENTIFIER_PATTERN.fullmatch(name) is None:
        msg = f"Invalid events channel name: {name}"
        raise EventChannelError(msg)
    return name
