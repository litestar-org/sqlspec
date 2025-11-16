"""Shared data models for the events subsystem."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import datetime

__all__ = ("EventMessage",)


@dataclass(slots=True)
class EventMessage:
    """Structured payload delivered to event handlers."""

    event_id: str
    channel: str
    payload: "dict[str, Any]"
    metadata: "dict[str, Any] | None"
    attempts: int
    available_at: "datetime"
    lease_expires_at: "datetime | None"
    created_at: "datetime"
