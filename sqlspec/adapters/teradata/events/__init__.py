"""Teradata event queue integration."""

from sqlspec.adapters.teradata.events.store import (
    TeradataEventQueueStore,
    TeradataSyncEventQueueStore,
)

__all__ = ("TeradataEventQueueStore", "TeradataSyncEventQueueStore")
