"""Psqlpy event queue store."""

from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("PsqlpyEventQueueStore",)


class PsqlpyEventQueueStore(BaseEventQueueStore[PsqlpyConfig]):
    """Provide PostgreSQL column mappings for the queue table.

    PostgreSQL uses JSONB for efficient binary JSON storage with indexing support,
    and TIMESTAMPTZ for timezone-aware timestamps.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return PostgreSQL-optimized column types for the event queue."""
        return "JSONB", "JSONB", "TIMESTAMPTZ"
