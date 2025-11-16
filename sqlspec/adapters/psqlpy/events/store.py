"""Psqlpy event queue store."""

from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.extensions.events._store import BaseEventQueueStore

__all__ = ("PsqlpyEventQueueStore",)


class PsqlpyEventQueueStore(BaseEventQueueStore[PsqlpyConfig]):
    """Provide PostgreSQL column mappings for the queue table."""

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        return "JSONB", "JSONB", "TIMESTAMPTZ"
