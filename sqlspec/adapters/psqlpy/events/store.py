"""Psqlpy event queue store."""

from typing import TYPE_CHECKING

from sqlspec.extensions.events._store import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy.config import PsqlpyConfig

__all__ = ("PsqlpyEventQueueStore",)


class PsqlpyEventQueueStore(BaseEventQueueStore["PsqlpyConfig"]):
    """Provide PostgreSQL column mappings for the queue table."""

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return "JSONB", "JSONB", "TIMESTAMPTZ"

