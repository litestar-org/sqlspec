"""CockroachDB event queue store for asyncpg driver."""

from sqlspec.adapters.cockroach_asyncpg.config import CockroachAsyncpgConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("CockroachAsyncpgEventQueueStore",)


class CockroachAsyncpgEventQueueStore(BaseEventQueueStore[CockroachAsyncpgConfig]):
    """Queue DDL for CockroachDB asyncpg configs.

    CockroachDB uses JSONB for efficient JSON storage and TIMESTAMPTZ for
    timezone-aware timestamps.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return CockroachDB-optimized column types for the event queue."""
        return "JSONB", "JSONB", "TIMESTAMPTZ"
