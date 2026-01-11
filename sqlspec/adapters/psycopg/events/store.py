"""Psycopg event queue stores for sync and async drivers."""

from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("PsycopgAsyncEventQueueStore", "PsycopgSyncEventQueueStore")


class PsycopgSyncEventQueueStore(BaseEventQueueStore[PsycopgSyncConfig]):
    """Queue DDL for psycopg synchronous configs.

    PostgreSQL uses JSONB for efficient binary JSON storage with indexing support,
    and TIMESTAMPTZ for timezone-aware timestamps.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return PostgreSQL-optimized column types for the event queue.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSONB", "JSONB", "TIMESTAMPTZ"


class PsycopgAsyncEventQueueStore(BaseEventQueueStore[PsycopgAsyncConfig]):
    """Queue DDL for psycopg async configs.

    PostgreSQL uses JSONB for efficient binary JSON storage with indexing support,
    and TIMESTAMPTZ for timezone-aware timestamps.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return PostgreSQL-optimized column types for the event queue.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSONB", "JSONB", "TIMESTAMPTZ"
