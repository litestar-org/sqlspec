"""CockroachDB event queue stores for psycopg sync/async drivers."""

from sqlspec.adapters.cockroach_psycopg.config import CockroachPsycopgAsyncConfig, CockroachPsycopgSyncConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("CockroachPsycopgAsyncEventQueueStore", "CockroachPsycopgSyncEventQueueStore")


class CockroachPsycopgSyncEventQueueStore(BaseEventQueueStore[CockroachPsycopgSyncConfig]):
    """Queue DDL for CockroachDB psycopg synchronous configs.

    CockroachDB uses JSONB for efficient JSON storage and TIMESTAMPTZ for
    timezone-aware timestamps.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return CockroachDB-optimized column types for the event queue."""
        return "JSONB", "JSONB", "TIMESTAMPTZ"


class CockroachPsycopgAsyncEventQueueStore(BaseEventQueueStore[CockroachPsycopgAsyncConfig]):
    """Queue DDL for CockroachDB psycopg async configs.

    CockroachDB uses JSONB for efficient JSON storage and TIMESTAMPTZ for
    timezone-aware timestamps.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return CockroachDB-optimized column types for the event queue."""
        return "JSONB", "JSONB", "TIMESTAMPTZ"
