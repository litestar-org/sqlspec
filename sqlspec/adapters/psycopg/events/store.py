"""Psycopg event queue stores for sync and async drivers."""

from typing import NotRequired

from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.psycopg.core import build_postgres_storage_params_sql
from sqlspec.config import EventsConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("PsycopgAsyncEventQueueStore", "PsycopgEventsConfig", "PsycopgSyncEventQueueStore")


class PsycopgEventsConfig(EventsConfig):
    """Psycopg events settings for queue storage and supported native transports."""

    fillfactor: NotRequired[int]
    """PostgreSQL queue-table fillfactor; omitted to use the server default."""

    autovacuum_vacuum_scale_factor: NotRequired[float]
    """PostgreSQL queue vacuum threshold fraction; omitted to use the server default."""

    autovacuum_analyze_scale_factor: NotRequired[float]
    """PostgreSQL queue analyze threshold fraction; omitted to use the server default."""

    listener_queue_capacity: NotRequired[int]
    """Maximum buffered notifications per native listener; omitted for an unbounded queue."""


class PsycopgSyncEventQueueStore(BaseEventQueueStore[PsycopgSyncConfig]):
    """Queue DDL for psycopg synchronous configs.

    PostgreSQL uses JSONB for efficient binary JSON storage with indexing support,
    and TIMESTAMPTZ for timezone-aware timestamps.
    """

    __slots__ = ()
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset({
        "autovacuum_analyze_scale_factor",
        "autovacuum_vacuum_scale_factor",
        "fillfactor",
        "listener_queue_capacity",
    })

    def _column_types(self) -> "tuple[str, str, str]":
        """Return PostgreSQL-optimized column types for the event queue.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSONB", "JSONB", "TIMESTAMPTZ"

    def _table_clause(self) -> str:
        """Return explicitly configured PostgreSQL table tuning options."""
        return build_postgres_storage_params_sql(self.settings, include_autovacuum=True, section="events")


class PsycopgAsyncEventQueueStore(BaseEventQueueStore[PsycopgAsyncConfig]):
    """Queue DDL for psycopg async configs.

    PostgreSQL uses JSONB for efficient binary JSON storage with indexing support,
    and TIMESTAMPTZ for timezone-aware timestamps.
    """

    __slots__ = ()
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset({
        "autovacuum_analyze_scale_factor",
        "autovacuum_vacuum_scale_factor",
        "fillfactor",
        "listener_queue_capacity",
    })

    def _column_types(self) -> "tuple[str, str, str]":
        """Return PostgreSQL-optimized column types for the event queue.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSONB", "JSONB", "TIMESTAMPTZ"

    def _table_clause(self) -> str:
        """Return explicitly configured PostgreSQL table tuning options."""
        return build_postgres_storage_params_sql(self.settings, include_autovacuum=True, section="events")
