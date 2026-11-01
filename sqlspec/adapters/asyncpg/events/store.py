"""AsyncPG event queue store for PostgreSQL JSONB storage."""

from typing import NotRequired

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.asyncpg.core import build_postgres_storage_params_sql
from sqlspec.config import EventsConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("AsyncpgEventQueueStore", "AsyncpgEventsConfig")


class AsyncpgEventsConfig(EventsConfig):
    """Asyncpg events settings for queue storage and supported native transports."""

    fillfactor: NotRequired[int]
    """PostgreSQL queue-table fillfactor; omitted to use the server default."""

    autovacuum_vacuum_scale_factor: NotRequired[float]
    """PostgreSQL queue vacuum threshold fraction; omitted to use the server default."""

    autovacuum_analyze_scale_factor: NotRequired[float]
    """PostgreSQL queue analyze threshold fraction; omitted to use the server default."""

    listener_queue_capacity: NotRequired[int]
    """Maximum buffered notifications per native listener; omitted for an unbounded queue."""


class AsyncpgEventQueueStore(BaseEventQueueStore[AsyncpgConfig]):
    """PostgreSQL event queue store with JSONB columns.

    Uses PostgreSQL-native JSONB for efficient JSON storage and querying.
    TIMESTAMPTZ ensures proper timezone handling.

    Args:
        config: AsyncpgConfig with extension_config["events"] settings.
    """

    __slots__ = ()
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset({
        "autovacuum_analyze_scale_factor",
        "autovacuum_vacuum_scale_factor",
        "fillfactor",
        "listener_queue_capacity",
    })

    def _column_types(self) -> "tuple[str, str, str]":
        """Return PostgreSQL-native column types.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSONB", "JSONB", "TIMESTAMPTZ"

    def _table_clause(self) -> str:
        """Return explicitly configured PostgreSQL table tuning options."""
        return build_postgres_storage_params_sql(self.settings, include_autovacuum=True, section="events")
