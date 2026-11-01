"""Psqlpy event queue store."""

from typing import NotRequired

from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psqlpy.core import build_postgres_storage_params_sql
from sqlspec.config import EventsConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("PsqlpyEventQueueStore", "PsqlpyEventsConfig")


class PsqlpyEventsConfig(EventsConfig):
    """Psqlpy events settings for queue storage and supported native transports."""

    fillfactor: NotRequired[int]
    """PostgreSQL queue-table fillfactor; omitted to use the server default."""

    autovacuum_vacuum_scale_factor: NotRequired[float]
    """PostgreSQL queue vacuum threshold fraction; omitted to use the server default."""

    autovacuum_analyze_scale_factor: NotRequired[float]
    """PostgreSQL queue analyze threshold fraction; omitted to use the server default."""


class PsqlpyEventQueueStore(BaseEventQueueStore[PsqlpyConfig]):
    """Provide PostgreSQL column mappings for the queue table.

    PostgreSQL uses JSONB for efficient binary JSON storage with indexing support,
    and TIMESTAMPTZ for timezone-aware timestamps.
    """

    __slots__ = ()
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset({
        "autovacuum_analyze_scale_factor",
        "autovacuum_vacuum_scale_factor",
        "fillfactor",
    })

    def _column_types(self) -> "tuple[str, str, str]":
        """Return PostgreSQL-optimized column types for the event queue."""
        return "JSONB", "JSONB", "TIMESTAMPTZ"

    def _table_clause(self) -> str:
        """Return explicitly configured PostgreSQL table tuning options."""
        return build_postgres_storage_params_sql(self.settings, include_autovacuum=True, section="events")
