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
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset({
        "autovacuum_analyze_scale_factor",
        "autovacuum_vacuum_scale_factor",
        "fillfactor",
    })

    def _column_types(self) -> "tuple[str, str, str]":
        """Return PostgreSQL-optimized column types for the event queue.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSONB", "JSONB", "TIMESTAMPTZ"

    def _table_clause(self) -> str:
        """Return explicitly configured PostgreSQL table tuning options."""
        return _postgres_table_options(self.settings)


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
    })

    def _column_types(self) -> "tuple[str, str, str]":
        """Return PostgreSQL-optimized column types for the event queue.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSONB", "JSONB", "TIMESTAMPTZ"

    def _table_clause(self) -> str:
        """Return explicitly configured PostgreSQL table tuning options."""
        return _postgres_table_options(self.settings)


def _postgres_table_options(settings: "dict[str, object]") -> str:
    options = _postgres_option_values(settings)
    return f" WITH ({', '.join(options)})" if options else ""


def _postgres_option_values(settings: "dict[str, object]") -> "list[str]":
    options: list[str] = []
    fillfactor = settings.get("fillfactor")
    if fillfactor is not None:
        if not isinstance(fillfactor, int) or isinstance(fillfactor, bool) or fillfactor not in range(10, 101):
            msg = "extension_config['events']['fillfactor'] must be an integer from 10 to 100"
            raise ValueError(msg)
        options.append(f"fillfactor = {fillfactor}")
    for key in ("autovacuum_vacuum_scale_factor", "autovacuum_analyze_scale_factor"):
        value = settings.get(key)
        if value is not None:
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not 0 <= float(value) <= 1:
                msg = f"extension_config['events']['{key}'] must be a number from 0 to 1"
                raise ValueError(msg)
            options.append(f"{key} = {float(value):g}")
    return options
