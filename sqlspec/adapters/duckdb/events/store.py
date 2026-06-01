"""DuckDB event queue store.

DuckDB uses native JSON type for efficient JSON storage and querying.
The TIMESTAMP type provides microsecond precision for event ordering.

Configuration (optional):
    extension_config={
    "events": {
    "queue_table": "custom_event_queue", # Override default table name
    }
    }
"""

from sqlspec.adapters.duckdb.config import DuckDBConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("DuckDBEventQueueStore",)


class DuckDBEventQueueStore(BaseEventQueueStore[DuckDBConfig]):
    """DuckDB event queue store with native JSON support.

    DuckDB supports native JSON type for efficient JSON storage and querying.
    The table uses TIMESTAMP for event ordering with microsecond precision.

    Args:
        config: DuckDBConfig with optional extension_config["events"] settings.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return DuckDB-optimized column types.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSON", "JSON", "TIMESTAMP"
