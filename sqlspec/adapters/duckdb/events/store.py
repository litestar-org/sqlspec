"""DuckDB event queue store.

DuckDB uses native JSON type for efficient JSON storage and querying.
The TIMESTAMP type provides microsecond precision for event ordering.

Configuration (optional):
    extension_config={
        "events": {
            "queue_table": "custom_event_queue",  # Override default table name
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

    Notes:
        Configuration is read from config.extension_config["events"]:
        - queue_table: Table name (default: "sqlspec_event_queue")

        DuckDB does not support native pub/sub, so events use the table-backed
        queue backend which provides durable, exactly-once delivery semantics.

    Example:
        from sqlspec.adapters.duckdb import DuckDBConfig
        from sqlspec.adapters.duckdb.events import DuckDBEventQueueStore

        config = DuckDBConfig(
            connection_config={"database": "events.db"},
            extension_config={"events": {"queue_table": "my_events"}}
        )
        store = DuckDBEventQueueStore(config)
        for stmt in store.create_statements():
            driver.execute_script(stmt)
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return DuckDB-optimized column types.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSON", "JSON", "TIMESTAMP"
