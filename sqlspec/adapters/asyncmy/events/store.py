"""AsyncMy event queue store with MySQL-specific DDL.

MySQL requires:
- JSON type for payload/metadata (5.7.8+)
- DATETIME(6) for microsecond precision timestamps
- Procedural SQL for conditional index creation (no IF NOT EXISTS for indexes)
- Information schema queries for existence checks
"""

from typing import Final

from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("AsyncmyEventQueueStore",)

SCHEMA_QUALIFIED_SEGMENTS: Final[int] = 2


class AsyncmyEventQueueStore(BaseEventQueueStore[AsyncmyConfig]):
    """MySQL-specific event queue store with conditional DDL.

    Generates DDL optimized for MySQL 5.7.8+ using native JSON type.
    Index creation uses procedural SQL to check for existing indexes
    since MySQL does not support IF NOT EXISTS for indexes.

    Args:
        config: AsyncmyConfig with extension_config["events"] settings.

    Notes:
        Configuration is read from config.extension_config["events"]:
        - queue_table: Table name (default: "sqlspec_event_queue")

    Example:
        from sqlspec.adapters.asyncmy import AsyncmyConfig
        from sqlspec.adapters.asyncmy.events import AsyncmyEventQueueStore

        config = AsyncmyConfig(
            connection_config={"host": "localhost", "database": "mydb"},
            extension_config={"events": {"queue_table": "my_events"}}
        )
        store = AsyncmyEventQueueStore(config)
        for stmt in store.create_statements():
            await driver.execute_script(stmt)
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return MySQL-specific column types.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
            Uses JSON for payload/metadata and DATETIME(6) for microsecond timestamps.
        """
        return "JSON", "JSON", "DATETIME(6)"

    def _timestamp_default(self) -> str:
        """Return MySQL timestamp default expression.

        MySQL requires CURRENT_TIMESTAMP(6) for DATETIME(6) columns,
        not just CURRENT_TIMESTAMP which is only valid for TIMESTAMP type.

        Returns:
            MySQL-specific timestamp default with microsecond precision.
        """
        return "CURRENT_TIMESTAMP(6)"

    def _build_index_sql(self) -> str | None:
        """Build MySQL conditional index creation SQL.

        MySQL does not support IF NOT EXISTS for CREATE INDEX, so this method
        generates procedural SQL that checks information_schema.statistics
        before attempting to create the index.

        Returns:
            Procedural SQL script for conditional index creation.
        """
        table_name = self.table_name
        segments = table_name.split(".", 1)

        if len(segments) == SCHEMA_QUALIFIED_SEGMENTS:
            schema = segments[0]
            table = segments[1]
            schema_selector = f"'{schema}'"
        else:
            table = segments[0]
            schema_selector = "DATABASE()"

        index_name = self._index_name()

        return (
            "SET @sqlspec_events_idx_exists := ("
            "SELECT COUNT(1) FROM information_schema.statistics "
            f"WHERE table_schema = {schema_selector} "
            f"AND table_name = '{table}' "
            f"AND index_name = '{index_name}');"
            "SET @sqlspec_events_idx_stmt := IF(@sqlspec_events_idx_exists = 0, "
            f"'ALTER TABLE {table_name} ADD INDEX {index_name} (channel, status, available_at)', "
            "'SELECT 1');"
            "PREPARE sqlspec_events_stmt FROM @sqlspec_events_idx_stmt;"
            "EXECUTE sqlspec_events_stmt;"
            "DEALLOCATE PREPARE sqlspec_events_stmt;"
        )
