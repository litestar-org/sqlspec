"""AsyncPG event queue store for PostgreSQL JSONB storage."""

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("AsyncpgEventQueueStore",)


class AsyncpgEventQueueStore(BaseEventQueueStore[AsyncpgConfig]):
    """PostgreSQL event queue store with JSONB columns.

    Uses PostgreSQL-native JSONB for efficient JSON storage and querying.
    TIMESTAMPTZ ensures proper timezone handling.

    Args:
        config: AsyncpgConfig with extension_config["events"] settings.

    Notes:
        Configuration is read from config.extension_config["events"]:
        - queue_table: Table name (default: "sqlspec_event_queue")

    Example:
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.adapters.asyncpg.events import AsyncpgEventQueueStore

        config = AsyncpgConfig(connection_config={"dsn": "postgresql://..."})
        store = AsyncpgEventQueueStore(config)
        for stmt in store.create_statements():
            await driver.execute_script(stmt)
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return PostgreSQL-native column types.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSONB", "JSONB", "TIMESTAMPTZ"
