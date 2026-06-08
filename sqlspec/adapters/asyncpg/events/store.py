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
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return PostgreSQL-native column types.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
        """
        return "JSONB", "JSONB", "TIMESTAMPTZ"
