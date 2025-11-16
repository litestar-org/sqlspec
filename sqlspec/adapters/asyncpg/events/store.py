"""AsyncPG event queue store."""

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.extensions.events._store import BaseEventQueueStore

__all__ = ("AsyncpgEventQueueStore",)


class AsyncpgEventQueueStore(BaseEventQueueStore[AsyncpgConfig]):
    """Provide PostgreSQL column mappings for the queue extension."""

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        return "JSONB", "JSONB", "TIMESTAMPTZ"
