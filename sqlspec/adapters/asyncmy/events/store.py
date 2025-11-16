"""AsyncMy event queue store."""

from sqlspec.adapters.asyncmy import AsyncmyConfig
from sqlspec.extensions.events._store import BaseEventQueueStore

__all__ = ("AsyncmyEventQueueStore",)


class AsyncmyEventQueueStore(BaseEventQueueStore[AsyncmyConfig]):
    """Provide MySQL column mappings for the queue table."""

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        return "JSON", "JSON", "DATETIME(6)"
