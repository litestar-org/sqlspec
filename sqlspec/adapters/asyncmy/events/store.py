"""AsyncMy event queue store."""

from typing import TYPE_CHECKING

from sqlspec.extensions.events._store import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.asyncmy.config import AsyncmyConfig

__all__ = ("AsyncmyEventQueueStore",)


class AsyncmyEventQueueStore(BaseEventQueueStore["AsyncmyConfig"]):
    """Provide MySQL column mappings for the queue table."""

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return "JSON", "JSON", "DATETIME(6)"

