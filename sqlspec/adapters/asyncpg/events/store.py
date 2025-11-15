"""AsyncPG event queue store."""

from typing import TYPE_CHECKING

from sqlspec.extensions.events._store import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg.config import AsyncpgConfig

__all__ = ("AsyncpgEventQueueStore",)


class AsyncpgEventQueueStore(BaseEventQueueStore["AsyncpgConfig"]):
    """Provide PostgreSQL column mappings for the queue extension."""

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return "JSONB", "JSONB", "TIMESTAMPTZ"

