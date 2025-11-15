"""AioSQLite event queue store."""

from typing import TYPE_CHECKING

from sqlspec.extensions.events._store import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.aiosqlite.config import AiosqliteConfig

__all__ = ("AiosqliteEventQueueStore",)


class AiosqliteEventQueueStore(BaseEventQueueStore["AiosqliteConfig"]):
    """Provide column definitions for the async SQLite adapter."""

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return "TEXT", "TEXT", "TIMESTAMP"

