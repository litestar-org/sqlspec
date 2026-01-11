"""AioSQLite event queue store."""

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("AiosqliteEventQueueStore",)


class AiosqliteEventQueueStore(BaseEventQueueStore[AiosqliteConfig]):
    """Provide column definitions for the async SQLite adapter.

    SQLite stores JSON as TEXT since it lacks a native JSON column type.
    JSON functions can still operate on TEXT columns containing valid JSON.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return SQLite-compatible column types for the event queue."""
        return "TEXT", "TEXT", "TIMESTAMP"
