"""SQLite event queue store."""

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("SqliteEventQueueStore",)


class SqliteEventQueueStore(BaseEventQueueStore[SqliteConfig]):
    """Provide SQLite-specific column types for the events queue.

    SQLite stores JSON as TEXT since it lacks a native JSON column type.
    JSON functions can still operate on TEXT columns containing valid JSON.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return SQLite-compatible column types for the event queue."""
        return "TEXT", "TEXT", "TIMESTAMP"
