"""SQLite event queue store."""

from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.extensions.events._store import BaseEventQueueStore

__all__ = ("SqliteEventQueueStore",)


class SqliteEventQueueStore(BaseEventQueueStore[SqliteConfig]):
    """Provide SQLite-specific column types for the events queue."""

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        return "TEXT", "TEXT", "TIMESTAMP"
