"""SQLite event queue store."""

from typing import TYPE_CHECKING

from sqlspec.extensions.events._store import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.sqlite.config import SqliteConfig

__all__ = ("SqliteEventQueueStore",)


class SqliteEventQueueStore(BaseEventQueueStore["SqliteConfig"]):
    """Provide SQLite-specific column types for the events queue."""

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return "TEXT", "TEXT", "TIMESTAMP"

