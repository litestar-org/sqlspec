"""IBM Db2 event queue store implementation."""

from typing import TYPE_CHECKING, Any

from sqlspec.extensions.events import BaseEventQueueStore

if TYPE_CHECKING:
    from sqlspec.adapters.db2.config import Db2Config
else:
    Db2Config = Any

__all__ = ("Db2EventQueueStore",)


class Db2EventQueueStore(BaseEventQueueStore[Db2Config]):
    """IBM Db2 event queue store.

    Uses CLOB for JSON payloads and TIMESTAMP with microsecond precision
    for event sequencing.
    """

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        """Return Db2-compatible column types for the event queue."""
        return "CLOB", "CLOB", "TIMESTAMP"

    def _timestamp_default(self) -> str:
        """Return Db2 default expression for current timestamp."""
        return "CURRENT TIMESTAMP"

    def _string_type(self, length: int) -> str:
        """Return Db2 string type."""
        return f"VARCHAR({length})"

    def _integer_type(self) -> str:
        """Return Db2 integer type."""
        return "INTEGER"
