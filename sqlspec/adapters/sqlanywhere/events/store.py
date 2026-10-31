"""SAP SQL Anywhere event queue store with SQL Anywhere-compatible DDL."""

from sqlspec.adapters.sqlanywhere.config import SqlanywhereConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("SqlanywhereEventQueueStore",)

_VARCHAR_TEXT_THRESHOLD = 4000


class SqlanywhereEventQueueStore(BaseEventQueueStore[SqlanywhereConfig]):
    """SAP SQL Anywhere DDL hooks for the event queue store."""

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        return "LONG VARCHAR", "LONG VARCHAR", "TIMESTAMP"

    def _string_type(self, length: int) -> str:
        if length >= _VARCHAR_TEXT_THRESHOLD:
            return "LONG VARCHAR"
        return f"VARCHAR({length})"

    def _integer_type(self) -> str:
        return "INTEGER"

    def _timestamp_default(self) -> str:
        return "CURRENT UTC TIMESTAMP"
