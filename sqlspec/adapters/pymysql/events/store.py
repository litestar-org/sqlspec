"""PyMySQL event queue store with MySQL-specific DDL."""

from typing import Final

from sqlspec.adapters.pymysql.config import PyMysqlConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("PyMysqlEventQueueStore",)

SCHEMA_QUALIFIED_SEGMENTS: Final[int] = 2


class PyMysqlEventQueueStore(BaseEventQueueStore[PyMysqlConfig]):
    """Queue DDL for PyMySQL configs."""

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return "JSON", "JSON", "DATETIME(6)"

    def _timestamp_default(self) -> str:
        return "CURRENT_TIMESTAMP(6)"

    def _index_ddl(self) -> str | None:
        """Build the plain MySQL ``ADD INDEX`` statement for the queue table.

        MySQL lacks an idempotent ``CREATE INDEX IF NOT EXISTS``. The migration
        consults ``driver.data_dictionary.get_indexes`` and only issues this
        statement when the index is absent, so the emitted DDL carries no
        existence probe.

        Returns:
            ``ALTER TABLE ... ADD INDEX`` statement for the queue index.
        """
        return f"ALTER TABLE {self.table_name} ADD INDEX {self._index_name()} (channel, status, available_at)"

    def _index_existence_target(self) -> "tuple[str | None, str] | None":
        """Return the ``(schema, table)`` target for the data-dictionary index check."""
        segments = self.table_name.split(".", 1)
        if len(segments) == SCHEMA_QUALIFIED_SEGMENTS:
            return segments[0], segments[1]
        return None, segments[0]
