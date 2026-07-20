"""aiomysql event queue store with MySQL-specific DDL.

MySQL requires:
    - JSON type for payload/metadata (5.7.8+)
    - DATETIME(6) for microsecond precision timestamps
    - A plain ``ALTER TABLE ... ADD INDEX`` gated by a data-dictionary index
      check at migration time (MySQL lacks ``CREATE INDEX IF NOT EXISTS``)
"""

from typing import Final

from sqlspec.adapters.aiomysql.config import AiomysqlConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("AiomysqlEventQueueStore",)

SCHEMA_QUALIFIED_SEGMENTS: Final[int] = 2


class AiomysqlEventQueueStore(BaseEventQueueStore[AiomysqlConfig]):
    """MySQL-specific event queue store.

    Generates DDL optimized for MySQL 5.7.8+ using native JSON type. Index
    creation emits a plain ``ADD INDEX`` statement; the migration gates it on a
    ``data_dictionary.get_indexes`` check since MySQL has no idempotent
    ``CREATE INDEX IF NOT EXISTS``.

    Args:
        config: AiomysqlConfig with extension_config["events"] settings.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return MySQL-specific column types.

        Returns:
            Tuple of (payload_type, metadata_type, timestamp_type).
            Uses JSON for payload/metadata and DATETIME(6) for microsecond timestamps.
        """
        return "JSON", "JSON", "DATETIME(6)"

    def _timestamp_default(self) -> str:
        """Return MySQL timestamp default expression.

        MySQL requires CURRENT_TIMESTAMP(6) for DATETIME(6) columns,
        not just CURRENT_TIMESTAMP which is only valid for TIMESTAMP type.

        Returns:
            MySQL-specific timestamp default with microsecond precision.
        """
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
