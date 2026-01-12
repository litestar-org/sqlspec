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

    def _build_index_sql(self) -> str | None:
        table_name = self.table_name
        segments = table_name.split(".", 1)

        if len(segments) == SCHEMA_QUALIFIED_SEGMENTS:
            schema = segments[0]
            table = segments[1]
            schema_selector = f"'{schema}'"
        else:
            table = segments[0]
            schema_selector = "DATABASE()"

        index_name = self._index_name()

        return (
            "SET @sqlspec_events_idx_exists := ("
            "SELECT COUNT(1) FROM information_schema.statistics "
            f"WHERE table_schema = {schema_selector} "
            f"AND table_name = '{table}' "
            f"AND index_name = '{index_name}');"
            "SET @sqlspec_events_idx_stmt := IF(@sqlspec_events_idx_exists = 0, "
            f"'ALTER TABLE {table_name} ADD INDEX {index_name} (channel, status, available_at)', "
            "'SELECT 1');"
            "PREPARE sqlspec_events_stmt FROM @sqlspec_events_idx_stmt;"
            "EXECUTE sqlspec_events_stmt;"
            "DEALLOCATE PREPARE sqlspec_events_stmt;"
        )
