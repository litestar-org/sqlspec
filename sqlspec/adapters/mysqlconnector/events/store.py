"""MysqlConnector event queue store with MySQL-specific DDL."""

from typing import Any, Final

from sqlspec.adapters.mysqlconnector.config import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("MysqlConnectorAsyncEventQueueStore", "MysqlConnectorSyncEventQueueStore")

SCHEMA_QUALIFIED_SEGMENTS: Final[int] = 2


def _mysql_column_types() -> "tuple[str, str, str]":
    """Return MySQL-specific column types for the event queue."""
    return "JSON", "JSON", "DATETIME(6)"


def _mysql_timestamp_default() -> str:
    """Return MySQL-specific timestamp default."""
    return "CURRENT_TIMESTAMP(6)"


def _mysql_build_index_sql(store: Any) -> str | None:
    """Build MySQL-specific index SQL that checks for existing indexes.

    MySQL doesn't support CREATE INDEX IF NOT EXISTS, so we use a workaround
    with information_schema to check if the index already exists.

    Args:
        store: Event queue store instance with table_name and _index_name().

    Returns:
        MySQL-specific index creation SQL using prepared statements.
    """
    table_name: str = store.table_name
    segments = table_name.split(".", 1)

    if len(segments) == SCHEMA_QUALIFIED_SEGMENTS:
        schema = segments[0]
        table = segments[1]
        schema_selector = f"'{schema}'"
    else:
        table = segments[0]
        schema_selector = "DATABASE()"

    index_name: str = store._index_name()

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


class MysqlConnectorSyncEventQueueStore(BaseEventQueueStore[MysqlConnectorSyncConfig]):
    """Queue DDL for mysql-connector synchronous configs.

    MySQL uses JSON for efficient JSON storage and DATETIME(6) for
    microsecond precision timestamps.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return _mysql_column_types()

    def _timestamp_default(self) -> str:
        return _mysql_timestamp_default()

    def _build_index_sql(self) -> str | None:
        return _mysql_build_index_sql(self)


class MysqlConnectorAsyncEventQueueStore(BaseEventQueueStore[MysqlConnectorAsyncConfig]):
    """Queue DDL for mysql-connector async configs.

    MySQL uses JSON for efficient JSON storage and DATETIME(6) for
    microsecond precision timestamps.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        return _mysql_column_types()

    def _timestamp_default(self) -> str:
        return _mysql_timestamp_default()

    def _build_index_sql(self) -> str | None:
        return _mysql_build_index_sql(self)
