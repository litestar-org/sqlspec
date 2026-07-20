"""MysqlConnector event queue store with MySQL-specific DDL."""

from typing import Any, Final

from sqlspec.adapters.mysqlconnector.config import MysqlConnectorAsyncConfig, MysqlConnectorSyncConfig
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("MysqlConnectorAsyncEventQueueStore", "MysqlConnectorSyncEventQueueStore")

SCHEMA_QUALIFIED_SEGMENTS: Final[int] = 2


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

    def _index_ddl(self) -> str | None:
        return _mysql_index_ddl(self)

    def _index_existence_target(self) -> "tuple[str | None, str] | None":
        return _mysql_index_existence_target(self)


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

    def _index_ddl(self) -> str | None:
        return _mysql_index_ddl(self)

    def _index_existence_target(self) -> "tuple[str | None, str] | None":
        return _mysql_index_existence_target(self)


def _mysql_column_types() -> "tuple[str, str, str]":
    """Return MySQL-specific column types for the event queue."""
    return "JSON", "JSON", "DATETIME(6)"


def _mysql_timestamp_default() -> str:
    """Return MySQL-specific timestamp default."""
    return "CURRENT_TIMESTAMP(6)"


def _mysql_index_ddl(store: Any) -> str | None:
    """Return the plain MySQL ``ADD INDEX`` statement for the queue table.

    MySQL lacks an idempotent ``CREATE INDEX IF NOT EXISTS``. The migration
    consults ``driver.data_dictionary.get_indexes`` and only issues this
    statement when the index is absent, so the emitted DDL carries no existence
    probe.

    Args:
        store: Event queue store instance with table_name and _index_name().

    Returns:
        ``ALTER TABLE ... ADD INDEX`` statement for the queue index.
    """
    return f"ALTER TABLE {store.table_name} ADD INDEX {store._index_name()} (channel, status, available_at)"


def _mysql_index_existence_target(store: Any) -> "tuple[str | None, str] | None":
    """Return the ``(schema, table)`` target for the data-dictionary index check."""
    segments = store.table_name.split(".", 1)
    if len(segments) == SCHEMA_QUALIFIED_SEGMENTS:
        return segments[0], segments[1]
    return None, segments[0]
