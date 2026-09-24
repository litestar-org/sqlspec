"""IBM Db2 event queue store implementation."""

from typing import TYPE_CHECKING, Final

from sqlspec.adapters.db2.config import Db2SyncConfig
from sqlspec.adapters.db2.core import split_db2_table_name
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("Db2SyncEventQueueStore",)

DB2_EVENT_TABLE_DDL: Final[str] = (
    "CREATE TABLE {table} (event_id VARCHAR(64) NOT NULL PRIMARY KEY, channel VARCHAR(128) NOT NULL, "
    "payload_json CLOB NOT NULL, metadata_json CLOB, status VARCHAR(32) NOT NULL DEFAULT 'pending', "
    "available_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP, lease_expires_at TIMESTAMP, "
    "attempts INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP, "
    "acknowledged_at TIMESTAMP)"
)


class _Db2EventStoreMixin:
    """Db2 LUW DDL hooks for sync and async event queue stores.

    The queue table is created with plain ``CREATE TABLE`` / ``CREATE INDEX`` statements; the
    events migration checks the catalog for the table and the index before running them.
    """

    __slots__ = ()

    if TYPE_CHECKING:

        @property
        def table_name(self) -> str: ...

    def _column_types(self) -> "tuple[str, str, str]":
        """Return the payload, metadata, and timestamp column types."""
        return "CLOB", "CLOB", "TIMESTAMP"

    def _table_ddl(self) -> str:
        """Return the queue table DDL with a NOT NULL primary key."""
        return DB2_EVENT_TABLE_DDL.format(table=self.table_name)

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        """Return the CREATE statement unchanged; existence is checked in the catalog."""
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        """Return the DROP statement unchanged."""
        return statement

    def _index_existence_target(self) -> "tuple[str | None, str]":
        """Return the catalog schema and table used to check for the queue index.

        The queue DDL writes the table name unquoted, so both parts are upper-folded.
        """
        return split_db2_table_name(self.table_name.upper())


class Db2SyncEventQueueStore(_Db2EventStoreMixin, BaseEventQueueStore[Db2SyncConfig]):
    """IBM Db2 event queue store for synchronous configs."""

    __slots__ = ()
