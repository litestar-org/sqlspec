"""Teradata Vantage event queue store implementation."""

from typing import Final

from sqlspec.adapters.teradata.config import TeradataConfig
from sqlspec.adapters.teradata.core import split_schema_and_table
from sqlspec.adapters.teradata.migrations import build_guarded_drop_table
from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.migrations.schema import DropStatement, SchemaTarget

__all__ = ("TeradataEventQueueStore",)

TERADATA_EVENT_TABLE_DDL: Final[str] = (
    "CREATE MULTISET TABLE {table} (event_id VARCHAR(64) NOT NULL PRIMARY KEY, "
    "channel VARCHAR(128) NOT NULL, payload_json JSON NOT NULL, metadata_json JSON, "
    "status VARCHAR(32) DEFAULT 'pending' NOT NULL, "
    "available_at TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6) NOT NULL, "
    "lease_expires_at TIMESTAMP(6) WITH TIME ZONE, attempts INTEGER DEFAULT 0 NOT NULL, "
    "created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6) NOT NULL, "
    "acknowledged_at TIMESTAMP(6) WITH TIME ZONE)"
)


class TeradataEventQueueStore(BaseEventQueueStore[TeradataConfig]):
    """Teradata Vantage event queue store with a catalog-checked secondary index.

    The queue table and index use plain ``CREATE`` statements; the events migration checks the
    catalog for the table and the index before running them.
    """

    __slots__ = ()

    def _column_types(self) -> "tuple[str, str, str]":
        """Return the payload, metadata, and timestamp column types."""
        return "JSON", "JSON", "TIMESTAMP(6) WITH TIME ZONE"

    def _table_ddl(self) -> str:
        """Return the MULTISET queue table DDL with a NOT NULL primary key."""
        return TERADATA_EVENT_TABLE_DDL.format(table=self.table_name)

    def _index_ddl(self) -> str:
        """Return the Teradata secondary index DDL for queue polling."""
        return f"CREATE INDEX {self._index_name()} (channel, status, available_at) ON {self.table_name}"

    def _schema_target(self) -> SchemaTarget:
        """Build the schema target with the database-qualified queue identity."""
        schema, table = split_schema_and_table(self.table_name)
        return SchemaTarget.from_ddl(table, self._table_ddl(), schema=schema, dialect="vantage")

    def _index_existence_target(self) -> "tuple[str | None, str]":
        """Return the catalog database and table used to check for the queue index."""
        return split_schema_and_table(self.table_name)

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        """Return the CREATE statement unchanged; existence is checked in the catalog."""
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        """Return the DROP statement unchanged."""
        return statement

    def downgrade_drops(self) -> "list[DropStatement]":
        """Return the catalog-probed drop for the queue table."""
        return [build_guarded_drop_table(self.table_name, statement) for statement in self.drop_statements()]
