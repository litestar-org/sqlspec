"""Teradata event queue schema and catalog guarded lifecycle."""

from typing import Any, ClassVar

from sqlspec.adapters.teradata.config import TeradataConfig
from sqlspec.adapters.teradata.core import split_teradata_table_name
from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.migrations.schema import SchemaTarget, metadata_has_index

__all__ = ("TeradataEventQueueStore", "TeradataSyncEventQueueStore")

_MAX_VARCHAR_LENGTH = 32000
_TABLE_EXISTS_SQL = "LOCKING ROW FOR ACCESS SELECT 1 FROM DBC.TablesV WHERE DatabaseName = COALESCE(?, DATABASE) AND TableName = ? AND TableKind IN ('T', 'O')"
_INDEX_EXISTS_SQL = "LOCKING ROW FOR ACCESS SELECT 1 FROM DBC.IndicesV WHERE DatabaseName = COALESCE(?, DATABASE) AND TableName = ? AND IndexName = ?"


class TeradataEventQueueStore(BaseEventQueueStore[TeradataConfig]):
    """Teradata JSON queue with independently reconciled secondary index."""

    __slots__ = ()
    dialect: ClassVar[str] = "teradata"

    def _column_types(self) -> tuple[str, str, str]:
        return "JSON", "JSON", "TIMESTAMP(6) WITH TIME ZONE"

    def _string_type(self, length: int) -> str:
        return "CLOB" if length > _MAX_VARCHAR_LENGTH else f"VARCHAR({length})"

    def _timestamp_default(self) -> str:
        return "CURRENT_TIMESTAMP(6)"

    def _table_ddl(self) -> str:
        return (
            f"CREATE MULTISET TABLE {self.table_name} (event_id VARCHAR(64) NOT NULL PRIMARY KEY, "
            "channel VARCHAR(128) NOT NULL, payload_json JSON NOT NULL, metadata_json JSON, "
            "status VARCHAR(32) DEFAULT 'pending' NOT NULL, "
            "available_at TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6) NOT NULL, "
            "lease_expires_at TIMESTAMP(6) WITH TIME ZONE, attempts INTEGER DEFAULT 0 NOT NULL, "
            "created_at TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6) NOT NULL, "
            "acknowledged_at TIMESTAMP(6) WITH TIME ZONE)"
        )

    def _index_ddl(self) -> str:
        return f"CREATE INDEX {self._index_name()} (channel, status, available_at) ON {self.table_name}"

    def _schema_target(self) -> SchemaTarget:
        schema, table = split_teradata_table_name(self.table_name)
        return SchemaTarget.from_ddl(table, self._table_ddl(), schema=schema, dialect="teradata")

    def _index_existence_target(self) -> tuple[str | None, str]:
        return split_teradata_table_name(self.table_name)

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        return statement

    def create_table(self) -> None:
        """Create missing queue objects using catalog probes."""
        schema, table = self._index_existence_target()
        with self._config.provide_session() as driver:
            if driver.select_one_or_none(_TABLE_EXISTS_SQL, (schema, table)) is None:
                driver.execute(self._table_ddl())
                driver.commit()
            self.reconcile_companion_objects_sync(driver)
            driver.commit()

    def drop_table(self) -> None:
        """Drop an existing queue table."""
        schema, table = self._index_existence_target()
        with self._config.provide_session() as driver:
            if driver.select_one_or_none(_TABLE_EXISTS_SQL, (schema, table)) is not None:
                driver.execute(f"DROP TABLE {self.table_name}")
                driver.commit()

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing indexes without committing the caller's transaction."""
        schema, table = self._index_existence_target()
        dictionary = getattr(driver, "data_dictionary", None)
        if dictionary is not None:
            rows = dictionary.get_indexes(driver, table=table, schema=schema)
            exists = metadata_has_index(rows, table, self._index_name(), schema_name=schema)
        else:
            exists = driver.select_one_or_none(_INDEX_EXISTS_SQL, (schema, table, self._index_name())) is not None
        if not exists:
            driver.execute(self._index_ddl())


TeradataSyncEventQueueStore = TeradataEventQueueStore
