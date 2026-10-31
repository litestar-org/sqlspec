"""Sybase ASE event queue store with ASE-compatible DDL."""

from sqlspec.adapters.sybase.config import SybaseConfig
from sqlspec.adapters.sybase.core import escape_sql_literal, quote_bracket_identifier, split_schema_and_table
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("SybaseEventQueueStore", "SybaseSyncEventQueueStore")

_VARCHAR_TEXT_THRESHOLD = 4000


class SybaseSyncEventQueueStore(BaseEventQueueStore[SybaseConfig]):
    """Sybase ASE DDL hooks for the event queue store."""

    __slots__ = ()

    def _column_types(self) -> tuple[str, str, str]:
        return "TEXT", "TEXT", "BIGDATETIME"

    def _string_type(self, length: int) -> str:
        if length >= _VARCHAR_TEXT_THRESHOLD:
            return "TEXT"
        return f"VARCHAR({length})"

    def _integer_type(self) -> str:
        return "INT"

    def _timestamp_default(self) -> str:
        return "GETUTCDATE()"

    def _table_ddl(self) -> str:
        """Build Sybase ASE-compatible CREATE TABLE SQL with DEFAULT before NOT NULL and explicit NULL."""
        payload_type, metadata_type, timestamp_type = self._column_types()
        string_64 = self._string_type(64)
        string_128 = self._string_type(128)
        string_32 = self._string_type(32)
        integer_type = self._integer_type()
        ts_default = self._timestamp_default()
        pk_inline = self._primary_key_syntax()
        table_clause = self._table_clause()

        pk_column = " PRIMARY KEY" if not pk_inline else ""

        return (
            f"CREATE TABLE {self.table_name} ("
            f"event_id {string_64}{pk_column} NOT NULL, "
            f"channel {string_128} NOT NULL, "
            f"payload_json {payload_type} NOT NULL, "
            f"metadata_json {metadata_type} NULL, "
            f"status {string_32} DEFAULT 'pending' NOT NULL, "
            f"available_at {timestamp_type} DEFAULT {ts_default} NOT NULL, "
            f"lease_expires_at {timestamp_type} NULL, "
            f"attempts {integer_type} DEFAULT 0 NOT NULL, "
            f"created_at {timestamp_type} DEFAULT {ts_default} NOT NULL, "
            f"acknowledged_at {timestamp_type} NULL"
            f"){pk_inline}{table_clause}"
        )

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        object_name = escape_sql_literal(_object_name(self.table_name))
        escaped_statement = escape_sql_literal(statement.rstrip().rstrip(";"))
        if object_type == "table":
            return f"IF OBJECT_ID('{object_name}', 'U') IS NULL EXECUTE('{escaped_statement}')"
        if object_type == "index":
            index_name = escape_sql_literal(self._index_name())
            return (
                f"IF NOT EXISTS (SELECT 1 FROM dbo.sysindexes WHERE name = '{index_name}' "
                f"AND id = OBJECT_ID('{object_name}')) EXECUTE('{escaped_statement}')"
            )
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        object_name = escape_sql_literal(_object_name(self.table_name))
        escaped_statement = escape_sql_literal(statement.rstrip().rstrip(";"))
        return f"IF OBJECT_ID('{object_name}', 'U') IS NOT NULL EXECUTE('{escaped_statement}');"


SybaseEventQueueStore = SybaseSyncEventQueueStore


def _object_name(table_name: str) -> str:
    schema_name, bare_table_name = split_schema_and_table(table_name)
    return f"{quote_bracket_identifier(schema_name)}.{quote_bracket_identifier(bare_table_name)}"
