"""arrow-odbc event queue store with T-SQL and Db2 DDL."""

import re
from typing import Final

from sqlspec.adapters.arrow_odbc.config import ArrowOdbcConfig
from sqlspec.adapters.arrow_odbc.core import split_db2_name
from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.utils.text import split_qualified_identifier

__all__ = ("ArrowOdbcEventQueueStore",)

_NVARCHAR_MAX_THRESHOLD = 4000
_QUALIFIED_IDENTIFIER_MIN_PARTS = 2
_DB2_EVENT_TABLE_DDL: Final[str] = (
    "CREATE TABLE {table} (event_id VARCHAR(64) NOT NULL PRIMARY KEY, channel VARCHAR(128) NOT NULL, "
    "payload_json CLOB NOT NULL, metadata_json CLOB, status VARCHAR(32) NOT NULL DEFAULT 'pending', "
    "available_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP, lease_expires_at TIMESTAMP, "
    "attempts INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP, "
    "acknowledged_at TIMESTAMP)"
)


class ArrowOdbcEventQueueStore(BaseEventQueueStore[ArrowOdbcConfig]):
    """Event queue DDL for arrow-odbc configs.

    SQL Server DDL with ``OBJECT_ID`` guards is used by default. A config whose
    dialect resolves to ``db2`` gets plain Db2 ``CREATE TABLE``/``CREATE INDEX``
    statements; the events migration checks the catalog for the table and the
    index before running them.
    """

    __slots__ = ("_db2",)

    def __init__(self, config: ArrowOdbcConfig) -> None:
        super().__init__(config)
        self._db2 = str(config.statement_config.dialect).lower() == "db2"

    def _column_types(self) -> tuple[str, str, str]:
        return "NVARCHAR(MAX)", "NVARCHAR(MAX)", "DATETIME2(6)"

    def _string_type(self, length: int) -> str:
        if length >= _NVARCHAR_MAX_THRESHOLD:
            return "NVARCHAR(MAX)"
        return f"NVARCHAR({length})"

    def _integer_type(self) -> str:
        return "INT"

    def _timestamp_default(self) -> str:
        return "SYSUTCDATETIME()"

    def _table_ddl(self) -> str:
        if self._db2:
            return _DB2_EVENT_TABLE_DDL.format(table=self.table_name)
        return super()._table_ddl()

    def _index_existence_target(self) -> "tuple[str | None, str] | None":
        """Return the upper-folded catalog schema and table checked for the Db2 queue index.

        SQL Server guards its index DDL itself, so no external check is needed there.
        """
        if self._db2:
            return split_db2_name(self.table_name)
        return None

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        if self._db2:
            return statement
        if object_type == "table":
            match = re.search(r"CREATE TABLE\s+(\S+)", statement, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                return f"IF OBJECT_ID(N'{_object_name(table_name)}', N'U') IS NULL BEGIN {statement}; END"
        if object_type == "index":
            match = re.search(r"CREATE INDEX\s+(\S+)\s+ON\s+(\S+)", statement, re.IGNORECASE)
            if match:
                index_name = match.group(1).strip("[]")
                table_name = match.group(2)
                return f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{index_name}' AND object_id = OBJECT_ID(N'{_object_name(table_name)}')) BEGIN {statement}; END"
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        if self._db2:
            return statement
        match = re.search(r"DROP TABLE\s+(\S+)", statement, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            return f"IF OBJECT_ID(N'{_object_name(table_name)}', N'U') IS NOT NULL DROP TABLE {table_name};"
        return statement


def _split_table_name(table_name: str) -> tuple[str, str]:
    parts = split_qualified_identifier(table_name, quote_chars='"')
    if len(parts) < _QUALIFIED_IDENTIFIER_MIN_PARTS:
        return "dbo", parts[0] if parts else table_name
    schema_name = ".".join(parts[:-1])
    return schema_name or "dbo", parts[-1]


def _object_name(table_name: str) -> str:
    schema_name, bare_table_name = _split_table_name(table_name)
    return f"{_quote_bracket_identifier(schema_name)}.{_quote_bracket_identifier(bare_table_name)}"


def _quote_bracket_identifier(identifier: str) -> str:
    return f"[{identifier.replace(']', ']]')}]"
