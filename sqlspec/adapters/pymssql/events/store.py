"""pymssql event queue store with T-SQL-specific DDL."""

from sqlspec.adapters.pymssql.config import PymssqlConfig
from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.utils.text import split_qualified_identifier

__all__ = ("PymssqlEventQueueStore",)

_NVARCHAR_MAX_THRESHOLD = 4000
_QUALIFIED_IDENTIFIER_MIN_PARTS = 2


class PymssqlEventQueueStore(BaseEventQueueStore[PymssqlConfig]):
    """T-SQL DDL hooks for the event queue store."""

    __slots__ = ()

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

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        if object_type == "table":
            return f"IF OBJECT_ID(N'{_object_name(self.table_name)}', N'U') IS NULL BEGIN {statement}; END"
        if object_type == "index":
            return f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{self._index_name()}' AND object_id = OBJECT_ID(N'{_object_name(self.table_name)}')) BEGIN {statement}; END"
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        return f"IF OBJECT_ID(N'{_object_name(self.table_name)}', N'U') IS NOT NULL {statement};"


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
