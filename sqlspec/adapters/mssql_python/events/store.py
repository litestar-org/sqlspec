"""mssql-python event queue store with T-SQL-specific DDL."""

from sqlspec.adapters.mssql_python.config import MssqlPythonConfig
from sqlspec.adapters.mssql_python.core import escape_sql_literal, quote_bracket_identifier, split_schema_and_table
from sqlspec.extensions.events import BaseEventQueueStore

__all__ = ("MssqlPythonEventQueueStore",)

_NVARCHAR_MAX_THRESHOLD = 4000


class MssqlPythonEventQueueStore(BaseEventQueueStore[MssqlPythonConfig]):
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
        object_name = escape_sql_literal(_object_name(self.table_name))
        if object_type == "table":
            return f"IF OBJECT_ID(N'{object_name}', N'U') IS NULL BEGIN {statement}; END"
        if object_type == "index":
            index_name = escape_sql_literal(self._index_name())
            return f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{index_name}' AND object_id = OBJECT_ID(N'{object_name}')) BEGIN {statement}; END"
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        object_name = escape_sql_literal(_object_name(self.table_name))
        return f"IF OBJECT_ID(N'{object_name}', N'U') IS NOT NULL {statement};"


def _object_name(table_name: str) -> str:
    schema_name, bare_table_name = split_schema_and_table(table_name)
    return f"{quote_bracket_identifier(schema_name)}.{quote_bracket_identifier(bare_table_name)}"
