"""mssql-python event queue store with T-SQL-specific DDL."""

import re

from sqlspec.adapters.mssql_python.config import MssqlPythonConfig
from sqlspec.extensions.events import BaseEventQueueStore
from sqlspec.utils.text import split_qualified_identifier

__all__ = ("MssqlPythonEventQueueStore",)

_NVARCHAR_MAX_THRESHOLD = 4000
_QUALIFIED_IDENTIFIER_MIN_PARTS = 2


class _MssqlPythonEventStoreMixin:
    """Shared T-SQL DDL hooks for sync and async event queue stores."""

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
        match = re.search(r"DROP TABLE\s+(\S+)", statement, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            return f"IF OBJECT_ID(N'{_object_name(table_name)}', N'U') IS NOT NULL DROP TABLE {table_name};"
        return statement


class MssqlPythonEventQueueStore(_MssqlPythonEventStoreMixin, BaseEventQueueStore[MssqlPythonConfig]):
    """Event queue DDL for mssql-python sync configs."""

    __slots__ = ()


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
