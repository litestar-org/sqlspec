"""SQLSpec: Safe and elegant SQL query building for Python."""

from sqlspec import adapters, base, driver, exceptions, extensions, loader, statement, typing, utils
from sqlspec.__metadata__ import __version__
from sqlspec._sql import SQLFactory
from sqlspec.base import SQLSpec
from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
from sqlspec.exceptions import (
    NotFoundError,
    ParameterError,
    SQLBuilderError,
    SQLFileNotFoundError,
    SQLFileParseError,
    SQLParsingError,
    SQLValidationError,
)
from sqlspec.loader import SQLFile, SQLFileLoader
from sqlspec.statement.builder import Column, ColumnExpression, Delete, FunctionColumn, Insert, Merge, Select, Update
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import ConnectionT, DictRow, ModelDTOT, ModelT, RowT, StatementParameters

sql = SQLFactory()

__all__ = (
    "SQL",
    "ArrowResult",
    "AsyncDatabaseConfig",
    "Column",
    "ColumnExpression",
    "ConnectionT",
    "Delete",
    "DictRow",
    "FunctionColumn",
    "Insert",
    "Merge",
    "ModelDTOT",
    "ModelT",
    "NotFoundError",
    "ParameterError",
    "RowT",
    "SQLBuilderError",
    "SQLConfig",
    "SQLFile",
    "SQLFileLoader",
    "SQLFileNotFoundError",
    "SQLFileParseError",
    "SQLParsingError",
    "SQLResult",
    "SQLSpec",
    "SQLValidationError",
    "Select",
    "StatementParameters",
    "SyncDatabaseConfig",
    "Update",
    "__version__",
    "adapters",
    "base",
    "driver",
    "exceptions",
    "extensions",
    "loader",
    "sql",
    "statement",
    "typing",
    "utils",
)
