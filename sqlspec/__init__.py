"""SQLSpec: Safe and elegant SQL query building for Python."""

from sqlspec import adapters, base, builder, core, driver, exceptions, extensions, loader, typing, utils
from sqlspec.__metadata__ import __version__
from sqlspec._sql import SQLFactory
from sqlspec.base import SQLSpec
from sqlspec.builder import (
    Column,
    ColumnExpression,
    CreateTable,
    Delete,
    DropTable,
    FunctionColumn,
    Insert,
    Merge,
    QueryBuilder,
    Select,
    Update,
)
from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
from sqlspec.core.cache import CacheConfig, CacheStats
from sqlspec.core.parameters import ParameterConverter, ParameterProcessor, ParameterStyle, ParameterStyleConfig
from sqlspec.core.result import ArrowResult, SQLResult
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase, ExecutionResult, SyncDriverAdapterBase
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
from sqlspec.typing import ConnectionT, DictRow, ModelDTOT, ModelT, RowT, StatementParameters, SupportedSchemaModel

sql = SQLFactory()

__all__ = (
    "SQL",
    "ArrowResult",
    "AsyncDatabaseConfig",
    "AsyncDriverAdapterBase",
    "CacheConfig",
    "CacheStats",
    "Column",
    "ColumnExpression",
    "ConnectionT",
    "CreateTable",
    "Delete",
    "DictRow",
    "DropTable",
    "ExecutionResult",
    "FunctionColumn",
    "Insert",
    "Merge",
    "ModelDTOT",
    "ModelT",
    "NotFoundError",
    "ParameterConverter",
    "ParameterError",
    "ParameterProcessor",
    "ParameterStyle",
    "ParameterStyleConfig",
    "QueryBuilder",
    "RowT",
    "SQLBuilderError",
    "SQLFactory",
    "SQLFile",
    "SQLFileLoader",
    "SQLFileNotFoundError",
    "SQLFileParseError",
    "SQLParsingError",
    "SQLResult",
    "SQLSpec",
    "SQLValidationError",
    "Select",
    "StatementConfig",
    "StatementParameters",
    "SupportedSchemaModel",
    "SyncDatabaseConfig",
    "SyncDriverAdapterBase",
    "Update",
    "__version__",
    "adapters",
    "base",
    "builder",
    "core",
    "driver",
    "exceptions",
    "extensions",
    "loader",
    "sql",
    "typing",
    "utils",
)
