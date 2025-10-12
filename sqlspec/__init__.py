"""SQLSpec: Type-safe SQL query mapper for Python."""

from sqlspec import adapters, base, builder, core, driver, exceptions, extensions, loader, migrations, typing, utils
from sqlspec.__metadata__ import __version__
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
    SQLFactory,
    Update,
    sql,
)
from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
from sqlspec.core import (
    SQL,
    ArrowResult,
    CacheConfig,
    CacheStats,
    ParameterConverter,
    ParameterProcessor,
    ParameterStyle,
    ParameterStyleConfig,
    SQLResult,
    Statement,
    StatementConfig,
)
from sqlspec.core import filters as filters
from sqlspec.driver import AsyncDriverAdapterBase, ExecutionResult, SyncDriverAdapterBase
from sqlspec.loader import SQLFile, SQLFileLoader
from sqlspec.typing import ConnectionT, PoolT, SchemaT, StatementParameters, SupportedSchemaModel
from sqlspec.utils.logging import suppress_erroneous_sqlglot_log_messages

suppress_erroneous_sqlglot_log_messages()

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
    "DropTable",
    "ExecutionResult",
    "FunctionColumn",
    "Insert",
    "Merge",
    "ParameterConverter",
    "ParameterProcessor",
    "ParameterStyle",
    "ParameterStyleConfig",
    "PoolT",
    "QueryBuilder",
    "SQLFactory",
    "SQLFile",
    "SQLFileLoader",
    "SQLResult",
    "SQLSpec",
    "SchemaT",
    "Select",
    "Statement",
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
    "filters",
    "loader",
    "migrations",
    "sql",
    "typing",
    "utils",
)
