"""SQLSpec: Type-safe SQL query mapper for Python."""

# ruff: noqa: E402
# Suppress noisy Google library deprecation warnings about Python version support.
# These are informational and clutter CLI output unnecessarily.
import warnings as _warnings

_warnings.filterwarnings(
    "ignore",
    message="You are using a Python version.*which Google will stop supporting",
    category=FutureWarning,
    module=r"google\.api_core\._python_version_support",
)
del _warnings

from typing import TYPE_CHECKING, Any

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
    ParameterDeclaration,
    ParameterProcessor,
    ParameterStyle,
    ParameterStyleConfig,
    ParamTypeMatcher,
    ProcessedState,
    SQLResult,
    StackOperation,
    StackResult,
    Statement,
    StatementConfig,
    StatementStack,
    matches_param_type,
    register_param_type,
    resolve_param_type,
)
from sqlspec.core import filters as filters
from sqlspec.driver import AsyncDriverAdapterBase, ExecutionResult, SyncDriverAdapterBase
from sqlspec.exceptions import StackExecutionError
from sqlspec.extensions.events import (
    AsyncEventChannel,
    AsyncEventListener,
    EventMessage,
    SyncEventChannel,
    SyncEventListener,
)
from sqlspec.loader import SQLFile, SQLFileLoader
from sqlspec.observability import (
    LoggingConfig,
    ObservabilityConfig,
    ObservabilityRuntime,
    RedactionConfig,
    StatementEvent,
    StatementObserver,
    TelemetryConfig,
    create_event,
    create_statement_observer,
    default_statement_observer,
    format_statement_event,
)
from sqlspec.typing import ConnectionT, PoolT, SchemaT, StatementParameters, SupportedSchemaModel
from sqlspec.utils.logging import suppress_erroneous_sqlglot_log_messages

if TYPE_CHECKING:
    from sqlspec import dialects

__all__ = (
    "SQL",
    "ArrowResult",
    "AsyncDatabaseConfig",
    "AsyncDriverAdapterBase",
    "AsyncEventChannel",
    "AsyncEventListener",
    "CacheConfig",
    "CacheStats",
    "Column",
    "ColumnExpression",
    "ConnectionT",
    "CreateTable",
    "Delete",
    "DropTable",
    "EventMessage",
    "ExecutionResult",
    "FunctionColumn",
    "Insert",
    "LoggingConfig",
    "Merge",
    "ObservabilityConfig",
    "ObservabilityRuntime",
    "ParamTypeMatcher",
    "ParameterConverter",
    "ParameterDeclaration",
    "ParameterProcessor",
    "ParameterStyle",
    "ParameterStyleConfig",
    "PoolT",
    "ProcessedState",
    "QueryBuilder",
    "RedactionConfig",
    "SQLFactory",
    "SQLFile",
    "SQLFileLoader",
    "SQLResult",
    "SQLSpec",
    "SchemaT",
    "Select",
    "StackExecutionError",
    "StackOperation",
    "StackResult",
    "Statement",
    "StatementConfig",
    "StatementEvent",
    "StatementObserver",
    "StatementParameters",
    "StatementStack",
    "SupportedSchemaModel",
    "SyncDatabaseConfig",
    "SyncDriverAdapterBase",
    "SyncEventChannel",
    "SyncEventListener",
    "TelemetryConfig",
    "Update",
    "__version__",
    "adapters",
    "base",
    "builder",
    "core",
    "create_event",
    "create_statement_observer",
    "default_statement_observer",
    "dialects",
    "driver",
    "exceptions",
    "extensions",
    "filters",
    "format_statement_event",
    "loader",
    "matches_param_type",
    "migrations",
    "register_param_type",
    "resolve_param_type",
    "sql",
    "typing",
    "utils",
)

suppress_erroneous_sqlglot_log_messages()


def __getattr__(name: str) -> "Any":
    if name == "dialects":
        import importlib

        return importlib.import_module("sqlspec.dialects")
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
