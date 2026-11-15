"""SQLSpec: Type-safe SQL query mapper for Python."""

# ruff: noqa: E402
import warnings as _warnings

_warnings.filterwarnings(
    "ignore",
    message="You are using a Python version.*which Google will stop supporting",
    category=FutureWarning,
    module=r"google\.api_core\._python_version_support",
)
del _warnings

from importlib import import_module
from importlib.machinery import EXTENSION_SUFFIXES
from typing import TYPE_CHECKING, Any

from sqlspec.__metadata__ import __version__
from sqlspec.utils import logging as _logging
from sqlspec.utils.logging import suppress_erroneous_sqlglot_log_messages

_COMPILED = (_logging.__file__ or "").endswith(tuple(EXTENSION_SUFFIXES))

if TYPE_CHECKING:
    from sqlspec import (
        adapters,
        base,
        builder,
        core,
        dialects,
        driver,
        exceptions,
        extensions,
        loader,
        migrations,
        typing,
        utils,
    )
    from sqlspec import config as config
    from sqlspec import observability as observability
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
    from sqlspec.config import AsyncDatabaseConfig, ReadReplicaGroup, SyncDatabaseConfig
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
    from sqlspec.core.routing import (
        RandomSelector,
        ReplicaHealthBreaker,
        ReplicaSelector,
        ReplicaSpec,
        RoundRobinSelector,
        SelectionStrategy,
    )
    from sqlspec.driver import (
        AsyncDriverAdapterBase,
        AsyncRoutingSession,
        ExecutionResult,
        SyncDriverAdapterBase,
        SyncRoutingSession,
    )
    from sqlspec.exceptions import (
        ReadOnlyTransactionError,
        ReplicaUnavailableError,
        RoutingContextError,
        StackExecutionError,
        TenantCleanupError,
        TenantContextError,
    )
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
    from sqlspec.utils.routing import (
        RoutingContext,
        RoutingContextPayload,
        RoutingEntry,
        RoutingMode,
        RoutingScope,
        SQLSpecRoutingMiddleware,
        sign_primary_pin,
        verify_primary_pin,
    )
    from sqlspec.utils.tenant import TenantContext, TenantContextPayload
    from sqlspec.utils.uuids import nanoid, uuid4, uuid6, uuid7

__all__ = (
    "SQL",
    "ArrowResult",
    "AsyncDatabaseConfig",
    "AsyncDriverAdapterBase",
    "AsyncEventChannel",
    "AsyncEventListener",
    "AsyncRoutingSession",
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
    "RandomSelector",
    "ReadOnlyTransactionError",
    "ReadReplicaGroup",
    "RedactionConfig",
    "ReplicaHealthBreaker",
    "ReplicaSelector",
    "ReplicaSpec",
    "ReplicaUnavailableError",
    "RoundRobinSelector",
    "RoutingContext",
    "RoutingContextError",
    "RoutingContextPayload",
    "RoutingEntry",
    "RoutingMode",
    "RoutingScope",
    "SQLFactory",
    "SQLFile",
    "SQLFileLoader",
    "SQLResult",
    "SQLSpec",
    "SQLSpecRoutingMiddleware",
    "SchemaT",
    "Select",
    "SelectionStrategy",
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
    "SyncRoutingSession",
    "TelemetryConfig",
    "TenantCleanupError",
    "TenantContext",
    "TenantContextError",
    "TenantContextPayload",
    "Update",
    "__version__",
    "adapters",
    "base",
    "builder",
    "config",
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
    "nanoid",
    "observability",
    "register_param_type",
    "resolve_param_type",
    "sign_primary_pin",
    "sql",
    "typing",
    "utils",
    "uuid4",
    "uuid6",
    "uuid7",
    "verify_primary_pin",
)

suppress_erroneous_sqlglot_log_messages()


_EXPORTS: dict[str, tuple[str, str | None]] = {
    "ReadReplicaGroup": ("sqlspec.config", "ReadReplicaGroup"),
    "RandomSelector": ("sqlspec.core.routing", "RandomSelector"),
    "ReplicaHealthBreaker": ("sqlspec.core.routing", "ReplicaHealthBreaker"),
    "ReplicaSelector": ("sqlspec.core.routing", "ReplicaSelector"),
    "ReplicaSpec": ("sqlspec.core.routing", "ReplicaSpec"),
    "RoundRobinSelector": ("sqlspec.core.routing", "RoundRobinSelector"),
    "SelectionStrategy": ("sqlspec.core.routing", "SelectionStrategy"),
    "RoutingContext": ("sqlspec.utils.routing", "RoutingContext"),
    "RoutingContextPayload": ("sqlspec.utils.routing", "RoutingContextPayload"),
    "RoutingEntry": ("sqlspec.utils.routing", "RoutingEntry"),
    "RoutingMode": ("sqlspec.utils.routing", "RoutingMode"),
    "RoutingScope": ("sqlspec.utils.routing", "RoutingScope"),
    "SQLSpecRoutingMiddleware": ("sqlspec.utils.routing", "SQLSpecRoutingMiddleware"),
    "sign_primary_pin": ("sqlspec.utils.routing", "sign_primary_pin"),
    "verify_primary_pin": ("sqlspec.utils.routing", "verify_primary_pin"),
    "ReadOnlyTransactionError": ("sqlspec.exceptions", "ReadOnlyTransactionError"),
    "ReplicaUnavailableError": ("sqlspec.exceptions", "ReplicaUnavailableError"),
    "RoutingContextError": ("sqlspec.exceptions", "RoutingContextError"),
    "ArrowResult": ("sqlspec.core.result._base", "ArrowResult"),
    "AsyncDatabaseConfig": ("sqlspec.config", "AsyncDatabaseConfig"),
    "AsyncDriverAdapterBase": ("sqlspec.driver._async", "AsyncDriverAdapterBase"),
    "AsyncRoutingSession": ("sqlspec.driver._routing", "AsyncRoutingSession"),
    "AsyncEventChannel": ("sqlspec.extensions.events._channel", "AsyncEventChannel"),
    "AsyncEventListener": ("sqlspec.extensions.events._channel", "AsyncEventListener"),
    "CacheConfig": ("sqlspec.core.cache", "CacheConfig"),
    "CacheStats": ("sqlspec.core.cache", "CacheStats"),
    "Column": ("sqlspec.builder._column", "Column"),
    "ColumnExpression": ("sqlspec.builder._column", "ColumnExpression"),
    "ConnectionT": ("sqlspec.typing", "ConnectionT"),
    "CreateTable": ("sqlspec.builder._ddl", "CreateTable"),
    "Delete": ("sqlspec.builder._delete", "Delete"),
    "DropTable": ("sqlspec.builder._ddl", "DropTable"),
    "EventMessage": ("sqlspec.extensions.events._models", "EventMessage"),
    "ExecutionResult": ("sqlspec.driver._common", "ExecutionResult"),
    "FunctionColumn": ("sqlspec.builder._column", "FunctionColumn"),
    "Insert": ("sqlspec.builder._insert", "Insert"),
    "LoggingConfig": ("sqlspec.observability._config", "LoggingConfig"),
    "Merge": ("sqlspec.builder._merge", "Merge"),
    "ObservabilityConfig": ("sqlspec.observability._config", "ObservabilityConfig"),
    "ObservabilityRuntime": ("sqlspec.observability._runtime", "ObservabilityRuntime"),
    "ParamTypeMatcher": ("sqlspec.core.parameters._declared", "ParamTypeMatcher"),
    "ParameterConverter": ("sqlspec.core.parameters._converter", "ParameterConverter"),
    "ParameterDeclaration": ("sqlspec.core.parameters._declared", "ParameterDeclaration"),
    "ParameterProcessor": ("sqlspec.core.parameters._processor", "ParameterProcessor"),
    "ParameterStyle": ("sqlspec.core.parameters._types", "ParameterStyle"),
    "ParameterStyleConfig": ("sqlspec.core.parameters._types", "ParameterStyleConfig"),
    "PoolT": ("sqlspec.typing", "PoolT"),
    "ProcessedState": ("sqlspec.core.statement", "ProcessedState"),
    "QueryBuilder": ("sqlspec.builder._base", "QueryBuilder"),
    "RedactionConfig": ("sqlspec.observability._config", "RedactionConfig"),
    "SQL": ("sqlspec.core.statement", "SQL"),
    "SQLFactory": ("sqlspec.builder._factory", "SQLFactory"),
    "SQLFile": ("sqlspec.loader", "SQLFile"),
    "SQLFileLoader": ("sqlspec.loader", "SQLFileLoader"),
    "SQLResult": ("sqlspec.core.result._base", "SQLResult"),
    "SQLSpec": ("sqlspec.base", "SQLSpec"),
    "SchemaT": ("sqlspec.typing", "SchemaT"),
    "Select": ("sqlspec.builder._select", "Select"),
    "StackExecutionError": ("sqlspec.exceptions", "StackExecutionError"),
    "StackOperation": ("sqlspec.core.stack", "StackOperation"),
    "StackResult": ("sqlspec.core.result._base", "StackResult"),
    "Statement": ("sqlspec.core.statement", "Statement"),
    "StatementConfig": ("sqlspec.core.statement", "StatementConfig"),
    "StatementEvent": ("sqlspec.observability._observer", "StatementEvent"),
    "StatementObserver": ("sqlspec.observability._config", "StatementObserver"),
    "StatementParameters": ("sqlspec.typing", "StatementParameters"),
    "StatementStack": ("sqlspec.core.stack", "StatementStack"),
    "SupportedSchemaModel": ("sqlspec.typing", "SupportedSchemaModel"),
    "SyncDatabaseConfig": ("sqlspec.config", "SyncDatabaseConfig"),
    "SyncDriverAdapterBase": ("sqlspec.driver._sync", "SyncDriverAdapterBase"),
    "SyncRoutingSession": ("sqlspec.driver._routing", "SyncRoutingSession"),
    "SyncEventChannel": ("sqlspec.extensions.events._channel", "SyncEventChannel"),
    "SyncEventListener": ("sqlspec.extensions.events._channel", "SyncEventListener"),
    "TelemetryConfig": ("sqlspec.observability._config", "TelemetryConfig"),
    "TenantCleanupError": ("sqlspec.exceptions", "TenantCleanupError"),
    "TenantContext": ("sqlspec.utils.tenant._context", "TenantContext"),
    "TenantContextError": ("sqlspec.exceptions", "TenantContextError"),
    "TenantContextPayload": ("sqlspec.utils.tenant._context", "TenantContextPayload"),
    "Update": ("sqlspec.builder._update", "Update"),
    "adapters": ("sqlspec.adapters", None),
    "base": ("sqlspec.base", None),
    "builder": ("sqlspec.builder", None),
    "config": ("sqlspec.config", None),
    "core": ("sqlspec.core", None),
    "create_event": ("sqlspec.observability._observer", "create_event"),
    "create_statement_observer": ("sqlspec.observability._observer", "create_statement_observer"),
    "default_statement_observer": ("sqlspec.observability._observer", "default_statement_observer"),
    "dialects": ("sqlspec.dialects", None),
    "driver": ("sqlspec.driver", None),
    "exceptions": ("sqlspec.exceptions", None),
    "extensions": ("sqlspec.extensions", None),
    "filters": ("sqlspec.core.filters", None),
    "format_statement_event": ("sqlspec.observability._observer", "format_statement_event"),
    "loader": ("sqlspec.loader", None),
    "matches_param_type": ("sqlspec.core.parameters._declared", "matches_param_type"),
    "migrations": ("sqlspec.migrations", None),
    "nanoid": ("sqlspec.utils.uuids", "nanoid"),
    "observability": ("sqlspec.observability", None),
    "register_param_type": ("sqlspec.core.parameters._declared", "register_param_type"),
    "resolve_param_type": ("sqlspec.core.parameters._declared", "resolve_param_type"),
    "sql": ("sqlspec.builder._factory", "sql"),
    "typing": ("sqlspec.typing", None),
    "utils": ("sqlspec.utils", None),
    "uuid4": ("sqlspec.utils.uuids", "uuid4"),
    "uuid6": ("sqlspec.utils.uuids", "uuid6"),
    "uuid7": ("sqlspec.utils.uuids", "uuid7"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attribute = target
    module = import_module(module_name)
    value = module if attribute is None else getattr(module, attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | _EXPORTS.keys())


if _COMPILED:
    for _name in __all__:
        if _name not in globals():
            __getattr__(_name)
