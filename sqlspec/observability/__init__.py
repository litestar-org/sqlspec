"""Public observability exports."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec import _COMPILED

if TYPE_CHECKING:
    from sqlspec.observability._common import (
        compute_sql_hash,
        get_trace_context,
        get_w3c_trace_context,
        resolve_db_system,
    )
    from sqlspec.observability._config import (
        LifecycleHook,
        LoggingConfig,
        ObservabilityConfig,
        RedactionConfig,
        StatementObserver,
        TelemetryConfig,
    )
    from sqlspec.observability._diagnostics import DiagnosticsPayload, TelemetryDiagnostics
    from sqlspec.observability._dispatcher import LifecycleContext, LifecycleDispatcher
    from sqlspec.observability._formatters import AWSLogFormatter, AzureLogFormatter, CloudLogFormatter, GCPLogFormatter
    from sqlspec.observability._formatting import OTelConsoleFormatter, OTelJSONFormatter
    from sqlspec.observability._observer import (
        SQL_LOGGER_NAME,
        StatementEvent,
        create_event,
        create_statement_observer,
        default_statement_observer,
        format_statement_event,
    )
    from sqlspec.observability._runtime import ObservabilityRuntime
    from sqlspec.observability._sampling import SamplingConfig
    from sqlspec.observability._spans import SpanManager

__all__ = (
    "SQL_LOGGER_NAME",
    "AWSLogFormatter",
    "AzureLogFormatter",
    "CloudLogFormatter",
    "DiagnosticsPayload",
    "GCPLogFormatter",
    "LifecycleContext",
    "LifecycleDispatcher",
    "LifecycleHook",
    "LoggingConfig",
    "OTelConsoleFormatter",
    "OTelJSONFormatter",
    "ObservabilityConfig",
    "ObservabilityRuntime",
    "RedactionConfig",
    "SamplingConfig",
    "SpanManager",
    "StatementEvent",
    "StatementObserver",
    "TelemetryConfig",
    "TelemetryDiagnostics",
    "compute_sql_hash",
    "create_event",
    "create_statement_observer",
    "default_statement_observer",
    "format_statement_event",
    "get_trace_context",
    "get_w3c_trace_context",
    "resolve_db_system",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "SQL_LOGGER_NAME": ("sqlspec.observability._observer", "SQL_LOGGER_NAME"),
    "AWSLogFormatter": ("sqlspec.observability._formatters._aws", "AWSLogFormatter"),
    "AzureLogFormatter": ("sqlspec.observability._formatters._azure", "AzureLogFormatter"),
    "CloudLogFormatter": ("sqlspec.observability._formatters._base", "CloudLogFormatter"),
    "DiagnosticsPayload": ("sqlspec.observability._diagnostics", "DiagnosticsPayload"),
    "GCPLogFormatter": ("sqlspec.observability._formatters._gcp", "GCPLogFormatter"),
    "LifecycleContext": ("sqlspec.observability._dispatcher", "LifecycleContext"),
    "LifecycleDispatcher": ("sqlspec.observability._dispatcher", "LifecycleDispatcher"),
    "LifecycleHook": ("sqlspec.observability._config", "LifecycleHook"),
    "LoggingConfig": ("sqlspec.observability._config", "LoggingConfig"),
    "OTelConsoleFormatter": ("sqlspec.observability._formatting", "OTelConsoleFormatter"),
    "OTelJSONFormatter": ("sqlspec.observability._formatting", "OTelJSONFormatter"),
    "ObservabilityConfig": ("sqlspec.observability._config", "ObservabilityConfig"),
    "ObservabilityRuntime": ("sqlspec.observability._runtime", "ObservabilityRuntime"),
    "RedactionConfig": ("sqlspec.observability._config", "RedactionConfig"),
    "SamplingConfig": ("sqlspec.observability._sampling", "SamplingConfig"),
    "SpanManager": ("sqlspec.observability._spans", "SpanManager"),
    "StatementEvent": ("sqlspec.observability._observer", "StatementEvent"),
    "StatementObserver": ("sqlspec.observability._config", "StatementObserver"),
    "TelemetryConfig": ("sqlspec.observability._config", "TelemetryConfig"),
    "TelemetryDiagnostics": ("sqlspec.observability._diagnostics", "TelemetryDiagnostics"),
    "compute_sql_hash": ("sqlspec.observability._common", "compute_sql_hash"),
    "create_event": ("sqlspec.observability._observer", "create_event"),
    "create_statement_observer": ("sqlspec.observability._observer", "create_statement_observer"),
    "default_statement_observer": ("sqlspec.observability._observer", "default_statement_observer"),
    "format_statement_event": ("sqlspec.observability._observer", "format_statement_event"),
    "get_trace_context": ("sqlspec.observability._common", "get_trace_context"),
    "get_w3c_trace_context": ("sqlspec.observability._common", "get_w3c_trace_context"),
    "resolve_db_system": ("sqlspec.observability._common", "resolve_db_system"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | _EXPORTS.keys())


if _COMPILED:
    for _name in __all__:
        if _name not in globals():
            __getattr__(_name)
