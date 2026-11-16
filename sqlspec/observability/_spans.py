"""Optional OpenTelemetry span helpers."""

from importlib import import_module
from typing import Any

from sqlspec.exceptions import MissingDependencyError
from sqlspec.observability._common import redact_sql_literals, resolve_db_system, resolve_effective_db_system
from sqlspec.observability._config import TelemetryConfig
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_opentelemetry
from sqlspec.utils.type_guards import has_tracer_provider

__all__ = ("SpanManager", "resolve_db_system")


logger = get_logger("sqlspec.observability.spans")


class SpanManager:
    """Lazy OpenTelemetry span manager with graceful degradation."""

    __slots__ = (
        "_context_api",
        "_enabled",
        "_provider_factory",
        "_resource_attributes",
        "_span_db_systems",
        "_span_kind",
        "_span_tokens",
        "_status_cls",
        "_status_code_cls",
        "_trace_api",
        "_tracer",
    )

    def __init__(self, telemetry: TelemetryConfig | None = None) -> None:
        telemetry = telemetry or TelemetryConfig()
        self._enabled = bool(telemetry.enable_spans)
        self._provider_factory = telemetry.provider_factory
        self._resource_attributes = dict(telemetry.resource_attributes or {})
        self._context_api: Any | None = None
        self._trace_api: Any | None = None
        self._status_cls: Any | None = None
        self._status_code_cls: Any | None = None
        self._span_kind: Any | None = None
        self._tracer: Any | None = None
        self._span_tokens: dict[int, Any] = {}
        self._span_db_systems: dict[int, str] = {}
        if self._enabled:
            self._resolve_api()

    @property
    def is_enabled(self) -> bool:
        """Return True once OpenTelemetry spans are available."""

        return bool(self._enabled and self._tracer)

    def start_query_span(
        self,
        *,
        driver: str,
        adapter: str,
        bind_key: str | None,
        sql: str,
        operation: str,
        connection_info: dict[str, Any] | None = None,
        storage_backend: str | None = None,
        correlation_id: str | None = None,
        db_system: str | None = None,
    ) -> Any:
        """Start a query span with SQLSpec semantic attributes."""

        if not self._enabled:
            return None
        resolved_db_system = resolve_effective_db_system(adapter, driver, db_system)
        attributes: dict[str, Any] = {
            "db.system": resolved_db_system,
            "db.operation": operation,
            "sqlspec.driver": driver,
        }
        if sql:
            attributes["db.statement"] = sql
        if bind_key:
            attributes["sqlspec.bind_key"] = bind_key
        if storage_backend:
            attributes["sqlspec.storage_backend"] = storage_backend
        if correlation_id:
            attributes["sqlspec.correlation_id"] = correlation_id
        if connection_info:
            attributes.update(connection_info)
        attributes.update(self._resource_attributes)
        return self._start_span("sqlspec.query", attributes)

    def start_span(self, name: str, attributes: dict[str, Any] | None = None) -> Any:
        """Start a generic span when instrumentation needs a custom name."""

        if not self._enabled:
            return None
        merged = dict(self._resource_attributes)
        if attributes:
            merged.update(attributes)
        return self._start_span(name, merged)

    def end_span(self, span: Any, error: Exception | None = None) -> None:
        """Close a span and record errors when provided."""

        if span is None:
            return
        token = self._span_tokens.pop(id(span), None)
        db_system = self._span_db_systems.pop(id(span), None)
        if token is not None and self._context_api is not None:
            try:
                self._context_api.detach(token)
            except Exception as exc:  # pragma: no cover
                logger.debug("Failed to detach span context: %s", exc)
        try:
            if error and self._status_cls and self._status_code_cls:
                sanitized_message = redact_sql_literals(str(error), dialect=db_system)
                sanitized_error, err_type_name = _sanitize_span_error(error, sanitized_message)
                try:
                    span.record_exception(sanitized_error, attributes={"exception.type": err_type_name})
                except TypeError:
                    span.record_exception(sanitized_error)
                status = self._status_cls(self._status_code_cls.ERROR, sanitized_message)
                span.set_status(status)
            span.end()
        except Exception as exc:  # pragma: no cover
            logger.debug("Failed to finish span: %s", exc)

    def _start_span(self, name: str, attributes: dict[str, Any]) -> Any:
        tracer = self._get_tracer()
        if tracer is None:
            return None
        span_kind = self._span_kind
        span = (
            tracer.start_span(name=name, attributes=attributes)
            if span_kind is None
            else tracer.start_span(name=name, attributes=attributes, kind=span_kind)
        )
        db_system = attributes.get("db.system")
        if span is not None and isinstance(db_system, str):
            self._span_db_systems[id(span)] = db_system
        if span is not None and self._context_api is not None and self._trace_api is not None:
            set_span = getattr(self._trace_api, "set_span_in_context", None)
            if callable(set_span):
                try:
                    token = self._context_api.attach(set_span(span))
                    self._span_tokens[id(span)] = token
                except Exception as exc:  # pragma: no cover
                    logger.debug("Failed to attach span context: %s", exc)
        return span

    def _get_tracer(self) -> Any:
        if not self._enabled:
            return None
        if self._tracer is None:
            self._resolve_api()
        return self._tracer

    def _resolve_api(self) -> None:
        try:
            ensure_opentelemetry()
        except MissingDependencyError:
            logger.debug("OpenTelemetry dependency missing - disabling spans")
            self._enabled = False
            self._tracer = None
            return

        try:
            trace = import_module("opentelemetry.trace")
            status_module = import_module("opentelemetry.trace.status")
        except ImportError:
            logger.debug("OpenTelemetry import failed - disabling spans")
            self._enabled = False
            self._tracer = None
            return

        try:
            self._context_api = import_module("opentelemetry.context")
        except ImportError:
            self._context_api = None

        span_kind_cls = trace.SpanKind
        status_cls = status_module.Status
        status_code_cls = status_module.StatusCode

        provider = None
        if self._provider_factory is not None:
            try:
                provider = self._provider_factory()
            except Exception as exc:  # pragma: no cover
                logger.debug("Tracer provider factory failed: %s", exc)
        if provider and has_tracer_provider(provider):
            self._tracer = provider.get_tracer("sqlspec.observability")
        else:
            self._tracer = trace.get_tracer("sqlspec.observability")
        self._trace_api = trace
        self._status_cls = status_cls
        self._status_code_cls = status_code_cls
        self._span_kind = span_kind_cls.CLIENT


def _sanitize_span_error(error: Exception, message: str) -> tuple[Exception, str]:
    """Return a cause-free exception holding only redacted message text and its original type name."""
    err_type = type(error)
    type_name = (
        err_type.__qualname__ if err_type.__module__ == "builtins" else f"{err_type.__module__}.{err_type.__qualname__}"
    )
    sanitized = Exception(message).with_traceback(error.__traceback__)
    sanitized.__cause__ = None
    sanitized.__suppress_context__ = True
    return sanitized, type_name
