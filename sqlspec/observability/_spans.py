"""Optional OpenTelemetry span helpers."""

from importlib import import_module
from typing import Any

from sqlspec.exceptions import MissingDependencyError
from sqlspec.observability._common import resolve_db_system
from sqlspec.observability._config import TelemetryConfig
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import ensure_opentelemetry
from sqlspec.utils.type_guards import has_tracer_provider

logger = get_logger("sqlspec.observability.spans")


class SpanManager:
    """Lazy OpenTelemetry span manager with graceful degradation."""

    __slots__ = (
        "_enabled",
        "_provider_factory",
        "_resource_attributes",
        "_span_kind",
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
        self._trace_api: Any | None = None
        self._status_cls: Any | None = None
        self._status_code_cls: Any | None = None
        self._span_kind: Any | None = None
        self._tracer: Any | None = None
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
    ) -> Any:
        """Start a query span with SQLSpec semantic attributes."""

        if not self._enabled:
            return None
        attributes: dict[str, Any] = {
            "db.system": resolve_db_system(adapter),
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
        try:
            if error and self._status_cls and self._status_code_cls:
                span.record_exception(error)
                status = self._status_cls(self._status_code_cls.ERROR, str(error))
                span.set_status(status)
            span.end()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.debug("Failed to finish span: %s", exc)

    def _start_span(self, name: str, attributes: dict[str, Any]) -> Any:
        tracer = self._get_tracer()
        if tracer is None:
            return None
        span_kind = self._span_kind
        if span_kind is None:
            return tracer.start_span(name=name, attributes=attributes)
        return tracer.start_span(name=name, attributes=attributes, kind=span_kind)

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

        span_kind_cls = trace.SpanKind
        status_cls = status_module.Status
        status_code_cls = status_module.StatusCode

        provider = None
        if self._provider_factory is not None:
            try:
                provider = self._provider_factory()
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.debug("Tracer provider factory failed: %s", exc)
        if provider and has_tracer_provider(provider):
            self._tracer = provider.get_tracer("sqlspec.observability")
        else:
            self._tracer = trace.get_tracer("sqlspec.observability")
        self._trace_api = trace
        self._status_cls = status_cls
        self._status_code_cls = status_code_cls
        self._span_kind = span_kind_cls.CLIENT


__all__ = ("SpanManager", "resolve_db_system")
