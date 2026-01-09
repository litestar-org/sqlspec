"""Runtime helpers that bundle lifecycle, observer, and span orchestration."""

import re
from typing import TYPE_CHECKING, Any, cast

from sqlspec.observability._common import compute_sql_hash, get_trace_context, resolve_db_system
from sqlspec.observability._config import LoggingConfig, ObservabilityConfig
from sqlspec.observability._dispatcher import LifecycleDispatcher, LifecycleHook
from sqlspec.observability._observer import StatementObserver, create_event, create_statement_observer
from sqlspec.observability._spans import SpanManager
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.type_guards import has_span_attribute

_LITERAL_PATTERN = re.compile(r"'(?:''|[^'])*'")

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlspec.storage import StorageTelemetry


class ObservabilityRuntime:
    """Aggregates dispatchers, observers, spans, and custom metrics."""

    __slots__ = (
        "_metrics",
        "_redaction",
        "_statement_observers",
        "bind_key",
        "config",
        "config_name",
        "lifecycle",
        "span_manager",
    )

    # Allow test injection with fake span managers (mypyc strict typing workaround)
    span_manager: "Any"

    def __init__(
        self, config: ObservabilityConfig | None = None, *, bind_key: str | None = None, config_name: str | None = None
    ) -> None:
        config = config.copy() if config else ObservabilityConfig()
        if config.logging is None:
            config.logging = LoggingConfig()
        self.config = config
        self.bind_key = bind_key
        self.config_name = config_name or "SQLSpecConfig"
        lifecycle_config = cast("dict[str, Iterable[LifecycleHook]] | None", config.lifecycle)
        self.lifecycle = LifecycleDispatcher(lifecycle_config)
        self.span_manager = SpanManager(config.telemetry)
        observers: list[StatementObserver] = []
        if config.statement_observers:
            observers.extend(config.statement_observers)
        if config.print_sql:
            observers.append(create_statement_observer(config.logging))
        self._statement_observers = tuple(observers)
        self._redaction = config.redaction.copy() if config.redaction else None
        self._metrics: dict[str, float] = {}

    @property
    def has_statement_observers(self) -> bool:
        """Return True when any observers are registered."""

        return bool(self._statement_observers)

    @property
    def diagnostics_key(self) -> str:
        """Derive diagnostics key from bind key or configuration name."""

        if self.bind_key:
            return self.bind_key
        return self.config_name

    def base_context(self) -> dict[str, Any]:
        """Return the base payload for lifecycle events."""

        context = {"config": self.config_name}
        if self.bind_key:
            context["bind_key"] = self.bind_key
        correlation_id = CorrelationContext.get()
        if correlation_id:
            context["correlation_id"] = correlation_id
        return context

    def _build_context(self, **extras: Any) -> dict[str, Any]:
        context = self.base_context()
        context.update({key: value for key, value in extras.items() if value is not None})
        return context

    def lifecycle_snapshot(self) -> dict[str, int]:
        """Return lifecycle counters keyed under the diagnostics prefix."""

        return self.lifecycle.snapshot(prefix=self.diagnostics_key)

    def metrics_snapshot(self) -> dict[str, float]:
        """Return accumulated custom metrics with diagnostics prefix."""

        if not self._metrics:
            return {}
        prefix = self.diagnostics_key
        return {f"{prefix}.{name}": value for name, value in self._metrics.items()}

    def increment_metric(self, name: str, amount: float = 1.0) -> None:
        """Increment a custom metric counter."""

        self._metrics[name] = self._metrics.get(name, 0.0) + amount

    def record_metric(self, name: str, value: float) -> None:
        """Set a custom metric to an explicit value."""

        self._metrics[name] = value

    def start_migration_span(
        self, event: str, *, version: "str | None" = None, metadata: "dict[str, Any] | None" = None
    ) -> Any:
        """Start a migration span when telemetry is enabled."""

        if not self.span_manager.is_enabled:
            return None
        attributes: dict[str, Any] = {"sqlspec.migration.event": event, "sqlspec.config": self.config_name}
        if self.bind_key:
            attributes["sqlspec.bind_key"] = self.bind_key
        correlation_id = CorrelationContext.get()
        if correlation_id:
            attributes["sqlspec.correlation_id"] = correlation_id
        if version:
            attributes["sqlspec.migration.version"] = version
        if metadata:
            for key, value in metadata.items():
                if value is not None:
                    attributes[f"sqlspec.migration.{key}"] = value
        return self.span_manager.start_span(f"sqlspec.migration.{event}", attributes)

    def end_migration_span(
        self, span: Any, *, duration_ms: "int | None" = None, error: "Exception | None" = None
    ) -> None:
        """Finish a migration span, attaching optional duration metadata."""

        if span is None:
            return
        if duration_ms is not None and has_span_attribute(span):
            span.set_attribute("sqlspec.migration.duration_ms", duration_ms)
        self.span_manager.end_span(span, error=error)

    def emit_pool_create(self, pool: Any) -> None:
        span = self._start_lifecycle_span("pool.create", subject=pool)
        try:
            if self.lifecycle.has_pool_create:
                self.lifecycle.emit_pool_create(self._build_context(pool=pool))
        finally:
            self.span_manager.end_span(span)

    def emit_pool_destroy(self, pool: Any) -> None:
        span = self._start_lifecycle_span("pool.destroy", subject=pool)
        try:
            if self.lifecycle.has_pool_destroy:
                self.lifecycle.emit_pool_destroy(self._build_context(pool=pool))
        finally:
            self.span_manager.end_span(span)

    def emit_connection_create(self, connection: Any) -> None:
        span = self._start_lifecycle_span("connection.create", subject=connection)
        try:
            if self.lifecycle.has_connection_create:
                self.lifecycle.emit_connection_create(self._build_context(connection=connection))
        finally:
            self.span_manager.end_span(span)

    def emit_connection_destroy(self, connection: Any) -> None:
        span = self._start_lifecycle_span("connection.destroy", subject=connection)
        try:
            if self.lifecycle.has_connection_destroy:
                self.lifecycle.emit_connection_destroy(self._build_context(connection=connection))
        finally:
            self.span_manager.end_span(span)

    def emit_session_start(self, session: Any) -> None:
        span = self._start_lifecycle_span("session.start", subject=session)
        try:
            if self.lifecycle.has_session_start:
                self.lifecycle.emit_session_start(self._build_context(session=session))
        finally:
            self.span_manager.end_span(span)

    def emit_session_end(self, session: Any) -> None:
        span = self._start_lifecycle_span("session.end", subject=session)
        try:
            if self.lifecycle.has_session_end:
                self.lifecycle.emit_session_end(self._build_context(session=session))
        finally:
            self.span_manager.end_span(span)

    def emit_query_start(self, **extras: Any) -> None:
        if self.lifecycle.has_query_start:
            self.lifecycle.emit_query_start(self._build_context(**extras))

    def emit_query_complete(self, **extras: Any) -> None:
        if self.lifecycle.has_query_complete:
            self.lifecycle.emit_query_complete(self._build_context(**extras))

    def emit_error(self, exception: Exception, **extras: Any) -> None:
        if self.lifecycle.has_error:
            payload = self._build_context(exception=exception)
            payload.update({key: value for key, value in extras.items() if value is not None})
            self.lifecycle.emit_error(payload)
        self.increment_metric("errors", 1.0)

    def emit_statement_event(
        self,
        *,
        sql: str,
        parameters: Any,
        driver: str,
        operation: str,
        execution_mode: str | None,
        is_many: bool,
        is_script: bool,
        rows_affected: int | None,
        duration_s: float,
        storage_backend: str | None,
        started_at: float | None = None,
    ) -> None:
        """Emit a statement event to all registered observers."""

        if not self._statement_observers:
            return
        sanitized_sql = self._redact_sql(sql)
        sanitized_params = self._redact_parameters(parameters)
        correlation_id = CorrelationContext.get()
        logging_config = self.config.logging
        db_system = resolve_db_system(self.config_name)
        sql_hash = None
        if logging_config and logging_config.include_sql_hash:
            sql_hash = compute_sql_hash(sanitized_sql)
        sql_truncation_length = logging_config.sql_truncation_length if logging_config else 2000
        sql_original_length = len(sanitized_sql)
        sql_truncated = sql_original_length > sql_truncation_length
        trace_id = None
        span_id = None
        if logging_config and logging_config.include_trace_context:
            trace_id, span_id = get_trace_context()
        event = create_event(
            sql=sanitized_sql,
            parameters=sanitized_params,
            driver=driver,
            adapter=self.config_name,
            bind_key=self.bind_key,
            db_system=db_system,
            operation=operation,
            execution_mode=execution_mode,
            is_many=is_many,
            is_script=is_script,
            rows_affected=rows_affected,
            duration_s=duration_s,
            correlation_id=correlation_id,
            storage_backend=storage_backend,
            started_at=started_at,
            sql_hash=sql_hash,
            sql_truncated=sql_truncated,
            sql_original_length=sql_original_length,
            trace_id=trace_id,
            span_id=span_id,
        )
        for observer in self._statement_observers:
            observer(event)

    def start_query_span(self, sql: str, operation: str, driver: str) -> Any:
        """Start a query span with runtime metadata."""

        sql_hash = compute_sql_hash(sql)
        connection_info = {"sqlspec.statement.hash": sql_hash, "sqlspec.statement.length": len(sql)}
        sql_payload = ""
        if self.config.print_sql:
            sql_payload = self._redact_sql(sql)
            sql_payload, truncated = _truncate_text(sql_payload, max_chars=4096)
            if truncated:
                connection_info["sqlspec.statement.truncated"] = True

        correlation_id = CorrelationContext.get()
        return self.span_manager.start_query_span(
            driver=driver,
            adapter=self.config_name,
            bind_key=self.bind_key,
            sql=sql_payload,
            operation=operation,
            connection_info=connection_info,
            correlation_id=correlation_id,
        )

    def start_storage_span(
        self, operation: str, *, destination: str | None = None, format_label: str | None = None
    ) -> Any:
        """Start a storage bridge span for read/write operations."""

        if not self.span_manager.is_enabled:
            return None
        attributes: dict[str, Any] = {"sqlspec.storage.operation": operation, "sqlspec.config": self.config_name}
        if self.bind_key:
            attributes["sqlspec.bind_key"] = self.bind_key
        correlation_id = CorrelationContext.get()
        if correlation_id:
            attributes["sqlspec.correlation_id"] = correlation_id
        if destination:
            attributes["sqlspec.storage.destination"] = destination
        if format_label:
            attributes["sqlspec.storage.format"] = format_label
        return self.span_manager.start_span(f"sqlspec.storage.{operation}", attributes)

    def start_span(self, name: str, *, attributes: dict[str, Any] | None = None) -> Any:
        """Start a custom span enriched with configuration context."""

        if not self.span_manager.is_enabled:
            return None
        merged: dict[str, Any] = attributes.copy() if attributes else {}
        merged.setdefault("sqlspec.config", self.config_name)
        if self.bind_key:
            merged.setdefault("sqlspec.bind_key", self.bind_key)
        correlation_id = CorrelationContext.get()
        if correlation_id:
            merged.setdefault("sqlspec.correlation_id", correlation_id)
        return self.span_manager.start_span(name, merged)

    def end_span(self, span: Any, *, error: Exception | None = None) -> None:
        """Finish a custom span."""

        self.span_manager.end_span(span, error=error)

    def end_storage_span(
        self, span: Any, *, telemetry: "StorageTelemetry | None" = None, error: Exception | None = None
    ) -> None:
        """Finish a storage span, attaching telemetry metadata when available."""

        if span is None:
            return
        if telemetry:
            telemetry = self.annotate_storage_telemetry(telemetry)
            self._attach_storage_telemetry(span, telemetry)
        self.span_manager.end_span(span, error=error)

    def annotate_storage_telemetry(self, telemetry: "StorageTelemetry") -> "StorageTelemetry":
        """Add bind key / config / correlation metadata to telemetry payloads."""

        annotated = telemetry
        base = self.base_context()
        correlation_id = base.get("correlation_id")
        if correlation_id and not annotated.get("correlation_id"):
            annotated["correlation_id"] = correlation_id
        annotated.setdefault("config", self.config_name)
        if self.bind_key and not annotated.get("bind_key"):
            annotated["bind_key"] = self.bind_key
        return annotated

    def _start_lifecycle_span(self, event: str, subject: Any | None = None) -> Any:
        if not self.span_manager.is_enabled:
            return None
        attributes: dict[str, Any] = {"sqlspec.lifecycle.event": event, "sqlspec.config": self.config_name}
        if self.bind_key:
            attributes["sqlspec.bind_key"] = self.bind_key
        correlation_id = CorrelationContext.get()
        if correlation_id:
            attributes["sqlspec.correlation_id"] = correlation_id
        if subject is not None:
            attributes["sqlspec.lifecycle.subject_type"] = type(subject).__name__
        return self.span_manager.start_span(f"sqlspec.lifecycle.{event}", attributes)

    def _attach_storage_telemetry(self, span: Any, telemetry: "StorageTelemetry") -> None:
        if not has_span_attribute(span):
            return
        if "backend" in telemetry and telemetry["backend"] is not None:
            span.set_attribute("sqlspec.storage.backend", telemetry["backend"])
        if "bytes_processed" in telemetry and telemetry["bytes_processed"] is not None:
            span.set_attribute("sqlspec.storage.bytes_processed", telemetry["bytes_processed"])
        if "rows_processed" in telemetry and telemetry["rows_processed"] is not None:
            span.set_attribute("sqlspec.storage.rows_processed", telemetry["rows_processed"])
        if "destination" in telemetry and telemetry["destination"] is not None:
            span.set_attribute("sqlspec.storage.destination", telemetry["destination"])
        if "format" in telemetry and telemetry["format"] is not None:
            span.set_attribute("sqlspec.storage.format", telemetry["format"])
        if "duration_s" in telemetry and telemetry["duration_s"] is not None:
            span.set_attribute("sqlspec.storage.duration_s", telemetry["duration_s"])
        if "correlation_id" in telemetry and telemetry["correlation_id"] is not None:
            span.set_attribute("sqlspec.correlation_id", telemetry["correlation_id"])

    def _redact_sql(self, sql: str) -> str:
        config = self._redaction
        if config is None or not config.mask_literals:
            return sql
        return _LITERAL_PATTERN.sub("'***'", sql)

    def _redact_parameters(self, parameters: Any) -> Any:
        config = self._redaction
        if config is None or not config.mask_parameters:
            return parameters
        allow_list = set(config.parameter_allow_list or ())
        return _mask_parameters(parameters, allow_list)


def _mask_parameters(value: Any, allow_list: set[str]) -> Any:
    if isinstance(value, dict):
        masked: dict[str, Any] = {}
        for key, item in value.items():
            if allow_list and key in allow_list:
                masked[key] = _mask_parameters(item, allow_list)
            else:
                masked[key] = "***"
        return masked
    if isinstance(value, list):
        return [_mask_parameters(item, allow_list) for item in value]
    if isinstance(value, tuple):
        return tuple(_mask_parameters(item, allow_list) for item in value)
    return "***"


def _truncate_text(value: str, *, max_chars: int) -> tuple[str, bool]:
    if len(value) <= max_chars:
        return value, False
    return value[:max_chars], True


__all__ = ("ObservabilityRuntime",)
