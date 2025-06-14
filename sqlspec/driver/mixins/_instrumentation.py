"""Instrumentation mixins for database drivers.

This module provides mixins that add telemetry, logging, and performance
monitoring capabilities to database drivers.
"""

import logging
import secrets
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast

from sqlspec._serialization import encode_json
from sqlspec.typing import OPENTELEMETRY_INSTALLED, PROMETHEUS_INSTALLED
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlspec.config import InstrumentationConfig

__all__ = ("AsyncInstrumentationMixin", "SyncInstrumentationMixin")

logger = get_logger("instrumentation")


class BaseInstrumentationMixin:
    """Base instrumentation mixin with common functionality."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize instrumentation components."""
        super().__init__(*args, **kwargs)

        # Get instrumentation config (should be provided by the driver)
        self._instrumentation_config: Optional[InstrumentationConfig] = getattr(self, "instrumentation_config", None)

        # Initialize telemetry components if enabled
        self._tracer: Any = None
        self._meter: Any = None
        self._query_counter: Any = None
        self._error_counter: Any = None
        self._latency_histogram: Any = None
        self._connection_gauge: Any = None

        if self._instrumentation_config:
            self._initialize_telemetry()

    def _initialize_prometheus(self, config: "InstrumentationConfig") -> None:
        if not config.enable_prometheus or not PROMETHEUS_INSTALLED:
            logger.debug("Prometheus metrics not enabled or not installed")
            return
        try:
            from prometheus_client import Counter, Gauge, Histogram  # pyright: ignore

            # Query counter
            self._query_counter = Counter(
                "sqlspec_queries_total",
                "Total number of database queries",
                ["operation", "status", "db_system", *list(config.custom_tags.keys())],
            )

            # Error counter
            self._error_counter = Counter(
                "sqlspec_errors_total",
                "Total number of database errors",
                ["operation", "error_type", "db_system", *list(config.custom_tags.keys())],
            )

            # Latency histogram
            buckets = config.prometheus_latency_buckets or [
                0.001,
                0.005,
                0.01,
                0.025,
                0.05,
                0.1,
                0.25,
                0.5,
                1.0,
                2.5,
                5.0,
                10.0,
            ]
            self._latency_histogram = Histogram(
                "sqlspec_query_duration_seconds",
                "Database query latency",
                ["operation", "db_system", *list(config.custom_tags.keys())],
                buckets=buckets,
            )

            # Connection gauge
            self._connection_gauge = Gauge(
                "sqlspec_connections_active",
                "Number of active database connections",
                ["db_system", *list(config.custom_tags.keys())],
            )

            logger.info(
                "Prometheus metrics initialized",
                extra={"service_name": config.service_name, "endpoint": config.metrics_endpoint},
            )
        except Exception as e:
            logger.exception("Failed to initialize Prometheus metrics", extra={"error": str(e)})

    def _initialize_opentelemetry(self, config: "InstrumentationConfig") -> None:
        """Initialize OpenTelemetry components if enabled."""
        if not config.enable_opentelemetry or not OPENTELEMETRY_INSTALLED:
            logger.debug("OpenTelemetry tracing not enabled or not installed")
            return

        try:
            from opentelemetry import trace
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter  # pyright: ignore
            from opentelemetry.sdk.resources import Resource  # pyright: ignore
            from opentelemetry.sdk.trace import TracerProvider  # pyright: ignore
            from opentelemetry.sdk.trace.export import BatchSpanProcessor  # pyright: ignore

            # Create resource with service name
            resource = Resource.create({"service.name": config.service_name, **config.custom_tags})

            # Create tracer provider
            provider = TracerProvider(resource=resource)

            # Add OTLP exporter if endpoint is configured
            if config.telemetry_endpoint:
                otlp_exporter = OTLPSpanExporter(
                    endpoint=config.telemetry_endpoint,
                    insecure=True,  # Use secure=False for development
                )
                provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

            # Set as global tracer provider
            trace.set_tracer_provider(provider)

            # Get tracer
            self._tracer = trace.get_tracer(
                f"{config.service_name}.driver", schema_url="https://opentelemetry.io/schemas/1.11.0"
            )

            logger.info(
                "OpenTelemetry tracing initialized",
                extra={"service_name": config.service_name, "endpoint": config.telemetry_endpoint},
            )

        except Exception as e:
            logger.exception("Failed to initialize OpenTelemetry", extra={"error": str(e)})

    def _initialize_telemetry(self) -> None:
        """Initialize OpenTelemetry and Prometheus components."""
        config = self._instrumentation_config
        if not config:
            return
        self._initialize_opentelemetry(config)
        self._initialize_prometheus(config)

    @staticmethod
    def _should_sample(rate: float) -> bool:
        """Determine if an operation should be sampled.

        Args:
            rate: Sampling rate between 0.0 and 1.0

        Returns:
            True if the operation should be sampled
        """
        return secrets.SystemRandom().random() < rate

    def _truncate_query(self, query: str) -> str:
        """Truncate query to configured maximum length.

        Args:
            query: The SQL query to truncate

        Returns:
            Truncated query with ellipsis if needed
        """
        if not self._instrumentation_config:
            return query

        max_length = self._instrumentation_config.max_query_log_length
        if len(query) <= max_length:
            return query

        return query[: max_length - 3] + "..."

    def _format_parameters(self, parameters: Any) -> str:
        """Format parameters for logging.

        Args:
            parameters: Query parameters

        Returns:
            JSON formatted parameters (truncated if needed)
        """
        if not self._instrumentation_config:
            return "[]"

        if not parameters:
            return "[]"

        # Limit number of parameters
        max_count = self._instrumentation_config.max_parameter_log_count

        limited: Any
        if isinstance(parameters, dict):
            limited = dict(list(parameters.items())[:max_count])
            if len(parameters) > max_count:
                limited["..."] = f"({len(parameters) - max_count} more)"
        elif isinstance(parameters, (list, tuple)):
            limited = list(parameters[:max_count])
            if len(parameters) > max_count:
                limited.append(f"... ({len(parameters) - max_count} more)")
        else:
            limited = parameters

        return encode_json(limited)

    @contextmanager
    @staticmethod
    def _log_slow_operation(operation_name: str, threshold_ms: float) -> "Generator[None, None, None]":
        """Context manager to log slow operations.

        Args:
            operation_name: Name of the operation
            threshold_ms: Threshold in milliseconds for slow operation warning

        Yields:
            None
        """
        start_time = time.monotonic()
        try:
            yield
        finally:
            duration_ms = (time.monotonic() - start_time) * 1000
            if duration_ms > threshold_ms:
                logger.warning(
                    "Slow %s operation detected",
                    operation_name,
                    extra={
                        "operation": operation_name,
                        "duration_ms": duration_ms,
                        "threshold_ms": threshold_ms,
                        "correlation_id": CorrelationContext.get(),
                    },
                )


class SyncInstrumentationMixin(BaseInstrumentationMixin):
    """Synchronous instrumentation mixin for database drivers."""

    def log_query_execution(
        self,
        query: str,
        parameters: Any = None,
        result_count: "Optional[int]" = None,
        duration_ms: "Optional[float]" = None,
        error: "Optional[Exception]" = None,
    ) -> None:
        """Log query execution details.

        Args:
            query: The SQL query
            parameters: Query parameters
            result_count: Number of rows returned/affected
            duration_ms: Query execution time in milliseconds
            error: Exception if query failed
        """
        if not self._instrumentation_config:
            return

        config = self._instrumentation_config

        # Check sampling
        if not self._should_sample(config.log_sample_rate):
            return

        # Build log entry
        extra_fields: dict[str, Any] = {
            "query": self._truncate_query(query) if config.log_queries else "[hidden]",
            "correlation_id": CorrelationContext.get(),
        }

        if config.log_parameters and parameters:
            extra_fields["parameters"] = self._format_parameters(parameters)

        if config.log_results_count and result_count is not None:
            extra_fields["result_count"] = result_count

        if duration_ms is not None:
            extra_fields["duration_ms"] = duration_ms

        if error:
            extra_fields["error"] = str(error)
            extra_fields["error_type"] = type(error).__name__
            logger.error("Query execution failed", extra=extra_fields)
        else:
            level = logging.WARNING if duration_ms and duration_ms > config.slow_query_threshold_ms else logging.INFO
            logger.log(level, "Query executed successfully", extra=extra_fields)

    def log_connection_event(
        self, event: str, connection_info: "Optional[dict[str, Any]]" = None, error: "Optional[Exception]" = None
    ) -> None:
        """Log connection lifecycle events.

        Args:
            event: Event name (created, closed, error)
            connection_info: Connection details
            error: Exception if connection failed
        """
        if not self._instrumentation_config:
            return

        if not self._instrumentation_config.log_connection_events:
            return

        extra_fields = {"event": event, "correlation_id": CorrelationContext.get()}

        if connection_info:
            extra_fields.update(connection_info)

        if error:
            extra_fields["error"] = str(error)
            extra_fields["error_type"] = type(error).__name__
            logger.error("Connection event: %s", event, extra=extra_fields)
        else:
            logger.info("Connection event: %s", event, extra=extra_fields)

        # Update metrics
        if self._connection_gauge and event == "created":
            self._connection_gauge.inc()
        elif self._connection_gauge and event == "closed":
            self._connection_gauge.dec()

    def log_transaction_event(
        self, event: str, transaction_info: "Optional[dict[str, Any]]" = None, error: "Optional[Exception]" = None
    ) -> None:
        """Log transaction lifecycle events.

        Args:
            event: Event name (begin, commit, rollback)
            transaction_info: Transaction details
            error: Exception if transaction failed
        """
        if not self._instrumentation_config:
            return

        if not self._instrumentation_config.log_transaction_events:
            return

        extra_fields = {"event": event, "correlation_id": CorrelationContext.get()}

        if transaction_info:
            extra_fields.update(transaction_info)

        if error:
            extra_fields["error"] = str(error)
            extra_fields["error_type"] = type(error).__name__
            logger.error("Transaction event: %s", event, extra=extra_fields)
        else:
            logger.info("Transaction event: %s", event, extra=extra_fields)


class AsyncInstrumentationMixin(BaseInstrumentationMixin):
    """Asynchronous instrumentation mixin for database drivers."""

    async def log_query_execution(
        self,
        query: str,
        parameters: Any = None,
        result_count: "Optional[int]" = None,
        duration_ms: "Optional[float]" = None,
        error: "Optional[Exception]" = None,
    ) -> None:
        """Log query execution details asynchronously.

        Args:
            query: The SQL query
            parameters: Query parameters
            result_count: Number of rows returned/affected
            duration_ms: Query execution time in milliseconds
            error: Exception if query failed
        """
        # Delegate to sync version - logging is already async-safe
        SyncInstrumentationMixin.log_query_execution(
            cast("SyncInstrumentationMixin", self), query, parameters, result_count, duration_ms, error
        )

    async def log_connection_event(
        self, event: str, connection_info: "Optional[dict[str, Any]]" = None, error: "Optional[Exception]" = None
    ) -> None:
        """Log connection lifecycle events asynchronously.

        Args:
            event: Event name (created, closed, error)
            connection_info: Connection details
            error: Exception if connection failed
        """
        # Delegate to sync version - logging is already async-safe
        SyncInstrumentationMixin.log_connection_event(
            cast("SyncInstrumentationMixin", self), event, connection_info, error
        )

    async def log_transaction_event(
        self, event: str, transaction_info: "Optional[dict[str, Any]]" = None, error: "Optional[Exception]" = None
    ) -> None:
        """Log transaction lifecycle events asynchronously.

        Args:
            event: Event name (begin, commit, rollback)
            transaction_info: Transaction details
            error: Exception if transaction failed
        """
        # Delegate to sync version - logging is already async-safe
        SyncInstrumentationMixin.log_transaction_event(
            cast("SyncInstrumentationMixin", self), event, transaction_info, error
        )
