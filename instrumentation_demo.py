#!/usr/bin/env python3
"""Comprehensive SQLSpec Instrumentation Demo

This script demonstrates the enhanced telemetry and logging capabilities
added to the entire SQLSpec stack, following OpenTelemetry best practices.

Features demonstrated:
- OpenTelemetry tracing with semantic conventions
- Prometheus metrics collection
- Structured logging with correlation
- Instrumentation at driver, config, and base levels
- Best practices for database observability

Prerequisites:
    pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-jaeger
    pip install prometheus-client
    pip install sqlspec[psycopg] (or your preferred adapter)

Usage:
    python instrumentation_demo.py
"""

import asyncio
import logging
import os
import time
from contextlib import contextmanager
from typing import Optional

# OpenTelemetry setup
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Prometheus setup
from prometheus_client import generate_latest, start_http_server

# SQLSpec imports
from sqlspec.base import SQLSpec
from sqlspec.config import InstrumentationConfig

__all__ = (
    "demo_async_operations",
    "demo_database_config",
    "demo_error_handling",
    "demo_performance_monitoring",
    "demo_sqlspec_integration",
    "demo_sync_operations",
    "main",
    "print_metrics_summary",
    "setup_opentelemetry",
    "setup_prometheus_metrics",
)


# Configure logging with structured format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(filename)s:%(lineno)d",
    handlers=[logging.StreamHandler(), logging.FileHandler("sqlspec_instrumentation.log")],
)

logger = logging.getLogger(__name__)


def setup_opentelemetry() -> None:
    """Set up OpenTelemetry with Jaeger exporter and proper resource attributes."""
    resource = Resource.create(
        {
            "service.name": "sqlspec-instrumentation-demo",
            "service.version": "1.0.0",
            "service.environment": "demo",
            "deployment.environment": "local",
        }
    )

    # Create tracer provider
    provider = TracerProvider(resource=resource)

    # Add Jaeger exporter
    jaeger_exporter = JaegerExporter(
        agent_host_name=os.getenv("JAEGER_AGENT_HOST", "localhost"),
        agent_port=int(os.getenv("JAEGER_AGENT_PORT", "6831")),
    )

    # Add batch processor for efficiency
    span_processor = BatchSpanProcessor(jaeger_exporter)
    provider.add_span_processor(span_processor)

    # Set the global tracer provider
    trace.set_tracer_provider(provider)

    logger.info("OpenTelemetry configured with Jaeger exporter")


def setup_prometheus_metrics() -> None:
    """Start Prometheus metrics server."""
    # Start Prometheus metrics server on port 8000
    start_http_server(8000)
    logger.info("Prometheus metrics server started on http://localhost:8000/metrics")


@contextmanager
def demo_database_config():
    """Create a demo database configuration with instrumentation enabled."""
    # This is a mock implementation - replace with actual database connection
    # For demo purposes, we'll simulate the configuration

    class MockConnection:
        def execute(self, sql: str, params: Optional[dict] = None):
            # Simulate database operation
            time.sleep(0.01)  # Simulate query latency
            return {"result": "success", "rows": 5}

        def close(self) -> None:
            pass

    class MockSyncConfig:
        def __init__(self) -> None:
            self.instrumentation = InstrumentationConfig(
                log_queries=True,
                log_runtime=True,
                log_parameters=True,
                log_results_count=True,
                log_pool_operations=True,
                enable_opentelemetry=True,
                enable_prometheus=True,
                service_name="sqlspec-demo",
                custom_tags={"environment": "demo", "version": "1.0.0"},
                slow_query_threshold_ms=100.0,
            )
            self.is_async = False
            self.support_connection_pooling = False

        def create_connection(self):
            return MockConnection()

        def provide_connection(self):
            return self.create_connection()

        def provide_session(self):
            # Would return a driver adapter in real implementation
            return self.create_connection()

    yield MockSyncConfig()


async def demo_async_operations() -> None:
    """Demonstrate async operations with instrumentation."""
    logger.info("=== Async Operations Demo ===")

    # Get a tracer for this demo
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("async_demo_session") as span:
        span.set_attribute("demo.type", "async")
        span.set_attribute("demo.operations", "multiple")

        # Simulate async database operations
        operations = ["select_users", "insert_user", "update_profile", "delete_session"]

        for i, operation in enumerate(operations):
            with tracer.start_as_current_span(f"async_{operation}") as op_span:
                op_span.set_attribute("db.operation", operation)
                op_span.set_attribute("db.system", "postgresql")
                op_span.set_attribute("operation.sequence", i + 1)

                # Simulate operation latency
                await asyncio.sleep(0.05)

                logger.info(
                    "Completed async operation: %s",
                    operation,
                    extra={
                        "operation_type": "database",
                        "operation_name": operation,
                        "sequence": i + 1,
                        "span_id": format(op_span.get_span_context().span_id, "016x"),
                        "trace_id": format(op_span.get_span_context().trace_id, "032x"),
                    },
                )


def demo_sync_operations() -> None:
    """Demonstrate sync operations with instrumentation."""
    logger.info("=== Sync Operations Demo ===")

    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("sync_demo_session") as span:
        span.set_attribute("demo.type", "sync")
        span.set_attribute("demo.operations", "crud")

        # Simulate CRUD operations
        operations = [
            ("CREATE", "CREATE TABLE demo_table (id INT, name TEXT)"),
            ("INSERT", "INSERT INTO demo_table VALUES (1, 'Demo User')"),
            ("SELECT", "SELECT * FROM demo_table WHERE id = 1"),
            ("UPDATE", "UPDATE demo_table SET name = 'Updated User' WHERE id = 1"),
            ("DELETE", "DELETE FROM demo_table WHERE id = 1"),
        ]

        for operation_type, sql in operations:
            with tracer.start_as_current_span(f"sync_{operation_type.lower()}") as op_span:
                op_span.set_attribute("db.operation", operation_type)
                op_span.set_attribute("db.statement", sql[:100])  # Truncate for demo
                op_span.set_attribute("db.system", "postgresql")

                # Simulate operation
                time.sleep(0.02)

                # Simulate different outcomes
                if operation_type == "SELECT":
                    op_span.set_attribute("db.rows_returned", 1)
                elif operation_type in ["INSERT", "UPDATE", "DELETE"]:
                    op_span.set_attribute("db.rows_affected", 1)

                logger.info(
                    "Completed sync operation: %s",
                    operation_type,
                    extra={
                        "operation_type": "database",
                        "sql_operation": operation_type,
                        "sql_length": len(sql),
                        "span_id": format(op_span.get_span_context().span_id, "016x"),
                        "trace_id": format(op_span.get_span_context().trace_id, "032x"),
                    },
                )


def demo_sqlspec_integration() -> None:
    """Demonstrate SQLSpec integration with instrumentation."""
    logger.info("=== SQLSpec Integration Demo ===")

    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("sqlspec_integration") as span:
        span.set_attribute("demo.component", "sqlspec")
        span.set_attribute("demo.features", "config_management")

        # Create SQLSpec instance
        sql_spec = SQLSpec()

        # Add configuration with instrumentation
        with demo_database_config() as config:
            config_type = sql_spec.add_config(config)

            logger.info(
                "Added configuration to SQLSpec registry",
                extra={
                    "config_type": config_type.__name__,
                    "instrumentation_enabled": config.instrumentation.enable_opentelemetry,
                    "prometheus_enabled": config.instrumentation.enable_prometheus,
                },
            )

            # Demonstrate connection retrieval with tracing
            with tracer.start_as_current_span("get_connection") as conn_span:
                conn_span.set_attribute("config.type", config_type.__name__)
                connection = sql_spec.get_connection(config_type)

                logger.info(
                    "Retrieved connection from SQLSpec",
                    extra={
                        "config_type": config_type.__name__,
                        "connection_type": type(connection).__name__,
                    },
                )

            # Demonstrate session management
            with tracer.start_as_current_span("session_management") as session_span:
                session_span.set_attribute("operation", "session_lifecycle")

                # In a real implementation, this would return a driver adapter
                session = sql_spec.get_session(config_type)

                logger.info(
                    "Created session from SQLSpec",
                    extra={
                        "config_type": config_type.__name__,
                        "session_type": type(session).__name__,
                    },
                )


def demo_error_handling() -> None:
    """Demonstrate error handling with instrumentation."""
    logger.info("=== Error Handling Demo ===")

    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("error_handling_demo") as span:
        span.set_attribute("demo.type", "error_scenarios")

        # Simulate various error scenarios
        error_scenarios = [
            ("connection_timeout", "Connection timeout error"),
            ("sql_syntax_error", "SQL syntax error"),
            ("permission_denied", "Permission denied error"),
            ("constraint_violation", "Constraint violation error"),
        ]

        for error_type, error_message in error_scenarios:
            with tracer.start_as_current_span(f"error_{error_type}") as error_span:
                error_span.set_attribute("error.type", error_type)
                error_span.set_attribute("error.expected", True)

                try:
                    # Simulate error condition
                    if error_type == "connection_timeout":
                        raise ConnectionError(error_message)
                    if error_type == "sql_syntax_error":
                        raise SyntaxError(error_message)
                    if error_type == "permission_denied":
                        raise PermissionError(error_message)
                    raise ValueError(error_message)

                except Exception as e:
                    # Record the exception in the span
                    error_span.record_exception(e)
                    error_span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))

                    logger.error(
                        "Simulated error: %s",
                        error_message,
                        extra={
                            "error_type": error_type,
                            "error_class": type(e).__name__,
                            "span_id": format(error_span.get_span_context().span_id, "016x"),
                            "trace_id": format(error_span.get_span_context().trace_id, "032x"),
                        },
                        exc_info=True,
                    )


def demo_performance_monitoring() -> None:
    """Demonstrate performance monitoring capabilities."""
    logger.info("=== Performance Monitoring Demo ===")

    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("performance_monitoring") as span:
        span.set_attribute("demo.type", "performance")

        # Simulate queries with different performance characteristics
        query_scenarios = [
            ("fast_query", 0.01, "SELECT id FROM users WHERE id = 1"),
            ("medium_query", 0.05, "SELECT * FROM users JOIN profiles ON users.id = profiles.user_id"),
            ("slow_query", 0.15, "SELECT * FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id"),
            ("very_slow_query", 0.25, "SELECT * FROM large_table WHERE unindexed_column LIKE '%pattern%'"),
        ]

        for query_type, duration, sql in query_scenarios:
            start_time = time.time()

            with tracer.start_as_current_span(f"query_{query_type}") as query_span:
                query_span.set_attribute("db.statement", sql[:100])
                query_span.set_attribute("db.operation", "SELECT")
                query_span.set_attribute("performance.expected_duration", duration)

                # Simulate query execution
                time.sleep(duration)

                actual_duration = time.time() - start_time
                query_span.set_attribute("performance.actual_duration", actual_duration)

                # Determine if query is slow
                is_slow = actual_duration > 0.1  # 100ms threshold
                query_span.set_attribute("performance.is_slow", is_slow)

                log_level = logging.WARNING if is_slow else logging.INFO
                logger.log(
                    log_level,
                    "Query performance: %s (%.3fs)",
                    query_type,
                    actual_duration,
                    extra={
                        "query_type": query_type,
                        "duration_ms": actual_duration * 1000,
                        "is_slow": is_slow,
                        "sql_preview": sql[:50],
                        "span_id": format(query_span.get_span_context().span_id, "016x"),
                    },
                )


def print_metrics_summary() -> None:
    """Print a summary of collected metrics."""
    logger.info("=== Metrics Summary ===")

    try:
        # Generate Prometheus metrics output
        metrics_output = generate_latest().decode("utf-8")

        # Filter for SQLSpec-related metrics
        sqlspec_metrics = [line for line in metrics_output.split("\n") if "sqlspec" in line.lower() or "db_" in line]

        if sqlspec_metrics:
            logger.info("SQLSpec Prometheus Metrics:")
            for metric in sqlspec_metrics[:10]:  # Show first 10 lines
                if metric.strip() and not metric.startswith("#"):
                    logger.info("  %s", metric)
        else:
            logger.info("No SQLSpec-specific metrics found (this is expected in demo mode)")

    except Exception as e:
        logger.warning("Could not generate metrics summary: %s", e)


async def main() -> None:
    """Main demo function showcasing all instrumentation features."""

    # Set up observability infrastructure
    setup_opentelemetry()
    setup_prometheus_metrics()

    # Get the main tracer
    tracer = trace.get_tracer(__name__)

    # Main demo span
    with tracer.start_as_current_span("sqlspec_instrumentation_demo") as main_span:
        main_span.set_attribute("demo.version", "1.0.0")
        main_span.set_attribute("demo.environment", "local")
        main_span.set_attribute("demo.features", "comprehensive")

        logger.info(
            "Starting SQLSpec instrumentation demo",
            extra={
                "trace_id": format(main_span.get_span_context().trace_id, "032x"),
                "span_id": format(main_span.get_span_context().span_id, "016x"),
            },
        )

        try:
            # Run all demo scenarios
            demo_sqlspec_integration()
            demo_sync_operations()
            await demo_async_operations()
            demo_performance_monitoring()
            demo_error_handling()

            # Print summary
            print_metrics_summary()

            logger.info("Demo completed successfully")

        except Exception as e:
            main_span.record_exception(e)
            main_span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            logger.error("Demo failed: %s", e, exc_info=True)
            raise


if __name__ == "__main__":
    # Run the demo
    asyncio.run(main())
