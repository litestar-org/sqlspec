"""Example of using SQLSpec's enhanced instrumentation features.

This example demonstrates:
- How to configure structured logging with correlation IDs
- OpenTelemetry tracing
- Prometheus metrics
- Performance monitoring

Note: Users need to configure their own logging handlers. SQLSpec provides
StructuredFormatter for JSON-formatted logs if desired.
"""

from contextlib import asynccontextmanager
from typing import Any

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.config import InstrumentationConfig
from sqlspec.utils.correlation import correlation_context

__all__ = ("lifespan_with_instrumentation", "main")


def main() -> None:
    """Example of comprehensive instrumentation setup."""
    # Note: Users should configure their own logging setup.
    # SQLSpec provides StructuredFormatter for JSON logging if desired:
    # import logging
    # from sqlspec.utils.logging import StructuredFormatter
    #
    # handler = logging.StreamHandler()
    # handler.setFormatter(StructuredFormatter())
    # logging.getLogger("sqlspec").addHandler(handler)
    # logging.getLogger("sqlspec").setLevel(logging.INFO)

    # Example 1: SQLite with basic instrumentation
    print("=== SQLite Example ===")

    with correlation_context() as correlation_id:
        print(f"Starting operation with correlation ID: {correlation_id}")

        config = SqliteConfig(
            database=":memory:",
            instrumentation=InstrumentationConfig(
                # Basic logging
                log_queries=True,
                log_runtime=True,
                log_results_count=True,
                slow_query_threshold_ms=100.0,
            ),
        )

        spec = SQLSpec()
        db = spec.add_config(config)

        with spec.provide_session(db) as session:
            # Create table
            session.execute(
                """CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL
                )"""
            )

            # Insert data with instrumentation
            session.execute("INSERT INTO users (name, email) VALUES (?, ?)", ["Alice", "alice@example.com"])

            # Query data (slow query simulation)
            import time

            time.sleep(0.2)  # Simulate slow query

            result = session.execute("SELECT * FROM users WHERE email = ?", ["alice@example.com"])
            print(f"Found {len(result.data)} users")

    # Example 2: Async database with pool monitoring
    print("\n=== Async PostgreSQL Example ===")

    AsyncpgConfig(
        pool_config={"min_size": 5, "max_size": 20},
        instrumentation=InstrumentationConfig(
            # Full telemetry
            enable_opentelemetry=True,
            enable_prometheus=True,
            # Structured logging
            structured_logging=True,
            # Storage and service logging
            log_storage_operations=True,
            log_service_operations=True,
            # Correlation tracking
            enable_correlation_ids=True,
            # Performance tuning
            slow_query_threshold_ms=500.0,
            slow_pool_operation_ms=1000.0,
            debug_sql_ast=False,
            debug_parameter_binding=False,
            # Custom fields
            custom_tags={"service": "user-api", "version": "1.0.0"},
            custom_fields={"datacenter": "us-east-1"},
        ),
    )

    # Example 3: Service layer with correlation
    print("\n=== Service Layer Example ===")

    from sqlspec.service.base import InstrumentedService

    class UserService(InstrumentedService):
        async def get_user_by_email(self, email: str) -> "Any":
            with self._instrument("get_user_by_email", email=email) as ctx:
                # Service operation with automatic instrumentation
                # SQL operations inherit correlation context
                result = await self.session.execute("SELECT * FROM users WHERE email = ?", [email])
                ctx["user_count"] = len(result.data)
                return result.data

    # Example 4: Query builder with instrumentation
    print("\n=== Query Builder Example ===")

    from sqlspec.base import sql

    with correlation_context("query-build-example"):
        query = sql.select("id", "name", "email").from_("users").where("email = ?", "alice@example.com").limit(1)

        # SQL generation is instrumented
        sql_obj = query
        print(f"Generated SQL: {sql_obj.to_sql()}")

    # Example 5: Storage operations with correlation
    print("\n=== Storage Example ===")

    from sqlspec.storage.registry import get_storage_backend

    with correlation_context("storage-example"):
        backend = get_storage_backend("file:///tmp/test.parquet")

        # Storage operations are instrumented
        data = b"test data"
        backend.write_bytes("test.txt", data)
        read_data = backend.read_bytes("test.txt")
        assert data == read_data

    # Example 6: Error tracking with correlation
    print("\n=== Error Tracking Example ===")

    try:
        with correlation_context("error-example"):
            # This will fail and be properly logged with correlation
            session.execute("SELECT * FROM nonexistent_table")
    except Exception as e:
        print(f"Error properly tracked with correlation: {e}")

    print("\n=== Instrumentation Summary ===")
    print("Structured logs are sent to stdout (configure as needed)")
    print("Metrics would be available at /metrics endpoint if Prometheus is configured")
    print("Traces sent to OpenTelemetry collector if configured")


@asynccontextmanager
async def lifespan_with_instrumentation(app: "Any") -> "Any":
    """Example lifespan handler for web frameworks."""
    # Initialize instrumentation on startup
    # Note: Configure your own logging as needed
    import logging

    from sqlspec.utils.logging import StructuredFormatter

    handler = logging.StreamHandler()
    handler.setFormatter(StructuredFormatter())
    sqlspec_logger = logging.getLogger("sqlspec")
    sqlspec_logger.addHandler(handler)
    sqlspec_logger.setLevel(logging.INFO)

    # Start Prometheus metrics server
    from prometheus_client import start_http_server

    start_http_server(8000)

    # Initialize OpenTelemetry
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer(__name__)

    otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317")
    span_processor = BatchSpanProcessor(otlp_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)

    # Auto-instrument SQLAlchemy operations
    SQLAlchemyInstrumentor().instrument()

    try:
        yield
    finally:
        # Cleanup on shutdown
        trace.get_tracer_provider().shutdown()


if __name__ == "__main__":
    main()
