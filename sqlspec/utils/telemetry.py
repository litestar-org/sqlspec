# ruff: noqa: SLF001
import time
from collections.abc import Generator
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any

from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

__all__ = ("instrument_operation", "instrument_operation_async")

logger = get_logger("telemetry")


@contextmanager
def instrument_operation(
    driver_obj: Any, operation_name: str, operation_type: str = "database", **custom_tags: Any
) -> Generator[None, None, None]:
    """Context manager for instrumenting synchronous operations.

    Args:
        driver_obj: The driver object that has instrumentation capabilities.
        operation_name: Name of the operation.
        operation_type: Type of operation, defaults to "database".
        custom_tags: Additional tags for the span.

    Yields:
        None: Context for the instrumented operation.
    """
    if not hasattr(driver_obj, "instrumentation_config"):
        yield
        return

    start_time = time.monotonic()

    # Merge custom tags
    final_custom_tags = getattr(driver_obj.instrumentation_config, "custom_tags", {}).copy()
    final_custom_tags.update(custom_tags)

    # Get correlation ID
    correlation_id = CorrelationContext.get()

    if driver_obj.instrumentation_config.log_queries:
        logger.info(
            "Starting %s operation",
            operation_name,
            extra={"operation_type": operation_type, "correlation_id": correlation_id},
        )

    span = None
    if hasattr(driver_obj, "_tracer") and driver_obj._tracer:
        span = driver_obj._tracer.start_span(operation_name)
        span.set_attribute("operation.type", operation_type)
        span.set_attribute("db.system", getattr(driver_obj, "dialect", "unknown"))
        span.set_attribute("service.name", driver_obj.instrumentation_config.service_name)

        # Add correlation ID to span
        if correlation_id:
            span.set_attribute("correlation_id", correlation_id)

        for key, value in final_custom_tags.items():
            span.set_attribute(key, value)

    try:
        yield
        latency = time.monotonic() - start_time

        if driver_obj.instrumentation_config.log_runtime:
            logger.info(
                "Completed %s in %.3fms",
                operation_name,
                latency * 1000,
                extra={
                    "operation_type": operation_type,
                    "latency_ms": latency * 1000,
                    "status": "success",
                    "correlation_id": correlation_id,
                },
            )

        # Update metrics
        if hasattr(driver_obj, "_query_counter") and driver_obj._query_counter:
            driver_obj._query_counter.labels(
                operation=operation_name,
                status="success",
                db_system=getattr(driver_obj, "dialect", "unknown"),
                **final_custom_tags,
            ).inc()

        if hasattr(driver_obj, "_latency_histogram") and driver_obj._latency_histogram:
            driver_obj._latency_histogram.labels(
                operation=operation_name, db_system=getattr(driver_obj, "dialect", "unknown"), **final_custom_tags
            ).observe(latency)

        if span:
            span.set_attribute("duration_ms", latency * 1000)

    except Exception as e:
        latency = time.monotonic() - start_time

        if driver_obj.instrumentation_config.log_queries:
            logger.exception(
                "Error in %s after %.3fms",
                operation_name,
                latency * 1000,
                extra={
                    "operation_type": operation_type,
                    "latency_ms": latency * 1000,
                    "status": "error",
                    "error_type": type(e).__name__,
                    "correlation_id": correlation_id,
                },
            )

        if span:
            span.record_exception(e)

        if hasattr(driver_obj, "_error_counter") and driver_obj._error_counter:
            driver_obj._error_counter.labels(
                operation=operation_name,
                error_type=type(e).__name__,
                db_system=getattr(driver_obj, "dialect", "unknown"),
                **final_custom_tags,
            ).inc()
        raise
    finally:
        if span:
            span.end()


@asynccontextmanager
async def instrument_operation_async(
    driver_obj: Any, operation_name: str, operation_type: str = "database", **custom_tags: Any
) -> "AsyncGenerator[None, None]":
    """Context manager for instrumenting asynchronous operations.

    Args:
        driver_obj: The driver object that has instrumentation capabilities.
        operation_name: Name of the operation.
        operation_type: Type of operation, defaults to "database".
        custom_tags: Additional tags for the span.

    Yields:
        None: Context for the instrumented operation.
    """
    if not hasattr(driver_obj, "instrumentation_config"):
        yield
        return

    start_time = time.monotonic()

    # Merge custom tags
    final_custom_tags = getattr(driver_obj.instrumentation_config, "custom_tags", {}).copy()
    final_custom_tags.update(custom_tags)

    # Get correlation ID
    correlation_id = CorrelationContext.get()

    if driver_obj.instrumentation_config.log_queries:
        logger.info(
            "Starting %s operation",
            operation_name,
            extra={"operation_type": operation_type, "correlation_id": correlation_id},
        )

    span = None
    if hasattr(driver_obj, "_tracer") and driver_obj._tracer:
        span = driver_obj._tracer.start_span(operation_name)
        span.set_attribute("operation.type", operation_type)
        span.set_attribute("db.system", getattr(driver_obj, "dialect", "unknown"))
        span.set_attribute("service.name", driver_obj.instrumentation_config.service_name)

        # Add correlation ID to span
        if correlation_id:
            span.set_attribute("correlation_id", correlation_id)

        for key, value in final_custom_tags.items():
            span.set_attribute(key, value)

    try:
        yield
        latency = time.monotonic() - start_time

        if driver_obj.instrumentation_config.log_runtime:
            logger.info(
                "Completed %s in %.3fms",
                operation_name,
                latency * 1000,
                extra={
                    "operation_type": operation_type,
                    "latency_ms": latency * 1000,
                    "status": "success",
                    "correlation_id": correlation_id,
                },
            )

        # Update metrics
        if hasattr(driver_obj, "_query_counter") and driver_obj._query_counter:
            driver_obj._query_counter.labels(
                operation=operation_name,
                status="success",
                db_system=getattr(driver_obj, "dialect", "unknown"),
                **final_custom_tags,
            ).inc()

        if hasattr(driver_obj, "_latency_histogram") and driver_obj._latency_histogram:
            driver_obj._latency_histogram.labels(
                operation=operation_name, db_system=getattr(driver_obj, "dialect", "unknown"), **final_custom_tags
            ).observe(latency)

        if span:
            span.set_attribute("duration_ms", latency * 1000)

    except Exception as e:
        latency = time.monotonic() - start_time

        if driver_obj.instrumentation_config.log_queries:
            logger.exception(
                "Error in %s after %.3fms",
                operation_name,
                latency * 1000,
                extra={
                    "operation_type": operation_type,
                    "latency_ms": latency * 1000,
                    "status": "error",
                    "error_type": type(e).__name__,
                    "correlation_id": correlation_id,
                },
            )

        if span:
            span.record_exception(e)

        if hasattr(driver_obj, "_error_counter") and driver_obj._error_counter:
            driver_obj._error_counter.labels(
                operation=operation_name,
                error_type=type(e).__name__,
                db_system=getattr(driver_obj, "dialect", "unknown"),
                **final_custom_tags,
            ).inc()
        raise
    finally:
        if span:
            span.end()
