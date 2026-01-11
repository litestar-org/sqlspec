"""Integration tests for observability logging format."""

import io
import logging
import tempfile

import pytest
from pytest_databases.docker.postgres import PostgresService

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.observability import LoggingConfig, ObservabilityConfig, OTelConsoleFormatter
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger

pytestmark = pytest.mark.xdist_group("observability")


def _capture_observability_output() -> "tuple[io.StringIO, logging.Handler, logging.Logger]":
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(OTelConsoleFormatter())
    logger = get_logger("sqlspec.observability")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)
    return stream, handler, logger


async def test_asyncpg_statement_logging_format(postgres_service: "PostgresService") -> None:
    stream, handler, logger = _capture_observability_output()
    config = AsyncpgConfig(
        connection_config={
            "dsn": f"postgres://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}",
            "min_size": 1,
            "max_size": 2,
        },
        observability_config=ObservabilityConfig(print_sql=True, logging=LoggingConfig(include_trace_context=False)),
    )
    try:
        async with config.provide_session() as driver:
            await driver.execute("SELECT 1")
    finally:
        logger.removeHandler(handler)
        logger.propagate = True

    output = stream.getvalue().strip()
    assert "db.query" in output
    assert "db.system=postgresql" in output
    assert "db.operation=SELECT" in output
    assert "db.statement=SELECT 1" in output


def test_sqlite_statement_logging_format_with_correlation_id() -> None:
    stream, handler, logger = _capture_observability_output()
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        config = SqliteConfig(
            connection_config={"database": tmp.name},
            observability_config=ObservabilityConfig(
                print_sql=True, logging=LoggingConfig(include_trace_context=False)
            ),
        )
        try:
            with CorrelationContext.context("cid-logging"):
                with config.provide_session() as driver:
                    driver.execute("SELECT 1")
        finally:
            logger.removeHandler(handler)
            logger.propagate = True

    output = stream.getvalue().strip()
    assert "db.query" in output
    assert "db.system=sqlite" in output
    assert "db.operation=SELECT" in output
    assert "db.statement=SELECT 1" in output
    assert "correlation_id=cid-logging" in output
