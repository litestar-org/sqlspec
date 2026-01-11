"""Unit tests for sqlspec logging utilities."""

import io
import json
import logging

from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import StructuredFormatter, get_logger, log_with_context


def test_structured_formatter_includes_logging_extra_fields() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(StructuredFormatter())

    logger = get_logger("tests.logging.extra")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)

    try:
        with CorrelationContext.context("cid-123"):
            logger.info("hello", extra={"foo": "bar"})
    finally:
        logger.removeHandler(handler)

    payload = json.loads(stream.getvalue().strip())
    assert payload["message"] == "hello"
    assert payload["foo"] == "bar"
    assert payload["correlation_id"] == "cid-123"


def test_log_with_context_preserves_source_location_and_fields() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(StructuredFormatter())

    logger = get_logger("tests.logging.context")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)

    try:
        log_with_context(logger, logging.INFO, "event.test", driver="Dummy")
    finally:
        logger.removeHandler(handler)

    payload = json.loads(stream.getvalue().strip())
    assert payload["message"] == "event.test"
    assert payload["driver"] == "Dummy"
    assert payload["line"] != 0
