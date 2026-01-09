"""Unit tests for observability formatters."""

import logging

from sqlspec.observability._formatting import OTelConsoleFormatter, OTelJSONFormatter


def test_otel_console_formatter_orders_fields() -> None:
    formatter = OTelConsoleFormatter(datefmt="%Y-%m-%d")
    record = logging.LogRecord(
        name="sqlspec.observability",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="db.query",
        args=(),
        exc_info=None,
    )
    record.__dict__.update(
        {
            "db.system": "sqlite",
            "db.operation": "SELECT",
            "trace_id": "trace",
            "span_id": "span",
            "correlation_id": "cid",
            "duration_ms": 12.5,
            "db.statement": "SELECT 1",
        }
    )
    output = formatter.format(record)
    assert output.index("db.system=sqlite") < output.index("db.operation=SELECT")
    assert output.index("db.operation=SELECT") < output.index("trace_id=trace")
    assert output.index("trace_id=trace") < output.index("span_id=span")
    assert output.index("span_id=span") < output.index("correlation_id=cid")
    assert output.index("correlation_id=cid") < output.index("duration_ms=12.5")
    assert output.index("duration_ms=12.5") < output.index("db.statement=SELECT 1")


def test_otel_console_formatter_bool_values() -> None:
    formatter = OTelConsoleFormatter()
    record = logging.LogRecord(
        name="sqlspec.observability",
        level=logging.INFO,
        pathname=__file__,
        lineno=12,
        msg="db.query",
        args=(),
        exc_info=None,
    )
    record.__dict__["is_many"] = True
    output = formatter.format(record)
    assert "is_many=true" in output


def test_otel_json_formatter_includes_fields() -> None:
    formatter = OTelJSONFormatter()
    record = logging.LogRecord(
        name="sqlspec.observability",
        level=logging.INFO,
        pathname=__file__,
        lineno=14,
        msg="db.query",
        args=(),
        exc_info=None,
    )
    record.module = "test_module"
    record.funcName = "test_func"
    record.__dict__.update({"db.system": "sqlite", "db.operation": "SELECT"})
    output = formatter.format(record)
    assert "\"db.system\":\"sqlite\"" in output
    assert "\"db.operation\":\"SELECT\"" in output
