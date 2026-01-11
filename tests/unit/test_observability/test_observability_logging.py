"""Unit tests for observability logging helpers."""

import logging

import pytest

from sqlspec import create_event, default_statement_observer
from sqlspec.observability import LoggingConfig, OTelConsoleFormatter, OTelJSONFormatter


def test_logging_config_defaults() -> None:
    config = LoggingConfig()
    assert config.include_sql_hash is True
    assert config.sql_truncation_length == 2000
    assert config.parameter_truncation_count == 100
    assert config.include_trace_context is True


def test_logging_config_custom_values() -> None:
    config = LoggingConfig(
        include_sql_hash=False, sql_truncation_length=512, parameter_truncation_count=25, include_trace_context=False
    )
    assert config.include_sql_hash is False
    assert config.sql_truncation_length == 512
    assert config.parameter_truncation_count == 25
    assert config.include_trace_context is False


def test_logging_config_copy_and_equality() -> None:
    config = LoggingConfig(
        include_sql_hash=False, sql_truncation_length=128, parameter_truncation_count=10, include_trace_context=False
    )
    clone = config.copy()
    assert clone == config
    assert clone is not config


def test_logging_config_unhashable() -> None:
    config = LoggingConfig()
    with pytest.raises(TypeError):
        hash(config)


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
    record.__dict__.update({
        "db.system": "sqlite",
        "db.operation": "SELECT",
        "trace_id": "trace",
        "span_id": "span",
        "correlation_id": "cid",
        "duration_ms": 12.5,
        "db.statement": "SELECT 1",
    })
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
    assert '"db.system":"sqlite"' in output
    assert '"db.operation":"SELECT"' in output


def test_default_statement_observer_info_excludes_parameters(caplog) -> None:
    caplog.set_level(logging.INFO, logger="sqlspec.observability")

    event = create_event(
        sql="SELECT 1",
        parameters={"a": 1},
        driver="DummyDriver",
        adapter="DummyAdapter",
        bind_key=None,
        operation="SELECT",
        execution_mode=None,
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.001,
        correlation_id="cid-1",
        storage_backend=None,
        started_at=0.0,
    )

    default_statement_observer(event)

    record = caplog.records[-1]
    assert record.getMessage() == "db.query"
    assert record.__dict__["db.statement"] == "SELECT 1"
    assert record.sql_truncated is False
    assert record.sql_length == len("SELECT 1")
    assert record.parameters_type == "dict"
    assert record.parameters_size == 1
    assert "parameters" not in record.__dict__


def test_default_statement_observer_debug_includes_parameters_and_truncates(caplog) -> None:
    caplog.set_level(logging.DEBUG, logger="sqlspec.observability")

    long_sql = "SELECT " + ("x" * 5000)
    parameters = list(range(101))
    event = create_event(
        sql=long_sql,
        parameters=parameters,
        driver="DummyDriver",
        adapter="DummyAdapter",
        bind_key=None,
        operation="SELECT",
        execution_mode=None,
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.001,
        correlation_id="cid-2",
        storage_backend=None,
        started_at=0.0,
    )

    default_statement_observer(event)

    record = caplog.records[-1]
    assert record.sql_truncated is True
    assert len(record.__dict__["db.statement"]) == 2000
    assert record.sql_length == len(long_sql)
    assert record.parameters_truncated is True
    assert isinstance(record.parameters, list)
    assert len(record.parameters) == 100
