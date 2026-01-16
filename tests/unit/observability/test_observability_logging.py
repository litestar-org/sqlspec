"""Unit tests for observability logging helpers."""

import logging

import pytest

from sqlspec import create_event, default_statement_observer
from sqlspec.observability import LoggingConfig, OTelConsoleFormatter, OTelJSONFormatter
from sqlspec.observability._observer import SQL_LOGGER_NAME


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
    caplog.set_level(logging.INFO, logger="sqlspec.sql")

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
    assert record.getMessage() == "SELECT"
    assert record.__dict__["db.statement"] == "SELECT 1"
    assert record.sql_truncated is False
    assert record.sql_length == len("SELECT 1")
    assert record.parameters_type == "dict"
    assert record.parameters_size == 1
    assert "parameters" not in record.__dict__


def test_default_statement_observer_debug_includes_parameters_and_truncates(caplog) -> None:
    caplog.set_level(logging.DEBUG, logger="sqlspec.sql")

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
    assert record.getMessage() == "SELECT"
    assert record.sql_truncated is True
    assert len(record.__dict__["db.statement"]) == 2000
    assert record.sql_length == len(long_sql)
    assert record.parameters_truncated is True
    assert isinstance(record.parameters, list)
    assert len(record.parameters) == 100


def test_sql_logger_name_constant() -> None:
    """Test that SQL_LOGGER_NAME constant is correctly defined."""
    assert SQL_LOGGER_NAME == "sqlspec.sql"


def test_sql_logs_use_dedicated_logger(caplog) -> None:
    """Test that SQL execution logs use the dedicated sqlspec.sql logger."""
    caplog.set_level(logging.INFO, logger="sqlspec.sql")

    event = create_event(
        sql="INSERT INTO users (name) VALUES (?)",
        parameters=["Alice"],
        driver="SqliteDriver",
        adapter="SqliteAdapter",
        bind_key="primary",
        operation="INSERT",
        execution_mode=None,
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.002,
        correlation_id="cid-3",
        storage_backend=None,
        started_at=0.0,
    )

    default_statement_observer(event)

    # Find the record from sqlspec.sql logger
    sql_records = [r for r in caplog.records if r.name == "sqlspec.sql"]
    assert len(sql_records) >= 1
    record = sql_records[-1]
    assert record.getMessage() == "INSERT"


def test_sql_log_message_is_operation_type(caplog) -> None:
    """Test that SQL log message is the operation type, not 'db.query'."""
    caplog.set_level(logging.INFO, logger="sqlspec.sql")

    operations = ["SELECT", "INSERT", "UPDATE", "DELETE", "DDL", "COPY"]

    for operation in operations:
        caplog.clear()
        event = create_event(
            sql=f"-- {operation} statement",
            parameters=None,
            driver="TestDriver",
            adapter="TestAdapter",
            bind_key=None,
            operation=operation,
            execution_mode=None,
            is_many=False,
            is_script=False,
            rows_affected=0,
            duration_s=0.001,
            correlation_id=None,
            storage_backend=None,
            started_at=0.0,
        )

        default_statement_observer(event)

        record = caplog.records[-1]
        assert record.getMessage() == operation, f"Expected message '{operation}' but got '{record.getMessage()}'"


def test_sql_log_driver_in_extra_fields(caplog) -> None:
    """Test that driver is in structured extra fields, not message."""
    caplog.set_level(logging.INFO, logger="sqlspec.sql")

    event = create_event(
        sql="SELECT 1",
        parameters=None,
        driver="AsyncpgDriver",
        adapter="AsyncpgAdapter",
        bind_key="primary",
        operation="SELECT",
        execution_mode=None,
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.001,
        correlation_id=None,
        storage_backend=None,
        started_at=0.0,
    )

    default_statement_observer(event)

    record = caplog.records[-1]
    # Driver should not be in the message
    assert "AsyncpgDriver" not in record.getMessage()
    # Driver should be in extra fields
    assert record.__dict__["sqlspec.driver"] == "AsyncpgDriver"


def test_sql_log_bind_key_in_extra_fields(caplog) -> None:
    """Test that bind_key is in structured extra fields for multi-database correlation."""
    caplog.set_level(logging.INFO, logger="sqlspec.sql")

    event = create_event(
        sql="SELECT 1",
        parameters=None,
        driver="PsycopgDriver",
        adapter="PsycopgAdapter",
        bind_key="analytics_db",
        operation="SELECT",
        execution_mode=None,
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.001,
        correlation_id=None,
        storage_backend=None,
        started_at=0.0,
    )

    default_statement_observer(event)

    record = caplog.records[-1]
    assert record.__dict__["sqlspec.bind_key"] == "analytics_db"


def test_sql_logger_independent_configuration(caplog) -> None:
    """Test that sqlspec.sql can be configured independently from sqlspec root."""
    # Set sqlspec root to WARNING, but sqlspec.sql to INFO
    logging.getLogger("sqlspec").setLevel(logging.WARNING)
    logging.getLogger("sqlspec.sql").setLevel(logging.INFO)

    caplog.set_level(logging.INFO, logger="sqlspec.sql")

    event = create_event(
        sql="SELECT 1",
        parameters=None,
        driver="TestDriver",
        adapter="TestAdapter",
        bind_key=None,
        operation="SELECT",
        execution_mode=None,
        is_many=False,
        is_script=False,
        rows_affected=1,
        duration_s=0.001,
        correlation_id=None,
        storage_backend=None,
        started_at=0.0,
    )

    default_statement_observer(event)

    # Should still receive the log since sqlspec.sql is at INFO
    sql_records = [r for r in caplog.records if r.name == "sqlspec.sql"]
    assert len(sql_records) >= 1
