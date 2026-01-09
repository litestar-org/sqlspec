"""Unit tests for default statement logging."""

import logging

from sqlspec import create_event, default_statement_observer


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
