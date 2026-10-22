"""Unit tests for StatementEvent and ObservabilityRuntime profile and script ordinal fields."""

import logging

import pytest

from sqlspec.observability import LoggingConfig, ObservabilityConfig, ObservabilityRuntime, StatementEvent
from sqlspec.observability._observer import SQL_LOGGER_NAME, create_statement_observer


def test_statement_event_and_runtime_support_profile_and_script_ordinals(caplog: pytest.LogCaptureFixture) -> None:
    """StatementEvent and ObservabilityRuntime preserve profile, script_ordinal, and script_statement_count."""
    captured: list[StatementEvent] = []
    config = ObservabilityConfig(statement_observers=(captured.append,))
    runtime = ObservabilityRuntime(config=config, bind_key="duck", config_name="DuckDBConfig")
    profile_payload = {"cpu_time": 0.01, "children": [{"operator_type": "SEQ_SCAN"}]}

    direct_event = runtime.create_statement_event(
        sql="SELECT 1",
        parameters=(),
        driver="DuckDBDriver",
        operation="SELECT",
        execution_mode=None,
        is_many=False,
        is_script=True,
        rows_affected=1,
        duration_s=0.002,
        storage_backend=None,
        profile=profile_payload,
        script_ordinal=0,
        script_statement_count=3,
    )
    assert direct_event.profile == profile_payload
    assert direct_event.script_ordinal == 0
    assert direct_event.script_statement_index == 0
    assert direct_event.script_statement_count == 3

    runtime.emit_statement_event(
        sql="SELECT 1",
        parameters=(),
        driver="DuckDBDriver",
        operation="SELECT",
        execution_mode=None,
        is_many=False,
        is_script=True,
        rows_affected=1,
        duration_s=0.002,
        storage_backend=None,
        profile=profile_payload,
        script_ordinal=0,
        script_statement_count=3,
    )
    assert len(captured) == 1
    assert captured[0].profile == profile_payload
    assert captured[0].script_ordinal == 0
    assert captured[0].script_statement_count == 3
    as_dict = captured[0].as_dict()
    assert as_dict["profile"] == profile_payload
    assert as_dict["script_ordinal"] == 0
    assert as_dict["script_statement_count"] == 3

    observer = create_statement_observer(LoggingConfig())
    with caplog.at_level(logging.INFO, logger=SQL_LOGGER_NAME):
        observer(captured[0])
    record = caplog.records[-1]
    assert getattr(record, "db.statement.profile") == profile_payload
    assert getattr(record, "sqlspec.script.ordinal") == 0
    assert getattr(record, "sqlspec.script.statement_count") == 3
