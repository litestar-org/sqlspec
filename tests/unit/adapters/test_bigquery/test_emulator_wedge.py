"""Tests for BigQuery emulator wedge handling."""

from typing import cast

import pytest
import requests.exceptions
from _pytest.reports import TestReport

from sqlspec.exceptions import OperationalError
from tests.integration.adapters.bigquery.bigquery import conftest as bigquery_conftest


def _failed_bigquery_report() -> TestReport:
    return TestReport(
        nodeid="tests/integration/adapters/bigquery/bigquery/test_arrow.py::test_select_to_arrow_empty_result",
        location=(
            "tests/integration/adapters/bigquery/bigquery/test_arrow.py",
            148,
            "test_select_to_arrow_empty_result",
        ),
        keywords={},
        outcome="failed",
        longrepr="failure",
        when="call",
    )


class _FakeExceptionInfo:
    def __init__(self, value: BaseException) -> None:
        self.value = value


class _FakeCall:
    def __init__(self, value: BaseException) -> None:
        self.excinfo = _FakeExceptionInfo(value)


class _FakeOutcome:
    def __init__(self, report: TestReport) -> None:
        self._report = report

    def get_result(self) -> TestReport:
        return self._report


def test_first_bigquery_emulator_wedge_report_is_marked_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    """The first wedged emulator timeout should not fail the whole integration shard."""
    monkeypatch.setattr(bigquery_conftest, "_emulator_wedge_reason", None)
    report = _failed_bigquery_report()
    error = requests.exceptions.ReadTimeout("read timed out")

    converted = bigquery_conftest._mark_report_skipped_for_emulator_wedge(report, error)

    assert converted is True
    assert report.outcome == "skipped"
    assert isinstance(report.longrepr, tuple)
    assert "BigQuery emulator wedged earlier in this session" in report.longrepr[2]
    assert bigquery_conftest._emulator_wedge_reason is not None
    assert "ReadTimeout" in bigquery_conftest._emulator_wedge_reason


def test_bigquery_closed_connection_error_is_marked_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    """The emulator's closed-connection HTTP 500 should skip the remaining BigQuery shard."""
    monkeypatch.setattr(bigquery_conftest, "_emulator_wedge_reason", None)
    report = _failed_bigquery_report()
    error = OperationalError(
        "BigQuery operational error [HTTP 500]: 500 POST "
        "http://127.0.0.1:32773/bigquery/v2/projects/emulator-test-project/jobs?prettyPrint=false: "
        "sql: connection is already closed"
    )

    converted = bigquery_conftest._mark_report_skipped_for_emulator_wedge(report, error)

    assert converted is True
    assert report.outcome == "skipped"
    assert bigquery_conftest._emulator_wedge_reason is not None
    assert "connection is already closed" in bigquery_conftest._emulator_wedge_reason


def test_repeated_bigquery_emulator_wedge_reports_are_marked_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Repeated cached setup failures should also be reported as skips."""
    monkeypatch.setattr(bigquery_conftest, "_emulator_wedge_reason", "ReadTimeout: read timed out")
    report = _failed_bigquery_report()
    error = OperationalError(
        "BigQuery operational error [HTTP 500]: 500 POST "
        "http://127.0.0.1:32773/bigquery/v2/projects/emulator-test-project/jobs?prettyPrint=false: "
        "sql: connection is already closed"
    )

    hook = bigquery_conftest.pytest_runtest_makereport(cast("pytest.CallInfo[None]", _FakeCall(error)))
    next(hook)
    with pytest.raises(StopIteration):
        hook.send(cast("pytest.CollectReport", _FakeOutcome(report)))

    assert report.outcome == "skipped"
    assert isinstance(report.longrepr, tuple)
    assert "connection is already closed" in report.longrepr[2]
