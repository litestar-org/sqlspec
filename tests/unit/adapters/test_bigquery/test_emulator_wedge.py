"""Tests for BigQuery emulator wedge handling."""

import pytest
import requests.exceptions
from _pytest.reports import TestReport

from tests.integration.adapters.bigquery import conftest as bigquery_conftest


def _failed_bigquery_report() -> TestReport:
    return TestReport(
        nodeid="tests/integration/adapters/bigquery/test_arrow.py::test_select_to_arrow_empty_result",
        location=("tests/integration/adapters/bigquery/test_arrow.py", 148, "test_select_to_arrow_empty_result"),
        keywords={},
        outcome="failed",
        longrepr="failure",
        when="call",
    )


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
