"""BigQuery integration test fixtures."""

from collections.abc import Generator
from typing import TYPE_CHECKING, Any

import pytest

from tests.integration.adapters.bigquery._wedge import describe_wedge, is_emulator_wedge

if TYPE_CHECKING:
    from _pytest.reports import TestReport

_emulator_wedge_reason: "str | None" = None


def _mark_report_skipped_for_emulator_wedge(report: "TestReport", error: BaseException) -> bool:
    """Record a wedged BigQuery emulator and mark the current report skipped."""
    global _emulator_wedge_reason
    if not is_emulator_wedge(error):
        return False
    _emulator_wedge_reason = describe_wedge(error)
    location_path, location_line, _ = report.location
    report.outcome = "skipped"
    report.longrepr = (
        location_path,
        0 if location_line is None else location_line,
        f"Skipped: BigQuery emulator wedged earlier in this session ({_emulator_wedge_reason})",
    )
    return True


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(call: "pytest.CallInfo[None]") -> "Generator[None, Any, None]":
    """Record the first emulator wedge so remaining BigQuery tests skip fast."""
    outcome = yield
    report = outcome.get_result()
    if report.outcome != "skipped" and call.excinfo is not None and is_emulator_wedge(call.excinfo.value):
        _mark_report_skipped_for_emulator_wedge(report, call.excinfo.value)


@pytest.fixture(autouse=True)
def _skip_after_emulator_wedge() -> None:
    """Skip once the emulator stopped responding instead of timing out per test."""
    if _emulator_wedge_reason is not None:
        pytest.skip(f"BigQuery emulator wedged earlier in this session ({_emulator_wedge_reason})")
