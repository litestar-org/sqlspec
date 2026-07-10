"""Tests for the import-time measurement helper."""

import pytest
from tools.scripts.import_time import calculate_reduction, parse_import_times


def test_parse_import_times_extracts_cumulative_module_timings() -> None:
    output = """
import time:       250 |        250 |   sqlspec._typing
import time:       500 |       1000 | sqlspec
"""
    assert parse_import_times(output) == {"sqlspec._typing": 250, "sqlspec": 1000}


def test_calculate_reduction_returns_percentage() -> None:
    assert calculate_reduction(681.0, 204.3) == pytest.approx(70.0)
