"""Regression-lock tests for mssql_python BulkCopy stats and wrapper defaults."""

import inspect
from typing import Any

import pytest

from sqlspec.adapters.mssql_python.driver import MssqlPythonAsyncDriver, MssqlPythonDriver, _coerce_bulk_copy_result


class _FakeCursor:
    def __init__(self, rowcount: int = 0) -> None:
        self.rowcount = rowcount


def test_coerce_bulk_copy_result_preserves_upstream_stats() -> None:
    stats: dict[str, Any] = {"rows_copied": 10, "batch_count": 2, "elapsed_time": 0.5, "future_key": "x"}
    result = _coerce_bulk_copy_result(stats, _FakeCursor(rowcount=0))
    assert result == stats


def test_coerce_bulk_copy_result_falls_back_to_rowcount() -> None:
    assert _coerce_bulk_copy_result(None, _FakeCursor(rowcount=7)) == {"rows_copied": 7}


def test_coerce_bulk_copy_result_copies_dict_not_aliases() -> None:
    stats: dict[str, Any] = {"rows_copied": 3}
    result = _coerce_bulk_copy_result(stats, _FakeCursor())
    result["rows_copied"] = 99
    assert stats["rows_copied"] == 3


@pytest.mark.parametrize("driver_cls", [MssqlPythonDriver, MssqlPythonAsyncDriver])
def test_bulk_copy_defaults_match_upstream(driver_cls: Any) -> None:
    pytest.importorskip("mssql_python")
    from mssql_python.cursor import Cursor

    upstream = inspect.signature(Cursor.bulkcopy).parameters
    wrapper = inspect.signature(driver_cls.bulk_copy).parameters
    for name in (
        "batch_size",
        "timeout",
        "column_mappings",
        "keep_identity",
        "check_constraints",
        "table_lock",
        "keep_nulls",
        "fire_triggers",
        "use_internal_transaction",
    ):
        assert wrapper[name].default == upstream[name].default
