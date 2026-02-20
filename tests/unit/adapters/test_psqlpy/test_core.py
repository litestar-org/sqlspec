"""Unit tests for psqlpy core helpers."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from sqlspec.adapters.psqlpy.core import coerce_records_for_execute_many, collect_rows, format_execute_many_parameters

pytestmark = pytest.mark.xdist_group("adapter_unit")


def test_format_execute_many_parameters_no_coercion_reuses_list_rows() -> None:
    """Formatting should preserve list rows when no numeric coercion is requested."""
    records = [[1, "a"], [2, "b"]]

    formatted = format_execute_many_parameters(records, coerce_numeric=False)

    assert formatted is records
    assert formatted[0] is records[0]
    assert formatted[1] is records[1]


def test_format_execute_many_parameters_no_coercion_converts_tuples() -> None:
    """Tuple rows should be converted to list rows for execute_many."""
    records = [(1, "a"), (2, "b")]

    formatted = format_execute_many_parameters(records, coerce_numeric=False)

    assert formatted == [[1, "a"], [2, "b"]]


def test_format_execute_many_parameters_with_coercion_converts_float_to_decimal() -> None:
    """Numeric write coercion should convert floats to Decimal values."""
    records = [(1.5, "a"), (2, "b")]

    formatted = format_execute_many_parameters(records, coerce_numeric=True)

    assert formatted[0][0] == Decimal("1.5")
    assert formatted[1][0] == 2


def test_format_execute_many_parameters_handles_scalar_input() -> None:
    """Scalar execute_many payloads should be normalized to a list containing one row."""
    formatted = format_execute_many_parameters(5, coerce_numeric=False)
    assert formatted == [[5]]


def test_coerce_records_for_execute_many_delegates_to_formatter() -> None:
    """coerce_records_for_execute_many should keep behavior via shared formatter."""
    records = [(1.25, "x"), (3, "y")]

    formatted = coerce_records_for_execute_many(records)

    assert formatted[0][0] == Decimal("1.25")
    assert formatted[1] == [3, "y"]


def test_collect_rows_names_from_first_row() -> None:
    """collect_rows should derive column order from first dict row key order."""
    result = SimpleNamespace(result=lambda: [{"id": 1, "name": "x"}])

    rows, columns = collect_rows(result)

    assert rows == [{"id": 1, "name": "x"}]
    assert columns == ["id", "name"]


def test_collect_rows_empty_result() -> None:
    """collect_rows should return empty structures for empty query results."""
    result = SimpleNamespace(result=lambda: [])

    rows, columns = collect_rows(result)

    assert rows == []
    assert columns == []


def test_collect_rows_prefers_metadata_column_names_when_available() -> None:
    """collect_rows should derive names from first row keys even with metadata available."""
    result = SimpleNamespace(result=lambda: [{"id": 1, "name": "x"}], column_names=("id", "name"))

    rows, columns = collect_rows(result)

    assert rows == [{"id": 1, "name": "x"}]
    assert columns == ["id", "name"]


def test_collect_rows_accepts_raw_list_payload() -> None:
    """collect_rows should accept pre-resolved row lists as direct payloads."""
    payload = [{"id": 1, "name": "x"}]

    rows, columns = collect_rows(payload)

    assert rows is payload
    assert columns == ["id", "name"]
