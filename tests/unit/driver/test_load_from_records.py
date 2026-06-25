"""Record normalization for the generic load_from_records base method."""

import pytest

from sqlspec.adapters.sqlite.driver import SqliteDriver
from sqlspec.exceptions import ImproperConfigurationError

_normalize = SqliteDriver._records_to_arrow_table


def test_dict_records_derive_columns_from_keys() -> None:
    table = _normalize([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}], None)
    assert table.column_names == ["a", "b"]
    assert table.num_rows == 2
    assert table.to_pydict() == {"a": [1, 2], "b": ["x", "y"]}


def test_dict_records_respect_explicit_column_order() -> None:
    table = _normalize([{"a": 1, "b": "x"}], ["b", "a"])
    assert table.column_names == ["b", "a"]


def test_positional_records_use_columns() -> None:
    table = _normalize([(1, "x"), (2, "y")], ["a", "b"])
    assert table.to_pydict() == {"a": [1, 2], "b": ["x", "y"]}


def test_empty_records_raise() -> None:
    with pytest.raises(ImproperConfigurationError):
        _normalize([], None)


def test_positional_without_columns_raises() -> None:
    with pytest.raises(ImproperConfigurationError):
        _normalize([(1, 2)], None)


def test_mismatched_dict_keys_raise() -> None:
    with pytest.raises(ImproperConfigurationError):
        _normalize([{"a": 1}, {"a": 1, "b": 2}], None)


def test_positional_wrong_width_raises() -> None:
    with pytest.raises(ImproperConfigurationError):
        _normalize([(1, 2, 3)], ["a", "b"])
