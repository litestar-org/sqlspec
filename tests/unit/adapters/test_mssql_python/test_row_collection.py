"""Row-shape fidelity tests for the mssql-python adapter.

``mssql-python`` returns ``mssql_python.Row`` objects that are iterable and
indexable but are *not* ``tuple`` subclasses. The driver declares
``row_format="tuple"``, so fetched rows must be materialized into real tuples to
keep that contract accurate (issue #630).
"""

from typing import Any

from sqlspec.adapters.mssql_python.core import materialize_tuple_rows


class _FakeRow:
    """Stand-in for ``mssql_python.Row``: iterable/indexable but not a tuple."""

    def __init__(self, values: "tuple[Any, ...]") -> None:
        self._values = list(values)

    def __iter__(self) -> "Any":
        return iter(self._values)

    def __getitem__(self, index: int) -> "Any":
        return self._values[index]

    def __len__(self) -> int:
        return len(self._values)


def test_materialize_tuple_rows_converts_row_objects_to_real_tuples() -> None:
    rows = [_FakeRow((1, "alice")), _FakeRow((2, "bob"))]

    result = materialize_tuple_rows(rows)

    assert result == [(1, "alice"), (2, "bob")]
    assert all(type(row) is tuple for row in result)


def test_materialize_tuple_rows_handles_empty_and_none() -> None:
    assert materialize_tuple_rows([]) == []
    assert materialize_tuple_rows(None) == []
