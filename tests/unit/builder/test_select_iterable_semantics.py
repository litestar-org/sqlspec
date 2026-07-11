"""Regression coverage for Select's Sequence-only parameter handling."""

from collections.abc import Iterator, Mapping
from typing import Any

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.exceptions import SQLBuilderError


class _CustomMapping(Mapping[str, int]):
    def __getitem__(self, key: str) -> int:
        return {"low": 1, "high": 2}[key]

    def __iter__(self) -> Iterator[str]:
        return iter(("low", "high"))

    def __len__(self) -> int:
        return 2


def _generator() -> Iterator[int]:
    yield 1
    yield 2


@pytest.mark.parametrize("value", [{1, 2}, _generator(), _CustomMapping()])
@pytest.mark.parametrize("method_name", ["_handle_in_operator", "_handle_not_in_operator"])
def test_in_operators_treat_non_sequence_iterables_as_scalar(method_name: str, value: Any) -> None:
    builder = sql.select("*").from_("items")

    expression = getattr(builder, method_name)(exp.column("value"), value, "value")

    assert sum(1 for _ in expression.find_all(exp.Placeholder)) == 1
    assert list(builder.parameters.values()) == [value]


@pytest.mark.parametrize("value", [{1, 2}, _generator(), _CustomMapping()])
@pytest.mark.parametrize("method_name", ["_handle_between_operator", "_handle_not_between_operator"])
def test_between_operators_reject_non_sequence_iterables(method_name: str, value: Any) -> None:
    builder = sql.select("*").from_("items")

    with pytest.raises(SQLBuilderError, match="requires a tuple of two values"):
        getattr(builder, method_name)(exp.column("value"), value, "value")


@pytest.mark.parametrize("value", [{1, 2}, _generator(), _CustomMapping()])
def test_any_operator_rejects_non_sequence_iterables(value: Any) -> None:
    builder = sql.select("*").from_("items")

    with pytest.raises(SQLBuilderError, match="Unsupported type for 'values' in WHERE ANY"):
        builder._create_any_condition(exp.column("value"), value, "value")
