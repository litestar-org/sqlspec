"""Tests for GROUP BY extensions shared by the factory and the Select builder."""

from collections.abc import Callable
from typing import Any, cast

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.exceptions import SQLBuilderError
from tests.conftest import is_compiled


@pytest.mark.parametrize("name", ["rollup", "cube"])
def test_group_by_factory_extension_matches_select_method(name: str) -> None:
    """Factory ROLLUP/CUBE wrappers compose into group_by and match the Select methods."""
    wrapper = getattr(sql, name)("a", exp.column("b"))
    composed = sql.select("a").from_("t").group_by(wrapper)
    direct = getattr(sql.select("a").from_("t"), f"group_by_{name}")("a", exp.column("b"))

    assert composed.to_sql() == direct.to_sql()
    assert " ".join(composed.to_sql().split()).endswith(f'GROUP BY {name.upper()} ( "t"."a", "t"."b" )')


@pytest.mark.parametrize("columns", [("a", "b"), ["a", "b"]])
def test_group_by_factory_grouping_sets_preserves_grand_total(columns: tuple[str, ...] | list[str]) -> None:
    """Tuple and list grouping sets, including the empty grand-total set, agree between both entry points."""
    composed = sql.select("a").from_("t").group_by(sql.grouping_sets(columns, ()))
    direct = sql.select("a").from_("t").group_by_grouping_sets(columns, ())

    assert composed.to_sql() == direct.to_sql()
    assert " ".join(composed.to_sql().split()).endswith('GROUP BY GROUPING SETS ( ("t"."a", "t"."b"), () )')


@pytest.mark.parametrize(
    "grouping_sets", [sql.grouping_sets, sql.select("a").from_("t").group_by_grouping_sets], ids=["factory", "select"]
)
def test_grouping_sets_rejects_bare_string(grouping_sets: Callable[..., Any]) -> None:
    """A bare string is rejected instead of being iterated character by character."""
    invalid_columns = cast("tuple[str, ...]", "ab")
    if is_compiled():
        with pytest.raises(TypeError, match="tuple, list"):
            grouping_sets(invalid_columns)
        return
    with pytest.raises(SQLBuilderError, match="tuple or list"):
        grouping_sets(invalid_columns)


def test_group_by_preserves_string_and_raw_expression_columns() -> None:
    """Plain strings and raw sqlglot expressions keep their existing group_by rendering."""
    query = sql.select("a").from_("t").group_by("a", exp.column("b"))
    assert " ".join(query.to_sql().split()).endswith('GROUP BY "t"."a", "t"."b"')


@pytest.mark.parametrize(
    ("column", "expected"),
    [("a", '"t"."a"'), ("schema.col", '"t"."schema.col"'), ('"Quoted Name"', '"t"."""Quoted Name"""')],
)
def test_group_by_preserves_literal_column_identifiers(column: str, expected: str) -> None:
    """String group_by inputs are treated as literal column names."""
    query = sql.select("*").from_("t").group_by(column)
    assert " ".join(query.to_sql().split()) == f'SELECT * FROM "t" AS "t" GROUP BY {expected}'
