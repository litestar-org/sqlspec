from typing import cast

import pytest
from sqlglot import exp

from sqlspec import sql
from sqlspec.exceptions import SQLBuilderError


@pytest.mark.parametrize("name", ["rollup", "cube"])
def test_group_by_factory_extension_matches_select_method(name: str) -> None:
    wrapper = getattr(sql, name)("a", exp.column("b"))
    composed = sql.select("a").from_("t").group_by(wrapper)
    direct = getattr(sql.select("a").from_("t"), f"group_by_{name}")("a", exp.column("b"))

    assert composed.to_sql() == direct.to_sql()
    assert " ".join(composed.to_sql().split()).endswith(f'GROUP BY {name.upper()} ( "t"."a", "t"."b" )')


@pytest.mark.parametrize("columns", [("a", "b"), ["a", "b"]])
def test_group_by_factory_grouping_sets_preserves_grand_total(columns: tuple[str, ...] | list[str]) -> None:
    composed = sql.select("a").from_("t").group_by(sql.grouping_sets(columns, ()))
    direct = sql.select("a").from_("t").group_by_grouping_sets(columns, ())

    assert composed.to_sql() == direct.to_sql()
    assert " ".join(composed.to_sql().split()).endswith('GROUP BY GROUPING SETS ( ("t"."a", "t"."b"), () )')


@pytest.mark.parametrize("factory", [True, False])
def test_grouping_sets_rejects_bare_string(factory: bool) -> None:
    invalid_columns = cast("tuple[str, ...]", "ab")
    with pytest.raises(SQLBuilderError, match="tuple or list"):
        if factory:
            sql.grouping_sets(invalid_columns)
        else:
            sql.select("a").from_("t").group_by_grouping_sets(invalid_columns)


def test_group_by_preserves_string_and_raw_expression_columns() -> None:
    query = sql.select("a").from_("t").group_by("a", exp.column("b"))
    assert " ".join(query.to_sql().split()).endswith('GROUP BY "t"."a", "t"."b"')


@pytest.mark.parametrize(
    ("column", "expected"),
    [("a", '"t"."a"'), ("schema.col", '"t"."schema.col"'), ('"Quoted Name"', '"t"."""Quoted Name"""')],
)
def test_group_by_preserves_literal_column_identifiers(column: str, expected: str) -> None:
    query = sql.select("*").from_("t").group_by(column)
    assert " ".join(query.to_sql().split()) == f'SELECT * FROM "t" AS "t" GROUP BY {expected}'
