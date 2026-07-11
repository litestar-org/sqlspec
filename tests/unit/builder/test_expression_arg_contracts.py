"""Render regressions for expression builders that silently dropped clauses.

Each construct here previously passed an argument key absent from the target
sqlglot expression's ``arg_types``; the generator ignores unknown keys, so the
clause vanished from rendered SQL without an error. Assertions render the
expression and require the full clause text.
"""

import pytest

from sqlspec import sql
from sqlspec.builder import Column
from sqlspec.exceptions import SQLBuilderError


def test_count_distinct_renders_distinct() -> None:
    rendered = sql.count_distinct("category").expression.sql(dialect="postgres")
    assert rendered == "COUNT(DISTINCT category)"


def test_count_with_distinct_flag_renders_distinct() -> None:
    rendered = sql.count("category", distinct=True).expression.sql(dialect="postgres")
    assert rendered == "COUNT(DISTINCT category)"


def test_count_star_distinct_raises() -> None:
    with pytest.raises(SQLBuilderError):
        sql.count("*", distinct=True)


def test_sum_distinct_renders_distinct() -> None:
    rendered = sql.sum("amount", distinct=True).expression.sql(dialect="postgres")
    assert rendered == "SUM(DISTINCT amount)"


def test_column_like_escape_renders_escape_clause() -> None:
    rendered = Column("name").like("50!%%", escape="!").sqlglot_expression.sql(dialect="postgres")
    assert rendered == "name LIKE '50!%%' ESCAPE '!'"


def test_where_like_escape_renders_escape_clause() -> None:
    stmt = sql.select("*").from_("t").where_like("name", "50!%%", escape="!").build()
    assert "ESCAPE" in stmt.sql


def test_column_round_renders_decimals() -> None:
    rendered = Column("price").round(2).sqlglot_expression.sql(dialect="postgres")
    assert rendered == "ROUND(price, 2)"


def test_factory_round_renders_decimals() -> None:
    rendered = sql.round("price", 2).expression.sql(dialect="postgres")
    assert rendered == "ROUND(price, 2)"


def test_column_substring_renders_start_and_length() -> None:
    rendered = Column("code").substring(2, 3).sqlglot_expression.sql()
    assert rendered == "SUBSTRING(code, 2, 3)"


def test_column_substring_renders_start_only() -> None:
    rendered = Column("code").substring(2).sqlglot_expression.sql()
    assert rendered == "SUBSTRING(code, 2)"


def test_column_any_renders_operand_array() -> None:
    rendered = Column("status").any_(["a", "b"]).sqlglot_expression.sql(dialect="postgres")
    assert rendered == "status = ANY(ARRAY['a', 'b'])"


def test_column_not_any_renders_operand_array() -> None:
    rendered = Column("status").not_any_(["a", "b"]).sqlglot_expression.sql(dialect="postgres")
    assert rendered == "status <> ANY(ARRAY['a', 'b'])"
