"""Dialect unit tests for the PGVector (PostgreSQL + pgvector) dialect."""

from sqlglot import parse_one

import sqlspec.adapters.asyncpg.dialect  # noqa: F401


def _render(sql: str) -> str:
    return parse_one(sql, dialect="pgvector").sql(dialect="pgvector")


def test_cosine_distance_operator() -> None:
    sql = "SELECT embedding <=> '[1,2,3]' FROM items"
    rendered = _render(sql)
    assert "<=>" in rendered


def test_negative_inner_product_operator() -> None:
    sql = "SELECT embedding <#> '[1,2,3]' FROM items"
    rendered = _render(sql)
    assert "<#>" in rendered


def test_l1_distance_operator() -> None:
    sql = "SELECT embedding <+> '[1,2,3]' FROM items"
    rendered = _render(sql)
    assert "<+>" in rendered


def test_hamming_distance_operator() -> None:
    sql = "SELECT embedding <~> '[1,0,1]' FROM items"
    rendered = _render(sql)
    assert "<~>" in rendered


def test_jaccard_distance_operator() -> None:
    sql = "SELECT embedding <%> '[1,0,1]' FROM items"
    rendered = _render(sql)
    assert "<%>" in rendered


def test_order_by_cosine_distance() -> None:
    sql = "SELECT * FROM items ORDER BY embedding <=> '[1,2,3]'"
    rendered = _render(sql)
    assert "ORDER BY" in rendered
    assert "<=>" in rendered


def test_distance_in_where_clause() -> None:
    sql = "SELECT * FROM items WHERE embedding <=> '[1,2,3]' < 0.5"
    rendered = _render(sql)
    assert "<=>" in rendered
    assert "WHERE" in rendered


def test_multiple_distance_operators() -> None:
    sql = "SELECT embedding <=> '[1,2,3]' AS cosine_dist, embedding <#> '[1,2,3]' AS inner_prod FROM items"
    rendered = _render(sql)
    assert "<=>" in rendered
    assert "<#>" in rendered
