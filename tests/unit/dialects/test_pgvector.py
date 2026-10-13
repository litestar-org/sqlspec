"""Dialect unit tests for the PGVector (PostgreSQL + pgvector) dialect."""

from sqlglot import exp, parse_one

import sqlspec.dialects.postgres._pg_textsearch  # noqa: F401
import sqlspec.dialects.postgres._pgvector  # noqa: F401
from sqlspec.dialects.postgres._operators import is_postgres_extension_operator, postgres_extension_operator


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


def test_distance_operator_precedence_with_arithmetic() -> None:
    """Verify pgvector distance operators have custom operator precedence (below + and -)."""
    parsed = parse_one("SELECT vec1 + vec2 <=> vec3 FROM items", dialect="pgvector")
    select_expr = parsed.expressions[0]
    assert isinstance(select_expr, exp.Operator)
    assert is_postgres_extension_operator(select_expr)
    assert postgres_extension_operator(select_expr) == "<=>"
    assert isinstance(select_expr.this, exp.Add)


def test_pg_textsearch_bm25_operator_isolated_from_postgres_and_pgvector() -> None:
    """Verify <@> only parses as BM25 distance in pg_textsearch."""
    bm25_parsed = parse_one("SELECT body <@> 'query' FROM docs", dialect="pg_textsearch")
    op_node = bm25_parsed.find(exp.Operator)
    assert op_node is not None
    assert is_postgres_extension_operator(op_node)
    assert postgres_extension_operator(op_node) == "<@>"
    assert bm25_parsed.sql(dialect="pg_textsearch") == "SELECT body <@> 'query' FROM docs"
