"""Unit tests for the PGTextSearch sqlglot dialect."""

from sqlglot import exp, parse_one

from sqlspec.dialects.postgres import PGTextSearch


def test_pg_textsearch_bm25_ranking_operator() -> None:
    """Verify BM25 relevance ranking operator <@> parses and generates."""
    sql = (
        "SELECT title, content <@> 'database system' AS score FROM documents ORDER BY content <@> 'database system' ASC"
    )
    expression = parse_one(sql, read=PGTextSearch)
    rendered = expression.sql(dialect=PGTextSearch)
    assert "<@>" in rendered
    assert "ORDER BY content <@> 'database system' ASC" in rendered


def test_pg_textsearch_inherits_pgvector_distance() -> None:
    """Verify pgvector distance operators are supported for hybrid search."""
    sql = "SELECT embedding <=> '[1,2,3]' FROM items"
    expression = parse_one(sql, read=PGTextSearch)
    rendered = expression.sql(dialect=PGTextSearch)
    assert "<=>" in rendered


def test_pg_textsearch_hybrid_query() -> None:
    """Verify combined vector distance and BM25 ranking in one statement."""
    sql = (
        "SELECT id, embedding <=> '[0.1, 0.2, 0.3]' AS vec_dist, content <@> 'fast database' AS bm25_score "
        "FROM documents ORDER BY content <@> 'fast database' ASC LIMIT 10"
    )
    expression = parse_one(sql, read=PGTextSearch)
    rendered = expression.sql(dialect=PGTextSearch)
    assert "<=>" in rendered
    assert "<@>" in rendered


def test_pg_textsearch_bm25_index_ddl() -> None:
    """Verify BM25 index creation statement parses."""
    sql = "CREATE INDEX idx_docs_bm25 ON documents USING bm25 (content) WITH (text_config='english', k1=1.2, b=0.75)"
    expression = parse_one(sql, read=PGTextSearch)
    rendered = expression.sql(dialect=PGTextSearch)
    assert "USING bm25" in rendered
    assert "text_config" in rendered


def test_pg_textsearch_uses_postgres_custom_operator_precedence() -> None:
    expression = parse_one("SELECT title || content <@> 'query' FROM documents", read=PGTextSearch)
    ranking = expression.expressions[0]
    assert isinstance(ranking, exp.Operator)
    assert isinstance(ranking.this, exp.DPipe)
    assert ranking.expression == exp.Literal.string("query")


def test_pg_textsearch_preserves_postgres_containment_operators() -> None:
    expression = parse_one("SELECT ARRAY[1] <@ ARRAY[1, 2], ARRAY[1, 2] @> ARRAY[1]", read=PGTextSearch)
    assert "<@" in expression.sql(dialect=PGTextSearch)
    assert "@>" in expression.sql(dialect=PGTextSearch)
    assert not list(expression.find_all(exp.Operator))
