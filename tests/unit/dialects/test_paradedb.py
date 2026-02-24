"""Dialect unit tests for the ParadeDB (PostgreSQL + pgvector + pg_search) dialect."""

from sqlglot import parse_one

import sqlspec.adapters.asyncpg.dialect  # noqa: F401


def _render(sql: str) -> str:
    return parse_one(sql, dialect="paradedb").sql(dialect="paradedb")


def test_bm25_search_operator() -> None:
    sql = "SELECT * FROM mock_items WHERE description @@@ 'shoes'"
    rendered = _render(sql)
    assert "@@@" in rendered


def test_match_conjunction_operator() -> None:
    sql = "SELECT * FROM mock_items WHERE description &&& 'running shoes'"
    rendered = _render(sql)
    assert "&&&" in rendered


def test_match_disjunction_operator() -> None:
    sql = "SELECT * FROM mock_items WHERE description ||| 'shoes'"
    rendered = _render(sql)
    assert "|||" in rendered


def test_term_query_operator() -> None:
    sql = "SELECT * FROM mock_items WHERE category === 'footwear'"
    rendered = _render(sql)
    assert "===" in rendered


def test_phrase_query_operator() -> None:
    sql = "SELECT * FROM mock_items WHERE description ### 'running shoes'"
    rendered = _render(sql)
    assert "###" in rendered


def test_proximity_operator() -> None:
    sql = "SELECT description, rating, category FROM mock_items WHERE description @@@ ('sleek' ## 1 ## 'shoes')"
    rendered = _render(sql)
    assert "##" in rendered


def test_directional_proximity_operator() -> None:
    sql = "SELECT description, rating, category FROM mock_items WHERE description @@@ ('sleek' ##> 1 ##> 'shoes')"
    rendered = _render(sql)
    assert "##>" in rendered


def test_snippet_with_named_args() -> None:
    sql = (
        "SELECT id, pdb.snippet(description, start_tag => '<i>', end_tag => '</i>') "
        "FROM mock_items WHERE description ||| 'shoes' LIMIT 5"
    )
    rendered = _render(sql)
    assert "|||" in rendered
    assert "pdb.snippet" in rendered


def test_pgvector_operators_still_work() -> None:
    sql = "SELECT embedding <=> '[1,2,3]' AS cosine_dist, embedding <#> '[1,2,3]' AS inner_prod FROM items"
    rendered = _render(sql)
    assert "<=>" in rendered
    assert "<#>" in rendered


def test_search_in_where_clause() -> None:
    sql = "SELECT * FROM mock_items WHERE description @@@ 'shoes' AND active = TRUE"
    rendered = _render(sql)
    assert "@@@" in rendered
    assert "WHERE" in rendered


def test_multiple_search_operators() -> None:
    sql = (
        "SELECT description ## 'query' AS snippet, description ### 'query' AS score "
        "FROM mock_items WHERE description @@@ 'query'"
    )
    rendered = _render(sql)
    assert "##" in rendered
    assert "###" in rendered
    assert "@@@" in rendered


def test_fuzzy_cast() -> None:
    sql = "SELECT * FROM mock_items WHERE description @@@ 'runing shose'::pdb.fuzzy(2)"
    rendered = _render(sql)
    assert "@@@" in rendered


def test_prox_regex() -> None:
    sql = "SELECT * FROM mock_items WHERE description @@@ pdb.prox_regex('sho.*', 2, 'run.*')"
    rendered = _render(sql)
    assert "@@@" in rendered
