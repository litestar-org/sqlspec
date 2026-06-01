"""Dialect unit tests for the ParadeDB (PostgreSQL + pgvector + pg_search) dialect."""

from sqlglot import parse_one

import sqlspec.dialects.postgres._paradedb  # noqa: F401
from sqlspec.dialects.postgres._operators import PARADEDB_OPERATOR_TOKENS, PGVECTOR_OPERATOR_TOKENS
from sqlspec.dialects.postgres._paradedb import ParadeDBTokenizer
from sqlspec.dialects.postgres._pgvector import PGVectorTokenizer


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


def test_paradedb_keywords_inherits_from_pgvector() -> None:
    assert ParadeDBTokenizer.KEYWORDS == {**PGVectorTokenizer.KEYWORDS, **PARADEDB_OPERATOR_TOKENS}


def test_paradedb_keywords_contains_paradedb_operators() -> None:
    for operator in PARADEDB_OPERATOR_TOKENS:
        assert operator in ParadeDBTokenizer.KEYWORDS


def test_paradedb_keywords_contains_pgvector_operators() -> None:
    for operator in PGVECTOR_OPERATOR_TOKENS:
        assert operator in ParadeDBTokenizer.KEYWORDS


def test_paradedb_tokenizer_inherits_from_pgvector_tokenizer() -> None:
    assert issubclass(ParadeDBTokenizer, PGVectorTokenizer)
