"""Unit tests for Cloud Spanner Full-Text Search functions, types, and DDL."""

from sqlglot import exp, parse_one

from sqlspec.dialects.spanner._expressions import (
    Score,
    Search,
    SearchSubstring,
    TokenizeFulltext,
    TokenizeNgrams,
    TokenizeSubstring,
)


def test_tokenlist_column_in_create_table() -> None:
    """Verify TOKENLIST column and generated column with TOKENIZE_FULLTEXT."""
    sql = "CREATE TABLE Albums (Id INT64, Content STRING(MAX), Tokens TOKENLIST AS (TOKENIZE_FULLTEXT(Content)) STORED) PRIMARY KEY (Id)"
    parsed = parse_one(sql, dialect="spanner")
    rendered = parsed.sql(dialect="spanner")
    assert "TOKENLIST" in rendered
    assert "TOKENIZE_FULLTEXT(Content)" in rendered
    assert "STORED" in rendered


def test_search_function_parsing_and_generation() -> None:
    """Verify SEARCH(Tokens, 'query') parses and round-trips."""
    sql = "SELECT Title FROM Albums WHERE SEARCH(Tokens, 'rock album')"
    parsed = parse_one(sql, dialect="spanner")
    node = parsed.find(Search)
    assert node is not None
    assert isinstance(node, Search)
    rendered = parsed.sql(dialect="spanner")
    assert "SEARCH(Tokens, 'rock album')" in rendered


def test_search_substring_function() -> None:
    """Verify SEARCH_SUBSTRING(Tokens, 'query') parses and round-trips."""
    sql = "SELECT Title FROM Albums WHERE SEARCH_SUBSTRING(Tokens, 'sub')"
    parsed = parse_one(sql, dialect="spanner")
    node = parsed.find(SearchSubstring)
    assert node is not None
    assert isinstance(node, SearchSubstring)
    rendered = parsed.sql(dialect="spanner")
    assert "SEARCH_SUBSTRING(Tokens, 'sub')" in rendered


def test_score_function_parsing_and_generation() -> None:
    """Verify SCORE(Tokens, 'query') parses and round-trips."""
    sql = "SELECT Title, SCORE(Tokens, 'rock') AS score FROM Albums ORDER BY score DESC"
    parsed = parse_one(sql, dialect="spanner")
    node = parsed.find(Score)
    assert node is not None
    assert isinstance(node, Score)
    rendered = parsed.sql(dialect="spanner")
    assert "SCORE(Tokens, 'rock')" in rendered


def test_tokenize_functions() -> None:
    """Verify tokenization functions parse to typed AST nodes."""
    sql = "SELECT TOKENIZE_FULLTEXT(c), TOKENIZE_SUBSTRING(c), TOKENIZE_NGRAMS(c) FROM t"
    parsed = parse_one(sql, dialect="spanner")
    assert parsed.find(TokenizeFulltext) is not None
    assert parsed.find(TokenizeSubstring) is not None
    assert parsed.find(TokenizeNgrams) is not None


def test_create_search_index_full_options() -> None:
    """Verify CREATE SEARCH INDEX with STORING, PARTITION BY, ORDER BY, and OPTIONS."""
    sql = (
        "CREATE SEARCH INDEX albums_search_idx ON Albums (Tokens) "
        "STORING (Title, ReleaseDate) PARTITION BY SingerId "
        "ORDER BY ReleaseDate DESC OPTIONS (sort_order_sharding = true)"
    )
    parsed = parse_one(sql, dialect="spanner")
    assert not isinstance(parsed, exp.Command)
    assert isinstance(parsed, exp.Index)
    assert parsed.args.get("kind") == "SEARCH"
    rendered = parsed.sql(dialect="spanner")
    assert "CREATE SEARCH INDEX albums_search_idx ON Albums (Tokens)" in rendered
    assert "STORING (Title, ReleaseDate)" in rendered
    assert "PARTITION BY SingerId" in rendered
    assert "ORDER BY ReleaseDate DESC" in rendered
    assert "sort_order_sharding = true" in rendered.lower()
