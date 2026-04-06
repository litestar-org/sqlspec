"""Tests for sqlspec.sqlcommenter — Google SQLCommenter spec compliance."""

from __future__ import annotations

from sqlspec.sqlcommenter import SQLCommenterAttributes, append_comment, generate_comment, parse_comment

# ── generate_comment ──────────────────────────────────────────────────────


def test_generate_comment_basic() -> None:
    attrs: SQLCommenterAttributes = {"db_driver": "asyncpg", "route": "/users"}
    result = generate_comment(attrs)
    assert result == "db_driver='asyncpg',route='%2Fusers'"


def test_generate_comment_empty_attrs() -> None:
    result = generate_comment({})
    assert result == ""


def test_generate_comment_lexicographic_sort() -> None:
    attrs: SQLCommenterAttributes = {"z_key": "z", "a_key": "a", "m_key": "m"}
    result = generate_comment(attrs)
    assert result == "a_key='a',m_key='m',z_key='z'"


def test_generate_comment_url_encodes_values() -> None:
    attrs: SQLCommenterAttributes = {"route": "/polls 1000"}
    result = generate_comment(attrs)
    assert result == "route='%2Fpolls%201000'"


def test_generate_comment_url_encodes_keys() -> None:
    attrs: SQLCommenterAttributes = {"route parameter": "value"}
    result = generate_comment(attrs)
    assert result == "route%20parameter='value'"


def test_generate_comment_encodes_single_quotes_in_values() -> None:
    attrs: SQLCommenterAttributes = {"key": "it's a test"}
    result = generate_comment(attrs)
    # Single quotes are URL-encoded to %27 by quote(safe="")
    assert result == "key='it%27s%20a%20test'"


def test_generate_comment_encodes_single_quotes_in_keys() -> None:
    attrs: SQLCommenterAttributes = {"it's": "value"}
    result = generate_comment(attrs)
    assert result == "it%27s='value'"


def test_generate_comment_special_characters() -> None:
    attrs: SQLCommenterAttributes = {"key": "a=b&c"}
    result = generate_comment(attrs)
    assert result == "key='a%3Db%26c'"


def test_generate_comment_none_values_skipped() -> None:
    attrs: SQLCommenterAttributes = {"key1": "val1", "key2": None}  # type: ignore[typeddict-item]
    result = generate_comment(attrs)
    assert result == "key1='val1'"


# ── append_comment ────────────────────────────────────────────────────────


def test_append_comment_basic() -> None:
    sql = "SELECT * FROM users"
    attrs: SQLCommenterAttributes = {"db_driver": "asyncpg"}
    result = append_comment(sql, attrs)
    assert result == "SELECT * FROM users /*db_driver='asyncpg'*/"


def test_append_comment_empty_attrs_returns_unchanged() -> None:
    sql = "SELECT * FROM users"
    result = append_comment(sql, {})
    assert result == sql


def test_append_comment_existing_comment_returns_unchanged() -> None:
    sql = "SELECT * FROM users /* existing comment */"
    attrs: SQLCommenterAttributes = {"db_driver": "asyncpg"}
    result = append_comment(sql, attrs)
    assert result == sql


def test_append_comment_existing_inline_comment_returns_unchanged() -> None:
    sql = "SELECT /* hint */ * FROM users"
    attrs: SQLCommenterAttributes = {"db_driver": "asyncpg"}
    result = append_comment(sql, attrs)
    assert result == sql


def test_append_comment_strips_trailing_whitespace() -> None:
    sql = "SELECT * FROM users  "
    attrs: SQLCommenterAttributes = {"db_driver": "asyncpg"}
    result = append_comment(sql, attrs)
    assert result == "SELECT * FROM users /*db_driver='asyncpg'*/"


def test_append_comment_preserves_semicolon() -> None:
    sql = "SELECT * FROM users;"
    attrs: SQLCommenterAttributes = {"db_driver": "asyncpg"}
    result = append_comment(sql, attrs)
    assert result == "SELECT * FROM users /*db_driver='asyncpg'*/;"


def test_append_comment_multiple_attrs_sorted() -> None:
    sql = "SELECT 1"
    attrs: SQLCommenterAttributes = {"framework": "litestar", "db_driver": "asyncpg", "action": "list"}
    result = append_comment(sql, attrs)
    assert result == "SELECT 1 /*action='list',db_driver='asyncpg',framework='litestar'*/"


# ── parse_comment ─────────────────────────────────────────────────────────


def test_parse_comment_basic() -> None:
    sql = "SELECT * FROM users /*db_driver='asyncpg',route='%2Fusers'*/"
    parsed_sql, attrs = parse_comment(sql)
    assert parsed_sql == "SELECT * FROM users"
    assert attrs == {"db_driver": "asyncpg", "route": "/users"}


def test_parse_comment_no_comment() -> None:
    sql = "SELECT * FROM users"
    parsed_sql, attrs = parse_comment(sql)
    assert parsed_sql == sql
    assert attrs == {}


def test_parse_comment_non_sqlcommenter_comment() -> None:
    sql = "SELECT * FROM users /* just a plain comment */"
    parsed_sql, attrs = parse_comment(sql)
    assert parsed_sql == sql
    assert attrs == {}


def test_parse_comment_url_decodes() -> None:
    sql = "SELECT 1 /*route='%2Fpolls%201000'*/"
    parsed_sql, attrs = parse_comment(sql)
    assert parsed_sql == "SELECT 1"
    assert attrs == {"route": "/polls 1000"}


def test_parse_comment_encoded_quotes() -> None:
    sql = "SELECT 1 /*key='it%27s%20a%20test'*/"
    parsed_sql, attrs = parse_comment(sql)
    assert parsed_sql == "SELECT 1"
    assert attrs == {"key": "it's a test"}


def test_parse_comment_round_trip() -> None:
    """generate_comment → append_comment → parse_comment should round-trip."""
    original_attrs: SQLCommenterAttributes = {
        "db_driver": "asyncpg",
        "framework": "litestar",
        "route": "/api/users",
        "controller": "UserController",
    }
    sql = "SELECT * FROM users WHERE id = :id"
    commented_sql = append_comment(sql, original_attrs)
    parsed_sql, parsed_attrs = parse_comment(commented_sql)
    assert parsed_sql == sql
    assert parsed_attrs == original_attrs


def test_parse_comment_with_semicolon() -> None:
    sql = "SELECT 1 /*db_driver='asyncpg'*/;"
    parsed_sql, attrs = parse_comment(sql)
    assert parsed_sql == "SELECT 1"
    assert attrs == {"db_driver": "asyncpg"}


# ── edge cases ────────────────────────────────────────────────────────────


def test_empty_string_value() -> None:
    attrs: SQLCommenterAttributes = {"key": ""}
    result = generate_comment(attrs)
    assert result == "key=''"


def test_unicode_values() -> None:
    attrs: SQLCommenterAttributes = {"key": "café"}
    comment = generate_comment(attrs)
    assert "key=" in comment
    # Round-trip through append/parse
    sql = append_comment("SELECT 1", attrs)
    _, parsed = parse_comment(sql)
    assert parsed["key"] == "café"


def test_traceparent_format() -> None:
    """W3C traceparent values should survive round-trip."""
    tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    attrs: SQLCommenterAttributes = {"traceparent": tp}
    sql = append_comment("SELECT 1", attrs)
    _, parsed = parse_comment(sql)
    assert parsed["traceparent"] == tp
