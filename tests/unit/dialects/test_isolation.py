"""Regression test verifying that Spanner dialects do not mutate base dialect behavior."""

from sqlglot import parse_one

import sqlspec.dialects.spanner
from sqlspec.dialects.spanner import _parsers

_ = sqlspec.dialects.spanner


def test_bigquery_generation_isolated() -> None:
    """Verify that BigQuery generation is isolated from Spanner transforms."""
    sql = "SELECT CAST(x AS FLOAT64) FROM t"
    rendered = parse_one(sql, dialect="bigquery").sql(dialect="bigquery")
    assert rendered == "SELECT CAST(x AS FLOAT64) FROM t"


def test_bigquery_search_with_kwargs_isolated() -> None:
    """Verify that BigQuery SEARCH with named arguments is not truncated by Spanner Search."""
    sql = "SELECT SEARCH(t, 'foo', json_scope => '$.a') FROM t"
    rendered = parse_one(sql, dialect="bigquery").sql(dialect="bigquery")
    assert rendered == "SELECT SEARCH(t, 'foo', json_scope => '$.a') FROM t"


def test_postgres_generation_isolated() -> None:
    """Verify that Postgres generation is isolated from Spangres transforms."""
    sql = "SELECT 1"
    rendered = parse_one(sql, dialect="postgres").sql(dialect="postgres")
    assert rendered == "SELECT 1"


def test_spanner_parsers_contain_spanner_property_parsers() -> None:
    """Verify that SpannerParser and SpangresParser contain Spanner property handlers."""
    assert "INTERLEAVE" in _parsers.SpannerParser.PROPERTY_PARSERS
    assert "ROW" in _parsers.SpannerParser.PROPERTY_PARSERS
    assert "TTL" in _parsers.SpannerParser.PROPERTY_PARSERS
    assert "INTERLEAVE" in _parsers.SpangresParser.PROPERTY_PARSERS
    assert "ROW" in _parsers.SpangresParser.PROPERTY_PARSERS
    assert "TTL" in _parsers.SpangresParser.PROPERTY_PARSERS
