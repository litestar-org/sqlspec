"""Regression test verifying that Spanner dialects do not mutate base dialect behavior."""

from sqlglot import exp, parse_one

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


def test_bigquery_and_postgres_vector_functions_isolated() -> None:
    """Verify that BigQuery and Postgres vector function calls are not forced into Spanner AST nodes."""
    bq_sql = "SELECT COSINE_DISTANCE(v1, v2, options => '{\"use_brute_force\": true}') FROM t"
    bq_parsed = parse_one(bq_sql, dialect="bigquery")
    assert isinstance(bq_parsed.find(exp.Anonymous), exp.Anonymous)
    assert bq_parsed.sql(dialect="bigquery") == bq_sql

    pg_sql = "SELECT EUCLIDEAN_DISTANCE(v1, v2, 1), DOT_PRODUCT(v1, v2, 1) FROM t"
    pg_parsed = parse_one(pg_sql, dialect="postgres")
    assert len(list(pg_parsed.find_all(exp.Anonymous))) == 2
    assert pg_parsed.sql(dialect="postgres") == pg_sql


def test_userdefined_datatype_without_tokenlist_kind_isolated() -> None:
    """Verify that non-TOKENLIST USERDEFINED data types are not rendered as TOKENLIST in Spanner."""
    custom_udt = exp.DataType(this=exp.DataType.Type.USERDEFINED, kind="CUSTOM_TYPE")
    assert custom_udt.sql(dialect="spanner") == "CUSTOM_TYPE"

    bare_udt = exp.DataType(this=exp.DataType.Type.USERDEFINED)
    assert bare_udt.sql(dialect="spanner") != "TOKENLIST"


def test_postgres_generation_isolated() -> None:
    """Verify that Postgres generation is isolated from Spangres transforms."""
    sql = "SELECT 1"
    rendered = parse_one(sql, dialect="postgres").sql(dialect="postgres")
    assert rendered == "SELECT 1"


def test_spanner_parsers_contain_spanner_property_parsers() -> None:
    """Verify that SpannerParser and SpangresParser contain Spanner property handlers."""
    assert "INTERLEAVE" in _parsers.SpannerParser.PROPERTY_PARSERS
    assert "PRIMARY KEY" in _parsers.SpannerParser.PROPERTY_PARSERS
    assert "ROW" in _parsers.SpannerParser.PROPERTY_PARSERS
    assert "TTL" in _parsers.SpannerParser.PROPERTY_PARSERS
    assert "INTERLEAVE" in _parsers.SpangresParser.PROPERTY_PARSERS
    assert "PRIMARY KEY" in _parsers.SpangresParser.PROPERTY_PARSERS
    assert "ROW" in _parsers.SpangresParser.PROPERTY_PARSERS
    assert "TTL" in _parsers.SpangresParser.PROPERTY_PARSERS
