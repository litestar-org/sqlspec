"""Regression test verifying that Spanner dialects do not mutate base dialects."""

from sqlglot import exp
from sqlglot.generators.bigquery import BigQueryGenerator
from sqlglot.generators.postgres import PostgresGenerator
from sqlglot.parsers.bigquery import BigQueryParser
from sqlglot.parsers.postgres import PostgresParser

import sqlspec.dialects.spanner
from sqlspec.dialects.spanner import _parsers

_ = sqlspec.dialects.spanner


def test_bigquery_generator_transforms_not_mutated() -> None:
    """Verify that BigQueryGenerator.TRANSFORMS does not have Spanner overrides."""
    property_transform = BigQueryGenerator.TRANSFORMS.get(exp.Property)
    assert property_transform is None or "spanner" not in getattr(property_transform, "__name__", "").lower()


def test_postgres_generator_transforms_not_mutated() -> None:
    """Verify that PostgresGenerator.TRANSFORMS does not have Spangres overrides."""
    property_transform = PostgresGenerator.TRANSFORMS.get(exp.Property)
    assert property_transform is None or "spangres" not in getattr(property_transform, "__name__", "").lower()


def test_bigquery_parser_property_parsers_not_mutated() -> None:
    """Verify that BigQueryParser.PROPERTY_PARSERS does not have Spanner property parsers."""
    assert "INTERLEAVE" not in BigQueryParser.PROPERTY_PARSERS
    assert BigQueryParser.PROPERTY_PARSERS["ROW"] is not _parsers._parse_row_deletion_policy
    assert BigQueryParser.PROPERTY_PARSERS["TTL"] is not _parsers._parse_ttl


def test_postgres_parser_property_parsers_not_mutated() -> None:
    """Verify that PostgresParser.PROPERTY_PARSERS does not have Spanner property parsers."""
    assert "INTERLEAVE" not in PostgresParser.PROPERTY_PARSERS
    assert PostgresParser.PROPERTY_PARSERS["ROW"] is not _parsers._parse_row_deletion_policy
    assert PostgresParser.PROPERTY_PARSERS["TTL"] is not _parsers._parse_ttl


def test_spanner_parsers_contain_spanner_property_parsers() -> None:
    """Verify that SpannerParser and SpangresParser contain Spanner property handlers."""
    assert _parsers.SpannerParser.PROPERTY_PARSERS["INTERLEAVE"] is _parsers._parse_interleave
    assert _parsers.SpannerParser.PROPERTY_PARSERS["ROW"] is _parsers._parse_row_deletion_policy
    assert _parsers.SpannerParser.PROPERTY_PARSERS["TTL"] is _parsers._parse_ttl
    assert _parsers.SpangresParser.PROPERTY_PARSERS["INTERLEAVE"] is _parsers._parse_interleave
    assert _parsers.SpangresParser.PROPERTY_PARSERS["ROW"] is _parsers._parse_row_deletion_policy
    assert _parsers.SpangresParser.PROPERTY_PARSERS["TTL"] is _parsers._parse_ttl
