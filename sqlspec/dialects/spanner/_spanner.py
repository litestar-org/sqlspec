"""Google Cloud Spanner SQL dialect (GoogleSQL variant).

Extends the BigQuery dialect with Spanner-only DDL features: ``INTERLEAVE IN
[PARENT]`` for interleaved tables and ``ROW DELETION POLICY`` for row-level
time-to-live policies. The PostgreSQL-interface ``TTL INTERVAL ... ON`` form
is accepted on parse and normalized to the canonical row deletion policy so
generation always emits valid GoogleSQL.
"""

from typing import Any

from sqlglot import exp
from sqlglot.dialects.bigquery import BigQuery

from sqlspec.dialects.spanner._generators import SpannerGenerator
from sqlspec.dialects.spanner._parsers import (
    attach_create_property,
    extract_interleave_property,
    register_spanner_property_parsers,
)

__all__ = ("Spanner",)

register_spanner_property_parsers()


class Spanner(BigQuery):
    """Google Cloud Spanner SQL dialect."""

    Generator = SpannerGenerator

    class Tokenizer(BigQuery.Tokenizer):
        """Tokenizer for Spanner GoogleSQL string literal escapes."""

        STRING_ESCAPES = ["'", "\\"]

    def parse(self, sql: str, **opts: Any) -> "list[exp.Expr | None]":
        """Repair CREATE TABLE statements that sqlglot still falls back to Command for."""
        expressions = super().parse(sql, **opts)
        if len(expressions) != 1 or not isinstance(expressions[0], exp.Command):
            return expressions

        repaired_sql, interleave_property = extract_interleave_property(sql)
        if interleave_property is None:
            return expressions

        reparsed = BigQuery.parse(self, repaired_sql, **opts)
        if len(reparsed) != 1 or not isinstance(reparsed[0], exp.Create):
            return expressions

        return [attach_create_property(reparsed[0], interleave_property)]
