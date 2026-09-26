"""Google Cloud Spanner SQL dialect (GoogleSQL variant).

Extends the BigQuery dialect with Spanner-only DDL features: ``INTERLEAVE IN
[PARENT]`` for interleaved tables and ``ROW DELETION POLICY`` for row-level
time-to-live policies. The PostgreSQL-interface ``TTL INTERVAL ... ON`` form
is accepted on parse and normalized to the canonical row deletion policy so
generation always emits valid GoogleSQL.
"""

from typing import TYPE_CHECKING, Any, cast

from sqlglot import exp
from sqlglot.dialects.bigquery import BigQuery
from sqlglot.tokenizer_core import TokenType

from sqlspec.dialects.spanner._generators import SpannerGenerator
from sqlspec.dialects.spanner._parsers import (
    SpannerParser,
    attach_create_property,
    attach_hints,
    extract_interleave_property,
    normalize_spanner_tokens,
)

if TYPE_CHECKING:
    from sqlglot.tokenizer_core import Token

__all__ = ("Spanner",)


class SpannerTokenizer(BigQuery.Tokenizer):
    """Tokenizer for Spanner GoogleSQL string literal escapes and brace hints."""

    STRING_ESCAPES = ["'", "\\"]
    KEYWORDS = {**BigQuery.Tokenizer.KEYWORDS, "FLOAT32": TokenType.FLOAT, "TOKENLIST": TokenType.USERDEFINED}

    def tokenize(self, sql: str) -> "list[Token]":
        """Tokenize Spanner SQL and normalize ``@{...}`` hints into token comments."""
        return normalize_spanner_tokens(super().tokenize(sql), sql)


class Spanner(BigQuery):
    """Google Cloud Spanner SQL dialect."""

    Tokenizer = SpannerTokenizer
    Parser = SpannerParser
    Generator = SpannerGenerator

    def parse(self, sql: str, **opts: Any) -> list[exp.Expr | None]:
        """Parse Spanner SQL statements, attaching hints and repairing CREATE TABLE statements."""
        expressions = super().parse(sql, **opts)
        if len(expressions) == 1 and isinstance(expressions[0], exp.Command):
            repaired_sql, interleave_property = extract_interleave_property(sql)
            if interleave_property is not None:
                reparsed = BigQuery.parse(self, repaired_sql, **opts)
                if len(reparsed) == 1 and isinstance(reparsed[0], exp.Create):
                    expressions = cast(
                        "list[exp.Expr | None]", [attach_create_property(reparsed[0], interleave_property)]
                    )

        for expression in expressions:
            if expression is not None:
                attach_hints(expression)
        return expressions

    def parse_into(self, expression_type: Any, sql: str, **opts: Any) -> list[exp.Expr | None]:
        """Parse into specific expression type with attached hints."""
        expressions = super().parse_into(expression_type, sql, **opts)
        for expression in expressions:
            if expression is not None:
                attach_hints(expression)
        return expressions
