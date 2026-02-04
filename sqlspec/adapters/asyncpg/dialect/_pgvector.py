"""PGVector dialect extending Postgres with vector distance operators.

Adds support for pgvector distance operators:
- <-> : L2 (Euclidean) distance (already in base Postgres)
- <#> : Negative inner product
- <=> : Cosine distance
- <+> : L1 (Taxicab/Manhattan) distance
- <~> : Hamming distance (binary vectors)
- <%> : Jaccard distance (binary vectors)
"""

from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType

__all__ = ("PGVector",)

# Use a single unused token type for all pgvector distance operators.
# The actual operator string is captured during parsing and stored in the expression.
# SQLGlot is not going to add extension operators, even as unused tokens, so this allows us
# to work around the limitation: https://github.com/tobymao/sqlglot/issues/6949
_PGVECTOR_DISTANCE_TOKEN = TokenType.CARET_AT


class VectorDistance(exp.Binary):
    """Vector distance operation that preserves the original operator."""

    arg_types = {"this": True, "expression": True, "operator": True}


class PGVectorTokenizer(Postgres.Tokenizer):
    """Tokenizer with pgvector distance operators."""

    KEYWORDS = {
        **Postgres.Tokenizer.KEYWORDS,
        "<#>": _PGVECTOR_DISTANCE_TOKEN,
        "<=>": _PGVECTOR_DISTANCE_TOKEN,
        "<+>": _PGVECTOR_DISTANCE_TOKEN,
        "<~>": _PGVECTOR_DISTANCE_TOKEN,
        "<%>": _PGVECTOR_DISTANCE_TOKEN,
    }


class PGVectorParser(Postgres.Parser):
    """Parser that captures the original operator string for pgvector operations."""

    FACTOR = {
        **Postgres.Parser.FACTOR,
        _PGVECTOR_DISTANCE_TOKEN: VectorDistance,
    }

    def _parse_factor(self) -> t.Optional[exp.Expression]:
        parse_method = self._parse_exponent if self.EXPONENT else self._parse_unary
        this = self._parse_at_time_zone(parse_method())

        while self._match_set(self.FACTOR):
            klass = self.FACTOR[self._prev.token_type]
            comments = self._prev_comments
            operator_text = self._prev.text
            expression = parse_method()

            if not expression and klass is exp.IntDiv and self._prev.text.isalpha():
                self._retreat(self._index - 1)
                return this

            if "operator" in klass.arg_types:
                this = self.expression(
                    klass, this=this, comments=comments, expression=expression, operator=operator_text
                )
            else:
                this = self.expression(klass, this=this, comments=comments, expression=expression)

            if isinstance(this, exp.Div):
                this.set("typed", self.dialect.TYPED_DIVISION)
                this.set("safe", self.dialect.SAFE_DIVISION)

        return this


class PGVectorGenerator(Postgres.Generator):
    """Generator that renders pgvector distance operators."""

    def vectordistance_sql(self, expression: VectorDistance) -> str:
        op = expression.args.get("operator", "<->")
        left = self.sql(expression, "this")
        right = self.sql(expression, "expression")
        return f"{left} {op} {right}"


class PGVector(Postgres):
    """PostgreSQL dialect with pgvector extension support."""

    Tokenizer = PGVectorTokenizer
    Parser = PGVectorParser
    Generator = PGVectorGenerator
