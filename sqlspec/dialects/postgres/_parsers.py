"""Compilable Parser and Expression classes for PostgreSQL extension dialects.

Parser subclasses and Expression types can be compiled by mypyc (sqlglot's
Parser has no __slots__).  Tokenizer subclasses cannot be compiled (Tokenizer
has __slots__) and remain in the dialect modules.
"""

from __future__ import annotations

from typing import ClassVar

from mypy_extensions import mypyc_attr
from sqlglot import exp
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokens import TokenType  # type: ignore[attr-defined]  # pyright: ignore[reportPrivateImportUsage]

__all__ = ("PGVectorParser", "ParadeDBParser", "SearchOperator", "VectorDistance")

# Use a single unused token type for all pgvector distance operators.
# The actual operator string is captured during parsing and stored in the expression.
# SQLGlot is not going to add extension operators, even as unused tokens, so this allows us
# to work around the limitation: https://github.com/tobymao/sqlglot/issues/6949
_PGVECTOR_DISTANCE_TOKEN = TokenType.CARET_AT

# ParadeDB search operators use a separate unused token type.
_PARADEDB_SEARCH_TOKEN = TokenType.DAT


@mypyc_attr(allow_interpreted_subclasses=True)
class VectorDistance(exp.Expression, exp.Binary):
    """Vector distance operation that preserves the original operator."""

    arg_types: ClassVar[dict[str, bool]] = {"this": True, "expression": True, "operator": True}


@mypyc_attr(allow_interpreted_subclasses=True)
class SearchOperator(exp.Expression, exp.Binary):
    """ParadeDB search operation that preserves the original operator."""

    arg_types: ClassVar[dict[str, bool]] = {"this": True, "expression": True, "operator": True}


@mypyc_attr(allow_interpreted_subclasses=True)
class PGVectorParser(PostgresParser):
    """Parser that captures the original operator string for pgvector operations."""

    FACTOR: ClassVar[dict[TokenType, type[exp.Binary]]] = {
        **PostgresParser.FACTOR,
        _PGVECTOR_DISTANCE_TOKEN: VectorDistance,
    }

    def _parse_factor(self) -> exp.Expr | None:
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
                    klass(this=this, expression=expression, operator=operator_text), comments=comments
                )
            else:
                this = self.expression(klass(this=this, expression=expression), comments=comments)

            if isinstance(this, exp.Div):
                this.set("typed", self.dialect.TYPED_DIVISION)
                this.set("safe", self.dialect.SAFE_DIVISION)

        return this


@mypyc_attr(allow_interpreted_subclasses=True)
class ParadeDBParser(PGVectorParser):
    """Parser with ParadeDB search operators and pgvector distance operators."""

    FACTOR: ClassVar[dict[TokenType, type[exp.Binary]]] = {
        **PGVectorParser.FACTOR,
        _PARADEDB_SEARCH_TOKEN: SearchOperator,
    }
