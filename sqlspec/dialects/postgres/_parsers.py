"""Compilable Parser and Tokenizer classes for PostgreSQL extension dialects.

These classes use the plain ``type`` metaclass and can be compiled by mypyc.
Generator and Dialect classes (which use custom metaclasses) remain in their
respective dialect modules.
"""

from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokens import TokenType  # type: ignore[attr-defined]  # pyright: ignore[reportPrivateImportUsage]

__all__ = (
    "ParadeDBParser",
    "ParadeDBTokenizer",
    "PGVectorParser",
    "PGVectorTokenizer",
    "SearchOperator",
    "VectorDistance",
)

# Use a single unused token type for all pgvector distance operators.
# The actual operator string is captured during parsing and stored in the expression.
# SQLGlot is not going to add extension operators, even as unused tokens, so this allows us
# to work around the limitation: https://github.com/tobymao/sqlglot/issues/6949
_PGVECTOR_DISTANCE_TOKEN = TokenType.CARET_AT

# ParadeDB search operators use a separate unused token type.
_PARADEDB_SEARCH_TOKEN = TokenType.DAT


class VectorDistance(exp.Expression, exp.Binary):
    """Vector distance operation that preserves the original operator."""

    arg_types = {"this": True, "expression": True, "operator": True}


class SearchOperator(exp.Expression, exp.Binary):
    """ParadeDB search operation that preserves the original operator."""

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


class PGVectorParser(PostgresParser):
    """Parser that captures the original operator string for pgvector operations."""

    FACTOR = {**PostgresParser.FACTOR, _PGVECTOR_DISTANCE_TOKEN: VectorDistance}

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


class ParadeDBTokenizer(PGVectorTokenizer):
    """Tokenizer with ParadeDB search operators and pgvector distance operators."""

    KEYWORDS = {
        **PGVectorTokenizer.KEYWORDS,
        "@@@": _PARADEDB_SEARCH_TOKEN,
        "&&&": _PARADEDB_SEARCH_TOKEN,
        "|||": _PARADEDB_SEARCH_TOKEN,
        "===": _PARADEDB_SEARCH_TOKEN,
        "###": _PARADEDB_SEARCH_TOKEN,
        "##": _PARADEDB_SEARCH_TOKEN,
        "##>": _PARADEDB_SEARCH_TOKEN,
    }


class ParadeDBParser(PGVectorParser):
    """Parser with ParadeDB search operators and pgvector distance operators."""

    FACTOR = {**PGVectorParser.FACTOR, _PARADEDB_SEARCH_TOKEN: SearchOperator}
