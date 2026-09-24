"""IBM Db2 sqlglot dialect.

The dialect renders through sqlglot's root generator with a per-instance
dispatch table that carries the Db2 handlers. Parsing removes Db2
select-statement tails from the token stream, parses with sqlglot's base
parser, and restores the tails and Db2-only function spellings on the
resulting AST, so shared sqlglot classes are never modified.
"""

from typing import TYPE_CHECKING, Any

from sqlglot import TokenType, exp, generator, tokens
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy

from sqlspec.dialects.db2._generators import db2_dispatch
from sqlspec.dialects.db2._parsers import (
    DB2_INVERSE_TIME_MAPPING,
    DB2_TIME_MAPPING,
    apply_statement_tails,
    normalize_db2_tokens,
    split_statement_tails,
)

if TYPE_CHECKING:
    from sqlglot.tokenizer_core import Token

__all__ = ("DB2", "DB2Tokenizer")


class DB2Tokenizer(tokens.Tokenizer):
    """Tokenizer for IBM Db2 SQL syntax."""

    IDENTIFIERS = ['"']
    STRING_ESCAPES = ["'"]

    KEYWORDS = {
        **tokens.Tokenizer.KEYWORDS,
        "BLOB": TokenType.BLOB,
        "VARCHAR_FORMAT": TokenType.VAR,
        "POSSTR": TokenType.VAR,
        "CURRENT TIMESTAMP": TokenType.CURRENT_TIMESTAMP,
        "CURRENT DATE": TokenType.CURRENT_DATE,
        "CURRENT TIME": TokenType.CURRENT_TIME,
        "CURRENT USER": TokenType.CURRENT_USER,
        "CURRENT SCHEMA": TokenType.CURRENT_SCHEMA,
    }

    def tokenize(self, sql: str) -> "list[Token]":
        """Tokenize Db2 SQL and normalise Db2-only token sequences.

        Args:
            sql: Db2 SQL text.

        Returns:
            Tokens ready for sqlglot's parser.
        """
        return normalize_db2_tokens(super().tokenize(sql))


class DB2(Dialect):
    """IBM Db2 SQL dialect."""

    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE
    NULL_ORDERING = "nulls_are_large"

    TIME_MAPPING = DB2_TIME_MAPPING
    INVERSE_TIME_MAPPING = DB2_INVERSE_TIME_MAPPING

    Tokenizer = DB2Tokenizer
    Generator = generator.Generator

    def generator(self, **opts: Any) -> "generator.Generator":
        """Return a root generator whose dispatch table carries the Db2 handlers.

        Args:
            **opts: sqlglot generator options.

        Returns:
            A generator that renders Db2 SQL.
        """
        instance = generator.Generator(dialect=self, **opts)
        instance._dispatch = db2_dispatch(instance._dispatch)  # pyright: ignore[reportPrivateUsage]
        return instance

    def parse(self, sql: str, **opts: Any) -> "list[exp.Expr | None]":
        """Parse Db2 SQL, including Db2 select-statement tails.

        Args:
            sql: Db2 SQL text.
            **opts: sqlglot parser options.

        Returns:
            One expression per statement.
        """
        tokens, tails = split_statement_tails(self.tokenize(sql))
        return apply_statement_tails(self.parser(**opts).parse(tokens, sql), tails)

    def parse_into(self, expression_type: Any, sql: str, **opts: Any) -> "list[exp.Expr | None]":
        """Parse Db2 SQL into a target expression type, including statement tails.

        Args:
            expression_type: Target sqlglot expression type or types.
            sql: Db2 SQL text.
            **opts: sqlglot parser options.

        Returns:
            One expression per statement.
        """
        tokens, tails = split_statement_tails(self.tokenize(sql))
        return apply_statement_tails(self.parser(**opts).parse_into(expression_type, tokens, sql), tails)
