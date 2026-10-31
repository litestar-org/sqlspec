"""Sybase ASE and SAP SQL Anywhere sqlglot dialects.

Both dialects inherit from sqlglot's ``TSQL`` dialect with
``NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE`` and install
per-instance generator dispatch overlays (``sybase_dispatch`` and
``sqlanywhere_dispatch``) so shared sqlglot generator classes are never
mutated or subclassed.
"""

from typing import TYPE_CHECKING, Any

from sqlglot import TokenType, exp, generator
from sqlglot.dialects.dialect import NormalizationStrategy
from sqlglot.dialects.tsql import TSQL

from sqlspec.dialects.sybase._generators import sqlanywhere_dispatch, sybase_dispatch
from sqlspec.dialects.sybase._parsers import (
    normalize_sqlanywhere_expression,
    normalize_sqlanywhere_tokens,
    normalize_sybase_expression,
    normalize_sybase_tokens,
)

if TYPE_CHECKING:
    from sqlglot.tokenizer_core import Token

__all__ = ("SQLAnywhere", "SQLAnywhereTokenizer", "Sybase", "SybaseTokenizer")

_DISPATCH_ATTR = "_dispatch"


class SybaseTokenizer(TSQL.Tokenizer):
    """Tokenizer for SAP Adaptive Server Enterprise (Sybase ASE) SQL syntax."""

    KEYWORDS = {
        **TSQL.Tokenizer.KEYWORDS,
        "BIGDATETIME": TokenType.DATETIME,
        "BIGTIME": TokenType.TIME,
        "BLOB": TokenType.BLOB,
    }

    def tokenize(self, sql: str) -> "list[Token]":
        """Tokenize Sybase ASE SQL and normalize ASE-only token sequences."""
        return normalize_sybase_tokens(super().tokenize(sql))


class Sybase(TSQL):
    """SAP Adaptive Server Enterprise (Sybase ASE) SQL dialect."""

    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE

    Tokenizer = SybaseTokenizer
    Generator = TSQL.Generator

    def generator(self, **opts: Any) -> "generator.Generator":
        """Return a generator whose dispatch table carries the Sybase ASE handlers."""
        instance = TSQL.Generator(dialect=self, **opts)
        base_dispatch = getattr(instance, _DISPATCH_ATTR)
        setattr(instance, _DISPATCH_ATTR, sybase_dispatch(base_dispatch))
        return instance

    def parse(self, sql: str, **opts: Any) -> "list[exp.Expr | None]":
        """Parse Sybase ASE SQL and normalize ASE function nodes."""
        parsed = self.parser(**opts).parse(self.tokenize(sql), sql)
        return [normalize_sybase_expression(expr) if expr is not None else None for expr in parsed]

    def parse_into(self, expression_type: Any, sql: str, **opts: Any) -> "list[exp.Expr | None]":
        """Parse Sybase ASE SQL into a target expression type."""
        parsed = self.parser(**opts).parse_into(expression_type, self.tokenize(sql), sql)
        return [normalize_sybase_expression(expr) if expr is not None else None for expr in parsed]


class SQLAnywhereTokenizer(TSQL.Tokenizer):
    """Tokenizer for SAP SQL Anywhere SQL syntax."""

    KEYWORDS = {
        **TSQL.Tokenizer.KEYWORDS,
        "BLOB": TokenType.BLOB,
        "CURRENT DATE": TokenType.CURRENT_DATE,
        "CURRENT TIME": TokenType.CURRENT_TIME,
        "CURRENT TIMESTAMP": TokenType.CURRENT_TIMESTAMP,
        "CURRENT USER": TokenType.CURRENT_USER,
        "TIMESTAMP": TokenType.TIMESTAMP,
        "UNIQUEIDENTIFIER": TokenType.UUID,
    }

    def tokenize(self, sql: str) -> "list[Token]":
        """Tokenize SAP SQL Anywhere SQL and normalize SQL Anywhere token sequences."""
        return normalize_sqlanywhere_tokens(super().tokenize(sql))


class SQLAnywhere(TSQL):
    """SAP SQL Anywhere SQL dialect."""

    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE

    Tokenizer = SQLAnywhereTokenizer
    Generator = TSQL.Generator

    def generator(self, **opts: Any) -> "generator.Generator":
        """Return a generator whose dispatch table carries the SAP SQL Anywhere handlers."""
        instance = TSQL.Generator(dialect=self, **opts)
        base_dispatch = getattr(instance, _DISPATCH_ATTR)
        setattr(instance, _DISPATCH_ATTR, sqlanywhere_dispatch(base_dispatch))
        return instance

    def parse(self, sql: str, **opts: Any) -> "list[exp.Expr | None]":
        """Parse SAP SQL Anywhere SQL and normalize SQL Anywhere function nodes."""
        parsed = self.parser(**opts).parse(self.tokenize(sql), sql)
        return [normalize_sqlanywhere_expression(expr) if expr is not None else None for expr in parsed]

    def parse_into(self, expression_type: Any, sql: str, **opts: Any) -> "list[exp.Expr | None]":
        """Parse SAP SQL Anywhere SQL into a target expression type."""
        parsed = self.parser(**opts).parse_into(expression_type, self.tokenize(sql), sql)
        return [normalize_sqlanywhere_expression(expr) if expr is not None else None for expr in parsed]
