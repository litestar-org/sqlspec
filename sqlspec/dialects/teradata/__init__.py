"""Teradata Vantage dialect registered as ``vantage`` beside sqlglot's built-in ``teradata`` dialect.

The ``Vantage`` dialect inherits sqlglot's ``Teradata`` dialect and installs a per-instance
generator dispatch overlay, so neither sqlglot's ``teradata`` registry entry nor the shared
``TeradataGenerator`` class is modified. SQLSpec resolves ``teradata`` to ``vantage`` internally.
"""

from typing import Any

from sqlglot import TokenType, generator
from sqlglot.dialects.teradata import Teradata as UpstreamTeradata

from sqlspec.dialects.teradata._generators import teradata_dispatch

__all__ = ("Vantage", "VantageTokenizer")

_DISPATCH_ATTR = "_dispatch"


class VantageTokenizer(UpstreamTeradata.Tokenizer):
    """Tokenize Vantage byte and binary types without narrowing BYTEINT."""

    KEYWORDS = {
        **UpstreamTeradata.Tokenizer.KEYWORDS,
        "BYTEINT": TokenType.TINYINT,
        "VARBYTE": TokenType.VARBINARY,
        "BYTE": TokenType.BINARY,
        "BLOB": TokenType.BLOB,
    }


class Vantage(UpstreamTeradata):
    """Generate Vantage SQL through an isolated dispatch overlay."""

    Tokenizer = VantageTokenizer
    Generator = UpstreamTeradata.Generator

    def generator(self, **opts: Any) -> generator.Generator:
        """Create a generator with Vantage pagination and type handlers."""
        instance = UpstreamTeradata.Generator(dialect=self, **opts)
        setattr(instance, _DISPATCH_ATTR, teradata_dispatch(getattr(instance, _DISPATCH_ATTR)))
        return instance
