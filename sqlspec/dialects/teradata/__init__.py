"""Teradata Vantage dialect with immutable pagination generation."""

from typing import Any

from sqlglot import TokenType, generator
from sqlglot.dialects.dialect import Dialect
from sqlglot.dialects.teradata import Teradata as UpstreamTeradata

from sqlspec.dialects.teradata._generators import teradata_dispatch

__all__ = ("Teradata", "TeradataTokenizer")

_DISPATCH_ATTR = "_dispatch"
_CLASSES_ATTR = "_classes"


class TeradataTokenizer(UpstreamTeradata.Tokenizer):
    """Tokenize Vantage byte and binary types without narrowing BYTEINT."""

    KEYWORDS = {
        **UpstreamTeradata.Tokenizer.KEYWORDS,
        "BYTEINT": TokenType.TINYINT,
        "VARBYTE": TokenType.VARBINARY,
        "BYTE": TokenType.BINARY,
        "BLOB": TokenType.BLOB,
    }


class Teradata(UpstreamTeradata):
    """Generate Vantage SQL through an isolated dispatch overlay."""

    Tokenizer = TeradataTokenizer
    Generator = UpstreamTeradata.Generator

    def generator(self, **opts: Any) -> generator.Generator:
        """Create a generator with Vantage pagination and type handlers."""
        instance = UpstreamTeradata.Generator(dialect=self, **opts)
        setattr(instance, _DISPATCH_ATTR, teradata_dispatch(getattr(instance, _DISPATCH_ATTR)))
        return instance


getattr(Dialect, _CLASSES_ATTR)["teradata"] = Teradata
