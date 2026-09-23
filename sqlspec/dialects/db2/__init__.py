"""IBM Db2 sqlglot dialect."""

from sqlglot import TokenType, tokens
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy

from sqlspec.dialects.db2._generators import DB2Generator
from sqlspec.dialects.db2._parsers import DB2Parser

__all__ = ("DB2", "DB2Generator", "DB2Parser", "DB2Tokenizer")


class DB2Tokenizer(tokens.Tokenizer):
    """Tokenizer for IBM Db2 SQL syntax."""

    IDENTIFIERS = ['"']
    STRING_ESCAPES = ["'"]

    KEYWORDS = {
        **tokens.Tokenizer.KEYWORDS,
        "DECFLOAT": getattr(TokenType, "DECFLOAT", TokenType.VAR),
        "DBCLOB": TokenType.TEXT,
        "VARCHAR_FORMAT": TokenType.VAR,
        "POSSTR": TokenType.VAR,
    }


class DB2(Dialect):
    """IBM Db2 SQL dialect."""

    NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE

    TIME_MAPPING = {
        "YYYY": "%Y",
        "YY": "%y",
        "MM": "%m",
        "DD": "%d",
        "HH24": "%H",
        "HH12": "%I",
        "HH": "%I",
        "MI": "%M",
        "SS": "%S",
        "NNNNNN": "%f",
        "SSSSSS": "%f",
    }

    Tokenizer = DB2Tokenizer
    Parser = DB2Parser
    Generator = DB2Generator
