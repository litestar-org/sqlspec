"""Db2 dialect parser."""

from typing import Any

from sqlglot import exp, parser
from sqlglot.helper import seq_get

__all__ = ("DB2Parser", "register_db2_function_parsers")


def _parse_posstr(self: Any) -> exp.StrPosition:
    """Parse POSSTR(haystack, needle) into canonical exp.StrPosition."""
    args = self._parse_csv(self._parse_conjunction)
    this = seq_get(args, 0)
    substr = seq_get(args, 1)
    return exp.StrPosition(this=this, substr=substr)


def register_db2_function_parsers() -> None:
    """Register Db2 function parsers idempotently."""
    parser.Parser.FUNCTION_PARSERS["POSSTR"] = _parse_posstr


register_db2_function_parsers()

DB2Parser: type[parser.Parser] = parser.Parser
