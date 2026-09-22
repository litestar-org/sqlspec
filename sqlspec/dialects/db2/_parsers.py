"""Db2 dialect parser."""

from sqlglot import exp, parser
from sqlglot.helper import seq_get

__all__ = ("DB2Parser",)


class DB2Parser(parser.Parser):
    """Parser for IBM Db2 SQL syntax."""

    FUNCTION_PARSERS = {**parser.Parser.FUNCTION_PARSERS, "POSSTR": lambda self: self._parse_posstr()}

    def _parse_posstr(self) -> exp.StrPosition:
        """Parse POSSTR(haystack, needle) into canonical exp.StrPosition."""
        args = self._parse_csv(self._parse_conjunction)
        this = seq_get(args, 0)
        substr = seq_get(args, 1)
        return exp.StrPosition(this=this, substr=substr)
