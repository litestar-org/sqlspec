from __future__ import annotations

from sqlspec.types.protocols import StatementType

try:
    import re2 as re  # pyright: ignore[reportMissingImports,reportMissingTypeStubs]
except ImportError:
    import re


# FIXME to be improved
VAR_REF = re.compile(  # pyright: ignore[reportUnknownMemberType]
    r'(?P<dquote>"(""|[^"])+")|'  # Double-quoted strings
    r"(?P<squote>'(''|[^'])*')|"  # Single-quoted strings
    r"(?P<lead>[^:]):(?P<var_name>\w+)"  # Colon-variables
)
"""Pattern to identify colon-variables (aka _named_ style) in SQL code"""


QUERY_DEF = re.compile(r"--\s*name\s*:\s*")  # pyright: ignore[reportUnknownMemberType]
"""Identifies name definition comments"""

QUERY_RECORD_DEF = re.compile(r"--\s*record_class\s*:\s*(\w+)\s*")  # pyright: ignore[reportUnknownMemberType]
""" identifies record class definition comments"""
# FIXME this accepts "1st" but seems to reject "é"
QUERY_OPERATION_NAME = re.compile(r"^(\w+)(|\^|\$|!|<!|\*!|#|@)$")  # pyright: ignore[reportUnknownMemberType]
"""extract a valid query name followed by an optional operation spec"""


BAD_PREFIX = re.compile(r"^\d")  # pyright: ignore[reportUnknownMemberType]
"""forbid numbers as first character"""

SQL_COMMENT = re.compile(r"\s*--\s*(.*)$")  # pyright: ignore[reportUnknownMemberType]
"""Get SQL comment contents"""

SQL_OPERATION_TYPES = {
    "<!": StatementType.INSERT_UPDATE_DELETE_RETURNING,
    "*!": StatementType.INSERT_UPDATE_DELETE_MANY,
    "!": StatementType.INSERT_UPDATE_DELETE,
    "#": StatementType.SCRIPT,
    "^": StatementType.SELECT_ONE,
    "$": StatementType.SELECT_VALUE,
    "@": StatementType.BULK_SELECT,
    "": StatementType.SELECT,
}
"""map operation suffixes to their type"""

UNCOMMENT = re.compile(  # pyright: ignore[reportUnknownMemberType]
    r"(?P<squote>'(?:''|[^'])*')"  # Single-quoted strings
    r"|(?P<dquote>\"(?:[^\"]|\"\")*\")"  # Double-quoted strings
    r"|(?P<oneline>--[^\n]*)"  # One-line comments
    r"|(?P<multiline>/\*[\s\S]*?\*/)"  # Multiline comments
)
"""extracting comments requires some kind of scanner"""
