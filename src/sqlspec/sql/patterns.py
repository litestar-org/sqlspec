from __future__ import annotations

from sqlspec.types.protocols import SQLOperationType

try:
    import re2 as re  # pylance: ignore[reportMissingImports]
except ImportError:
    import re


# FIXME to be improved
VAR_REF = re.compile(
    # NOTE probably pg specific?
    r'(?P<dquote>"(""|[^"])+")|'
    # FIXME mysql/mariadb use backslash escapes
    r"(?P<squote>\'(\'\'|[^\'])*\')|"
    # NOTE beware of overlapping re
    r"(?P<lead>[^:]):(?P<var_name>\w+)(?=[^:]?)"
)
"""Pattern to identify colon-variables (aka _named_ style) in SQL code"""

# NOTE see comments above
VAR_REF_DOT = re.compile(
    r'(?P<dquote>"(""|[^"])+")|' r"(?P<squote>\'(\'\'|[^\'])*\')|" r"(?P<lead>[^:]):(?P<var_name>\w+\.\w+)(?=[^:]?)"
)
"""Pattern to identify colon-variables with a simple attribute in SQL code."""


QUERY_DEF = re.compile(r"--\s*name\s*:\s*")
"""Identifies name definition comments"""

QUERY_RECORD_DEF = re.compile(r"--\s*record_class\s*:\s*(\w+)\s*")
""" identifies record class definition comments"""
# FIXME this accepts "1st" but seems to reject "é"
QUERY_OPERATION_NAME = re.compile(r"^(\w+)(|\^|\$|!|<!|\*!|#)$")
"""extract a valid query name followed by an optional operation spec"""


BAD_PREFIX = re.compile(r"^\d")
"""forbid numbers as first character"""

SQL_COMMENT = re.compile(r"\s*--\s*(.*)$")
"""Get SQL comment contents"""

SQL_OPERATION_TYPES = {
    "<!": SQLOperationType.INSERT_RETURNING,
    "*!": SQLOperationType.INSERT_UPDATE_DELETE_MANY,
    "!": SQLOperationType.INSERT_UPDATE_DELETE,
    "#": SQLOperationType.SCRIPT,
    "^": SQLOperationType.SELECT_ONE,
    "$": SQLOperationType.SELECT_VALUE,
    "": SQLOperationType.SELECT,
}
"""map operation suffixes to their type"""

UNCOMMENT = re.compile(
    # single quote strings
    r"(?P<squote>\'(\'\'|[^\'])*\')|"
    # double quote strings
    r'(?P<dquote>"(""|[^"])+")|'
    # one-line comment
    r"(?P<oneline>--.*?$)|"
    # multi-line comment
    r"(?P<multiline>/\*.*?\*/)",
    re.DOTALL | re.MULTILINE,
)
"""extracting comments requires some kind of scanner"""
