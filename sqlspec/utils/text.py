"""Text processing utilities for SQLSpec.

Provides functions for string manipulation including case conversion,
slugification, and email validation. Used primarily for identifier
generation and data validation.
"""

import re
import unicodedata
from functools import lru_cache

__all__ = (
    "camelize",
    "kebabize",
    "normalize_identifier",
    "pascalize",
    "quote_backtick_identifier",
    "quote_identifier",
    "slugify",
    "snake_case",
)


_SLUGIFY_REMOVE_NON_ALPHANUMERIC = re.compile(r"[^\w]+", re.UNICODE)
_SLUGIFY_HYPHEN_COLLAPSE = re.compile(r"-+")

_SNAKE_CASE_LOWER_OR_DIGIT_TO_UPPER = re.compile(r"(?<=[a-z0-9])(?=[A-Z])", re.UNICODE)
_SNAKE_CASE_UPPER_TO_UPPER_LOWER = re.compile(r"(?<=[A-Z])(?=[A-Z][a-z])", re.UNICODE)
_SNAKE_CASE_HYPHEN_SPACE = re.compile(r"[.\s@-]+", re.UNICODE)
_SNAKE_CASE_REMOVE_NON_WORD = re.compile(r"[^\w]+", re.UNICODE)
_SNAKE_CASE_MULTIPLE_UNDERSCORES = re.compile(r"__+", re.UNICODE)
_MIN_QUOTED_IDENTIFIER_LENGTH = 2


def slugify(value: str, allow_unicode: bool = False, separator: str | None = None) -> str:
    """Convert a string to a URL-friendly slug.

    Args:
        value: The string to slugify
        allow_unicode: Allow unicode characters in slug.
        separator: Separator character for word boundaries. Defaults to "-".

    Returns:
        A slugified string.
    """
    if allow_unicode:
        value = unicodedata.normalize("NFKC", value)
    else:
        value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = value.lower().strip()
    sep = separator if separator is not None else "-"
    if not sep:
        return _SLUGIFY_REMOVE_NON_ALPHANUMERIC.sub("", value)
    value = _SLUGIFY_REMOVE_NON_ALPHANUMERIC.sub(sep, value)
    if sep == "-":
        value = value.strip("-")
        return _SLUGIFY_HYPHEN_COLLAPSE.sub("-", value)
    value = re.sub(rf"^{re.escape(sep)}+|{re.escape(sep)}+$", "", value)
    return re.sub(rf"{re.escape(sep)}+", sep, value)


@lru_cache(maxsize=100)
def camelize(string: str) -> str:
    """Convert a string to camel case.

    Args:
        string: The string to convert.

    Returns:
        The converted string.
    """
    return "".join(word if index == 0 else word.capitalize() for index, word in enumerate(string.split("_")))


@lru_cache(maxsize=100)
def kebabize(string: str) -> str:
    """Convert a string to kebab-case.

    Args:
        string: The string to convert.

    Returns:
        The kebab-case version of the string.
    """
    return "-".join(word.lower() for word in string.split("_") if word)


@lru_cache(maxsize=100)
def pascalize(string: str) -> str:
    """Convert a string to PascalCase.

    Args:
        string: The string to convert.

    Returns:
        The PascalCase version of the string.
    """
    return "".join(word.capitalize() for word in string.split("_") if word)


@lru_cache(maxsize=100)
def snake_case(string: str) -> str:
    """Convert a string to snake_case.

    Args:
        string: The string to convert.

    Returns:
        The snake_case version of the string.
    """
    if not string:
        return ""
    s = _SNAKE_CASE_HYPHEN_SPACE.sub("_", string)
    s = _SNAKE_CASE_REMOVE_NON_WORD.sub("", s)
    s = _SNAKE_CASE_LOWER_OR_DIGIT_TO_UPPER.sub("_", s)
    s = _SNAKE_CASE_UPPER_TO_UPPER_LOWER.sub("_", s)
    s = s.lower()
    s = s.strip("_")
    return _SNAKE_CASE_MULTIPLE_UNDERSCORES.sub("_", s)


def quote_identifier(identifier: str) -> str:
    """Quote a SQL identifier with double-quote escaping.

    Wraps the value in double quotes and escapes any embedded double quote
    per the SQL standard (``"`` -> ``""``). Used to safely interpolate
    identifier-shaped values (schemas, tables, columns) into SQL where
    bind parameters are not allowed (DDL identifiers, ``SET`` commands).

    Dialect-aware case-folding is handled by ``normalize_identifier``.

    Args:
        identifier: SQL identifier (schema, table, column, ...).

    Returns:
        Double-quoted, escape-safe identifier.
    """
    return '"' + identifier.replace('"', '""') + '"'


def normalize_identifier(identifier: str, dialect: str) -> str:
    """Normalize an identifier-shaped value for dialect metadata lookups.

    Args:
        identifier: SQL identifier supplied by the caller.
        dialect: SQL dialect name.

    Returns:
        Identifier in the form expected by the dialect's metadata tables.
    """
    value = identifier.strip()
    if len(value) >= _MIN_QUOTED_IDENTIFIER_LENGTH and value[0] == value[-1] == '"':
        return value[1:-1].replace('""', '"')
    if len(value) >= _MIN_QUOTED_IDENTIFIER_LENGTH and value[0] == value[-1] == "`":
        return value[1:-1].replace("``", "`")

    normalized_dialect = dialect.lower().replace("-", "_")
    if normalized_dialect in {"postgres", "postgresql", "cockroach", "cockroachdb"}:
        return value.lower()
    if normalized_dialect == "oracle" and value.islower():
        return value.upper()
    return value


def quote_backtick_identifier(identifier: str) -> str:
    """Quote a SQL identifier with backtick escaping (MySQL family).

    Wraps the value in backticks and escapes any embedded backtick by
    doubling it (`` ` `` -> `` `` ``). Used by MySQL-family adapters
    (asyncmy, aiomysql, mysqlconnector, pymysql) where the backtick is
    the dialect's identifier delimiter.

    Args:
        identifier: SQL identifier (schema, table, column, ...).

    Returns:
        Backtick-quoted, escape-safe identifier.
    """
    return "`" + identifier.replace("`", "``") + "`"
