"""General utility functions."""

import logging
import re
import unicodedata
from typing import Any, Optional, Union

import sqlglot

__all__ = (
    "bind_parameters",
    "check_email",
    "slugify",
)

logger = logging.getLogger("sqlspec")


def check_email(email: str) -> str:
    """Validate an email."""
    if "@" not in email:
        msg = "Invalid email!"
        raise ValueError(msg)
    return email.lower()


def slugify(value: str, allow_unicode: bool = False, separator: "Optional[str]" = None) -> str:
    """Slugify.

    Convert to ASCII if ``allow_unicode`` is ``False``. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.

    Args:
        value (str): the string to slugify
        allow_unicode (bool, optional): allow unicode characters in slug. Defaults to False.
        separator (str, optional): by default a `-` is used to delimit word boundaries.
            Set this to configure something different.

    Returns:
        str: a slugified string of the value parameter
    """
    if allow_unicode:
        value = unicodedata.normalize("NFKC", value)
    else:
        value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w\s-]", "", value.lower())
    if separator is not None:
        return re.sub(r"[-\s]+", "-", value).strip("-_").replace("-", separator)
    return re.sub(r"[-\s]+", "-", value).strip("-_")


def bind_parameters(
    sql: str,
    parameters: Optional[Union[dict[str, Any], list[Any], tuple[Any, ...]]] = None,
    dialect: str = "generic",
) -> tuple[str, Optional[Union[dict[str, Any], list[Any], tuple[Any, ...]]]]:
    """Bind parameters to SQL using SQLGlot with fallback to original SQL/params.

    Args:
        sql: The SQL query string.
        parameters: The parameters to bind (dict, list, tuple, or None).
        dialect: The SQL dialect for parameter substitution.

    Returns:
        A tuple of (possibly rewritten SQL, parameters for driver).
    """
    if not parameters:
        return sql, None

    try:
        # For named parameters (dict)
        if isinstance(parameters, dict):
            bound_sql = sqlglot.transpile(sql, args=parameters, write=dialect)[0]
            return bound_sql, parameters  # Keep dict for drivers that need it
        # For positional parameters (list/tuple), just return as is for now
        # (could extend to support ? -> $1, $2, etc. if needed)
        return sql, parameters
    except Exception as e:
        logger.debug(f"SQLGlot parameter binding failed: {e}. Using original SQL and parameters.")
        return sql, parameters
