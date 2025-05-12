"""General utility functions."""

import re
import unicodedata
from functools import lru_cache
from typing import Optional

# Compiled regex for slugify
_SLUGIFY_REMOVE_INVALID_CHARS_RE = re.compile(r"[^\w\s-]")
_SLUGIFY_COLLAPSE_SEPARATORS_RE = re.compile(r"[-\s]+")

# Compiled regex for snake_case
# Handles sequences like "HTTPRequest" -> "HTTP_Request" or "SSLError" -> "SSL_Error"
_SNAKE_CASE_RE_ACRONYM_SEQUENCE = re.compile(r"([A-Z\d]+)([A-Z][a-z])")
# Handles transitions like "camelCase" -> "camel_Case" or "PascalCase" -> "Pascal_Case" (partially)
_SNAKE_CASE_RE_LOWER_UPPER_TRANSITION = re.compile(r"([a-z\d])([A-Z])")
# Replaces hyphens, spaces, and dots with a single underscore
_SNAKE_CASE_RE_REPLACE_SEP = re.compile(r"[-\s.]+")
# Cleans up multiple consecutive underscores
_SNAKE_CASE_RE_CLEAN_MULTIPLE_UNDERSCORE = re.compile(r"__+")

__all__ = (
    "camelize",
    "check_email",
    "slugify",
    "snake_case",
)


def check_email(email: str) -> str:
    """Validate an email.

    Very simple email validation.

    Args:
        email (str): The email to validate.

    Raises:
        ValueError: If the email is invalid.

    Returns:
        str: The validated email.
    """
    if "@" not in email:
        msg = "Invalid email!"
        raise ValueError(msg)
    return email.lower()


def slugify(value: str, allow_unicode: bool = False, separator: Optional[str] = None) -> str:
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
    value = _SLUGIFY_REMOVE_INVALID_CHARS_RE.sub("", value.lower())
    if separator is not None:
        return _SLUGIFY_COLLAPSE_SEPARATORS_RE.sub("-", value).strip("-_").replace("-", separator)
    return _SLUGIFY_COLLAPSE_SEPARATORS_RE.sub("-", value).strip("-_")


@lru_cache(maxsize=100)
def camelize(string: str) -> str:
    """Convert a string to camel case.

    Args:
        string (str): The string to convert.

    Returns:
        str: The converted string.
    """
    return "".join(word if index == 0 else word.capitalize() for index, word in enumerate(string.split("_")))


@lru_cache(maxsize=100)
def snake_case(string: str) -> str:
    """Convert a string to snake_case.

    Handles CamelCase, PascalCase, strings with spaces, hyphens, or dots
    as separators, and ensures single underscores. It also correctly
    handles acronyms (e.g., "HTTPRequest" becomes "http_request").

    Args:
        string: The string to convert.

    Returns:
        The snake_case version of the string.
    """
    s = _SNAKE_CASE_RE_ACRONYM_SEQUENCE.sub(r"\1_\2", string)
    s = _SNAKE_CASE_RE_LOWER_UPPER_TRANSITION.sub(r"\1_\2", s)
    s = _SNAKE_CASE_RE_REPLACE_SEP.sub("_", s).lower()
    s = _SNAKE_CASE_RE_CLEAN_MULTIPLE_UNDERSCORE.sub("_", s)
    return s.strip("_")
