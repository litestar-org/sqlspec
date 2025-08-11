"""General utility functions."""

import re
import unicodedata
from functools import lru_cache
from typing import Optional

_SLUGIFY_REMOVE_NON_ALPHANUMERIC = re.compile(r"[^\w]+", re.UNICODE)
_SLUGIFY_HYPHEN_COLLAPSE = re.compile(r"-+")

_SNAKE_CASE_LOWER_OR_DIGIT_TO_UPPER = re.compile(r"(?<=[a-z0-9])(?=[A-Z])", re.UNICODE)
_SNAKE_CASE_UPPER_TO_UPPER_LOWER = re.compile(r"(?<=[A-Z])(?=[A-Z][a-z])", re.UNICODE)
_SNAKE_CASE_HYPHEN_SPACE = re.compile(r"[.\s@-]+", re.UNICODE)
_SNAKE_CASE_REMOVE_NON_WORD = re.compile(r"[^\w]+", re.UNICODE)
_SNAKE_CASE_MULTIPLE_UNDERSCORES = re.compile(r"__+", re.UNICODE)

__all__ = ("camelize", "check_email", "slugify", "snake_case")


def check_email(email: str) -> str:
    """Validate an email address.

    Args:
        email: The email to validate.

    Raises:
        ValueError: If the email is invalid.

    Returns:
        The validated email.
    """
    if "@" not in email:
        msg = "Invalid email!"
        raise ValueError(msg)
    return email.lower()


def slugify(value: str, allow_unicode: bool = False, separator: Optional[str] = None) -> str:
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
