"""General utility functions."""

import re
import unicodedata
from functools import lru_cache
from typing import Optional

# Compiled regex for slugify
_SLUGIFY_REMOVE_NON_ALPHANUMERIC = re.compile(r"[^\w]+", re.UNICODE)
_SLUGIFY_HYPHEN_COLLAPSE = re.compile(r"-+")

# Compiled regex for snake_case
# Insert underscore between lowercase/digit and uppercase letter
_SNAKE_CASE_LOWER_OR_DIGIT_TO_UPPER = re.compile(r"(?<=[a-z0-9])(?=[A-Z])", re.UNICODE)
# Insert underscore between uppercase letter and uppercase followed by lowercase
_SNAKE_CASE_UPPER_TO_UPPER_LOWER = re.compile(r"(?<=[A-Z])(?=[A-Z][a-z])", re.UNICODE)
# Replace hyphens, spaces, dots, and @ symbols with underscores for snake_case
_SNAKE_CASE_HYPHEN_SPACE = re.compile(r"[.\s@-]+", re.UNICODE)
# Collapse multiple underscores
_SNAKE_CASE_MULTIPLE_UNDERSCORES = re.compile(r"__+", re.UNICODE)

__all__ = ("camelize", "check_email", "slugify", "snake_case")


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
    value = value.lower().strip()
    sep = separator if separator is not None else "-"
    if not sep:
        # Remove all non-alphanumeric characters and return
        return _SLUGIFY_REMOVE_NON_ALPHANUMERIC.sub("", value)
    # Replace all runs of non-alphanumeric chars with the separator
    value = _SLUGIFY_REMOVE_NON_ALPHANUMERIC.sub(sep, value)
    # Remove leading/trailing separators and collapse multiple separators
    # For dynamic separators, we need to use re.sub with escaped separator
    if sep == "-":
        # Use pre-compiled regex for common case
        value = value.strip("-")
        return _SLUGIFY_HYPHEN_COLLAPSE.sub("-", value)
    # For other separators, use dynamic regex
    value = re.sub(rf"^{re.escape(sep)}+|{re.escape(sep)}+$", "", value)
    return re.sub(rf"{re.escape(sep)}+", sep, value)


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
    Handles Unicode letters and numbers.

    Args:
        string: The string to convert.

    Returns:
        The snake_case version of the string.
    """
    if not string:
        return ""
    # 1. Replace hyphens and spaces with underscores
    s = _SNAKE_CASE_HYPHEN_SPACE.sub("_", string)

    # 2. Remove all non-alphanumeric characters except underscores
    # TODO: move to a compiled regex at the top of the file
    s = re.sub(r"[^\w]+", "", s, flags=re.UNICODE)

    # 3. Insert an underscore between a lowercase/digit and an uppercase letter.
    #    e.g., "helloWorld" -> "hello_World"
    #    e.g., "Python3IsGreat" -> "Python3_IsGreat"
    #    Uses a positive lookbehind `(?<=[...])` and a positive lookahead `(?=[...])`
    s = _SNAKE_CASE_LOWER_OR_DIGIT_TO_UPPER.sub("_", s)

    # 4. Insert an underscore between an uppercase letter and another
    #    uppercase letter followed by a lowercase letter.
    #    e.g., "HTTPRequest" -> "HTTP_Request"
    #    This handles acronyms gracefully.
    s = _SNAKE_CASE_UPPER_TO_UPPER_LOWER.sub("_", s)

    # 5. Convert the entire string to lowercase.
    s = s.lower()

    # 6. Remove any leading or trailing underscores that might have been created.
    s = s.strip("_")

    # 7. Collapse multiple consecutive underscores into a single one.
    return _SNAKE_CASE_MULTIPLE_UNDERSCORES.sub("_", s)
