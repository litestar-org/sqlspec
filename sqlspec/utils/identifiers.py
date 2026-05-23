"""SQL identifier validation helpers."""

import re
from typing import Final

__all__ = ("DEFAULT_MAX_IDENTIFIER_LENGTH", "validate_identifier")

_IDENTIFIER_PATTERN: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_MAX_IDENTIFIER_LENGTH: Final = 63


def validate_identifier(
    name: str,
    *,
    max_length: int = DEFAULT_MAX_IDENTIFIER_LENGTH,
    allow_schema_qualifier: bool = False,
    error_cls: type[Exception] = ValueError,
    label: str = "identifier",
) -> str:
    """Validate a SQL identifier and return it unchanged.

    Args:
        name: Identifier to validate.
        max_length: Maximum length per identifier segment.
        allow_schema_qualifier: Whether dotted schema-qualified identifiers are allowed.
        error_cls: Exception class raised when validation fails.
        label: Domain-specific label to use in error messages.

    Returns:
        The validated identifier.

    Raises:
        error_cls: If the identifier is empty, too long, schema-qualified when not allowed, or malformed.
    """
    label_lower = label.lower()
    label_title = label_lower.capitalize()
    if not name:
        msg = f"{label_title} cannot be empty"
        raise error_cls(msg)

    if not allow_schema_qualifier and "." in name:
        msg = f"Schema qualifier not allowed for {label_lower}: {name!r}"
        raise error_cls(msg)

    segments = name.split(".") if allow_schema_qualifier else [name]
    for segment in segments:
        if len(segment) > max_length:
            msg = f"{label_title} too long: {len(segment)} chars (max {max_length}) in {name!r}"
            raise error_cls(msg)
        if not _IDENTIFIER_PATTERN.match(segment):
            msg = (
                f"Invalid {label_lower}: {name!r}. "
                "Must start with letter/underscore and contain only alphanumeric characters and underscores"
            )
            raise error_cls(msg)
    return name
