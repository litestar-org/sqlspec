"""PostgreSQL-specific type conversion for psqlpy adapter.

Provides specialized type handling for PostgreSQL databases, including
PostgreSQL-specific types like intervals and arrays while preserving
backward compatibility.
"""

import re
from typing import Any, Final, Optional

from sqlspec.core.type_conversion import BaseTypeConverter

# PostgreSQL-specific regex patterns for types not covered by base BaseTypeConverter
PG_SPECIFIC_REGEX: Final[re.Pattern[str]] = re.compile(
    r"^(?:"
    r"(?P<interval>(?:(?:\d+\s+(?:year|month|day|hour|minute|second)s?\s*)+)|(?:P(?:\d+Y)?(?:\d+M)?(?:\d+D)?(?:T(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?)?))|"
    r"(?P<pg_array>\{(?:[^{}]+|\{[^{}]*\})*\})"
    r")$",
    re.IGNORECASE,
)


class PostgreSQLTypeConverter(BaseTypeConverter):
    """PostgreSQL-specific type converter with interval and array support.

    Extends the base BaseTypeConverter with PostgreSQL-specific functionality
    while maintaining backward compatibility for interval and array types.
    """

    __slots__ = ()

    def detect_type(self, value: str) -> Optional[str]:
        """Detect types including PostgreSQL-specific types.

        Args:
            value: String value to analyze.

        Returns:
            Type name if detected, None otherwise.
        """
        # First try generic types (UUID, JSON, datetime, etc.)
        detected_type = super().detect_type(value)
        if detected_type:
            return detected_type

        # Then check PostgreSQL-specific types
        match = PG_SPECIFIC_REGEX.match(value)
        if match:
            for group_name in ["interval", "pg_array"]:
                if match.group(group_name):
                    return group_name

        return None

    def convert_value(self, value: str, detected_type: str) -> Any:
        """Convert value with PostgreSQL-specific handling.

        Args:
            value: String value to convert.
            detected_type: Detected type name.

        Returns:
            Converted value or original string for PostgreSQL-specific types.
        """
        # For PostgreSQL-specific types, preserve as strings for backward compatibility
        if detected_type in ("interval", "pg_array"):
            return value  # Pass through as strings - psqlpy will handle casting

        # Use base converter for standard types
        return super().convert_value(value, detected_type)


__all__ = ("PG_SPECIFIC_REGEX", "PostgreSQLTypeConverter")
