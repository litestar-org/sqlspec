"""PostgreSQL-specific type conversion for psqlpy adapter.

Provides specialized type handling for PostgreSQL databases, including
PostgreSQL-specific types like intervals and arrays while preserving
backward compatibility.
"""

import re
from functools import lru_cache
from typing import Any, Final, Optional

from sqlspec.core.type_conversion import BaseTypeConverter

PG_SPECIFIC_REGEX: Final[re.Pattern[str]] = re.compile(
    r"^(?:"
    r"(?P<interval>(?:(?:\d+\s+(?:year|month|day|hour|minute|second)s?\s*)+)|(?:P(?:\d+Y)?(?:\d+M)?(?:\d+D)?(?:T(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?)?))|"
    r"(?P<pg_array>\{(?:[^{}]+|\{[^{}]*\})*\})"
    r")$",
    re.IGNORECASE,
)

PG_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({"{", "-", ":", "T", ".", "P", "[", "Y", "M", "D", "H", "S"})


class PostgreSQLTypeConverter(BaseTypeConverter):
    """PostgreSQL-specific type converter with interval and array support.

    Extends the base BaseTypeConverter with PostgreSQL-specific functionality
    while maintaining backward compatibility for interval and array types.
    Includes per-instance LRU cache for improved performance.
    """

    __slots__ = ("_convert_cache",)

    def __init__(self, cache_size: int = 5000) -> None:
        """Initialize converter with per-instance conversion cache.

        Args:
            cache_size: Maximum number of string values to cache (default: 5000)
        """
        super().__init__()

        @lru_cache(maxsize=cache_size)
        def _cached_convert(value: str) -> Any:
            if not value or not any(c in value for c in PG_SPECIAL_CHARS):
                return value
            detected_type = self.detect_type(value)
            return self.convert_value(value, detected_type) if detected_type else value

        self._convert_cache = _cached_convert

    def convert_if_detected(self, value: Any) -> Any:
        """Convert string if special type detected (cached).

        Args:
            value: Value to potentially convert

        Returns:
            Converted value or original value
        """
        if not isinstance(value, str):
            return value
        return self._convert_cache(value)

    def detect_type(self, value: str) -> Optional[str]:
        """Detect types including PostgreSQL-specific types.

        Args:
            value: String value to analyze.

        Returns:
            Type name if detected, None otherwise.
        """
        detected_type = super().detect_type(value)
        if detected_type:
            return detected_type

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
        if detected_type in {"interval", "pg_array"}:
            return value

        return super().convert_value(value, detected_type)


__all__ = ("PG_SPECIAL_CHARS", "PG_SPECIFIC_REGEX", "PostgreSQLTypeConverter")
