"""PostgreSQL-specific type conversion for psqlpy adapter.

Combines output conversion (database results → Python) and input conversion
(Python params → PostgreSQL format) in a single module. Designed for mypyc
compilation with no nested functions.

Output conversion handles:
- PostgreSQL-specific types like intervals and arrays
- Standard type detection (UUID, JSON, datetime, etc.)

Input conversion handles:
- pgvector type handlers (placeholder for future support)
"""

import re
from typing import TYPE_CHECKING, Any, Final

from sqlspec.core.type_converter import CachedOutputConverter
from sqlspec.typing import PGVECTOR_INSTALLED
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from psqlpy import Connection

__all__ = ("PG_SPECIAL_CHARS", "PG_SPECIFIC_REGEX", "PostgreSQLOutputConverter", "register_pgvector")

logger = get_logger(__name__)

PG_SPECIFIC_REGEX: Final[re.Pattern[str]] = re.compile(
    r"^(?:"
    r"(?P<interval>(?:(?:\d+\s+(?:year|month|day|hour|minute|second)s?\s*)+)|(?:P(?:\d+Y)?(?:\d+M)?(?:\d+D)?(?:T(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?)?))|"
    r"(?P<pg_array>\{(?:[^{}]+|\{[^{}]*\})*\})"
    r")$",
    re.IGNORECASE,
)

PG_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({"{", "-", ":", "T", ".", "P", "[", "Y", "M", "D", "H", "S"})


class PostgreSQLOutputConverter(CachedOutputConverter):
    """PostgreSQL-specific output conversion with interval and array support.

    Extends CachedOutputConverter with PostgreSQL-specific functionality
    for interval and array type handling.
    """

    __slots__ = ()

    def __init__(self, cache_size: int = 5000) -> None:
        """Initialize converter with PostgreSQL-specific options.

        Args:
            cache_size: Maximum number of string values to cache (default: 5000)
        """
        super().__init__(special_chars=PG_SPECIAL_CHARS, cache_size=cache_size)

    def _convert_detected(self, value: str, detected_type: str) -> Any:
        """Convert value with PostgreSQL-specific handling.

        Args:
            value: String value to convert.
            detected_type: Detected type name.

        Returns:
            Converted value or original for PostgreSQL-specific types.
        """
        if detected_type in {"interval", "pg_array"}:
            return value
        try:
            return self.convert_value(value, detected_type)
        except Exception:
            return value

    def detect_type(self, value: str) -> "str | None":
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


def register_pgvector(connection: "Connection") -> None:
    """Register pgvector type handlers on psqlpy connection.

    Currently a placeholder for future implementation. The psqlpy library
    does not yet expose a type handler registration API compatible with
    pgvector's automatic conversion system.

    Args:
        connection: Psqlpy connection instance.

    Note:
        When psqlpy adds type handler support, this function will:
        - Register pgvector extension on the connection
        - Enable automatic NumPy array <-> PostgreSQL vector conversion
        - Support vector similarity search operations
    """
    if not PGVECTOR_INSTALLED:
        return
