"""DuckDB-specific type conversion with native UUID support.

Provides specialized type handling for DuckDB, including native UUID
support and standardized datetime formatting.
"""

from datetime import datetime
from functools import lru_cache
from typing import Any, Final
from uuid import UUID

from sqlspec.core.type_conversion import BaseTypeConverter, convert_uuid, format_datetime_rfc3339

DUCKDB_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({"-", ":", "T", ".", "[", "{"})


class DuckDBTypeConverter(BaseTypeConverter):
    """DuckDB-specific type conversion with native UUID support.

    Extends the base TypeDetector with DuckDB-specific functionality
    including native UUID handling and standardized datetime formatting.
    Includes per-instance LRU cache for improved performance.
    """

    __slots__ = ("_convert_cache", "_enable_uuid_conversion")

    def __init__(self, cache_size: int = 5000, enable_uuid_conversion: bool = True) -> None:
        """Initialize converter with per-instance conversion cache.

        Args:
            cache_size: Maximum number of string values to cache (default: 5000)
            enable_uuid_conversion: Enable automatic UUID string conversion (default: True)
        """
        super().__init__()
        self._enable_uuid_conversion = enable_uuid_conversion

        @lru_cache(maxsize=cache_size)
        def _cached_convert(value: str) -> Any:
            if not value or not any(c in value for c in DUCKDB_SPECIAL_CHARS):
                return value
            detected_type = self.detect_type(value)
            if detected_type:
                if detected_type == "uuid" and not self._enable_uuid_conversion:
                    return value
                try:
                    return self.convert_value(value, detected_type)
                except Exception:
                    return value
            return value

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

    def handle_uuid(self, value: Any) -> Any:
        """Handle UUID conversion for DuckDB.

        Args:
            value: Value that might be a UUID.

        Returns:
            UUID object if value is UUID-like and conversion enabled, original value otherwise.
        """
        if isinstance(value, UUID):
            return value

        if isinstance(value, str) and self._enable_uuid_conversion:
            detected_type = self.detect_type(value)
            if detected_type == "uuid":
                return convert_uuid(value)

        return value

    def format_datetime(self, dt: datetime) -> str:
        """Standardized datetime formatting for DuckDB.

        Args:
            dt: datetime object to format.

        Returns:
            RFC 3339 formatted datetime string.
        """
        return format_datetime_rfc3339(dt)

    def convert_duckdb_value(self, value: Any) -> Any:
        """Convert value with DuckDB-specific handling.

        Args:
            value: Value to convert.

        Returns:
            Converted value appropriate for DuckDB.
        """
        if isinstance(value, (str, UUID)):
            uuid_value = self.handle_uuid(value)
            if isinstance(uuid_value, UUID):
                return uuid_value

        if isinstance(value, str):
            return self.convert_if_detected(value)

        if isinstance(value, datetime):
            return self.format_datetime(value)

        return value

    def prepare_duckdb_parameter(self, value: Any) -> Any:
        """Prepare parameter for DuckDB execution.

        Args:
            value: Parameter value to prepare.

        Returns:
            Value ready for DuckDB parameter binding.
        """
        converted = self.convert_duckdb_value(value)
        if isinstance(converted, UUID):
            return converted
        return converted


__all__ = ("DUCKDB_SPECIAL_CHARS", "DuckDBTypeConverter")
