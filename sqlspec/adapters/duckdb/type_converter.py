"""DuckDB-specific type conversion with native UUID support.

Provides specialized type handling for DuckDB, including native UUID
support and standardized datetime formatting.
"""

from datetime import datetime
from typing import Any, Final
from uuid import UUID

from sqlspec.core.type_converter import CachedOutputConverter, convert_uuid, format_datetime_rfc3339

__all__ = ("DUCKDB_SPECIAL_CHARS", "DuckDBOutputConverter")

DUCKDB_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({"-", ":", "T", ".", "[", "{"})


class DuckDBOutputConverter(CachedOutputConverter):
    """DuckDB-specific output conversion with native UUID support.

    Extends CachedOutputConverter with DuckDB-specific functionality
    including native UUID handling and standardized datetime formatting.
    """

    __slots__ = ("_enable_uuid_conversion",)

    def __init__(self, cache_size: int = 5000, enable_uuid_conversion: bool = True) -> None:
        """Initialize converter with DuckDB-specific options.

        Args:
            cache_size: Maximum number of string values to cache (default: 5000)
            enable_uuid_conversion: Enable automatic UUID string conversion (default: True)
        """
        super().__init__(special_chars=DUCKDB_SPECIAL_CHARS, cache_size=cache_size)
        self._enable_uuid_conversion = enable_uuid_conversion

    def _convert_detected(self, value: str, detected_type: str) -> Any:
        """Convert value with DuckDB-specific UUID handling.

        Args:
            value: String value to convert.
            detected_type: Detected type name.

        Returns:
            Converted value, respecting UUID conversion setting.
        """
        if detected_type == "uuid" and not self._enable_uuid_conversion:
            return value
        try:
            return self.convert_value(value, detected_type)
        except Exception:
            return value

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
            return self.convert(value)

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
