"""DuckDB-specific type conversion with native UUID support.

Provides specialized type handling for DuckDB, including native UUID
support and standardized datetime formatting.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlspec.core.type_conversion import TypeDetector, convert_uuid, format_datetime_rfc3339


class DuckDBTypeConverter(TypeDetector):
    """DuckDB-specific type conversion with native UUID support.

    Extends the base TypeDetector with DuckDB-specific functionality
    including native UUID handling and standardized datetime formatting.
    """

    __slots__ = ()

    def handle_uuid(self, value: Any) -> Any:
        """Handle UUID conversion for DuckDB.

        Args:
            value: Value that might be a UUID.

        Returns:
            UUID object if value is UUID-like, original value otherwise.
        """
        if isinstance(value, UUID):
            return value  # DuckDB supports UUID natively

        if isinstance(value, str):
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
        # Handle UUIDs
        if isinstance(value, (str, UUID)):
            uuid_value = self.handle_uuid(value)
            if isinstance(uuid_value, UUID):
                return uuid_value

        # Handle other string types
        if isinstance(value, str):
            detected_type = self.detect_type(value)
            if detected_type:
                try:
                    return self.convert_value(value, detected_type)
                except Exception:
                    # If conversion fails, return original value
                    return value

        # Handle datetime formatting
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
        # DuckDB can handle most Python types natively
        converted = self.convert_duckdb_value(value)

        # Ensure UUIDs are properly handled
        if isinstance(converted, UUID):
            return converted  # DuckDB native UUID support

        return converted


__all__ = ("DuckDBTypeConverter",)
