"""DuckDB-specific type conversion with native UUID support.

Provides specialized type handling for DuckDB, including native UUID
support and UUID conversion control.
"""

from typing import Any

from typing_extensions import final

from sqlspec.core.type_converter import BaseTypeConverter

__all__ = ("DuckDBOutputConverter",)


@final
class DuckDBOutputConverter(BaseTypeConverter):
    """DuckDB-specific output conversion with native UUID support.

    Extends BaseTypeConverter with DuckDB-specific UUID handling.
    """

    __slots__ = ("_enable_uuid_conversion",)

    def __init__(self, enable_uuid_conversion: bool = True) -> None:
        """Initialize converter with DuckDB-specific options.

        Args:
            enable_uuid_conversion: Enable automatic UUID string conversion (default: True)
        """
        self._enable_uuid_conversion = enable_uuid_conversion

    def convert_if_detected(self, value: Any) -> Any:
        """Convert string values while respecting the UUID conversion flag."""
        if not self._enable_uuid_conversion and isinstance(value, str) and self.detect_type(value) == "uuid":
            return value
        return super().convert_if_detected(value)
