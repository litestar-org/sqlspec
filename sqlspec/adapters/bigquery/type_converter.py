"""BigQuery-specific type conversion with native UUID support.

Provides specialized type handling for BigQuery, including UUID support
for the native BigQuery driver and parameter creation.
"""

from typing import Any
from uuid import UUID

from sqlspec.core.type_converter import CachedOutputConverter, convert_uuid

__all__ = ("BIGQUERY_SPECIAL_CHARS", "BQ_TYPE_MAP", "BigQueryOutputConverter")

BQ_TYPE_MAP: "dict[str, str]" = {
    "str": "STRING",
    "int": "INT64",
    "float": "FLOAT64",
    "bool": "BOOL",
    "datetime": "DATETIME",
    "date": "DATE",
    "time": "TIME",
    "UUID": "STRING",
    "uuid": "STRING",
    "Decimal": "NUMERIC",
    "bytes": "BYTES",
    "list": "ARRAY",
    "dict": "STRUCT",
}

BIGQUERY_SPECIAL_CHARS: "frozenset[str]" = frozenset({"{", "[", "-", ":", "T", "."})


class BigQueryOutputConverter(CachedOutputConverter):
    """BigQuery-specific output conversion with native UUID support.

    Extends CachedOutputConverter with BigQuery-specific functionality
    including UUID handling and parameter creation for the native BigQuery driver.
    """

    __slots__ = ("_enable_uuid_conversion",)

    def __init__(self, cache_size: int = 5000, enable_uuid_conversion: bool = True) -> None:
        """Initialize converter with BigQuery-specific options.

        Args:
            cache_size: Maximum number of string values to cache (default: 5000)
            enable_uuid_conversion: Enable automatic UUID string conversion (default: True)
        """
        super().__init__(special_chars=BIGQUERY_SPECIAL_CHARS, cache_size=cache_size)
        self._enable_uuid_conversion = enable_uuid_conversion

    def _convert_detected(self, value: str, detected_type: str) -> Any:
        """Convert value with BigQuery-specific handling.

        Args:
            value: String value to convert.
            detected_type: Detected type name.

        Returns:
            Converted value, respecting UUID conversion setting.
        """
        try:
            return self.convert_value(value, detected_type)
        except Exception:
            return value

    def create_parameter(self, name: str, value: Any) -> "Any | None":
        """Create BigQuery parameter with proper type mapping.

        Args:
            name: Parameter name.
            value: Parameter value.

        Returns:
            ScalarQueryParameter for native BigQuery driver, None if not available.
        """
        try:
            from google.cloud.bigquery import ScalarQueryParameter
        except ImportError:
            return None

        if self._enable_uuid_conversion:
            if isinstance(value, UUID):
                return ScalarQueryParameter(name, "STRING", str(value))

            if isinstance(value, str):
                detected_type = self.detect_type(value)
                if detected_type == "uuid":
                    uuid_obj = convert_uuid(value)
                    return ScalarQueryParameter(name, "STRING", str(uuid_obj))

        param_type = BQ_TYPE_MAP.get(type(value).__name__, "STRING")
        return ScalarQueryParameter(name, param_type, value)

    def convert_bigquery_value(self, value: Any, column_type: str) -> Any:
        """Convert BigQuery value based on column type.

        Args:
            value: Value to convert.
            column_type: BigQuery column type.

        Returns:
            Converted value appropriate for the column type.
        """
        if column_type == "STRING" and isinstance(value, str):
            return self.convert(value)
        return value
