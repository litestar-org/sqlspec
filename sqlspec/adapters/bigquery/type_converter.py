"""BigQuery-specific type conversion with UUID support.

Provides specialized type handling for BigQuery, including UUID support
for the native BigQuery driver.
"""

from functools import lru_cache
from typing import Any, Final
from uuid import UUID

from sqlspec.core import BaseTypeConverter, convert_uuid

try:
    from google.cloud.bigquery import ScalarQueryParameter
except ImportError:
    ScalarQueryParameter = None  # type: ignore[assignment,misc]

BQ_TYPE_MAP: Final[dict[str, str]] = {
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

BIGQUERY_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({"{", "[", "-", ":", "T", "."})


class BigQueryTypeConverter(BaseTypeConverter):
    """BigQuery-specific type conversion with UUID support.

    Extends the base TypeDetector with BigQuery-specific functionality
    including UUID parameter handling for the native BigQuery driver.
    Includes per-instance LRU cache for improved performance.
    """

    __slots__ = ("_convert_cache", "_enable_uuid_conversion")

    def __init__(self, cache_size: int = 5000, *, enable_uuid_conversion: bool = True) -> None:
        """Initialize converter with per-instance conversion cache.

        Args:
            cache_size: Maximum number of string values to cache (default: 5000)
            enable_uuid_conversion: Whether to enable automatic UUID conversion (default: True)
        """
        super().__init__()
        self._enable_uuid_conversion = enable_uuid_conversion

        @lru_cache(maxsize=cache_size)
        def _cached_convert(value: str) -> Any:
            if not value or not any(c in value for c in BIGQUERY_SPECIAL_CHARS):
                return value
            detected_type = self.detect_type(value)
            if detected_type:
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

    def create_parameter(self, name: str, value: Any) -> Any | None:
        """Create BigQuery parameter with proper type mapping.

        Args:
            name: Parameter name.
            value: Parameter value.

        Returns:
            ScalarQueryParameter for native BigQuery driver, None if not available.
        """
        if ScalarQueryParameter is None:
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
            return self.convert_if_detected(value)
        return value


__all__ = ("BIGQUERY_SPECIAL_CHARS", "BQ_TYPE_MAP", "BigQueryTypeConverter")
