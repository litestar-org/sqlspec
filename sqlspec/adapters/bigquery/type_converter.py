"""BigQuery-specific type conversion with UUID support.

Provides specialized type handling for BigQuery, including UUID support
for the native BigQuery driver.
"""

from typing import Any, Final, Optional
from uuid import UUID

from sqlspec.core.type_conversion import BaseTypeConverter, convert_uuid

try:
    from google.cloud.bigquery import ScalarQueryParameter
except ImportError:
    ScalarQueryParameter = None  # type: ignore[assignment,misc]

# Enhanced BigQuery type mapping with UUID support
BQ_TYPE_MAP: Final[dict[str, str]] = {
    "str": "STRING",
    "int": "INT64",
    "float": "FLOAT64",
    "bool": "BOOL",
    "datetime": "DATETIME",
    "date": "DATE",
    "time": "TIME",
    "UUID": "STRING",  # UUID as STRING in BigQuery
    "uuid": "STRING",
    "Decimal": "NUMERIC",
    "bytes": "BYTES",
    "list": "ARRAY",
    "dict": "STRUCT",
}


class BigQueryTypeConverter(BaseTypeConverter):
    """BigQuery-specific type conversion with UUID support.

    Extends the base TypeDetector with BigQuery-specific functionality
    including UUID parameter handling for the native BigQuery driver.
    """

    __slots__ = ()

    def create_parameter(self, name: str, value: Any) -> Optional[Any]:
        """Create BigQuery parameter with proper type mapping.

        Args:
            name: Parameter name.
            value: Parameter value.

        Returns:
            ScalarQueryParameter for native BigQuery driver, None if not available.
        """
        if ScalarQueryParameter is None:
            return None

        if isinstance(value, UUID):
            return ScalarQueryParameter(name, "STRING", str(value))

        if isinstance(value, str):
            detected_type = self.detect_type(value)
            if detected_type == "uuid":
                uuid_obj = convert_uuid(value)
                return ScalarQueryParameter(name, "STRING", str(uuid_obj))

        # Handle other types
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
            # Try to detect if this is a special type
            detected_type = self.detect_type(value)
            if detected_type:
                try:
                    return self.convert_value(value, detected_type)
                except Exception:
                    # If conversion fails, return original value
                    return value

        return value


__all__ = ("BQ_TYPE_MAP", "BigQueryTypeConverter")
