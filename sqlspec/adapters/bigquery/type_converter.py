"""BigQuery-specific type conversion with UUID support.

Provides specialized type handling for BigQuery, including UUID support
for both standard and ADBC drivers, with appropriate fallbacks for limitations.
"""

from typing import Any, Final, Optional
from uuid import UUID

from sqlspec._serialization import encode_json
from sqlspec.core.type_conversion import TypeDetector, convert_uuid

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


class BigQueryTypeConverter(TypeDetector):
    """BigQuery-specific type conversion with UUID support.

    Extends the base TypeDetector with BigQuery-specific functionality
    including UUID parameter handling for both standard and ADBC drivers.
    """

    __slots__ = ()

    def create_parameter(self, name: str, value: Any) -> Optional[Any]:
        """Create BigQuery parameter with proper type mapping.

        Args:
            name: Parameter name.
            value: Parameter value.

        Returns:
            ScalarQueryParameter for standard driver, None if not available.
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


class ADBCBigQueryTypeConverter(BigQueryTypeConverter):
    """ADBC-specific BigQuery type handling.

    Handles limitations of the ADBC BigQuery driver, particularly
    around STRUCT types which must be converted to JSON strings.
    """

    __slots__ = ()

    def handle_struct_as_json(self, value: dict[str, Any]) -> str:
        """ADBC converts STRUCT to String, handle as JSON.

        Args:
            value: Dictionary to convert to JSON.

        Returns:
            JSON string representation.
        """
        return encode_json(value, as_bytes=False)

    def create_adbc_parameter(self, name: str, value: Any) -> tuple[str, Any]:
        """Create ADBC-compatible parameter tuple.

        Args:
            name: Parameter name.
            value: Parameter value.

        Returns:
            Tuple of (name, converted_value) suitable for ADBC.
        """
        if isinstance(value, UUID):
            return (name, str(value))

        if isinstance(value, str):
            detected_type = self.detect_type(value)
            if detected_type == "uuid":
                uuid_obj = convert_uuid(value)
                return (name, str(uuid_obj))

        if isinstance(value, dict):
            # ADBC limitation: convert dict types to JSON strings
            return (name, self.handle_struct_as_json(value))

        if isinstance(value, list):
            # ADBC limitation: convert list types to JSON strings
            from sqlspec._serialization import encode_json

            return (name, encode_json(value, as_bytes=False))

        return (name, value)

    def handle_adbc_result(self, value: Any, expected_type: str) -> Any:
        """Handle ADBC result conversion with type expectations.

        Args:
            value: Result value from ADBC.
            expected_type: Expected type from schema.

        Returns:
            Converted value based on expected type.
        """
        if expected_type == "STRUCT" and isinstance(value, str):
            # ADBC returns STRUCTs as JSON strings, parse them back
            detected = self.detect_type(value)
            if detected == "json":
                return self.convert_value(value, detected)

        return self.convert_bigquery_value(value, expected_type)


__all__ = ("BQ_TYPE_MAP", "ADBCBigQueryTypeConverter", "BigQueryTypeConverter")
