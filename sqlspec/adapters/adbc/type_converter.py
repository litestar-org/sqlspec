"""ADBC-specific type conversion with multi-dialect support.

Provides specialized type handling for ADBC adapters, including dialect-aware
type conversion for different database backends (PostgreSQL, SQLite, DuckDB,
MySQL, BigQuery, Snowflake).
"""

from typing import Any

from sqlspec.core.type_converter import BaseTypeConverter
from sqlspec.utils.serializers import to_json

__all__ = ("ADBCOutputConverter", "get_adbc_type_converter")

# Native type support by dialect
_NATIVE_SUPPORT: "dict[str, list[str]]" = {
    "postgres": ["uuid", "json", "interval", "pg_array"],
    "postgresql": ["uuid", "json", "interval", "pg_array"],
    "pgvector": ["uuid", "json", "interval", "pg_array"],
    "paradedb": ["uuid", "json", "interval", "pg_array"],
    "duckdb": ["uuid", "json"],
    "bigquery": ["json"],
    "sqlite": [],
    "mysql": ["json"],
    "snowflake": ["json"],
}


class ADBCOutputConverter(BaseTypeConverter):
    """ADBC-specific output conversion with dialect awareness.

    Extends BaseTypeConverter with ADBC multi-backend functionality
    including dialect-specific type handling for different database systems.
    """

    __slots__ = ("dialect",)

    def __init__(self, dialect: str) -> None:
        """Initialize with dialect-specific configuration.

        Args:
            dialect: Target database dialect (postgres, sqlite, duckdb, etc.)
        """
        self.dialect = dialect.lower()

    def convert_if_detected(self, value: Any) -> Any:
        """Convert string values with dialect-specific handling."""
        if self.dialect == "sqlite" and isinstance(value, str) and self.detect_type(value) == "uuid":
            return str(value)
        return super().convert_if_detected(value)

    def convert_dict(self, value: "dict[str, Any]") -> Any:
        """Convert dictionary values with dialect-specific handling.

        Args:
            value: Dictionary to convert.

        Returns:
            Converted value appropriate for the dialect.
        """
        if self.dialect in {"postgres", "postgresql", "pgvector", "paradedb", "bigquery"}:
            return to_json(value)
        return value

    def supports_native_type(self, type_name: str) -> bool:
        """Check if dialect supports native handling of a type.

        Args:
            type_name: Type name to check

        Returns:
            True if dialect supports native handling, False otherwise.
        """
        return type_name in _NATIVE_SUPPORT.get(self.dialect, [])

    def get_dialect_specific_converter(self, value: Any, target_type: str) -> Any:
        """Apply dialect-specific conversion logic.

        Args:
            value: Value to convert.
            target_type: Target type for conversion.

        Returns:
            Converted value according to dialect requirements.
        """
        if self.dialect == "sqlite" and target_type == "uuid":
            return str(value)
        if self.dialect == "bigquery" and target_type == "uuid":
            return str(self.convert_value(value, target_type))
        return self.convert_value(value, target_type)


def get_adbc_type_converter(dialect: str) -> ADBCOutputConverter:
    """Factory function to create dialect-specific ADBC type converter.

    Args:
        dialect: Database dialect name.

    Returns:
        Configured ADBCOutputConverter instance.
    """
    return ADBCOutputConverter(dialect)
