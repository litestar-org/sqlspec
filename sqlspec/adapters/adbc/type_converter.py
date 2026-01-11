"""ADBC-specific type conversion with multi-dialect support.

Provides specialized type handling for ADBC adapters, including dialect-aware
type conversion for different database backends (PostgreSQL, SQLite, DuckDB,
MySQL, BigQuery, Snowflake).
"""

from typing import Any

from sqlspec.core.type_converter import CachedOutputConverter
from sqlspec.utils.serializers import to_json

__all__ = ("ADBC_SPECIAL_CHARS", "ADBCOutputConverter", "get_adbc_type_converter")

ADBC_SPECIAL_CHARS: "frozenset[str]" = frozenset({"{", "[", "-", ":", "T", "."})

# Native type support by dialect
_NATIVE_SUPPORT: "dict[str, list[str]]" = {
    "postgres": ["uuid", "json", "interval", "pg_array"],
    "postgresql": ["uuid", "json", "interval", "pg_array"],
    "duckdb": ["uuid", "json"],
    "bigquery": ["json"],
    "sqlite": [],
    "mysql": ["json"],
    "snowflake": ["json"],
}


class ADBCOutputConverter(CachedOutputConverter):
    """ADBC-specific output conversion with dialect awareness.

    Extends CachedOutputConverter with ADBC multi-backend functionality
    including dialect-specific type handling for different database systems.
    """

    __slots__ = ("dialect",)

    def __init__(self, dialect: str, cache_size: int = 5000) -> None:
        """Initialize with dialect-specific configuration.

        Args:
            dialect: Target database dialect (postgres, sqlite, duckdb, etc.)
            cache_size: Maximum number of string values to cache (default: 5000)
        """
        super().__init__(special_chars=ADBC_SPECIAL_CHARS, cache_size=cache_size)
        self.dialect = dialect.lower()

    def _convert_detected(self, value: str, detected_type: str) -> Any:
        """Convert value with dialect-specific handling.

        Args:
            value: String value to convert.
            detected_type: Detected type name.

        Returns:
            Converted value according to dialect requirements.
        """
        try:
            if self.dialect in {"postgres", "postgresql"}:
                if detected_type in {"uuid", "interval"}:
                    return self.convert_value(value, detected_type)
            elif self.dialect == "duckdb":
                if detected_type == "uuid":
                    return self.convert_value(value, detected_type)
            elif self.dialect == "sqlite":
                if detected_type == "uuid":
                    return str(value)
            elif self.dialect == "bigquery":
                if detected_type == "uuid":
                    return self.convert_value(value, detected_type)
            elif self.dialect in {"mysql", "snowflake"} and detected_type in {"uuid", "json"}:
                return self.convert_value(value, detected_type)
            return self.convert_value(value, detected_type)
        except Exception:
            return value

    def convert_dict(self, value: "dict[str, Any]") -> Any:
        """Convert dictionary values with dialect-specific handling.

        Args:
            value: Dictionary to convert.

        Returns:
            Converted value appropriate for the dialect.
        """
        if self.dialect in {"postgres", "postgresql", "bigquery"}:
            return to_json(value)
        return value

    def supports_native_type(self, type_name: str) -> bool:
        """Check if dialect supports native handling of a type.

        Args:
            type_name: Type name to check (e.g., 'uuid', 'json')

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
        if self.dialect in {"postgres", "postgresql"}:
            if target_type in {"uuid", "json", "interval"}:
                return self.convert_value(value, target_type)
        elif self.dialect == "duckdb":
            if target_type in {"uuid", "json"}:
                return self.convert_value(value, target_type)
        elif self.dialect == "sqlite":
            if target_type == "uuid":
                return str(value)
            if target_type == "json":
                return self.convert_value(value, target_type)
        elif self.dialect == "bigquery":
            if target_type == "uuid":
                return str(self.convert_value(value, target_type))
            if target_type == "json":
                return self.convert_value(value, target_type)
        return self.convert_value(value, target_type)


def get_adbc_type_converter(dialect: str, cache_size: int = 5000) -> ADBCOutputConverter:
    """Factory function to create dialect-specific ADBC type converter.

    Args:
        dialect: Database dialect name.
        cache_size: Maximum number of string values to cache (default: 5000)

    Returns:
        Configured ADBCOutputConverter instance.
    """
    return ADBCOutputConverter(dialect, cache_size)
