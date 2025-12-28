"""ADBC-specific type conversion with multi-dialect support.

Provides specialized type handling for ADBC adapters, including dialect-aware
type conversion for different database backends (PostgreSQL, SQLite, DuckDB,
MySQL, BigQuery, Snowflake).
"""

from functools import lru_cache
from typing import Any, Final

from sqlspec.core import BaseTypeConverter
from sqlspec.utils.serializers import to_json

ADBC_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({"{", "[", "-", ":", "T", "."})


class ADBCTypeConverter(BaseTypeConverter):
    """ADBC-specific type converter with dialect awareness.

    Extends the base BaseTypeConverter with ADBC multi-backend functionality
    including dialect-specific type handling for different database systems.
    Includes per-instance LRU cache for improved performance.
    """

    __slots__ = ("_convert_cache", "dialect")

    def __init__(self, dialect: str, cache_size: int = 5000) -> None:
        """Initialize with dialect-specific configuration and conversion cache.

        Args:
            dialect: Target database dialect (postgres, sqlite, duckdb, etc.)
            cache_size: Maximum number of string values to cache (default: 5000)
        """
        super().__init__()
        self.dialect = dialect.lower()

        @lru_cache(maxsize=cache_size)
        def _cached_convert(value: str) -> Any:
            if not value or not any(c in value for c in ADBC_SPECIAL_CHARS):
                return value
            detected_type = self.detect_type(value)
            if detected_type:
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
            return value

        self._convert_cache = _cached_convert

    def convert_if_detected(self, value: Any) -> Any:
        """Convert value with dialect-specific handling (cached).

        Args:
            value: Value to potentially convert.

        Returns:
            Converted value if special type detected, original value otherwise.
        """
        if not isinstance(value, str):
            return value
        return self._convert_cache(value)

    def convert_dict(self, value: dict[str, Any]) -> Any:
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
        native_support: dict[str, list[str]] = {
            "postgres": ["uuid", "json", "interval", "pg_array"],
            "postgresql": ["uuid", "json", "interval", "pg_array"],
            "duckdb": ["uuid", "json"],
            "bigquery": ["json"],
            "sqlite": [],
            "mysql": ["json"],
            "snowflake": ["json"],
        }
        return type_name in native_support.get(self.dialect, [])

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


def get_adbc_type_converter(dialect: str, cache_size: int = 5000) -> ADBCTypeConverter:
    """Factory function to create dialect-specific ADBC type converter.

    Args:
        dialect: Database dialect name.
        cache_size: Maximum number of string values to cache (default: 5000)

    Returns:
        Configured ADBCTypeConverter instance.
    """
    return ADBCTypeConverter(dialect, cache_size)


__all__ = ("ADBC_SPECIAL_CHARS", "ADBCTypeConverter", "get_adbc_type_converter")
