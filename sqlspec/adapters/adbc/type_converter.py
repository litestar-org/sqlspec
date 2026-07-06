"""ADBC-specific type conversion with multi-dialect support.

Provides specialized type handling for ADBC adapters, including dialect-aware
type conversion for different database backends (PostgreSQL, SQLite, DuckDB,
MySQL, BigQuery, Snowflake).
"""

from typing import Any

from sqlspec.utils.serializers import to_json

__all__ = ("ADBCOutputConverter", "get_adbc_type_converter")


class ADBCOutputConverter:
    """ADBC-specific parameter conversion with dialect awareness."""

    __slots__ = ("dialect",)

    def __init__(self, dialect: str) -> None:
        """Initialize with dialect-specific configuration.

        Args:
            dialect: Target database dialect (postgres, sqlite, duckdb, etc.)
        """
        self.dialect = dialect.lower()

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


def get_adbc_type_converter(dialect: str) -> ADBCOutputConverter:
    """Factory function to create dialect-specific ADBC type converter.

    Args:
        dialect: Database dialect name.

    Returns:
        Configured ADBCOutputConverter instance.
    """
    return ADBCOutputConverter(dialect)
