"""ADBC-specific type conversion with multi-dialect support.

Provides specialized type handling for ADBC adapters, including dialect-aware
type conversion for different database backends (PostgreSQL, SQLite, DuckDB,
MySQL, BigQuery, Snowflake).
"""

from typing import TYPE_CHECKING, Any, Final

from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ("ADBCOutputConverter", "get_adbc_type_converter")

_JSON_DICT_DIALECTS: Final[frozenset[str]] = frozenset({
    "postgres",
    "postgresql",
    "pgvector",
    "paradedb",
    "pg_textsearch",
    "pgtextsearch",
    "bigquery",
})


class ADBCOutputConverter:
    """ADBC-specific parameter conversion with dialect awareness."""

    __slots__ = ("_json_serializer", "dialect")

    def __init__(self, dialect: str, *, json_serializer: "Callable[[Any], str]" = to_json) -> None:
        """Initialize with dialect-specific configuration.

        Args:
            dialect: Target database dialect (postgres, sqlite, duckdb, etc.)
            json_serializer: JSON serializer callable for dictionary parameters.
        """
        self.dialect = dialect.lower()
        self._json_serializer = json_serializer

    def convert_dict(self, value: "dict[str, Any]", *, json_serializer: "Callable[[Any], str] | None" = None) -> Any:
        """Convert dictionary values with dialect-specific handling.

        Args:
            value: Dictionary to convert.
            json_serializer: Optional per-call JSON serializer override.

        Returns:
            Converted value appropriate for the dialect.
        """
        if self.dialect in _JSON_DICT_DIALECTS:
            serializer = json_serializer or self._json_serializer
            return serializer(value)
        return {
            key: self.convert_dict(val, json_serializer=json_serializer)
            if isinstance(val, dict)
            else self.convert_sequence(val, json_serializer=json_serializer)
            if isinstance(val, (list, tuple))
            else val
            for key, val in value.items()
        }

    def convert_sequence(
        self, value: "list[Any] | tuple[Any, ...]", *, json_serializer: "Callable[[Any], str] | None" = None
    ) -> "list[Any]":
        """Convert sequence/array parameter values with dialect awareness.

        Recursively converts nested sequences and dictionary items while
        preserving ``None`` elements.

        Args:
            value: Sequence to convert.
            json_serializer: Optional per-call JSON serializer override.

        Returns:
            Converted list parameter appropriate for the dialect.
        """
        converted: list[Any] = []
        for item in value:
            if isinstance(item, dict):
                converted.append(self.convert_dict(item, json_serializer=json_serializer))
            elif isinstance(item, (list, tuple)):
                converted.append(self.convert_sequence(item, json_serializer=json_serializer))
            else:
                converted.append(item)
        return converted


_CONVERTER_CACHE: Final[dict[tuple[str, Any], ADBCOutputConverter]] = {}


def get_adbc_type_converter(
    dialect: str, cache_size: int = 5000, *, json_serializer: "Callable[[Any], str] | None" = None
) -> ADBCOutputConverter:
    """Factory function to create dialect-specific ADBC type converter.

    Args:
        dialect: Database dialect name.
        cache_size: Optional converter cache size accepted for compatibility.
        json_serializer: Optional custom JSON serializer callable.

    Returns:
        Configured ADBCOutputConverter instance.
    """
    _ = cache_size
    serializer = json_serializer or to_json
    cache_key = (dialect, serializer)
    converter = _CONVERTER_CACHE.get(cache_key)
    if converter is None:
        converter = ADBCOutputConverter(dialect, json_serializer=serializer)
        _CONVERTER_CACHE[cache_key] = converter
    return converter
