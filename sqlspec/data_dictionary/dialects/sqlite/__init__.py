"""Dialect configuration for sqlite."""

from sqlspec.data_dictionary.dialects.sqlite.config import (
    SQLITE_CONFIG,
    SQLITE_VERSION_PATTERN,
    format_sqlite_schema_prefix,
    list_sqlite_available_features,
    normalize_sqlite_schema_name,
    resolve_sqlite_json_type,
)

__all__ = (
    "SQLITE_CONFIG",
    "SQLITE_VERSION_PATTERN",
    "format_sqlite_schema_prefix",
    "list_sqlite_available_features",
    "normalize_sqlite_schema_name",
    "resolve_sqlite_json_type",
)
