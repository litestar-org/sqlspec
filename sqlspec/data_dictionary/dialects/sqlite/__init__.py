"""Dialect configuration for sqlite."""

from sqlspec.data_dictionary.dialects.sqlite.config import (
    SQLITE_CONFIG,
    SQLITE_VERSION_PATTERN,
    list_sqlite_available_features,
    resolve_sqlite_json_type,
)

__all__ = ("SQLITE_CONFIG", "SQLITE_VERSION_PATTERN", "list_sqlite_available_features", "resolve_sqlite_json_type")
