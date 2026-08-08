"""Dialect configuration for cockroachdb."""

from sqlspec.data_dictionary.dialects.cockroachdb.config import (
    COCKROACHDB_CONFIG,
    COCKROACHDB_VERSION_PATTERN,
    resolve_cockroachdb_json_type,
)

__all__ = ("COCKROACHDB_CONFIG", "COCKROACHDB_VERSION_PATTERN", "resolve_cockroachdb_json_type")
