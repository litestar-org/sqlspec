"""Dialect configuration for cockroachdb."""

from sqlspec.data_dictionary.dialects.cockroachdb.config import (
    COCKROACH_METADATA_DOMAINS,
    COCKROACH_SUPPORTED_DOMAINS,
    COCKROACHDB_CONFIG,
    COCKROACHDB_VERSION_PATTERN,
    resolve_cockroachdb_json_type,
)

__all__ = (
    "COCKROACHDB_CONFIG",
    "COCKROACHDB_VERSION_PATTERN",
    "COCKROACH_METADATA_DOMAINS",
    "COCKROACH_SUPPORTED_DOMAINS",
    "resolve_cockroachdb_json_type",
)
