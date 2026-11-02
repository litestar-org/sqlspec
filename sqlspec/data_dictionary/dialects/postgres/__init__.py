"""Dialect configuration for postgres."""

from sqlspec.data_dictionary.dialects.postgres.config import (
    POSTGRES_CONFIG,
    POSTGRES_METADATA_DOMAINS,
    POSTGRES_SUPPORTED_DOMAINS,
    POSTGRES_SYSTEM_METADATA_QUERIES,
    POSTGRES_VERSION_PATTERN,
    resolve_postgres_json_type,
)

__all__ = (
    "POSTGRES_CONFIG",
    "POSTGRES_METADATA_DOMAINS",
    "POSTGRES_SUPPORTED_DOMAINS",
    "POSTGRES_SYSTEM_METADATA_QUERIES",
    "POSTGRES_VERSION_PATTERN",
    "resolve_postgres_json_type",
)
