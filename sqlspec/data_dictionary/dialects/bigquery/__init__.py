"""Dialect configuration for bigquery."""

from sqlspec.data_dictionary.dialects.bigquery.config import (
    BIGQUERY_CONFIG,
    BIGQUERY_VERSION_PATTERN,
    format_bigquery_information_schema_tables,
    format_bigquery_schema_prefix,
)

__all__ = (
    "BIGQUERY_CONFIG",
    "BIGQUERY_VERSION_PATTERN",
    "format_bigquery_information_schema_tables",
    "format_bigquery_schema_prefix",
)
