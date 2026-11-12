"""Shared Snowflake dialect configuration and catalog key parsers."""

from sqlspec.data_dictionary.dialects.snowflake.config import (
    SNOWFLAKE_CONFIG,
    SNOWFLAKE_FEATURE_FLAGS,
    SNOWFLAKE_FEATURE_VERSIONS,
    SNOWFLAKE_TYPE_MAPPINGS,
    SNOWFLAKE_VERSION_PATTERN,
    match_snowflake_catalog_rows,
    parse_snowflake_imported_keys,
    parse_snowflake_primary_or_unique_keys,
    qualify_snowflake_information_schema,
    snowflake_identifier_quoting,
    snowflake_scope_exactness,
)

__all__ = (
    "SNOWFLAKE_CONFIG",
    "SNOWFLAKE_FEATURE_FLAGS",
    "SNOWFLAKE_FEATURE_VERSIONS",
    "SNOWFLAKE_TYPE_MAPPINGS",
    "SNOWFLAKE_VERSION_PATTERN",
    "match_snowflake_catalog_rows",
    "parse_snowflake_imported_keys",
    "parse_snowflake_primary_or_unique_keys",
    "qualify_snowflake_information_schema",
    "snowflake_identifier_quoting",
    "snowflake_scope_exactness",
)
