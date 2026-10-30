"""SAP SQL Anywhere dialect static configuration and reflection helpers."""

from sqlspec.data_dictionary.dialects.sqlanywhere.config import (
    SQLANYWHERE_CONFIG,
    SQLANYWHERE_FEATURE_FLAGS,
    SQLANYWHERE_FEATURE_VERSIONS,
    SQLANYWHERE_METADATA_DOMAINS,
    SQLANYWHERE_TYPE_MAPPINGS,
    SQLANYWHERE_VERSION_PATTERN,
    SqlanywhereVersionInfo,
    build_sqlanywhere_metadata_capability_profile,
    extract_sqlanywhere_version_value,
    list_sqlanywhere_available_features,
    merge_sqlanywhere_table_lists,
    resolve_sqlanywhere_feature_flag,
)

__all__ = (
    "SQLANYWHERE_CONFIG",
    "SQLANYWHERE_FEATURE_FLAGS",
    "SQLANYWHERE_FEATURE_VERSIONS",
    "SQLANYWHERE_METADATA_DOMAINS",
    "SQLANYWHERE_TYPE_MAPPINGS",
    "SQLANYWHERE_VERSION_PATTERN",
    "SqlanywhereVersionInfo",
    "build_sqlanywhere_metadata_capability_profile",
    "extract_sqlanywhere_version_value",
    "list_sqlanywhere_available_features",
    "merge_sqlanywhere_table_lists",
    "resolve_sqlanywhere_feature_flag",
)
