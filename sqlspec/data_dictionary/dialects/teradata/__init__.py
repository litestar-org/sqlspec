"""Dialect configuration for Teradata."""

from sqlspec.data_dictionary.dialects.teradata.config import (
    TERADATA_CONFIG,
    TERADATA_FEATURE_FLAGS,
    TERADATA_FEATURE_VERSIONS,
    TERADATA_METADATA_DOMAINS,
    TERADATA_TYPE_MAPPINGS,
    TERADATA_VERSION_PATTERN,
    TeradataVersionInfo,
    build_teradata_metadata_capability_profile,
    extract_teradata_version_value,
    list_teradata_available_features,
    merge_teradata_table_lists,
    resolve_teradata_feature_flag,
)

__all__ = (
    "TERADATA_CONFIG",
    "TERADATA_FEATURE_FLAGS",
    "TERADATA_FEATURE_VERSIONS",
    "TERADATA_METADATA_DOMAINS",
    "TERADATA_TYPE_MAPPINGS",
    "TERADATA_VERSION_PATTERN",
    "TeradataVersionInfo",
    "build_teradata_metadata_capability_profile",
    "extract_teradata_version_value",
    "list_teradata_available_features",
    "merge_teradata_table_lists",
    "resolve_teradata_feature_flag",
)
