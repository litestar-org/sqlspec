"""Dialect configuration for IBM Db2."""

from sqlspec.data_dictionary.dialects.db2.config import (
    DB2_CONFIG,
    DB2_FEATURE_FLAGS,
    DB2_FEATURE_VERSIONS,
    DB2_METADATA_DOMAINS,
    DB2_TYPE_MAPPINGS,
    DB2_VERSION_PATTERN,
    Db2VersionInfo,
    build_db2_metadata_capability_profile,
    extract_db2_version_value,
    list_db2_available_features,
    merge_db2_table_lists,
    resolve_db2_feature_flag,
)

__all__ = (
    "DB2_CONFIG",
    "DB2_FEATURE_FLAGS",
    "DB2_FEATURE_VERSIONS",
    "DB2_METADATA_DOMAINS",
    "DB2_TYPE_MAPPINGS",
    "DB2_VERSION_PATTERN",
    "Db2VersionInfo",
    "build_db2_metadata_capability_profile",
    "extract_db2_version_value",
    "list_db2_available_features",
    "merge_db2_table_lists",
    "resolve_db2_feature_flag",
)
