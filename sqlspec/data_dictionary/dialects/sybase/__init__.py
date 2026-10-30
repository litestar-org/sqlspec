"""Sybase ASE dialect static configuration and reflection helpers."""

from sqlspec.data_dictionary.dialects.sybase.config import (
    SYBASE_CONFIG,
    SYBASE_FEATURE_FLAGS,
    SYBASE_FEATURE_VERSIONS,
    SYBASE_METADATA_DOMAINS,
    SYBASE_TYPE_MAPPINGS,
    SYBASE_VERSION_PATTERN,
    SybaseVersionInfo,
    build_sybase_metadata_capability_profile,
    extract_sybase_version_value,
    list_sybase_available_features,
    merge_sybase_table_lists,
    resolve_sybase_feature_flag,
)

__all__ = (
    "SYBASE_CONFIG",
    "SYBASE_FEATURE_FLAGS",
    "SYBASE_FEATURE_VERSIONS",
    "SYBASE_METADATA_DOMAINS",
    "SYBASE_TYPE_MAPPINGS",
    "SYBASE_VERSION_PATTERN",
    "SybaseVersionInfo",
    "build_sybase_metadata_capability_profile",
    "extract_sybase_version_value",
    "list_sybase_available_features",
    "merge_sybase_table_lists",
    "resolve_sybase_feature_flag",
)
