"""Dialect configuration for oracle."""

from sqlspec.data_dictionary.dialects.oracle.config import (
    ORACLE_CONFIG,
    ORACLE_JSON_STORAGE_BLOB_JSON,
    ORACLE_JSON_STORAGE_BLOB_PLAIN,
    ORACLE_JSON_STORAGE_NATIVE,
    ORACLE_VERSION_PATTERN,
    extract_oracle_version_value,
    list_oracle_available_features,
    merge_oracle_table_lists,
    oracle_supports_json_blob,
    oracle_supports_native_json,
    oracle_supports_oson_blob,
    parse_oracle_compatible_major,
    parse_oracle_version_components,
    resolve_oracle_feature_flag,
    resolve_oracle_json_storage,
    resolve_oracle_json_type,
)

__all__ = (
    "ORACLE_CONFIG",
    "ORACLE_JSON_STORAGE_BLOB_JSON",
    "ORACLE_JSON_STORAGE_BLOB_PLAIN",
    "ORACLE_JSON_STORAGE_NATIVE",
    "ORACLE_VERSION_PATTERN",
    "extract_oracle_version_value",
    "list_oracle_available_features",
    "merge_oracle_table_lists",
    "oracle_supports_json_blob",
    "oracle_supports_native_json",
    "oracle_supports_oson_blob",
    "parse_oracle_compatible_major",
    "parse_oracle_version_components",
    "resolve_oracle_feature_flag",
    "resolve_oracle_json_storage",
    "resolve_oracle_json_type",
)
