"""Dialect configuration for oracle."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary.dialects.oracle.config import (
    ORACLE_CONFIG,
    ORACLE_JSON_STORAGE_BLOB_JSON,
    ORACLE_JSON_STORAGE_BLOB_PLAIN,
    ORACLE_JSON_STORAGE_NATIVE,
    ORACLE_VERSION_PATTERN,
    OracleVersionCache,
    extract_oracle_version_value,
    list_oracle_available_features,
    merge_oracle_table_lists,
    oracle_supports_json_blob,
    oracle_supports_native_json,
    oracle_supports_oson_blob,
    parse_oracle_compatible_major,
    parse_oracle_version_components,
    resolve_oracle_connection_compatible_major,
    resolve_oracle_connection_major,
    resolve_oracle_feature_flag,
    resolve_oracle_json_storage,
    resolve_oracle_json_type,
)

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.oracle.dictionary import (
        JSONStorageType,
        OracleAsyncDataDictionary,
        OracleSyncDataDictionary,
        OracleVersionInfo,
        storage_type_from_version,
    )

__all__ = (
    "ORACLE_CONFIG",
    "ORACLE_JSON_STORAGE_BLOB_JSON",
    "ORACLE_JSON_STORAGE_BLOB_PLAIN",
    "ORACLE_JSON_STORAGE_NATIVE",
    "ORACLE_VERSION_PATTERN",
    "JSONStorageType",
    "OracleAsyncDataDictionary",
    "OracleSyncDataDictionary",
    "OracleVersionCache",
    "OracleVersionInfo",
    "extract_oracle_version_value",
    "list_oracle_available_features",
    "merge_oracle_table_lists",
    "oracle_supports_json_blob",
    "oracle_supports_native_json",
    "oracle_supports_oson_blob",
    "parse_oracle_compatible_major",
    "parse_oracle_version_components",
    "resolve_oracle_connection_compatible_major",
    "resolve_oracle_connection_major",
    "resolve_oracle_feature_flag",
    "resolve_oracle_json_storage",
    "resolve_oracle_json_type",
    "storage_type_from_version",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "JSONStorageType": ("sqlspec.data_dictionary.dialects.oracle.dictionary", "JSONStorageType"),
    "OracleAsyncDataDictionary": ("sqlspec.data_dictionary.dialects.oracle.dictionary", "OracleAsyncDataDictionary"),
    "OracleSyncDataDictionary": ("sqlspec.data_dictionary.dialects.oracle.dictionary", "OracleSyncDataDictionary"),
    "OracleVersionInfo": ("sqlspec.data_dictionary.dialects.oracle.dictionary", "OracleVersionInfo"),
    "storage_type_from_version": ("sqlspec.data_dictionary.dialects.oracle.dictionary", "storage_type_from_version"),
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = _EXPORTS[name]
    module = import_module(module_path)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))
