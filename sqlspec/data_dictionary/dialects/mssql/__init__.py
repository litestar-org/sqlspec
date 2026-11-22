"""Dialect configuration for mssql."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary.dialects.mssql.config import (
    MSSQL_CONFIG,
    MSSQL_PRODUCT_VERSION_PATTERN,
    MSSQL_VERSION_PATTERN,
    MssqlVersionInfo,
    build_mssql_metadata_capability_profile,
    build_mssql_system_metadata_capability,
    build_mssql_system_metadata_result,
    build_mssql_table_ddl_result,
    extract_mssql_version_value,
    get_mssql_data_dictionary_options,
    is_mssql_azure_sql,
    list_mssql_available_features,
    merge_mssql_table_lists,
    mssql_supports_greatest_least,
    mssql_supports_json_functions,
    mssql_supports_native_json,
    mssql_supports_string_agg,
    mssql_system_metadata_denied,
    parse_mssql_engine_edition,
    parse_mssql_version_components,
    resolve_mssql_feature_flag,
    validate_mssql_system_metadata_options,
)

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.mssql.dictionary import MssqlAsyncDataDictionary, MssqlSyncDataDictionary

__all__ = (
    "MSSQL_CONFIG",
    "MSSQL_PRODUCT_VERSION_PATTERN",
    "MSSQL_VERSION_PATTERN",
    "MssqlAsyncDataDictionary",
    "MssqlSyncDataDictionary",
    "MssqlVersionInfo",
    "build_mssql_metadata_capability_profile",
    "build_mssql_system_metadata_capability",
    "build_mssql_system_metadata_result",
    "build_mssql_table_ddl_result",
    "extract_mssql_version_value",
    "get_mssql_data_dictionary_options",
    "is_mssql_azure_sql",
    "list_mssql_available_features",
    "merge_mssql_table_lists",
    "mssql_supports_greatest_least",
    "mssql_supports_json_functions",
    "mssql_supports_native_json",
    "mssql_supports_string_agg",
    "mssql_system_metadata_denied",
    "parse_mssql_engine_edition",
    "parse_mssql_version_components",
    "resolve_mssql_feature_flag",
    "validate_mssql_system_metadata_options",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "MssqlAsyncDataDictionary": ("sqlspec.data_dictionary.dialects.mssql.dictionary", "MssqlAsyncDataDictionary"),
    "MssqlSyncDataDictionary": ("sqlspec.data_dictionary.dialects.mssql.dictionary", "MssqlSyncDataDictionary"),
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
