"""SAP SQL Anywhere dialect static configuration and reflection helpers."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

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

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.sqlanywhere.dictionary import (
        SqlanywhereSyncDataDictionary,
        build_column_metadata,
        build_foreign_key_metadata,
        build_index_metadata,
        parse_sqlanywhere_version,
    )

__all__ = (
    "SQLANYWHERE_CONFIG",
    "SQLANYWHERE_FEATURE_FLAGS",
    "SQLANYWHERE_FEATURE_VERSIONS",
    "SQLANYWHERE_METADATA_DOMAINS",
    "SQLANYWHERE_TYPE_MAPPINGS",
    "SQLANYWHERE_VERSION_PATTERN",
    "SqlanywhereSyncDataDictionary",
    "SqlanywhereVersionInfo",
    "build_column_metadata",
    "build_foreign_key_metadata",
    "build_index_metadata",
    "build_sqlanywhere_metadata_capability_profile",
    "extract_sqlanywhere_version_value",
    "list_sqlanywhere_available_features",
    "merge_sqlanywhere_table_lists",
    "parse_sqlanywhere_version",
    "resolve_sqlanywhere_feature_flag",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "SqlanywhereSyncDataDictionary": (
        "sqlspec.data_dictionary.dialects.sqlanywhere.dictionary",
        "SqlanywhereSyncDataDictionary",
    ),
    "build_column_metadata": ("sqlspec.data_dictionary.dialects.sqlanywhere.dictionary", "build_column_metadata"),
    "build_foreign_key_metadata": (
        "sqlspec.data_dictionary.dialects.sqlanywhere.dictionary",
        "build_foreign_key_metadata",
    ),
    "build_index_metadata": ("sqlspec.data_dictionary.dialects.sqlanywhere.dictionary", "build_index_metadata"),
    "parse_sqlanywhere_version": (
        "sqlspec.data_dictionary.dialects.sqlanywhere.dictionary",
        "parse_sqlanywhere_version",
    ),
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
