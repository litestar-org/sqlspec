"""Dialect configuration for IBM Db2."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

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

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.db2.dictionary import (
        Db2AsyncDataDictionary,
        Db2SyncDataDictionary,
        build_column_metadata,
        build_foreign_key_metadata,
        build_index_metadata,
        parse_db2_version,
    )

__all__ = (
    "DB2_CONFIG",
    "DB2_FEATURE_FLAGS",
    "DB2_FEATURE_VERSIONS",
    "DB2_METADATA_DOMAINS",
    "DB2_TYPE_MAPPINGS",
    "DB2_VERSION_PATTERN",
    "Db2AsyncDataDictionary",
    "Db2SyncDataDictionary",
    "Db2VersionInfo",
    "build_column_metadata",
    "build_db2_metadata_capability_profile",
    "build_foreign_key_metadata",
    "build_index_metadata",
    "extract_db2_version_value",
    "list_db2_available_features",
    "merge_db2_table_lists",
    "parse_db2_version",
    "resolve_db2_feature_flag",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "Db2AsyncDataDictionary": ("sqlspec.data_dictionary.dialects.db2.dictionary", "Db2AsyncDataDictionary"),
    "Db2SyncDataDictionary": ("sqlspec.data_dictionary.dialects.db2.dictionary", "Db2SyncDataDictionary"),
    "build_column_metadata": ("sqlspec.data_dictionary.dialects.db2.dictionary", "build_column_metadata"),
    "build_foreign_key_metadata": ("sqlspec.data_dictionary.dialects.db2.dictionary", "build_foreign_key_metadata"),
    "build_index_metadata": ("sqlspec.data_dictionary.dialects.db2.dictionary", "build_index_metadata"),
    "parse_db2_version": ("sqlspec.data_dictionary.dialects.db2.dictionary", "parse_db2_version"),
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
