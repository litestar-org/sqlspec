"""Sybase ASE dialect static configuration and reflection helpers."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

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

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.sybase.dictionary import (
        SybaseSyncDataDictionary,
        build_column_metadata,
        build_foreign_key_metadata,
        build_index_metadata,
        parse_sybase_version,
    )

__all__ = (
    "SYBASE_CONFIG",
    "SYBASE_FEATURE_FLAGS",
    "SYBASE_FEATURE_VERSIONS",
    "SYBASE_METADATA_DOMAINS",
    "SYBASE_TYPE_MAPPINGS",
    "SYBASE_VERSION_PATTERN",
    "SybaseSyncDataDictionary",
    "SybaseVersionInfo",
    "build_column_metadata",
    "build_foreign_key_metadata",
    "build_index_metadata",
    "build_sybase_metadata_capability_profile",
    "extract_sybase_version_value",
    "list_sybase_available_features",
    "merge_sybase_table_lists",
    "parse_sybase_version",
    "resolve_sybase_feature_flag",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "SybaseSyncDataDictionary": ("sqlspec.data_dictionary.dialects.sybase.dictionary", "SybaseSyncDataDictionary"),
    "build_column_metadata": ("sqlspec.data_dictionary.dialects.sybase.dictionary", "build_column_metadata"),
    "build_foreign_key_metadata": ("sqlspec.data_dictionary.dialects.sybase.dictionary", "build_foreign_key_metadata"),
    "build_index_metadata": ("sqlspec.data_dictionary.dialects.sybase.dictionary", "build_index_metadata"),
    "parse_sybase_version": ("sqlspec.data_dictionary.dialects.sybase.dictionary", "parse_sybase_version"),
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
