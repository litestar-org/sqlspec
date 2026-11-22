"""Dialect configuration for Teradata."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary.dialects.teradata.config import (
    TERADATA_COLUMN_TYPE_CODES,
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

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.teradata.dictionary import (
        TeradataSyncDataDictionary,
        build_column_metadata,
        build_foreign_key_metadata,
        build_index_metadata,
        parse_teradata_version,
    )

__all__ = (
    "TERADATA_COLUMN_TYPE_CODES",
    "TERADATA_CONFIG",
    "TERADATA_FEATURE_FLAGS",
    "TERADATA_FEATURE_VERSIONS",
    "TERADATA_METADATA_DOMAINS",
    "TERADATA_TYPE_MAPPINGS",
    "TERADATA_VERSION_PATTERN",
    "TeradataSyncDataDictionary",
    "TeradataVersionInfo",
    "build_column_metadata",
    "build_foreign_key_metadata",
    "build_index_metadata",
    "build_teradata_metadata_capability_profile",
    "extract_teradata_version_value",
    "list_teradata_available_features",
    "merge_teradata_table_lists",
    "parse_teradata_version",
    "resolve_teradata_feature_flag",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "TeradataSyncDataDictionary": (
        "sqlspec.data_dictionary.dialects.teradata.dictionary",
        "TeradataSyncDataDictionary",
    ),
    "build_column_metadata": ("sqlspec.data_dictionary.dialects.teradata.dictionary", "build_column_metadata"),
    "build_foreign_key_metadata": (
        "sqlspec.data_dictionary.dialects.teradata.dictionary",
        "build_foreign_key_metadata",
    ),
    "build_index_metadata": ("sqlspec.data_dictionary.dialects.teradata.dictionary", "build_index_metadata"),
    "parse_teradata_version": ("sqlspec.data_dictionary.dialects.teradata.dictionary", "parse_teradata_version"),
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
