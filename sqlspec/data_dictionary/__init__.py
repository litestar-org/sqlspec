"""Centralized data dictionary helpers."""

from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary._registry import (
    get_dialect_config,
    list_registered_dialects,
    normalize_dialect_name,
    register_dialect,
)
from sqlspec.data_dictionary._types import (
    ColumnMetadata,
    DialectConfig,
    FeatureFlags,
    FeatureVersions,
    ForeignKeyMetadata,
    IndexMetadata,
    TableMetadata,
    TableStatisticsMetadata,
    VersionCacheResult,
    VersionInfo,
)

if TYPE_CHECKING:
    from sqlspec.data_dictionary._loader import DataDictionaryLoader, get_data_dictionary_loader

__all__ = (
    "ColumnMetadata",
    "DataDictionaryLoader",
    "DialectConfig",
    "FeatureFlags",
    "FeatureVersions",
    "ForeignKeyMetadata",
    "IndexMetadata",
    "TableMetadata",
    "TableStatisticsMetadata",
    "VersionCacheResult",
    "VersionInfo",
    "get_data_dictionary_loader",
    "get_dialect_config",
    "list_registered_dialects",
    "normalize_dialect_name",
    "register_dialect",
)


def __getattr__(name: str) -> Any:
    if name == "DataDictionaryLoader":
        from sqlspec.data_dictionary._loader import DataDictionaryLoader

        return DataDictionaryLoader
    if name == "get_data_dictionary_loader":
        from sqlspec.data_dictionary._loader import get_data_dictionary_loader

        return get_data_dictionary_loader
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
