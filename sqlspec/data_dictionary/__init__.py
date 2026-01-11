"""Centralized data dictionary helpers."""

from sqlspec.data_dictionary._loader import DataDictionaryLoader, get_data_dictionary_loader
from sqlspec.data_dictionary._registry import (
    get_dialect_config,
    list_registered_dialects,
    normalize_dialect_name,
    register_dialect,
)
from sqlspec.data_dictionary._types import DialectConfig, FeatureFlags, FeatureVersions

__all__ = (
    "DataDictionaryLoader",
    "DialectConfig",
    "FeatureFlags",
    "FeatureVersions",
    "get_data_dictionary_loader",
    "get_dialect_config",
    "list_registered_dialects",
    "normalize_dialect_name",
    "register_dialect",
)
