"""Dialect configuration for sqlite."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary.dialects.sqlite.config import (
    SQLITE_CONFIG,
    SQLITE_VERSION_PATTERN,
    format_sqlite_schema_prefix,
    list_sqlite_available_features,
    normalize_sqlite_schema_name,
    resolve_sqlite_json_type,
)

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.sqlite.dictionary import SqliteAsyncDataDictionary, SqliteSyncDataDictionary

__all__ = (
    "SQLITE_CONFIG",
    "SQLITE_VERSION_PATTERN",
    "SqliteAsyncDataDictionary",
    "SqliteSyncDataDictionary",
    "format_sqlite_schema_prefix",
    "list_sqlite_available_features",
    "normalize_sqlite_schema_name",
    "resolve_sqlite_json_type",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "SqliteAsyncDataDictionary": ("sqlspec.data_dictionary.dialects.sqlite.dictionary", "SqliteAsyncDataDictionary"),
    "SqliteSyncDataDictionary": ("sqlspec.data_dictionary.dialects.sqlite.dictionary", "SqliteSyncDataDictionary"),
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
