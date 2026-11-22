"""Dialect configuration for duckdb."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary.dialects.duckdb.config import DUCKDB_CONFIG, DUCKDB_VERSION_PATTERN

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.duckdb.dictionary import DuckDBAsyncDataDictionary, DuckDBSyncDataDictionary

__all__ = ("DUCKDB_CONFIG", "DUCKDB_VERSION_PATTERN", "DuckDBAsyncDataDictionary", "DuckDBSyncDataDictionary")

_EXPORTS: dict[str, tuple[str, str]] = {
    "DuckDBAsyncDataDictionary": ("sqlspec.data_dictionary.dialects.duckdb.dictionary", "DuckDBAsyncDataDictionary"),
    "DuckDBSyncDataDictionary": ("sqlspec.data_dictionary.dialects.duckdb.dictionary", "DuckDBSyncDataDictionary"),
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
