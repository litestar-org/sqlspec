"""Dialect configuration for cockroachdb."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary.dialects.cockroachdb.config import (
    COCKROACH_METADATA_DOMAINS,
    COCKROACH_SUPPORTED_DOMAINS,
    COCKROACHDB_CONFIG,
    COCKROACHDB_VERSION_PATTERN,
    resolve_cockroachdb_json_type,
)

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.cockroachdb.dictionary import (
        CockroachDBAsyncDataDictionary,
        CockroachDBSyncDataDictionary,
    )

__all__ = (
    "COCKROACHDB_CONFIG",
    "COCKROACHDB_VERSION_PATTERN",
    "COCKROACH_METADATA_DOMAINS",
    "COCKROACH_SUPPORTED_DOMAINS",
    "CockroachDBAsyncDataDictionary",
    "CockroachDBSyncDataDictionary",
    "resolve_cockroachdb_json_type",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "CockroachDBAsyncDataDictionary": (
        "sqlspec.data_dictionary.dialects.cockroachdb.dictionary",
        "CockroachDBAsyncDataDictionary",
    ),
    "CockroachDBSyncDataDictionary": (
        "sqlspec.data_dictionary.dialects.cockroachdb.dictionary",
        "CockroachDBSyncDataDictionary",
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
