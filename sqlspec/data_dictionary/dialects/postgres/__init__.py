"""Dialect configuration for postgres."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary.dialects.postgres.config import (
    POSTGRES_CONFIG,
    POSTGRES_METADATA_DOMAINS,
    POSTGRES_SUPPORTED_DOMAINS,
    POSTGRES_SYSTEM_METADATA_QUERIES,
    POSTGRES_VERSION_PATTERN,
    resolve_postgres_json_type,
)

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.postgres.dictionary import (
        PostgresAsyncDataDictionary,
        PostgresSyncDataDictionary,
    )

__all__ = (
    "POSTGRES_CONFIG",
    "POSTGRES_METADATA_DOMAINS",
    "POSTGRES_SUPPORTED_DOMAINS",
    "POSTGRES_SYSTEM_METADATA_QUERIES",
    "POSTGRES_VERSION_PATTERN",
    "PostgresAsyncDataDictionary",
    "PostgresSyncDataDictionary",
    "resolve_postgres_json_type",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "PostgresAsyncDataDictionary": (
        "sqlspec.data_dictionary.dialects.postgres.dictionary",
        "PostgresAsyncDataDictionary",
    ),
    "PostgresSyncDataDictionary": (
        "sqlspec.data_dictionary.dialects.postgres.dictionary",
        "PostgresSyncDataDictionary",
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
