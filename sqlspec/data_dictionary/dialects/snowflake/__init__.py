"""Shared Snowflake dialect configuration and catalog key parsers."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary.dialects.snowflake.config import (
    SNOWFLAKE_CONFIG,
    SNOWFLAKE_FEATURE_FLAGS,
    SNOWFLAKE_FEATURE_VERSIONS,
    SNOWFLAKE_TYPE_MAPPINGS,
    SNOWFLAKE_VERSION_PATTERN,
    format_snowflake_identifier,
    format_snowflake_show_scope,
    match_snowflake_catalog_rows,
    parse_snowflake_imported_keys,
    parse_snowflake_primary_or_unique_keys,
    qualify_snowflake_information_schema,
    snowflake_identifier_quoting,
    snowflake_scope_exactness,
)

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.snowflake.dictionary import SnowflakeSyncDataDictionary

__all__ = (
    "SNOWFLAKE_CONFIG",
    "SNOWFLAKE_FEATURE_FLAGS",
    "SNOWFLAKE_FEATURE_VERSIONS",
    "SNOWFLAKE_TYPE_MAPPINGS",
    "SNOWFLAKE_VERSION_PATTERN",
    "SnowflakeSyncDataDictionary",
    "format_snowflake_identifier",
    "format_snowflake_show_scope",
    "match_snowflake_catalog_rows",
    "parse_snowflake_imported_keys",
    "parse_snowflake_primary_or_unique_keys",
    "qualify_snowflake_information_schema",
    "snowflake_identifier_quoting",
    "snowflake_scope_exactness",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "SnowflakeSyncDataDictionary": (
        "sqlspec.data_dictionary.dialects.snowflake.dictionary",
        "SnowflakeSyncDataDictionary",
    )
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
