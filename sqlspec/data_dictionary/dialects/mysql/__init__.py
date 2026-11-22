"""Dialect configuration for mysql."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary.dialects.mysql.config import (
    MARIADB_CONFIG,
    MYSQL_CONFIG,
    MYSQL_VERSION_PATTERN,
    MySQLEngineVersion,
    build_mysql_metadata_capability_profile,
    build_mysql_show_create_statement,
    build_mysql_system_metadata_capability,
    format_mysql_identifier,
    make_mysql_ddl_result,
    mysql_system_metadata_query_name,
    parse_mysql_engine_version,
    resolve_mysql_json_type,
)

if TYPE_CHECKING:
    from sqlspec.data_dictionary.dialects.mysql.dictionary import MySQLAsyncDataDictionary, MySQLSyncDataDictionary

__all__ = (
    "MARIADB_CONFIG",
    "MYSQL_CONFIG",
    "MYSQL_VERSION_PATTERN",
    "MySQLAsyncDataDictionary",
    "MySQLEngineVersion",
    "MySQLSyncDataDictionary",
    "build_mysql_metadata_capability_profile",
    "build_mysql_show_create_statement",
    "build_mysql_system_metadata_capability",
    "format_mysql_identifier",
    "make_mysql_ddl_result",
    "mysql_system_metadata_query_name",
    "parse_mysql_engine_version",
    "resolve_mysql_json_type",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "MySQLAsyncDataDictionary": ("sqlspec.data_dictionary.dialects.mysql.dictionary", "MySQLAsyncDataDictionary"),
    "MySQLSyncDataDictionary": ("sqlspec.data_dictionary.dialects.mysql.dictionary", "MySQLSyncDataDictionary"),
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
