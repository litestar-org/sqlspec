"""Dialect configuration for mysql."""

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

__all__ = (
    "MARIADB_CONFIG",
    "MYSQL_CONFIG",
    "MYSQL_VERSION_PATTERN",
    "MySQLEngineVersion",
    "build_mysql_metadata_capability_profile",
    "build_mysql_show_create_statement",
    "build_mysql_system_metadata_capability",
    "format_mysql_identifier",
    "make_mysql_ddl_result",
    "mysql_system_metadata_query_name",
    "parse_mysql_engine_version",
    "resolve_mysql_json_type",
)
