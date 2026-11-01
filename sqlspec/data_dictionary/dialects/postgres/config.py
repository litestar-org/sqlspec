"""PostgreSQL dialect static configuration and reflection helpers."""

import re

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, VersionInfo, register_dialect

__all__ = (
    "POSTGRES_CONFIG",
    "POSTGRES_FEATURE_FLAGS",
    "POSTGRES_FEATURE_VERSIONS",
    "POSTGRES_METADATA_DOMAINS",
    "POSTGRES_SUPPORTED_DOMAINS",
    "POSTGRES_SYSTEM_METADATA_QUERIES",
    "POSTGRES_TYPE_MAPPINGS",
    "POSTGRES_VERSION_PATTERN",
    "resolve_postgres_json_type",
)

POSTGRES_METADATA_DOMAINS: tuple[str, ...] = (
    "schemas",
    "objects",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
    "materialized_views",
    "sequences",
    "types",
    "routines",
    "triggers",
    "comments",
    "privileges",
    "dependencies",
    "extensions",
    "partitions",
    "ddl",
    "system",
)
POSTGRES_SUPPORTED_DOMAINS: frozenset[str] = frozenset(d for d in POSTGRES_METADATA_DOMAINS if d != "system")
POSTGRES_SYSTEM_METADATA_QUERIES: dict[str, str] = {
    "settings": "settings",
    "statement_history": "pg_stat_statements",
    "pg_stat_statements": "pg_stat_statements",
    "table_statistics": "table_stats",
    "table_stats": "table_stats",
}


POSTGRES_VERSION_PATTERN = re.compile(r"PostgreSQL (\d+)\.(\d+)(?:\.(\d+))?")

POSTGRES_FEATURE_VERSIONS: "FeatureVersions" = {
    "supports_json": VersionInfo(9, 2, 0),
    "supports_jsonb": VersionInfo(9, 4, 0),
    "supports_returning": VersionInfo(8, 2, 0),
    "supports_upsert": VersionInfo(9, 5, 0),
    "supports_window_functions": VersionInfo(8, 4, 0),
    "supports_cte": VersionInfo(8, 4, 0),
    "supports_partitioning": VersionInfo(10, 0, 0),
    "supports_skip_locked": VersionInfo(9, 5, 0),
}

POSTGRES_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_uuid": True,
    "supports_arrays": True,
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_for_update": True,
    "supports_on_conflict": True,
    "supports_update_from": True,
}

POSTGRES_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "UUID",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP WITH TIME ZONE",
    "text": "TEXT",
    "blob": "BYTEA",
    "array": "ARRAY",
    "json": "JSONB",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "float": "DOUBLE PRECISION",
    "varchar": "VARCHAR({length})",
}


POSTGRES_CONFIG = DialectConfig(
    name="postgres",
    feature_versions=POSTGRES_FEATURE_VERSIONS,
    feature_flags=POSTGRES_FEATURE_FLAGS,
    type_mappings=POSTGRES_TYPE_MAPPINGS,
    version_pattern=POSTGRES_VERSION_PATTERN,
    default_schema="public",
)

register_dialect(POSTGRES_CONFIG)


def resolve_postgres_json_type(version_info: "VersionInfo | None") -> str:
    """Resolve the best PostgreSQL JSON storage type for a database version.

    Args:
        version_info: Parsed PostgreSQL version, or None when version detection failed.

    Returns:
        JSONB when available, JSON before JSONB support, otherwise TEXT.
    """
    jsonb_version = POSTGRES_CONFIG.get_feature_version("supports_jsonb")
    if version_info and jsonb_version and version_info >= jsonb_version:
        return "JSONB"

    json_version = POSTGRES_CONFIG.get_feature_version("supports_json")
    if version_info and json_version and version_info >= json_version:
        return "JSON"

    return "TEXT"
