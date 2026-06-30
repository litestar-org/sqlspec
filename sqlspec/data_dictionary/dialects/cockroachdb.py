import re

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, VersionInfo, register_dialect

__all__ = ("resolve_cockroachdb_json_type",)


COCKROACHDB_VERSION_PATTERN = re.compile(r"CockroachDB (?:CCL )?v(\d+)\.(\d+)\.(\d+)")

COCKROACHDB_FEATURE_VERSIONS: "FeatureVersions" = {
    "supports_json": VersionInfo(20, 1, 0),
    "supports_returning": VersionInfo(20, 1, 0),
    "supports_upsert": VersionInfo(19, 2, 0),
    "supports_window_functions": VersionInfo(19, 1, 0),
    "supports_cte": VersionInfo(19, 1, 0),
}

COCKROACHDB_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_uuid": True,
    "supports_arrays": True,
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_for_update": True,
    "supports_skip_locked": True,
}

COCKROACHDB_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "UUID",
    "boolean": "BOOL",
    "timestamp": "TIMESTAMPTZ",
    "text": "STRING",
    "blob": "BYTES",
    "array": "ARRAY",
    "json": "JSONB",
}

COCKROACHDB_CONFIG = DialectConfig(
    name="cockroachdb",
    feature_versions=COCKROACHDB_FEATURE_VERSIONS,
    feature_flags=COCKROACHDB_FEATURE_FLAGS,
    type_mappings=COCKROACHDB_TYPE_MAPPINGS,
    version_pattern=COCKROACHDB_VERSION_PATTERN,
    default_schema="public",
)

register_dialect(COCKROACHDB_CONFIG)


def resolve_cockroachdb_json_type(version_info: "VersionInfo | None") -> str:
    """Resolve the best CockroachDB JSON storage type for a database version."""
    json_version = COCKROACHDB_CONFIG.get_feature_version("supports_json")
    if version_info and json_version and version_info >= json_version:
        return COCKROACHDB_CONFIG.get_optimal_type("json")
    return "TEXT"
