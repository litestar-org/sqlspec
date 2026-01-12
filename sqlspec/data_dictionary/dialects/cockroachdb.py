import re

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, register_dialect
from sqlspec.typing import VersionInfo

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
