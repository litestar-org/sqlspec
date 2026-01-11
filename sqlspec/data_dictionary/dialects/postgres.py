import re

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, register_dialect
from sqlspec.typing import VersionInfo

POSTGRES_VERSION_PATTERN = re.compile(r"PostgreSQL (\d+)\.(\d+)(?:\.(\d+))?")

POSTGRES_FEATURE_VERSIONS: "FeatureVersions" = {
    "supports_json": VersionInfo(9, 2, 0),
    "supports_jsonb": VersionInfo(9, 4, 0),
    "supports_returning": VersionInfo(8, 2, 0),
    "supports_upsert": VersionInfo(9, 5, 0),
    "supports_window_functions": VersionInfo(8, 4, 0),
    "supports_cte": VersionInfo(8, 4, 0),
    "supports_partitioning": VersionInfo(10, 0, 0),
}

POSTGRES_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_uuid": True,
    "supports_arrays": True,
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
}

POSTGRES_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "UUID",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP WITH TIME ZONE",
    "text": "TEXT",
    "blob": "BYTEA",
    "array": "ARRAY",
    "json": "JSONB",
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
