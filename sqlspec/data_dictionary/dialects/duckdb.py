import re

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, register_dialect
from sqlspec.typing import VersionInfo

DUCKDB_VERSION_PATTERN = re.compile(r"v?(\d+)\.(\d+)\.(\d+)")

DUCKDB_FEATURE_VERSIONS: "FeatureVersions" = {
    "supports_returning": VersionInfo(0, 8, 0),
    "supports_upsert": VersionInfo(0, 8, 0),
}

DUCKDB_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_json": True,
    "supports_arrays": True,
    "supports_maps": True,
    "supports_structs": True,
    "supports_window_functions": True,
    "supports_cte": True,
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_uuid": True,
}

DUCKDB_TYPE_MAPPINGS: dict[str, str] = {
    "json": "JSON",
    "uuid": "UUID",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "text": "TEXT",
    "blob": "BLOB",
    "array": "LIST",
    "map": "MAP",
    "struct": "STRUCT",
}


DUCKDB_CONFIG = DialectConfig(
    name="duckdb",
    feature_versions=DUCKDB_FEATURE_VERSIONS,
    feature_flags=DUCKDB_FEATURE_FLAGS,
    type_mappings=DUCKDB_TYPE_MAPPINGS,
    version_pattern=DUCKDB_VERSION_PATTERN,
)

register_dialect(DUCKDB_CONFIG)
