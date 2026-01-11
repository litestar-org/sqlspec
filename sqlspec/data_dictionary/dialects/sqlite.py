import re

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, register_dialect
from sqlspec.typing import VersionInfo

SQLITE_VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")

SQLITE_FEATURE_VERSIONS: "FeatureVersions" = {
    "supports_json": VersionInfo(3, 38, 0),
    "supports_returning": VersionInfo(3, 35, 0),
    "supports_upsert": VersionInfo(3, 24, 0),
    "supports_window_functions": VersionInfo(3, 25, 0),
    "supports_cte": VersionInfo(3, 8, 3),
}

SQLITE_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": False,
    "supports_arrays": False,
    "supports_uuid": False,
}

SQLITE_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "TEXT",
    "boolean": "INTEGER",
    "timestamp": "TIMESTAMP",
    "text": "TEXT",
    "blob": "BLOB",
    "json": "JSON",
}


SQLITE_CONFIG = DialectConfig(
    name="sqlite",
    feature_versions=SQLITE_FEATURE_VERSIONS,
    feature_flags=SQLITE_FEATURE_FLAGS,
    type_mappings=SQLITE_TYPE_MAPPINGS,
    version_pattern=SQLITE_VERSION_PATTERN,
)

register_dialect(SQLITE_CONFIG)
