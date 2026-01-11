import re

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, register_dialect
from sqlspec.typing import VersionInfo

MYSQL_VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")

MYSQL_FEATURE_VERSIONS: "FeatureVersions" = {
    "supports_json": VersionInfo(5, 7, 8),
    "supports_cte": VersionInfo(8, 0, 1),
    "supports_window_functions": VersionInfo(8, 0, 2),
}

MYSQL_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_returning": False,
    "supports_upsert": True,
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_arrays": False,
    "supports_uuid": False,
}

MYSQL_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "VARCHAR(36)",
    "boolean": "TINYINT(1)",
    "timestamp": "TIMESTAMP",
    "text": "TEXT",
    "blob": "BLOB",
    "json": "JSON",
}


MYSQL_CONFIG = DialectConfig(
    name="mysql",
    feature_versions=MYSQL_FEATURE_VERSIONS,
    feature_flags=MYSQL_FEATURE_FLAGS,
    type_mappings=MYSQL_TYPE_MAPPINGS,
    version_pattern=MYSQL_VERSION_PATTERN,
)

register_dialect(MYSQL_CONFIG)
