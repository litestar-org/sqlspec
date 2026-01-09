import re

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, register_dialect

ORACLE_VERSION_PATTERN = re.compile(r"(\d+)")

ORACLE_FEATURE_VERSIONS: "FeatureVersions" = {}

ORACLE_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_in_memory": True,
}

ORACLE_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "RAW(16)",
    "boolean": "NUMBER(1)",
    "timestamp": "TIMESTAMP",
    "text": "CLOB",
    "blob": "BLOB",
    "json": "JSON",
}


ORACLE_CONFIG = DialectConfig(
    name="oracle",
    feature_versions=ORACLE_FEATURE_VERSIONS,
    feature_flags=ORACLE_FEATURE_FLAGS,
    type_mappings=ORACLE_TYPE_MAPPINGS,
    version_pattern=ORACLE_VERSION_PATTERN,
)

register_dialect(ORACLE_CONFIG)
