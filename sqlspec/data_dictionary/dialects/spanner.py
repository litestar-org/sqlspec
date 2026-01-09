import re

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, register_dialect

SPANNER_VERSION_PATTERN = re.compile(r".*")

SPANNER_FEATURE_VERSIONS: "FeatureVersions" = {}

SPANNER_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_json": True,
    "supports_generators": False,
    "supports_index_clustering": True,
    "supports_interleaved_tables": True,
}

SPANNER_TYPE_MAPPINGS: dict[str, str] = {
    "json": "JSON",
    "uuid": "BYTES(16)",
    "boolean": "BOOL",
    "timestamp": "TIMESTAMP",
    "text": "STRING(MAX)",
    "blob": "BYTES(MAX)",
    "numeric": "NUMERIC",
    "bignumeric": "NUMERIC",
    "array": "ARRAY",
}


SPANNER_CONFIG = DialectConfig(
    name="spanner",
    feature_versions=SPANNER_FEATURE_VERSIONS,
    feature_flags=SPANNER_FEATURE_FLAGS,
    type_mappings=SPANNER_TYPE_MAPPINGS,
    version_pattern=SPANNER_VERSION_PATTERN,
)

register_dialect(SPANNER_CONFIG)
