import re

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, register_dialect

BIGQUERY_VERSION_PATTERN = re.compile(r".*")

BIGQUERY_FEATURE_VERSIONS: "FeatureVersions" = {}

BIGQUERY_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_json": True,
    "supports_arrays": True,
    "supports_structs": True,
    "supports_geography": True,
    "supports_returning": False,
    "supports_upsert": True,
    "supports_window_functions": True,
    "supports_cte": True,
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_partitioning": True,
    "supports_clustering": True,
    "supports_uuid": False,
}

BIGQUERY_TYPE_MAPPINGS: dict[str, str] = {
    "json": "JSON",
    "uuid": "STRING",
    "boolean": "BOOL",
    "timestamp": "TIMESTAMP",
    "text": "STRING",
    "blob": "BYTES",
    "array": "ARRAY",
    "struct": "STRUCT",
    "geography": "GEOGRAPHY",
    "numeric": "NUMERIC",
    "bignumeric": "BIGNUMERIC",
}


BIGQUERY_CONFIG = DialectConfig(
    name="bigquery",
    feature_versions=BIGQUERY_FEATURE_VERSIONS,
    feature_flags=BIGQUERY_FEATURE_FLAGS,
    type_mappings=BIGQUERY_TYPE_MAPPINGS,
    version_pattern=BIGQUERY_VERSION_PATTERN,
)

register_dialect(BIGQUERY_CONFIG)
