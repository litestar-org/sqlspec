import re

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, register_dialect

__all__ = ("format_bigquery_information_schema_tables", "format_bigquery_schema_prefix")


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
    "supports_for_update": False,
    "supports_skip_locked": False,
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


def format_bigquery_information_schema_tables(schema: "str | None") -> "tuple[str, str, str]":
    """Format BigQuery INFORMATION_SCHEMA table identifiers for metadata queries.

    Args:
        schema: Optional BigQuery project.dataset schema qualifier.

    Returns:
        TABLES, KEY_COLUMN_USAGE, and REFERENTIAL_CONSTRAINTS identifiers.
    """
    if schema:
        return (
            f"`{schema}.INFORMATION_SCHEMA.TABLES`",
            f"`{schema}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`",
            f"`{schema}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS`",
        )
    return (
        "INFORMATION_SCHEMA.TABLES",
        "INFORMATION_SCHEMA.KEY_COLUMN_USAGE",
        "INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS",
    )


def format_bigquery_schema_prefix(schema: "str | None") -> str:
    """Format a BigQuery schema prefix for INFORMATION_SCHEMA queries."""
    if schema:
        return f"`{schema}`."
    return ""
