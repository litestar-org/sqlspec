import re
from typing import TYPE_CHECKING, Any, Final

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, register_dialect

if TYPE_CHECKING:
    from sqlspec.typing import TableMetadata, VersionInfo

__all__ = (
    "extract_mssql_version_value",
    "is_mssql_azure_sql",
    "list_mssql_available_features",
    "merge_mssql_table_lists",
    "mssql_supports_greatest_least",
    "mssql_supports_json_functions",
    "mssql_supports_native_json",
    "mssql_supports_string_agg",
    "parse_mssql_engine_edition",
    "parse_mssql_version_components",
    "resolve_mssql_default_schema",
    "resolve_mssql_feature_flag",
)

MSSQL_VERSION_PATTERN = re.compile(r"(\d+)")
MSSQL_PRODUCT_VERSION_PATTERN = re.compile(r"\b(\d+)\.(\d+)\.(\d+)\.(\d+)\b")
MSSQL_VERSION_PARTS_COUNT: Final[int] = 4
MSSQL_MIN_JSON_FUNCTIONS_VERSION: Final[int] = 13
MSSQL_MIN_STRING_AGG_VERSION: Final[int] = 14
MSSQL_MIN_GREATEST_LEAST_VERSION: Final[int] = 16
MSSQL_MIN_NATIVE_JSON_VERSION: Final[int] = 17
MSSQL_ENGINE_EDITION_AZURE_SET: Final[frozenset[int]] = frozenset({5, 8, 11})

MSSQL_DYNAMIC_FEATURES: Final[tuple[str, ...]] = (
    "is_azure_sql",
    "supports_json_functions",
    "supports_string_agg",
    "supports_greatest_least",
    "supports_native_json",
)

MSSQL_FEATURE_VERSIONS: "FeatureVersions" = {}

MSSQL_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_in_memory": True,
}

MSSQL_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "UNIQUEIDENTIFIER",
    "boolean": "BIT",
    "text": "NVARCHAR(MAX)",
    "json": "NVARCHAR(MAX)",
    "jsonb": "NVARCHAR(MAX)",
    "timestamp": "DATETIME2(6)",
    "timestamptz": "DATETIMEOFFSET(6)",
    "bytea": "VARBINARY(MAX)",
    "blob": "VARBINARY(MAX)",
}

MSSQL_CONFIG = DialectConfig(
    name="mssql",
    feature_versions=MSSQL_FEATURE_VERSIONS,
    feature_flags=MSSQL_FEATURE_FLAGS,
    type_mappings=MSSQL_TYPE_MAPPINGS,
    version_pattern=MSSQL_VERSION_PATTERN,
    default_schema="dbo",
    parameter_style="qmark",
)

register_dialect(MSSQL_CONFIG)


def extract_mssql_version_value(row: object) -> "str | None":
    """Extract a SQL Server version string from a row-like object."""
    if isinstance(row, dict):
        for key in ("version", "VERSION", "Version", "product_version", "ProductVersion"):
            value = row.get(key)
            if value:
                return str(value)
    if isinstance(row, (list, tuple)) and row:
        return str(row[0])
    if row is not None:
        return str(row)
    return None


def parse_mssql_version_components(version_string: str) -> tuple[int, int, int, int]:
    """Parse MSSQL version text into major, minor, build, revision components."""
    product_version = MSSQL_PRODUCT_VERSION_PATTERN.search(version_string)
    if product_version is not None:
        groups = product_version.groups()
        return int(groups[0]), int(groups[1]), int(groups[2]), int(groups[3])

    parts = [int(value) for value in MSSQL_VERSION_PATTERN.findall(version_string)]
    if not parts:
        return 0, 0, 0, 0
    while len(parts) < MSSQL_VERSION_PARTS_COUNT:
        parts.append(0)
    return parts[0], parts[1], parts[2], parts[3]


def parse_mssql_engine_edition(value: Any) -> int | None:
    """Parse a SQL Server EngineEdition value into an integer."""
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode(errors="ignore")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_mssql_azure_sql(engine_edition: int | None) -> bool:
    """Return whether an EngineEdition value represents Azure SQL."""
    return engine_edition in MSSQL_ENGINE_EDITION_AZURE_SET


def resolve_mssql_default_schema() -> str:
    """Return the default MSSQL schema used for introspection."""
    return "dbo"


def mssql_supports_json_functions(major: int) -> bool:
    """Return whether the SQL Server version supports JSON functions."""
    return major >= MSSQL_MIN_JSON_FUNCTIONS_VERSION


def mssql_supports_string_agg(major: int) -> bool:
    """Return whether the SQL Server version supports STRING_AGG."""
    return major >= MSSQL_MIN_STRING_AGG_VERSION


def mssql_supports_greatest_least(major: int) -> bool:
    """Return whether the SQL Server version supports GREATEST and LEAST."""
    return major >= MSSQL_MIN_GREATEST_LEAST_VERSION


def mssql_supports_native_json(major: int, is_azure_sql: bool = False) -> bool:
    """Return whether the SQL Server version supports the native JSON type."""
    return is_azure_sql or major >= MSSQL_MIN_NATIVE_JSON_VERSION


def resolve_mssql_feature_flag(
    feature: str,
    *,
    major: int,
    is_azure_sql: bool = False,
    config: "DialectConfig | None" = None,
    version_info: "VersionInfo | None" = None,
) -> bool:
    """Resolve an MSSQL feature flag from static config and version details."""
    if version_info is not None:
        major = version_info.major
    if feature == "is_azure_sql":
        return is_azure_sql
    if feature == "supports_json_functions":
        return mssql_supports_json_functions(major)
    if feature == "supports_string_agg":
        return mssql_supports_string_agg(major)
    if feature == "supports_greatest_least":
        return mssql_supports_greatest_least(major)
    if feature == "supports_native_json":
        return mssql_supports_native_json(major, is_azure_sql=is_azure_sql)

    dialect_config = config or MSSQL_CONFIG
    flag = dialect_config.get_feature_flag(feature)
    if flag is not None:
        return flag
    required_version = dialect_config.get_feature_version(feature)
    if required_version is None or version_info is None:
        return False
    return bool(version_info >= required_version)


def list_mssql_available_features(config: "DialectConfig | None" = None) -> list[str]:
    """List static and dynamic MSSQL data-dictionary feature flags."""
    dialect_config = config or MSSQL_CONFIG
    features: set[str] = set()
    features.update(dialect_config.feature_flags.keys())
    features.update(dialect_config.feature_versions.keys())
    features.update(MSSQL_DYNAMIC_FEATURES)
    return sorted(features)


def merge_mssql_table_lists(ordered: "list[TableMetadata]", all_rows: "list[TableMetadata]") -> "list[TableMetadata]":
    """Merge dependency-ordered table rows with catalog rows not in the dependency tree."""
    merged: list[TableMetadata] = []
    seen: set[tuple[str | None, str | None]] = set()

    for row in ordered:
        key = (row.get("schema_name") or row.get("table_schema"), row.get("table_name"))
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)

    for row in all_rows:
        key = (row.get("schema_name") or row.get("table_schema"), row.get("table_name"))
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)

    return merged
