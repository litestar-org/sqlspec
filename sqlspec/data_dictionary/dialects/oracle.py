import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Final

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, register_dialect

if TYPE_CHECKING:
    from sqlspec.typing import TableMetadata, VersionInfo

__all__ = (
    "extract_oracle_version_value",
    "list_oracle_available_features",
    "merge_oracle_table_lists",
    "oracle_supports_json_blob",
    "oracle_supports_native_json",
    "oracle_supports_oson_blob",
    "parse_oracle_compatible_major",
    "parse_oracle_version_components",
    "resolve_oracle_feature_flag",
    "resolve_oracle_json_type",
)


ORACLE_VERSION_PATTERN = re.compile(r"(\d+)")
ORACLE_VERSION_PARTS_COUNT: Final[int] = 3
ORACLE_MIN_JSON_NATIVE_VERSION: Final[int] = 21
ORACLE_MIN_JSON_NATIVE_COMPATIBLE: Final[int] = 20
ORACLE_MIN_JSON_BLOB_VERSION: Final[int] = 12
ORACLE_MIN_OSON_VERSION: Final[int] = 19

ORACLE_DYNAMIC_FEATURES: Final[tuple[str, ...]] = (
    "is_autonomous",
    "supports_native_json",
    "supports_oson_blob",
    "supports_json_blob",
    "supports_json",
)

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


def parse_oracle_compatible_major(compatible: "str | None") -> "int | None":
    """Parse the major version from an Oracle compatible parameter value.

    Args:
        compatible: Oracle compatible parameter value.

    Returns:
        Compatible major version or None when unavailable.
    """
    if not compatible:
        return None
    parts = compatible.split(".")
    if not parts:
        return None
    return int(parts[0])


def oracle_supports_native_json(major: int, compatible_major: "int | None") -> bool:
    """Return whether an Oracle version supports the native JSON data type."""
    return major >= ORACLE_MIN_JSON_NATIVE_VERSION and (compatible_major or 0) >= ORACLE_MIN_JSON_NATIVE_COMPATIBLE


def oracle_supports_oson_blob(major: int, is_autonomous: bool) -> bool:
    """Return whether an Oracle version supports BLOB with OSON format."""
    if major >= ORACLE_MIN_JSON_NATIVE_VERSION:
        return True
    return major >= ORACLE_MIN_OSON_VERSION and is_autonomous


def oracle_supports_json_blob(major: int) -> bool:
    """Return whether an Oracle version supports BLOB with JSON validation."""
    return major >= ORACLE_MIN_JSON_BLOB_VERSION


def extract_oracle_version_value(row: object) -> "str | None":
    """Extract an Oracle version string from a row-like object.

    Args:
        row: Row value returned by an Oracle version query.

    Returns:
        Version string when one can be found, otherwise None.
    """
    if isinstance(row, Mapping):
        for key in ("version", "VERSION", "Version"):
            value = row.get(key)
            if value:
                return str(value)
    if isinstance(row, (list, tuple)) and row:
        return str(row[0])
    if row is not None:
        return str(row)
    return None


def parse_oracle_version_components(version_str: str) -> "tuple[int, int, int] | None":
    """Parse Oracle version text into major, minor, patch components."""
    parts = [int(value) for value in ORACLE_VERSION_PATTERN.findall(version_str)]
    if not parts:
        return None
    while len(parts) < ORACLE_VERSION_PARTS_COUNT:
        parts.append(0)
    return parts[0], parts[1], parts[2]


def resolve_oracle_json_type(
    version_info: "VersionInfo | None", *, compatible_major: "int | None", is_autonomous: bool
) -> str:
    """Resolve the best Oracle JSON storage type for a database version."""
    if version_info is None:
        return "CLOB"
    if oracle_supports_native_json(version_info.major, compatible_major):
        return "JSON"
    if oracle_supports_oson_blob(version_info.major, is_autonomous):
        return "BLOB"
    if oracle_supports_json_blob(version_info.major):
        return "BLOB"
    return "CLOB"


def resolve_oracle_feature_flag(
    config: DialectConfig,
    version_info: "VersionInfo | None",
    feature: str,
    *,
    compatible_major: "int | None",
    is_autonomous: bool,
) -> bool:
    """Resolve an Oracle feature flag from static config and version details."""
    if feature == "is_autonomous":
        return bool(version_info and is_autonomous)
    if version_info is None:
        return False
    if feature == "supports_native_json":
        return oracle_supports_native_json(version_info.major, compatible_major)
    if feature == "supports_oson_blob":
        return oracle_supports_oson_blob(version_info.major, is_autonomous)
    if feature == "supports_json_blob":
        return oracle_supports_json_blob(version_info.major)
    if feature == "supports_json":
        return oracle_supports_json_blob(version_info.major)

    flag = config.get_feature_flag(feature)
    if flag is not None:
        return flag
    required_version = config.get_feature_version(feature)
    if required_version is None:
        return False
    return bool(version_info >= required_version)


def list_oracle_available_features(config: DialectConfig) -> "list[str]":
    """List static and dynamic Oracle data-dictionary feature flags."""
    features: set[str] = set()
    features.update(config.feature_flags.keys())
    features.update(config.feature_versions.keys())
    features.update(ORACLE_DYNAMIC_FEATURES)
    return sorted(features)


def merge_oracle_table_lists(
    ordered: "list[TableMetadata]", all_tables: "list[TableMetadata]"
) -> "list[TableMetadata]":
    """Merge dependency-ordered Oracle tables with unordered remainder rows."""
    if not ordered:
        return sorted(all_tables, key=lambda item: item.get("table_name") or "")
    ordered_names = {item.get("table_name") for item in ordered if item.get("table_name")}
    remainder = [item for item in all_tables if item.get("table_name") not in ordered_names]
    return ordered + remainder
