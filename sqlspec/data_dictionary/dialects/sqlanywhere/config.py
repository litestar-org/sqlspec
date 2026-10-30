"""SAP SQL Anywhere dialect static configuration and reflection helpers."""

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Final

from sqlspec.data_dictionary import register_dialect
from sqlspec.data_dictionary._types import (
    DialectConfig,
    FeatureFlags,
    FeatureVersions,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataSource,
    MetadataSupport,
    VersionInfo,
)

if TYPE_CHECKING:
    from sqlspec.data_dictionary import TableMetadata

__all__ = (
    "SQLANYWHERE_CONFIG",
    "SQLANYWHERE_FEATURE_FLAGS",
    "SQLANYWHERE_FEATURE_VERSIONS",
    "SQLANYWHERE_METADATA_DOMAINS",
    "SQLANYWHERE_TYPE_MAPPINGS",
    "SQLANYWHERE_VERSION_PATTERN",
    "SqlanywhereVersionInfo",
    "build_sqlanywhere_metadata_capability_profile",
    "extract_sqlanywhere_version_value",
    "list_sqlanywhere_available_features",
    "merge_sqlanywhere_table_lists",
    "resolve_sqlanywhere_feature_flag",
)

SQLANYWHERE_VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")

SQLANYWHERE_FEATURE_VERSIONS: FeatureVersions = {
    "supports_cte": VersionInfo(10, 0, 0),
    "supports_window_functions": VersionInfo(10, 0, 0),
    "supports_merge": VersionInfo(11, 0, 0),
}

SQLANYWHERE_FEATURE_FLAGS: FeatureFlags = {
    "supports_clustering": False,
    "supports_returning": False,
    "supports_upsert": True,
    "supports_on_conflict": False,
    "supports_window_functions": True,
    "supports_cte": True,
    "supports_json": False,
    "supports_arrays": False,
    "supports_uuid": True,
    "supports_schemas": True,
    "supports_merge": True,
    "supports_for_update": True,
    "supports_skip_locked": True,
    "supports_transactional_ddl": True,
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_in_memory": False,
    "supports_update_from": True,
    "supports_sequences": True,
}

SQLANYWHERE_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "UNIQUEIDENTIFIER",
    "boolean": "BIT",
    "timestamp": "TIMESTAMP",
    "datetime": "TIMESTAMP",
    "date": "DATE",
    "time": "TIME",
    "text": "LONG VARCHAR",
    "blob": "LONG BINARY",
    "bytes": "VARBINARY(32767)",
    "json": "LONG VARCHAR",
    "decimal": "DECIMAL",
    "float": "DOUBLE",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "string": "VARCHAR(255)",
    "varchar": "VARCHAR(255)",
    "AutoIncrement": "INTEGER DEFAULT AUTOINCREMENT",
}

SQLANYWHERE_CONFIG = DialectConfig(
    name="sqlanywhere",
    feature_versions=SQLANYWHERE_FEATURE_VERSIONS,
    feature_flags=SQLANYWHERE_FEATURE_FLAGS,
    type_mappings=SQLANYWHERE_TYPE_MAPPINGS,
    version_pattern=SQLANYWHERE_VERSION_PATTERN,
    default_schema="DBA",
    parameter_style="qmark",
)

register_dialect(SQLANYWHERE_CONFIG)

SQLANYWHERE_METADATA_DOMAINS: Final[tuple[str, ...]] = (
    "schemas",
    "objects",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "foreign_keys",
    "views",
    "version",
)


class SqlanywhereVersionInfo(VersionInfo):
    """SAP SQL Anywhere database version info with optional build number."""

    __slots__ = ("build_number",)

    def __init__(self, major: int, minor: int = 0, patch: int = 0, build_number: str | None = None) -> None:
        """Initialize SAP SQL Anywhere version info."""
        super().__init__(major, minor, patch)
        self.build_number = build_number

    def __str__(self) -> str:
        """Format version string including build number when present."""
        base = f"{self.major}.{self.minor}.{self.patch}"
        return f"{base} ({self.build_number})" if self.build_number else base


def extract_sqlanywhere_version_value(row: object) -> str | None:
    """Extract a SAP SQL Anywhere version string from a row-like object."""
    if isinstance(row, Mapping):
        for key in ("version_string", "VERSION_STRING", "version", "VERSION"):
            value = row.get(key)
            if value:
                return str(value)
    if isinstance(row, (list, tuple)) and row:
        return str(row[0])
    if row is not None:
        return str(row)
    return None


def resolve_sqlanywhere_feature_flag(feature: str, version_info: VersionInfo | None) -> bool:
    """Resolve a SAP SQL Anywhere feature flag using config and version details."""
    flag = SQLANYWHERE_CONFIG.get_feature_flag(feature)
    if flag is not None:
        return bool(flag)
    required_version = SQLANYWHERE_CONFIG.get_feature_version(feature)
    if required_version is None or version_info is None:
        return False
    return bool(version_info >= required_version)


def list_sqlanywhere_available_features() -> list[str]:
    """List static and versioned SAP SQL Anywhere data-dictionary feature flags."""
    features = set(SQLANYWHERE_CONFIG.feature_flags.keys())
    features.update(SQLANYWHERE_CONFIG.feature_versions.keys())
    return sorted(features)


def merge_sqlanywhere_table_lists(
    ordered: list["TableMetadata"], all_tables: list["TableMetadata"]
) -> list["TableMetadata"]:
    """Merge dependency-ordered SAP SQL Anywhere tables with catalog remainder rows."""
    if not ordered:
        return sorted(all_tables, key=lambda item: item.get("table_name") or "")
    seen = {(item.get("schema_name"), item.get("table_name")) for item in ordered if item.get("table_name")}
    remainder = [item for item in all_tables if (item.get("schema_name"), item.get("table_name")) not in seen]
    return ordered + remainder


def build_sqlanywhere_metadata_capability_profile(
    adapter: str | None, domains: Sequence[str] | None = None
) -> MetadataCapabilityProfile:
    """Build SAP SQL Anywhere data-dictionary capability metadata."""
    requested_domains = tuple(domains) if domains is not None else SQLANYWHERE_METADATA_DOMAINS
    capabilities: list[MetadataCapability] = []
    for domain in requested_domains:
        if domain in SQLANYWHERE_METADATA_DOMAINS:
            capabilities.append(
                MetadataCapability(
                    domain=domain,
                    support=MetadataSupport.SUPPORTED,
                    fidelity=MetadataFidelity.NATIVE,
                    source=MetadataSource.CATALOG,
                )
            )
        else:
            capabilities.append(MetadataCapability.unsupported(domain))
    return MetadataCapabilityProfile("sqlanywhere", adapter=adapter, capabilities=tuple(capabilities))
