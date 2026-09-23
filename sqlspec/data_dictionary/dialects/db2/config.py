"""IBM Db2 dialect static configuration and reflection helpers."""

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
    "DB2_CONFIG",
    "DB2_FEATURE_FLAGS",
    "DB2_FEATURE_VERSIONS",
    "DB2_METADATA_DOMAINS",
    "DB2_TYPE_MAPPINGS",
    "DB2_VERSION_PATTERN",
    "Db2VersionInfo",
    "build_db2_metadata_capability_profile",
    "extract_db2_version_value",
    "list_db2_available_features",
    "merge_db2_table_lists",
    "resolve_db2_feature_flag",
)

DB2_VERSION_PATTERN = re.compile(r"(?:v|V)?(\d+)\.(\d+)(?:\.(\d+))?")

DB2_FEATURE_VERSIONS: FeatureVersions = {
    "supports_skip_locked": VersionInfo(11, 1, 0),
    "supports_cte": VersionInfo(8, 1, 0),
    "supports_window_functions": VersionInfo(8, 1, 0),
    "supports_generated_columns": VersionInfo(8, 1, 0),
}

DB2_FEATURE_FLAGS: FeatureFlags = {
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_in_memory": False,
    "supports_for_update": True,
    "supports_skip_locked": True,
    "supports_on_conflict": False,
    "supports_update_from": False,
    "supports_returning": False,
    "supports_sequences": True,
}

DB2_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "VARCHAR(36)",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "text": "CLOB",
    "blob": "BLOB",
    "json": "CLOB",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "float": "DOUBLE",
    "decimal": "DECFLOAT",
    "varchar": "VARCHAR(255)",
}

DB2_CONFIG = DialectConfig(
    name="db2",
    feature_versions=DB2_FEATURE_VERSIONS,
    feature_flags=DB2_FEATURE_FLAGS,
    type_mappings=DB2_TYPE_MAPPINGS,
    version_pattern=DB2_VERSION_PATTERN,
    parameter_style="qmark",
)

register_dialect(DB2_CONFIG)

DB2_METADATA_DOMAINS: Final[tuple[str, ...]] = (
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


class Db2VersionInfo(VersionInfo):
    """Db2 database version info with service level."""

    __slots__ = ("service_level",)

    def __init__(self, major: int, minor: int = 0, patch: int = 0, service_level: str | None = None) -> None:
        """Initialize Db2 version info."""
        super().__init__(major, minor, patch)
        self.service_level = service_level

    def __str__(self) -> str:
        """Format version string including service level if present."""
        base = f"{self.major}.{self.minor}.{self.patch}"
        return f"{base} ({self.service_level})" if self.service_level else base


def extract_db2_version_value(row: object) -> str | None:
    """Extract a Db2 version string from a row-like object."""
    if isinstance(row, Mapping):
        for key in ("SERVICE_LEVEL", "service_level", "version", "VERSION"):
            value = row.get(key)
            if value:
                return str(value)
    if isinstance(row, (list, tuple)) and row:
        return str(row[0])
    if row is not None:
        return str(row)
    return None


def resolve_db2_feature_flag(feature: str, version_info: VersionInfo | None) -> bool:
    """Resolve a Db2 feature flag using config and version details."""
    flag = DB2_CONFIG.get_feature_flag(feature)
    if flag is not None:
        return bool(flag)
    required_version = DB2_CONFIG.get_feature_version(feature)
    if required_version is None or version_info is None:
        return False
    return bool(version_info >= required_version)


def list_db2_available_features() -> list[str]:
    """List static and versioned Db2 data-dictionary feature flags."""
    features = set(DB2_CONFIG.feature_flags.keys())
    features.update(DB2_CONFIG.feature_versions.keys())
    return sorted(features)


def merge_db2_table_lists(ordered: list["TableMetadata"], all_tables: list["TableMetadata"]) -> list["TableMetadata"]:
    """Merge dependency-ordered Db2 tables with catalog remainder rows."""
    if not ordered:
        return sorted(all_tables, key=lambda item: item.get("table_name") or "")
    seen = {(item.get("schema_name"), item.get("table_name")) for item in ordered if item.get("table_name")}
    remainder = [item for item in all_tables if (item.get("schema_name"), item.get("table_name")) not in seen]
    return ordered + remainder


def build_db2_metadata_capability_profile(
    adapter: str | None, domains: Sequence[str] | None = None
) -> MetadataCapabilityProfile:
    """Build Db2 data-dictionary capability metadata."""
    requested_domains = tuple(domains) if domains is not None else DB2_METADATA_DOMAINS
    capabilities: list[MetadataCapability] = []
    for domain in requested_domains:
        if domain in DB2_METADATA_DOMAINS:
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
    return MetadataCapabilityProfile("db2", adapter=adapter, capabilities=tuple(capabilities))
