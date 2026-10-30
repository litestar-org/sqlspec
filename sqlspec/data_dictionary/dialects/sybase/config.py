"""Sybase ASE dialect static configuration and reflection helpers."""

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
    "SYBASE_CONFIG",
    "SYBASE_FEATURE_FLAGS",
    "SYBASE_FEATURE_VERSIONS",
    "SYBASE_METADATA_DOMAINS",
    "SYBASE_TYPE_MAPPINGS",
    "SYBASE_VERSION_PATTERN",
    "SybaseVersionInfo",
    "build_sybase_metadata_capability_profile",
    "extract_sybase_version_value",
    "list_sybase_available_features",
    "merge_sybase_table_lists",
    "resolve_sybase_feature_flag",
)

SYBASE_VERSION_PATTERN = re.compile(r"Adaptive Server Enterprise/(\d+)\.(\d+)(?:\s+SP(\d+)|\.\d+)?", re.IGNORECASE)

SYBASE_FEATURE_VERSIONS: FeatureVersions = {}

SYBASE_FEATURE_FLAGS: FeatureFlags = {
    "supports_clustering": False,
    "supports_returning": False,
    "supports_upsert": False,
    "supports_on_conflict": False,
    "supports_window_functions": False,
    "supports_cte": False,
    "supports_json": False,
    "supports_arrays": False,
    "supports_uuid": False,
    "supports_schemas": True,
    "supports_merge": False,
    "supports_for_update": False,
    "supports_skip_locked": False,
    "supports_transactional_ddl": False,
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_in_memory": False,
    "supports_update_from": True,
    "supports_sequences": False,
}

SYBASE_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "VARCHAR(36)",
    "boolean": "BIT",
    "timestamp": "BIGDATETIME",
    "datetime": "BIGDATETIME",
    "date": "DATE",
    "time": "BIGTIME",
    "text": "TEXT",
    "blob": "IMAGE",
    "bytes": "VARBINARY(16384)",
    "json": "TEXT",
    "decimal": "DECIMAL",
    "float": "FLOAT",
    "integer": "INT",
    "bigint": "BIGINT",
    "smallint": "SMALLINT",
    "string": "VARCHAR(255)",
    "varchar": "VARCHAR(255)",
    "AutoIncrement": "INT IDENTITY",
}

SYBASE_CONFIG = DialectConfig(
    name="sybase",
    feature_versions=SYBASE_FEATURE_VERSIONS,
    feature_flags=SYBASE_FEATURE_FLAGS,
    type_mappings=SYBASE_TYPE_MAPPINGS,
    version_pattern=SYBASE_VERSION_PATTERN,
    default_schema="dbo",
    parameter_style="pyformat_named",
)

register_dialect(SYBASE_CONFIG)

SYBASE_METADATA_DOMAINS: Final[tuple[str, ...]] = (
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


class SybaseVersionInfo(VersionInfo):
    """Sybase ASE database version info with optional service pack string."""

    __slots__ = ("service_pack",)

    def __init__(self, major: int, minor: int = 0, patch: int = 0, service_pack: str | None = None) -> None:
        """Initialize Sybase ASE version info."""
        super().__init__(major, minor, patch)
        self.service_pack = service_pack

    def __str__(self) -> str:
        """Format version string including service pack when present."""
        base = f"{self.major}.{self.minor}.{self.patch}"
        return f"{base} ({self.service_pack})" if self.service_pack else base


def extract_sybase_version_value(row: object) -> str | None:
    """Extract a Sybase ASE version string from a row-like object."""
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


def resolve_sybase_feature_flag(feature: str, version_info: VersionInfo | None) -> bool:
    """Resolve a Sybase ASE feature flag using config and version details."""
    flag = SYBASE_CONFIG.get_feature_flag(feature)
    if flag is not None:
        return bool(flag)
    required_version = SYBASE_CONFIG.get_feature_version(feature)
    if required_version is None or version_info is None:
        return False
    return bool(version_info >= required_version)


def list_sybase_available_features() -> list[str]:
    """List static and versioned Sybase ASE data-dictionary feature flags."""
    features = set(SYBASE_CONFIG.feature_flags.keys())
    features.update(SYBASE_CONFIG.feature_versions.keys())
    return sorted(features)


def merge_sybase_table_lists(
    ordered: list["TableMetadata"], all_tables: list["TableMetadata"]
) -> list["TableMetadata"]:
    """Merge dependency-ordered Sybase ASE tables with catalog remainder rows."""
    if not ordered:
        return sorted(all_tables, key=lambda item: item.get("table_name") or "")
    seen = {(item.get("schema_name"), item.get("table_name")) for item in ordered if item.get("table_name")}
    remainder = [item for item in all_tables if (item.get("schema_name"), item.get("table_name")) not in seen]
    return ordered + remainder


def build_sybase_metadata_capability_profile(
    adapter: str | None, domains: Sequence[str] | None = None
) -> MetadataCapabilityProfile:
    """Build Sybase ASE data-dictionary capability metadata."""
    requested_domains = tuple(domains) if domains is not None else SYBASE_METADATA_DOMAINS
    capabilities: list[MetadataCapability] = []
    for domain in requested_domains:
        if domain in SYBASE_METADATA_DOMAINS:
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
    return MetadataCapabilityProfile("sybase", adapter=adapter, capabilities=tuple(capabilities))
