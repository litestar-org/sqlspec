"""Teradata dialect static configuration and reflection helpers."""

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
    "TERADATA_CONFIG",
    "TERADATA_FEATURE_FLAGS",
    "TERADATA_FEATURE_VERSIONS",
    "TERADATA_METADATA_DOMAINS",
    "TERADATA_TYPE_MAPPINGS",
    "TERADATA_VERSION_PATTERN",
    "TeradataVersionInfo",
    "build_teradata_metadata_capability_profile",
    "extract_teradata_version_value",
    "list_teradata_available_features",
    "merge_teradata_table_lists",
    "resolve_teradata_feature_flag",
)

TERADATA_VERSION_PATTERN: Final[re.Pattern[str]] = re.compile(r"(\d+)\.(\d+)(?:\.(\d+))?(?:\.(\d+))?")

TERADATA_FEATURE_VERSIONS: FeatureVersions = {
    "supports_cte": VersionInfo(15, 10, 0),
    "supports_window_functions": VersionInfo(15, 10, 0),
    "supports_merge": VersionInfo(15, 10, 0),
    "supports_upsert": VersionInfo(15, 10, 0),
    "supports_json": VersionInfo(15, 10, 0),
    "supports_partitioning": VersionInfo(15, 10, 0),
}

TERADATA_FEATURE_FLAGS: FeatureFlags = {
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": True,
    "supports_json": True,
    "supports_cte": True,
    "supports_window_functions": True,
    "supports_merge": True,
    "supports_upsert": True,
    "supports_update_from": True,
    "supports_transactional_ddl": False,
    "supports_returning": False,
    "supports_arrays": False,
    "supports_for_update": False,
    "supports_skip_locked": False,
    "supports_on_conflict": False,
    "supports_in_memory": False,
    "supports_uuid": False,
    "supports_sequences": False,
}

TERADATA_TYPE_MAPPINGS: dict[str, str] = {
    "json": "JSON",
    "boolean": "BYTEINT",
    "varchar": "VARCHAR({length})",
    "text": "CLOB",
    "blob": "BLOB",
    "timestamp": "TIMESTAMP(6) WITH TIME ZONE",
    "datetime": "TIMESTAMP(6) WITH TIME ZONE",
    "uuid": "VARCHAR(36)",
    "integer": "INTEGER",
    "smallint": "SMALLINT",
    "bigint": "BIGINT",
    "float": "DOUBLE PRECISION",
    "decimal": "DECIMAL(18, 4)",
    "date": "DATE",
    "time": "TIME(6)",
}

TERADATA_CONFIG = DialectConfig(
    name="teradata",
    feature_versions=TERADATA_FEATURE_VERSIONS,
    feature_flags=TERADATA_FEATURE_FLAGS,
    type_mappings=TERADATA_TYPE_MAPPINGS,
    version_pattern=TERADATA_VERSION_PATTERN,
    parameter_style="qmark",
)

register_dialect(TERADATA_CONFIG)

TERADATA_METADATA_DOMAINS: Final[tuple[str, ...]] = (
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


class TeradataVersionInfo(VersionInfo):
    """Teradata version with the optional fourth build component."""

    __slots__ = ("build_number",)

    def __init__(self, major: int, minor: int = 0, patch: int = 0, build_number: int | str | None = None) -> None:
        """Initialize version components."""
        super().__init__(major, minor, patch)
        self.build_number = (
            str(build_number).strip() if build_number is not None and str(build_number).strip() else None
        )

    @classmethod
    def from_version_string(cls, raw_version: str) -> "TeradataVersionInfo | None":
        """Parse a database version string."""
        match = TERADATA_VERSION_PATTERN.search(raw_version)
        if match is None:
            return None
        return cls(int(match.group(1)), int(match.group(2)), int(match.group(3) or 0), match.group(4))

    def __str__(self) -> str:
        """Render the database version including build when available."""
        if self.build_number is not None:
            return f"{self.major}.{self.minor:02d}.{self.patch:02d}.{self.build_number}"
        return f"{self.major}.{self.minor}.{self.patch}"


def extract_teradata_version_value(row: object) -> str | None:
    """Extract a nonempty version string from a catalog row."""
    if isinstance(row, Mapping):
        for key in ("version_string", "VERSION_STRING", "InfoData", "infodata", "version", "VERSION"):
            value = row.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return None
    if isinstance(row, (list, tuple)):
        return str(row[0]).strip() or None if row else None
    return row.strip() or None if isinstance(row, str) else None


def resolve_teradata_feature_flag(feature: str, version_info: VersionInfo | None) -> bool:
    """Resolve static and version-dependent database capabilities."""
    flag = TERADATA_CONFIG.get_feature_flag(feature)
    required = TERADATA_CONFIG.get_feature_version(feature)
    if flag is False:
        return False
    if required is not None and version_info is not None:
        return bool(version_info >= required)
    return bool(flag) if flag is not None else False


def list_teradata_available_features() -> list[str]:
    """List static and versioned Teradata data-dictionary feature flags."""
    features = set(TERADATA_CONFIG.feature_flags.keys())
    features.update(TERADATA_CONFIG.feature_versions.keys())
    return sorted(features)


def merge_teradata_table_lists(
    ordered: list["TableMetadata"], all_tables: list["TableMetadata"]
) -> list["TableMetadata"]:
    """Merge dependency-ordered Teradata tables with catalog remainder rows."""
    if not ordered:
        return sorted(all_tables, key=lambda item: item.get("table_name") or "")
    seen = {(item.get("schema_name"), item.get("table_name")) for item in ordered if item.get("table_name")}
    remainder = [item for item in all_tables if (item.get("schema_name"), item.get("table_name")) not in seen]
    return ordered + remainder


def build_teradata_metadata_capability_profile(
    adapter: str | None, domains: Sequence[str] | None = None
) -> MetadataCapabilityProfile:
    """Build Teradata data-dictionary capability metadata."""
    requested_domains = tuple(domains) if domains is not None else TERADATA_METADATA_DOMAINS
    capabilities: list[MetadataCapability] = []
    for domain in requested_domains:
        if domain in TERADATA_METADATA_DOMAINS:
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
    return MetadataCapabilityProfile("teradata", adapter=adapter, capabilities=tuple(capabilities))
