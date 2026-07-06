"""Centralized data dictionary helpers."""

from typing import TYPE_CHECKING, Any

from sqlspec.data_dictionary._capabilities import (
    DEFAULT_SYSTEM_METADATA_DOMAINS,
    ensure_system_metadata_request,
    system_metadata_capabilities_from_domains,
    system_metadata_gated_result,
    unsupported_system_metadata_capability,
)
from sqlspec.data_dictionary._dependencies import (
    DependencyCycle,
    DependencyCycleError,
    DependencyDirection,
    DependencyEdge,
    DependencyEdgeKind,
    DependencySortResult,
    DependencyStrength,
    dependency_edges_from_foreign_keys,
    sort_dependencies,
)
from sqlspec.data_dictionary._registry import (
    get_dialect_config,
    list_registered_dialects,
    normalize_dialect_name,
    register_dialect,
)
from sqlspec.data_dictionary._types import (
    ColumnDetails,
    ColumnMetadata,
    CommentMetadata,
    ConstraintMetadata,
    DDLResult,
    DependencyMetadata,
    DialectConfig,
    FeatureFlags,
    FeatureVersions,
    ForeignKeyMetadata,
    IndexDetails,
    IndexMetadata,
    MetadataCapability,
    MetadataCapabilityProfile,
    MetadataFidelity,
    MetadataQuery,
    MetadataResult,
    MetadataRisk,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    ObjectMetadata,
    PartitionMetadata,
    PrivilegeMetadata,
    RoutineMetadata,
    SchemaMetadata,
    SystemMetadata,
    SystemMetadataCapability,
    SystemMetadataRedactionPolicy,
    SystemMetadataRequest,
    SystemMetadataResult,
    TableDetails,
    TableMetadata,
    TableStatisticsMetadata,
    TriggerMetadata,
    VersionCacheResult,
    VersionInfo,
    ViewMetadata,
)

if TYPE_CHECKING:
    from sqlspec.data_dictionary._loader import DataDictionaryLoader, get_data_dictionary_loader

__all__ = (
    "DEFAULT_SYSTEM_METADATA_DOMAINS",
    "ColumnDetails",
    "ColumnMetadata",
    "CommentMetadata",
    "ConstraintMetadata",
    "DDLResult",
    "DataDictionaryLoader",
    "DependencyCycle",
    "DependencyCycleError",
    "DependencyDirection",
    "DependencyEdge",
    "DependencyEdgeKind",
    "DependencyMetadata",
    "DependencySortResult",
    "DependencyStrength",
    "DialectConfig",
    "FeatureFlags",
    "FeatureVersions",
    "ForeignKeyMetadata",
    "IndexDetails",
    "IndexMetadata",
    "MetadataCapability",
    "MetadataCapabilityProfile",
    "MetadataFidelity",
    "MetadataQuery",
    "MetadataResult",
    "MetadataRisk",
    "MetadataSource",
    "MetadataSupport",
    "ObjectIdentity",
    "ObjectMetadata",
    "PartitionMetadata",
    "PrivilegeMetadata",
    "RoutineMetadata",
    "SchemaMetadata",
    "SystemMetadata",
    "SystemMetadataCapability",
    "SystemMetadataRedactionPolicy",
    "SystemMetadataRequest",
    "SystemMetadataResult",
    "TableDetails",
    "TableMetadata",
    "TableStatisticsMetadata",
    "TriggerMetadata",
    "VersionCacheResult",
    "VersionInfo",
    "ViewMetadata",
    "dependency_edges_from_foreign_keys",
    "ensure_system_metadata_request",
    "get_data_dictionary_loader",
    "get_dialect_config",
    "list_registered_dialects",
    "normalize_dialect_name",
    "register_dialect",
    "system_metadata_capabilities_from_domains",
    "system_metadata_gated_result",
    "unsupported_system_metadata_capability",
    "sort_dependencies",
)


def __getattr__(name: str) -> Any:
    if name == "DataDictionaryLoader":
        from sqlspec.data_dictionary._loader import DataDictionaryLoader

        return DataDictionaryLoader
    if name == "get_data_dictionary_loader":
        from sqlspec.data_dictionary._loader import get_data_dictionary_loader

        return get_data_dictionary_loader
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
