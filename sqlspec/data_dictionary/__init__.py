"""Centralized data dictionary helpers."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec import _COMPILED

if TYPE_CHECKING:
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
        dependency_edges_from_metadata,
        sort_ddl_results,
        sort_dependencies,
    )
    from sqlspec.data_dictionary._loader import DataDictionaryLoader, get_data_dictionary_loader
    from sqlspec.data_dictionary._registry import (
        get_dialect_config,
        list_registered_dialects,
        normalize_dialect_name,
        register_dialect,
    )
    from sqlspec.data_dictionary._types import (
        DDL_PARENT_TABLE_CONTEXT_KEY,
        ColumnDetails,
        ColumnMetadata,
        ColumnProfileMetadata,
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

__all__ = (
    "DDL_PARENT_TABLE_CONTEXT_KEY",
    "DEFAULT_SYSTEM_METADATA_DOMAINS",
    "ColumnDetails",
    "ColumnMetadata",
    "ColumnProfileMetadata",
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
    "dependency_edges_from_metadata",
    "ensure_system_metadata_request",
    "get_data_dictionary_loader",
    "get_dialect_config",
    "list_registered_dialects",
    "normalize_dialect_name",
    "register_dialect",
    "sort_ddl_results",
    "sort_dependencies",
    "system_metadata_capabilities_from_domains",
    "system_metadata_gated_result",
    "unsupported_system_metadata_capability",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "DDL_PARENT_TABLE_CONTEXT_KEY": ("sqlspec.data_dictionary._types", "DDL_PARENT_TABLE_CONTEXT_KEY"),
    "DEFAULT_SYSTEM_METADATA_DOMAINS": ("sqlspec.data_dictionary._capabilities", "DEFAULT_SYSTEM_METADATA_DOMAINS"),
    "ColumnDetails": ("sqlspec.data_dictionary._types", "ColumnDetails"),
    "ColumnMetadata": ("sqlspec.data_dictionary._types", "ColumnMetadata"),
    "ColumnProfileMetadata": ("sqlspec.data_dictionary._types", "ColumnProfileMetadata"),
    "CommentMetadata": ("sqlspec.data_dictionary._types", "CommentMetadata"),
    "ConstraintMetadata": ("sqlspec.data_dictionary._types", "ConstraintMetadata"),
    "DDLResult": ("sqlspec.data_dictionary._types", "DDLResult"),
    "DataDictionaryLoader": ("sqlspec.data_dictionary._loader", "DataDictionaryLoader"),
    "DependencyCycle": ("sqlspec.data_dictionary._dependencies", "DependencyCycle"),
    "DependencyCycleError": ("sqlspec.data_dictionary._dependencies", "DependencyCycleError"),
    "DependencyDirection": ("sqlspec.data_dictionary._dependencies", "DependencyDirection"),
    "DependencyEdge": ("sqlspec.data_dictionary._dependencies", "DependencyEdge"),
    "DependencyEdgeKind": ("sqlspec.data_dictionary._dependencies", "DependencyEdgeKind"),
    "DependencyMetadata": ("sqlspec.data_dictionary._types", "DependencyMetadata"),
    "DependencySortResult": ("sqlspec.data_dictionary._dependencies", "DependencySortResult"),
    "DependencyStrength": ("sqlspec.data_dictionary._dependencies", "DependencyStrength"),
    "DialectConfig": ("sqlspec.data_dictionary._types", "DialectConfig"),
    "FeatureFlags": ("sqlspec.data_dictionary._types", "FeatureFlags"),
    "FeatureVersions": ("sqlspec.data_dictionary._types", "FeatureVersions"),
    "ForeignKeyMetadata": ("sqlspec.data_dictionary._types", "ForeignKeyMetadata"),
    "IndexDetails": ("sqlspec.data_dictionary._types", "IndexDetails"),
    "IndexMetadata": ("sqlspec.data_dictionary._types", "IndexMetadata"),
    "MetadataCapability": ("sqlspec.data_dictionary._types", "MetadataCapability"),
    "MetadataCapabilityProfile": ("sqlspec.data_dictionary._types", "MetadataCapabilityProfile"),
    "MetadataFidelity": ("sqlspec.data_dictionary._types", "MetadataFidelity"),
    "MetadataQuery": ("sqlspec.data_dictionary._types", "MetadataQuery"),
    "MetadataResult": ("sqlspec.data_dictionary._types", "MetadataResult"),
    "MetadataRisk": ("sqlspec.data_dictionary._types", "MetadataRisk"),
    "MetadataSource": ("sqlspec.data_dictionary._types", "MetadataSource"),
    "MetadataSupport": ("sqlspec.data_dictionary._types", "MetadataSupport"),
    "ObjectIdentity": ("sqlspec.data_dictionary._types", "ObjectIdentity"),
    "ObjectMetadata": ("sqlspec.data_dictionary._types", "ObjectMetadata"),
    "PartitionMetadata": ("sqlspec.data_dictionary._types", "PartitionMetadata"),
    "PrivilegeMetadata": ("sqlspec.data_dictionary._types", "PrivilegeMetadata"),
    "RoutineMetadata": ("sqlspec.data_dictionary._types", "RoutineMetadata"),
    "SchemaMetadata": ("sqlspec.data_dictionary._types", "SchemaMetadata"),
    "SystemMetadata": ("sqlspec.data_dictionary._types", "SystemMetadata"),
    "SystemMetadataCapability": ("sqlspec.data_dictionary._types", "SystemMetadataCapability"),
    "SystemMetadataRedactionPolicy": ("sqlspec.data_dictionary._types", "SystemMetadataRedactionPolicy"),
    "SystemMetadataRequest": ("sqlspec.data_dictionary._types", "SystemMetadataRequest"),
    "SystemMetadataResult": ("sqlspec.data_dictionary._types", "SystemMetadataResult"),
    "TableDetails": ("sqlspec.data_dictionary._types", "TableDetails"),
    "TableMetadata": ("sqlspec.data_dictionary._types", "TableMetadata"),
    "TableStatisticsMetadata": ("sqlspec.data_dictionary._types", "TableStatisticsMetadata"),
    "TriggerMetadata": ("sqlspec.data_dictionary._types", "TriggerMetadata"),
    "VersionCacheResult": ("sqlspec.data_dictionary._types", "VersionCacheResult"),
    "VersionInfo": ("sqlspec.data_dictionary._types", "VersionInfo"),
    "ViewMetadata": ("sqlspec.data_dictionary._types", "ViewMetadata"),
    "dependency_edges_from_foreign_keys": (
        "sqlspec.data_dictionary._dependencies",
        "dependency_edges_from_foreign_keys",
    ),
    "dependency_edges_from_metadata": ("sqlspec.data_dictionary._dependencies", "dependency_edges_from_metadata"),
    "ensure_system_metadata_request": ("sqlspec.data_dictionary._capabilities", "ensure_system_metadata_request"),
    "get_data_dictionary_loader": ("sqlspec.data_dictionary._loader", "get_data_dictionary_loader"),
    "get_dialect_config": ("sqlspec.data_dictionary._registry", "get_dialect_config"),
    "list_registered_dialects": ("sqlspec.data_dictionary._registry", "list_registered_dialects"),
    "normalize_dialect_name": ("sqlspec.data_dictionary._registry", "normalize_dialect_name"),
    "register_dialect": ("sqlspec.data_dictionary._registry", "register_dialect"),
    "sort_ddl_results": ("sqlspec.data_dictionary._dependencies", "sort_ddl_results"),
    "sort_dependencies": ("sqlspec.data_dictionary._dependencies", "sort_dependencies"),
    "system_metadata_capabilities_from_domains": (
        "sqlspec.data_dictionary._capabilities",
        "system_metadata_capabilities_from_domains",
    ),
    "system_metadata_gated_result": ("sqlspec.data_dictionary._capabilities", "system_metadata_gated_result"),
    "unsupported_system_metadata_capability": (
        "sqlspec.data_dictionary._capabilities",
        "unsupported_system_metadata_capability",
    ),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | _EXPORTS.keys())


if _COMPILED:
    for _name in __all__:
        if _name not in globals():
            __getattr__(_name)
