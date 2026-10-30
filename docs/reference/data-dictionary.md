# Data Dictionary

```python
from sqlspec.data_dictionary import (
    ColumnMetadata,
    ColumnProfileMetadata,
    DataDictionaryLoader,
    DDLResult,
    DependencySortResult,
    DialectConfig,
    ForeignKeyMetadata,
    IndexMetadata,
    MetadataCapabilityProfile,
    MetadataResult,
    TableMetadata,
    VersionInfo,
    dependency_edges_from_foreign_keys,
    dependency_edges_from_metadata,
    get_data_dictionary_loader,
    get_dialect_config,
    list_registered_dialects,
    sort_ddl_results,
    sort_dependencies,
)
```

Cross-database catalog introspection metadata, DDL extraction, column profiling, capability profiles, dialect registry, and foreign-key topological sorting. Access runtime introspection through `driver.data_dictionary` on any [`SyncDriverAdapterBase`](driver/sync.md) or [`AsyncDriverAdapterBase`](driver/async.md) session (see [Runtime Introspection & DDL Synthesis](data-dictionary/runtime.md)).

## Loader & Dialect Registry

::: sqlspec.data_dictionary._loader.DataDictionaryLoader
    options:
      heading_level: 3
      members:
        - get_query
        - get_query_text
        - list_queries
        - get_domain_query
        - get_domain_query_text
        - get_system_query
        - list_domains

::: sqlspec.data_dictionary._loader.get_data_dictionary_loader
    options:
      heading_level: 4

::: sqlspec.data_dictionary._registry.get_dialect_config
    options:
      heading_level: 4

::: sqlspec.data_dictionary._registry.list_registered_dialects
    options:
      heading_level: 4

::: sqlspec.data_dictionary._registry.normalize_dialect_name
    options:
      heading_level: 4

::: sqlspec.data_dictionary._registry.register_dialect
    options:
      heading_level: 4

## Schema & Catalog Metadata Types

::: sqlspec.data_dictionary._types.VersionInfo
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.TableMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.ColumnMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.ColumnProfileMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.IndexMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.ForeignKeyMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.ConstraintMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.ViewMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.RoutineMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.TriggerMetadata
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.DDLResult
    options:
      heading_level: 4
      members: false

## Capability Profiles & Result Envelopes

::: sqlspec.data_dictionary._types.DialectConfig
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.MetadataCapabilityProfile
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.MetadataResult
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.SystemMetadataRequest
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._types.SystemMetadataResult
    options:
      heading_level: 4
      members: false

## Dependency Graph & Topological Sorting

::: sqlspec.data_dictionary._dependencies.DependencyEdge
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._dependencies.DependencySortResult
    options:
      heading_level: 4
      members: false

::: sqlspec.data_dictionary._dependencies.dependency_edges_from_foreign_keys
    options:
      heading_level: 4

::: sqlspec.data_dictionary._dependencies.dependency_edges_from_metadata
    options:
      heading_level: 4

::: sqlspec.data_dictionary._dependencies.sort_dependencies
    options:
      heading_level: 4

::: sqlspec.data_dictionary._dependencies.sort_ddl_results
    options:
      heading_level: 4
