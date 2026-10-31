# Runtime Introspection & DDL Synthesis

Runtime introspection, DDL extraction, and column profiling methods provided by `driver.data_dictionary` on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md). For dialect-specific system catalog SQL queries (`-- name: ...`) and capability profiles, see [Dialect SQL Catalogs](dialects/index.md).

## Synchronous Introspection

::: sqlspec.driver._sync.SyncDataDictionaryBase
    options:
      heading_level: 3
      members:
        - get_version
        - get_feature_flag
        - get_optimal_type
        - list_available_features
        - get_metadata_capabilities
        - get_schemas
        - get_tables
        - get_columns
        - get_indexes
        - get_foreign_keys
        - get_constraints
        - get_views
        - get_routines
        - get_sequences
        - get_types
        - get_ddl
        - get_object_ddl
        - get_schema_ddl
        - get_table_profile
        - infer_table_ddl_from_data
        - get_system_metadata

## Asynchronous Introspection

::: sqlspec.driver._async.AsyncDataDictionaryBase
    options:
      heading_level: 3
      members:
        - get_version
        - get_feature_flag
        - get_optimal_type
        - list_available_features
        - get_metadata_capabilities
        - get_schemas
        - get_tables
        - get_columns
        - get_indexes
        - get_foreign_keys
        - get_constraints
        - get_views
        - get_routines
        - get_sequences
        - get_types
        - get_ddl
        - get_object_ddl
        - get_schema_ddl
        - get_table_profile
        - infer_table_ddl_from_data
        - get_system_metadata
