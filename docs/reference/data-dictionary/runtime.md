# Runtime Introspection & DDL Synthesis

```python
from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
```

Runtime introspection, DDL extraction, and column profiling methods provided by `driver.data_dictionary` on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md). For dialect-specific system catalog SQL queries (`-- name: ...`) and capability profiles, see [Dialect SQL Catalogs](dialects/index.md).

Adapter authors building for one of SQLSpec's 15 supported engines should subclass the matching shared dialect base from `sqlspec.data_dictionary.dialects.<dialect>` rather than subclassing `SyncDataDictionaryBase` or `AsyncDataDictionaryBase` directly (see [Shared Dialect Base Classes](index.md#shared-dialect-base-classes)). Direct subclassing of `SyncDataDictionaryBase` or `AsyncDataDictionaryBase` is intended for new dialects and multi-dialect transport adapters such as `adbc` and `arrow_odbc`.

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

`AsyncDataDictionaryBase` exposes `async` counterparts for every method on `SyncDataDictionaryBase` above:

::: sqlspec.driver._async.AsyncDataDictionaryBase
    options:
      heading_level: 3
      members: false
