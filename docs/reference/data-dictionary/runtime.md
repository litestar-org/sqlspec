# Runtime Introspection & DDL Synthesis

Runtime introspection, DDL extraction, and column profiling methods provided by `driver.data_dictionary` on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md).

## Synchronous Introspection

::: sqlspec.driver._sync.SyncDataDictionaryBase
    options:
      heading_level: 3
      members:
        - get_ddl
        - get_object_ddl
        - get_schema_ddl
        - get_table_profile
        - infer_table_ddl_from_data

## Asynchronous Introspection

::: sqlspec.driver._async.AsyncDataDictionaryBase
    options:
      heading_level: 3
      members:
        - get_ddl
        - get_object_ddl
        - get_schema_ddl
        - get_table_profile
        - infer_table_ddl_from_data
