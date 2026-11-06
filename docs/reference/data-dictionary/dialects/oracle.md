# Oracle Database Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("oracle")
```

Introspects `ALL_TABLES`, `ALL_TAB_COLS`, `ALL_INDEXES`, `ALL_IND_COLUMNS`, `ALL_CONSTRAINTS`, `ALL_CONS_COLUMNS`, `ALL_VIEWS`, `ALL_OBJECTS`, `ALL_TRIGGERS`, `ALL_SEQUENCES`, and `DBMS_METADATA`. See the [Oracle Database Guide](../../../databases/oracledb.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `oracle` |
| **Default Schema** | `None` |
| **Version Pattern** | `(\d+)` |
| **DDL Strategy** | Native `DBMS_METADATA.GET_DDL` + AST fallback |
| **Catalog `.sql` Files** | `22` files (36 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `False` | `bigint` | `NUMBER(19)` |
| `supports_cte` | `True` | `blob` | `BLOB` |
| `supports_for_update` | `True` | `boolean` | `NUMBER(1)` |
| `supports_in_memory` | `True` | `float` | `BINARY_DOUBLE` |
| `supports_on_conflict` | `False` | `integer` | `NUMBER(10)` |
| `supports_prepared_statements` | `True` | `json` | `JSON` |
| `supports_returning` | `True` | `text` | `CLOB` |
| `supports_schemas` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_skip_locked` | `True` | `uuid` | `RAW(16)` |
| `supports_transactions` | `True` | `varchar` | `VARCHAR2({length})` |
| `supports_update_from` | `False` | - | - |
| `supports_upsert` | `True` | - | - |
| `supports_window_functions` | `True` | - | - |
| `supports_json` | `>= 12.1.0` | - | - |
| `supports_vectors` | `>= 23.4.0` | - | - |

## Adapter Data Dictionary Classes

- [`OracledbSyncDataDictionary`](../../adapters/oracledb.md)
- [`OracledbAsyncDataDictionary`](../../adapters/oracledb.md)
- [`OracleVersionInfo`](../../adapters/oracledb.md)
- [`JSONStorageType`](../../adapters/oracledb.md)

## Catalog SQL Queries

The `oracle` dialect pack defines 36 named queries across `22` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/oracle/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `arguments.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/arguments.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `columns.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/columns.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `comments.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/comments.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/constraints.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `ddl.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/ddl.sql` | `by_object` (1 named query) | `:object_name`, `:object_type`, `:owner` |
| `dependencies.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/dependencies.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/foreign_keys.sql` | `by_table`, `by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `grants.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/grants.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/indexes.sql` | `by_schema`, `by_table`, `expressions` (3 named queries) | `:schema_name`, `:table_name` |
| `lob_storage.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/lob_storage.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `materialized_views.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/materialized_views.sql` | `by_schema` (1 named query) | `:mview_name`, `:schema_name` |
| `objects.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/objects.sql` | `by_schema`, `cloud_ai_available` (2 named queries) | `:object_name`, `:schema_name` |
| `partitions.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/partitions.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `routines.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/routines.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/schemas.sql` | `by_schema` (1 named query) | `:schema_name` |
| `sequences.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/sequences.sql` | `by_schema` (1 named query) | `:schema_name`, `:sequence_name` |
| `source.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/source.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `system.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/system.sql` | `awr`, `no_diagnostics`, `statspack`, `vector_memory_pool` (4 named queries) | `:acknowledge_license`, `:include_diagnostics` |
| `tables.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/tables.sql` | `by_schema`, `names_by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `triggers.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/triggers.sql` | `by_schema` (1 named query) | `:schema_name`, `:trigger_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/version.sql` | `current`, `autonomous_service`, `compatible`, `apex_release` (4 named queries) | *none* |
| `views.sql` | `sqlspec/data_dictionary/dialects/oracle/sql/views.sql` | `by_schema`, `json_duality_views`, `json_duality_views_by_schema` (3 named queries) | `:schema_name`, `:view_name` |
