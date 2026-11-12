# Snowflake Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("snowflake")
```

Queries Snowflake INFORMATION_SCHEMA, GET_DDL, and SHOW PRIMARY, UNIQUE, and IMPORTED KEYS through the native connector and ADBC adapters. See the [Snowflake Guide](../../../databases/snowflake.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `snowflake` |
| **Default Schema** | `"PUBLIC"` |
| **Version Pattern** | `^(\d+)\.(\d+)(?:\.(\d+))?` |
| **DDL Strategy** | Native GET_DDL and SHOW KEYS metadata |
| **Catalog `.sql` Files** | `11` files (26 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `True` | `array` | `ARRAY` |
| `supports_clustering` | `True` | `bigint` | `NUMBER(38,0)` |
| `supports_cte` | `True` | `binary` | `BINARY` |
| `supports_for_update` | `False` | `blob` | `BINARY` |
| `supports_geography` | `True` | `boolean` | `BOOLEAN` |
| `supports_json` | `True` | `date` | `DATE` |
| `supports_maps` | `True` | `datetime` | `TIMESTAMP_TZ` |
| `supports_merge` | `True` | `decimal` | `NUMBER` |
| `supports_on_conflict` | `False` | `double` | `FLOAT` |
| `supports_prepared_statements` | `True` | `float` | `FLOAT` |
| `supports_returning` | `False` | `geography` | `GEOGRAPHY` |
| `supports_roles` | `True` | `integer` | `NUMBER(38,0)` |
| `supports_schemas` | `True` | `json` | `VARIANT` |
| `supports_sequences` | `True` | `numeric` | `NUMBER` |
| `supports_skip_locked` | `False` | `struct` | `OBJECT` |
| `supports_structs` | `True` | `text` | `VARCHAR` |
| `supports_time_travel` | `True` | `time` | `TIME` |
| `supports_transactional_ddl` | `False` | `timestamp` | `TIMESTAMP_TZ` |
| `supports_transactions` | `True` | `timestamptz` | `TIMESTAMP_TZ` |
| `supports_update_from` | `True` | `uuid` | `VARCHAR(36)` |
| `supports_upsert` | `True` | `varchar` | `VARCHAR({length})` |
| `supports_uuid` | `False` | - | - |
| `supports_vectors` | `True` | - | - |
| `supports_window_functions` | `True` | - | - |

## Adapter Data Dictionary Classes

- [`SnowflakeSyncDataDictionary`](../../adapters/snowflake.md)
- [`AdbcDataDictionary`](../../adapters/adbc.md)

## Catalog SQL Queries

The `snowflake` dialect pack defines 26 named queries across `11` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/snowflake/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/snowflake/sql/columns.sql` | `by_schema`, `by_table`, `by_table_all_schemas`, `by_database` (4 named queries) | `:schema_name`, `:table_name` |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/snowflake/sql/constraints.sql` | `by_schema`, `by_table`, `primary_keys_by_schema`, `primary_keys_by_table`, `unique_keys_by_schema`, `unique_keys_by_table` (6 named queries) | `:schema_name`, `:table_name` |
| `ddl.sql` | `sqlspec/data_dictionary/dialects/snowflake/sql/ddl.sql` | `by_object` (1 named query) | `:object_name`, `:object_type` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/snowflake/sql/foreign_keys.sql` | `by_schema`, `by_table` (2 named queries) | *none* |
| `objects.sql` | `sqlspec/data_dictionary/dialects/snowflake/sql/objects.sql` | `by_schema` (1 named query) | `:schema_name` |
| `routines.sql` | `sqlspec/data_dictionary/dialects/snowflake/sql/routines.sql` | `by_schema`, `functions_by_schema`, `procedures_by_schema` (3 named queries) | `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/snowflake/sql/schemas.sql` | `list` (1 named query) | *none* |
| `sequences.sql` | `sqlspec/data_dictionary/dialects/snowflake/sql/sequences.sql` | `by_schema`, `by_name` (2 named queries) | `:schema_name`, `:sequence_name` |
| `tables.sql` | `sqlspec/data_dictionary/dialects/snowflake/sql/tables.sql` | `by_schema`, `by_database`, `by_table` (3 named queries) | `:schema_name`, `:table_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/snowflake/sql/version.sql` | `current` (1 named query) | *none* |
| `views.sql` | `sqlspec/data_dictionary/dialects/snowflake/sql/views.sql` | `by_schema`, `by_name` (2 named queries) | `:schema_name`, `:view_name` |
