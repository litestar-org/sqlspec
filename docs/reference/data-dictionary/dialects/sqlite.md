# SQLite Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("sqlite")
```

Introspects `sqlite_master` and table-valued `pragma_table_xinfo`, `pragma_index_list`, `pragma_index_info`, and `pragma_foreign_key_list` functions across `sqlite` and `aiosqlite`. See the [SQLite Guide](../../../databases/sqlite.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `sqlite` |
| **Default Schema** | `None` |
| **Version Pattern** | `(\d+)\.(\d+)\.(\d+)` |
| **DDL Strategy** | Native `sqlite_master.sql` + `PRAGMA` synthesis |
| **Catalog `.sql` Files** | `15` files (23 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `False` | `bigint` | `INTEGER` |
| `supports_for_update` | `False` | `blob` | `BLOB` |
| `supports_on_conflict` | `True` | `boolean` | `INTEGER` |
| `supports_prepared_statements` | `True` | `float` | `REAL` |
| `supports_schemas` | `False` | `integer` | `INTEGER` |
| `supports_skip_locked` | `False` | `json` | `JSON` |
| `supports_transactions` | `True` | `text` | `TEXT` |
| `supports_update_from` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_uuid` | `False` | `uuid` | `TEXT` |
| `supports_cte` | `>= 3.8.3` | `varchar` | `TEXT` |
| `supports_json` | `>= 3.38.0` | - | - |
| `supports_returning` | `>= 3.35.0` | - | - |
| `supports_upsert` | `>= 3.24.0` | - | - |
| `supports_window_functions` | `>= 3.25.0` | - | - |

## Adapter Data Dictionary Classes

- [`SqliteDataDictionary`](../../adapters/sqlite.md)
- [`AiosqliteDataDictionary`](../../adapters/aiosqlite.md)

## Catalog SQL Queries

The `sqlite` dialect pack defines 23 named queries across `15` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/sqlite/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/columns.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `compile_options.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/compile_options.sql` | `list` (1 named query) | *none* |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/constraints.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `ddl.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/ddl.sql` | `by_object`, `by_schema` (2 named queries) | `:object_name`, `:object_type`, `:schema_name` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/foreign_keys.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `functions.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/functions.sql` | `list` (1 named query) | *none* |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/indexes.sql` | `by_schema`, `by_table`, `columns_by_index`, `index_columns_by_index` (4 named queries) | `:index_name`, `:schema_name`, `:table_name` |
| `modules.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/modules.sql` | `list` (1 named query) | *none* |
| `objects.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/objects.sql` | `by_schema` (1 named query) | `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/schemas.sql` | `list` (1 named query) | *none* |
| `system.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/system.sql` | `foreign_key_check`, `integrity_check` (2 named queries) | *none* |
| `tables.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/tables.sql` | `by_schema` (1 named query) | `:schema_name` |
| `triggers.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/triggers.sql` | `by_schema` (1 named query) | `:schema_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/version.sql` | `current` (1 named query) | *none* |
| `views.sql` | `sqlspec/data_dictionary/dialects/sqlite/sql/views.sql` | `by_schema` (1 named query) | `:schema_name` |
