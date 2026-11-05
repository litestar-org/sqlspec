# SQL Server Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("mssql")
```

Introspects `sys.tables`, `sys.columns`, `sys.types`, `sys.indexes`, `sys.foreign_keys`, `sys.check_constraints`, `sys.default_constraints`, `sys.sequences`, and `sys.sql_modules` across `mssql_python` and `pymssql`. See the [SQL Server Guide](../../../databases/mssql.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `mssql` |
| **Default Schema** | `"dbo"` |
| **Version Pattern** | `(\d+)` |
| **DDL Strategy** | AST synthesis + `OBJECT_DEFINITION()` for views/routines/triggers |
| **Catalog `.sql` Files** | `19` files (31 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_cte` | `True` | `bigint` | `BIGINT` |
| `supports_for_update` | `False` | `blob` | `VARBINARY(MAX)` |
| `supports_in_memory` | `True` | `boolean` | `BIT` |
| `supports_on_conflict` | `False` | `bytea` | `VARBINARY(MAX)` |
| `supports_prepared_statements` | `True` | `float` | `FLOAT` |
| `supports_returning` | `True` | `integer` | `INT` |
| `supports_schemas` | `True` | `json` | `NVARCHAR(MAX)` |
| `supports_skip_locked` | `False` | `jsonb` | `NVARCHAR(MAX)` |
| `supports_transactions` | `True` | `text` | `NVARCHAR(MAX)` |
| `supports_update_from` | `True` | `timestamp` | `DATETIME2(6)` |
| `supports_upsert` | `True` | `timestamptz` | `DATETIMEOFFSET(6)` |
| `supports_window_functions` | `True` | `uuid` | `UNIQUEIDENTIFIER` |
| `supports_json` | `>= 13.0.0` | `varchar` | `NVARCHAR({length})` |
| `supports_vectors` | `>= 17.0.0` | - | - |

## Adapter Data Dictionary Classes

- [`MssqlPythonSyncDataDictionary`](../../adapters/mssql-python.md)
- [`PymssqlSyncDataDictionary`](../../adapters/pymssql.md)

## Catalog SQL Queries

The `mssql` dialect pack defines 31 named queries across `19` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/mssql/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/columns.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/constraints.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `ddl.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/ddl.sql` | `index_inputs_by_table`, `table_inputs_by_table` (2 named queries) | `:ddl_index_inputs`, `:ddl_table_inputs`, `:mssql`, `:schema_name`, `:table_name` |
| `dependencies.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/dependencies.sql` | `by_schema` (1 named query) | `:schema_name` |
| `extended_properties.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/extended_properties.sql` | `by_schema` (1 named query) | `:schema_name` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/foreign_keys.sql` | `by_table`, `by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/indexes.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `modules.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/modules.sql` | `by_schema` (1 named query) | `:schema_name` |
| `objects.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/objects.sql` | `by_schema` (1 named query) | `:schema_name` |
| `permissions.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/permissions.sql` | `by_schema`, `role_members` (2 named queries) | `:schema_name` |
| `routines.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/routines.sql` | `by_schema`, `parameters_by_schema` (2 named queries) | `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/schemas.sql` | `list`, `current` (2 named queries) | *none* |
| `sequences.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/sequences.sql` | `by_schema` (1 named query) | `:schema_name` |
| `storage.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/storage.sql` | `by_schema` (1 named query) | `:schema_name` |
| `system.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/system.sql` | `dmv_exec_requests`, `query_store_runtime` (2 named queries) | `:dmv_exec_requests`, `:mssql`, `:query_store_runtime`, `@SPID` |
| `tables.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/tables.sql` | `all_by_schema`, `by_schema`, `details_by_table`, `names_by_schema` (4 named queries) | `:schema_name`, `:table_name` |
| `triggers.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/triggers.sql` | `by_schema` (1 named query) | `:schema_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/version.sql` | `current` (1 named query) | `@VERSION` |
| `views.sql` | `sqlspec/data_dictionary/dialects/mssql/sql/views.sql` | `by_schema` (1 named query) | `:schema_name` |
