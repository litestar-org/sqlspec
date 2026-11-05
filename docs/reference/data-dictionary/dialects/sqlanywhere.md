# SAP SQL Anywhere Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("sqlanywhere")
```

Introspects SAP SQL Anywhere `SYS` catalog views (`SYS.SYSTAB`, `SYS.SYSTABCOL`, `SYS.SYSIDX`, `SYS.SYSIDXCOL`, `SYS.SYSFKEY`, `SYS.SYSCONSTRAINT`, `SYS.SYSVIEW`, `SYS.SYSPROCEDURE`, `SYS.SYSTRIGGER`, `SYS.SYSSEQUENCE`). See the [SAP SQL Anywhere Guide](../../../databases/sqlanywhere.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `sqlanywhere` |
| **Default Schema** | `"DBA"` |
| **Version Pattern** | `(\d+)\.(\d+)\.(\d+)` |
| **DDL Strategy** | AST synthesis + `SYS.SYSTAB` / `SYS.SYSPROCEDURE` definitions |
| **Catalog `.sql` Files** | `9` files (16 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `False` | `AutoIncrement` | `INTEGER DEFAULT AUTOINCREMENT` |
| `supports_clustering` | `False` | `bigint` | `BIGINT` |
| `supports_cte` | `True` | `blob` | `LONG BINARY` |
| `supports_for_update` | `True` | `boolean` | `BIT` |
| `supports_in_memory` | `False` | `bytes` | `VARBINARY(32767)` |
| `supports_json` | `False` | `date` | `DATE` |
| `supports_merge` | `True` | `datetime` | `TIMESTAMP` |
| `supports_on_conflict` | `False` | `decimal` | `DECIMAL` |
| `supports_prepared_statements` | `True` | `float` | `DOUBLE` |
| `supports_returning` | `False` | `integer` | `INTEGER` |
| `supports_schemas` | `True` | `json` | `LONG VARCHAR` |
| `supports_sequences` | `True` | `smallint` | `SMALLINT` |
| `supports_skip_locked` | `True` | `string` | `VARCHAR(255)` |
| `supports_transactional_ddl` | `False` | `text` | `LONG VARCHAR` |
| `supports_transactions` | `True` | `time` | `TIME` |
| `supports_update_from` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_upsert` | `True` | `uuid` | `UNIQUEIDENTIFIER` |
| `supports_uuid` | `True` | `varchar` | `VARCHAR(255)` |
| `supports_window_functions` | `True` | - | - |
| `supports_cte` | `>= 10.0.0` | - | - |
| `supports_merge` | `>= 11.0.0` | - | - |
| `supports_window_functions` | `>= 10.0.0` | - | - |

## Adapter Data Dictionary Classes

- [`SqlanywhereSyncDataDictionary`](../../adapters/sqlanywhere.md)

## Catalog SQL Queries

The `sqlanywhere` dialect pack defines 16 named queries across `9` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/sqlanywhere/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/sqlanywhere/sql/columns.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/sqlanywhere/sql/constraints.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/sqlanywhere/sql/foreign_keys.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/sqlanywhere/sql/indexes.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `objects.sql` | `sqlspec/data_dictionary/dialects/sqlanywhere/sql/objects.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/sqlanywhere/sql/schemas.sql` | `by_schema`, `current` (2 named queries) | `:schema_name` |
| `tables.sql` | `sqlspec/data_dictionary/dialects/sqlanywhere/sql/tables.sql` | `by_schema`, `all_by_schema`, `names_by_schema` (3 named queries) | `:schema_name`, `:table_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/sqlanywhere/sql/version.sql` | `current` (1 named query) | *none* |
| `views.sql` | `sqlspec/data_dictionary/dialects/sqlanywhere/sql/views.sql` | `by_schema` (1 named query) | `:schema_name`, `:view_name` |
