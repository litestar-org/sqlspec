# Sybase ASE Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("sybase")
```

Introspects SAP Adaptive Server Enterprise system tables (`dbo.sysobjects`, `dbo.syscolumns`, `dbo.sysindexes`, `dbo.sysreferences`, `dbo.syscomments`, `dbo.systypes`, `dbo.sysusers`). See the [Sybase ASE Guide](../../../databases/sybase.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `sybase` |
| **Default Schema** | `"dbo"` |
| **Version Pattern** | `Adaptive Server Enterprise/(\d+)\.(\d+)(?:\s+SP(\d+)|\.\d+)?` |
| **DDL Strategy** | AST synthesis + `dbo.syscomments` object text reassembly |
| **Catalog `.sql` Files** | `9` files (16 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `False` | `AutoIncrement` | `INT IDENTITY` |
| `supports_clustering` | `False` | `bigint` | `BIGINT` |
| `supports_cte` | `False` | `blob` | `IMAGE` |
| `supports_for_update` | `False` | `boolean` | `BIT` |
| `supports_in_memory` | `False` | `bytes` | `VARBINARY(16384)` |
| `supports_json` | `False` | `date` | `DATE` |
| `supports_merge` | `False` | `datetime` | `BIGDATETIME` |
| `supports_on_conflict` | `False` | `decimal` | `DECIMAL` |
| `supports_prepared_statements` | `True` | `float` | `FLOAT` |
| `supports_returning` | `False` | `integer` | `INT` |
| `supports_schemas` | `True` | `json` | `TEXT` |
| `supports_sequences` | `False` | `smallint` | `SMALLINT` |
| `supports_skip_locked` | `False` | `string` | `VARCHAR(255)` |
| `supports_transactional_ddl` | `False` | `text` | `TEXT` |
| `supports_transactions` | `True` | `time` | `BIGTIME` |
| `supports_update_from` | `True` | `timestamp` | `BIGDATETIME` |
| `supports_upsert` | `False` | `uuid` | `VARCHAR(36)` |
| `supports_uuid` | `False` | `varchar` | `VARCHAR(255)` |
| `supports_window_functions` | `False` | - | - |

## Adapter Data Dictionary Classes

- [`SybaseSyncDataDictionary`](../../adapters/sybase.md)

## Catalog SQL Queries

The `sybase` dialect pack defines 16 named queries across `9` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/sybase/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/sybase/sql/columns.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/sybase/sql/constraints.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/sybase/sql/foreign_keys.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/sybase/sql/indexes.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `objects.sql` | `sqlspec/data_dictionary/dialects/sybase/sql/objects.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/sybase/sql/schemas.sql` | `by_schema`, `current` (2 named queries) | `:schema_name` |
| `tables.sql` | `sqlspec/data_dictionary/dialects/sybase/sql/tables.sql` | `by_schema`, `all_by_schema`, `names_by_schema` (3 named queries) | `:schema_name`, `:table_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/sybase/sql/version.sql` | `current` (1 named query) | `@version` |
| `views.sql` | `sqlspec/data_dictionary/dialects/sybase/sql/views.sql` | `by_schema` (1 named query) | `:schema_name`, `:view_name` |
