# IBM Db2 Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("db2")
```

Introspects IBM Db2 `SYSCAT` views (`SYSCAT.TABLES`, `SYSCAT.COLUMNS`, `SYSCAT.INDEXES`, `SYSCAT.REFERENCES`, `SYSCAT.TABCONST`, `SYSCAT.VIEWS`, `SYSCAT.ROUTINES`, `SYSCAT.TRIGGERS`, `SYSCAT.SEQUENCES`, `SYSCAT.DATATYPES`). See the [IBM Db2 Guide](../../../databases/db2.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `db2` |
| **Default Schema** | `None` |
| **Version Pattern** | `(?:v|V)?(\d+)\.(\d+)(?:\.(\d+))?` |
| **DDL Strategy** | AST synthesis + `SYSCAT` catalog text definitions |
| **Catalog `.sql` Files** | `9` files (14 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_for_update` | `True` | `bigint` | `BIGINT` |
| `supports_in_memory` | `False` | `blob` | `BLOB` |
| `supports_on_conflict` | `False` | `boolean` | `BOOLEAN` |
| `supports_prepared_statements` | `True` | `decimal` | `DECIMAL(31, 10)` |
| `supports_returning` | `False` | `float` | `DOUBLE` |
| `supports_schemas` | `True` | `integer` | `INTEGER` |
| `supports_sequences` | `True` | `json` | `CLOB` |
| `supports_skip_locked` | `True` | `text` | `CLOB` |
| `supports_transactions` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_update_from` | `False` | `uuid` | `VARCHAR(36)` |
| `supports_cte` | `>= 8.1.0` | `varchar` | `VARCHAR(255)` |
| `supports_generated_columns` | `>= 8.1.0` | - | - |
| `supports_window_functions` | `>= 8.1.0` | - | - |

## Adapter Data Dictionary Classes

- [`Db2SyncDataDictionary`](../../adapters/db2.md)
- [`Db2AsyncDataDictionary`](../../adapters/db2.md)

## Catalog SQL Queries

The `db2` dialect pack defines 14 named queries across `9` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/db2/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/db2/sql/columns.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/db2/sql/constraints.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/db2/sql/foreign_keys.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/db2/sql/indexes.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `objects.sql` | `sqlspec/data_dictionary/dialects/db2/sql/objects.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/db2/sql/schemas.sql` | `by_schema` (1 named query) | `:schema_name` |
| `tables.sql` | `sqlspec/data_dictionary/dialects/db2/sql/tables.sql` | `by_schema`, `all_by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/db2/sql/version.sql` | `current` (1 named query) | *none* |
| `views.sql` | `sqlspec/data_dictionary/dialects/db2/sql/views.sql` | `by_schema` (1 named query) | `:schema_name`, `:view_name` |
