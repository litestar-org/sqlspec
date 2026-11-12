# Teradata Vantage Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("teradata")
```

Introspects modern Unicode DBC.*V views using LOCKING ROW FOR ACCESS; columns support scoped ColumnsQV fallback. See the [Teradata Vantage Guide](../../../databases/teradata.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `teradata` |
| **Default Schema** | `None` |
| **Version Pattern** | `(\d+)\.(\d+)(?:\.(\d+))?(?:\.(\d+))?` |
| **DDL Strategy** | DBC.TableTextV object text and catalog-backed AST synthesis |
| **Catalog `.sql` Files** | `9` files (17 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `False` | `bigint` | `BIGINT` |
| `supports_cte` | `True` | `blob` | `BLOB` |
| `supports_for_update` | `False` | `boolean` | `BYTEINT` |
| `supports_in_memory` | `False` | `date` | `DATE` |
| `supports_json` | `True` | `datetime` | `TIMESTAMP(6) WITH TIME ZONE` |
| `supports_merge` | `True` | `decimal` | `DECIMAL(18, 4)` |
| `supports_on_conflict` | `False` | `float` | `DOUBLE PRECISION` |
| `supports_prepared_statements` | `True` | `integer` | `INTEGER` |
| `supports_returning` | `False` | `json` | `JSON` |
| `supports_schemas` | `True` | `smallint` | `SMALLINT` |
| `supports_sequences` | `False` | `text` | `CLOB` |
| `supports_skip_locked` | `False` | `time` | `TIME(6)` |
| `supports_transactional_ddl` | `False` | `timestamp` | `TIMESTAMP(6) WITH TIME ZONE` |
| `supports_transactions` | `True` | `uuid` | `VARCHAR(36)` |
| `supports_update_from` | `True` | `varchar` | `VARCHAR({length})` |
| `supports_upsert` | `True` | - | - |
| `supports_uuid` | `False` | - | - |
| `supports_window_functions` | `True` | - | - |
| `supports_cte` | `>= 15.10.0` | - | - |
| `supports_json` | `>= 15.10.0` | - | - |
| `supports_merge` | `>= 15.10.0` | - | - |
| `supports_partitioning` | `>= 15.10.0` | - | - |
| `supports_upsert` | `>= 15.10.0` | - | - |
| `supports_window_functions` | `>= 15.10.0` | - | - |

## Adapter Data Dictionary Classes

- [`TeradataDataDictionary`](../../adapters/teradata.md)
- [`AdbcDataDictionary`](../../adapters/adbc.md)

## Catalog SQL Queries

The `teradata` dialect pack defines 17 named queries across `9` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/teradata/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/teradata/sql/columns.sql` | `by_schema`, `by_table`, `qv_by_table` (3 named queries) | `:schema_name`, `:table_name` |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/teradata/sql/constraints.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/teradata/sql/foreign_keys.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/teradata/sql/indexes.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `objects.sql` | `sqlspec/data_dictionary/dialects/teradata/sql/objects.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/teradata/sql/schemas.sql` | `current`, `by_schema` (2 named queries) | `:schema_name` |
| `tables.sql` | `sqlspec/data_dictionary/dialects/teradata/sql/tables.sql` | `by_schema`, `all_by_schema`, `names_by_schema` (3 named queries) | `:schema_name`, `:table_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/teradata/sql/version.sql` | `current` (1 named query) | *none* |
| `views.sql` | `sqlspec/data_dictionary/dialects/teradata/sql/views.sql` | `by_schema` (1 named query) | `:schema_name` |
